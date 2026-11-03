# SPDX-License-Identifier: GPL-3.0-or-later
"""Dead letter queue locale pour les échecs d'indexation permanents.

Porté de RsysLogstach (session 13, issue #25) lors du chantier « sorties
agnostiques » (#2) : la couche `outputs` du socle a besoin de la DLQ que
l'`indexer` historique utilisait (décision #111 — le connecteur dépend
de la bibliothèque, jamais l'inverse ; RsysLogstach réexporte ce module).

Un document qui provoque une erreur de mapping permanente (ex. un champ
qui ne correspond pas au type attendu par l'index template) ne devient
jamais valide en le rejouant : le retenir indéfiniment côté rsyslog
(retenté toutes les 30 s) ne fait que boucler pour toujours. Ce module
fournit la voie de sortie : `LocalDeadLetterQueue.write()` consigne le
document dans un fichier JSONL local (une ligne par document,
append-only). Les sorties (`outputs.elasticsearch_bulk`) l'appellent
uniquement pour les erreurs identifiées comme permanentes (4xx), jamais
pour une erreur de connexion ou un timeout (transitoires).
Ne dépend que de `tracing`, comme les autres modules feuilles du projet
(graphe acyclique vérifié par `tests/test_no_cross_imports.py`).
"""

from __future__ import annotations

import asyncio
import gzip
import json
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger

from pystashtemplate.tracing import traced

_SIZE_UNITS = {"": 1, "b": 1, "k": 1024, "kb": 1024, "m": 1024**2, "mb": 1024**2, "g": 1024**3, "gb": 1024**3}
_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 7 * 86400}
_ROTATED_SUFFIX = re.compile(r"\.(\d{8}T\d{6}Z)(?:-(\d+))?(\.gz)?$")


@traced
def parse_size(text: str) -> int:
    """Taille lisible -> octets.

    Accepte un entier d'octets ou un suffixe binaire insensible à la casse :
    ``100M``, ``100MB``, ``512k``, ``1G``. ``0`` désactive la rotation par
    taille.

    Raises:
        ValueError: valeur négative ou format non reconnu.
    """
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]*)\s*", text)
    if not match or match.group(2).lower() not in _SIZE_UNITS:
        raise ValueError(f"taille invalide : {text!r} (ex. 100M, 512k, 1G, 1048576)")
    return int(float(match.group(1)) * _SIZE_UNITS[match.group(2).lower()])


@traced
def parse_duration(text: str) -> timedelta:
    """Durée lisible -> `timedelta`.

    Formats : ``7d``, ``12h``, ``30m``, ``90s``, ``2w`` ; un entier seul est
    compté en secondes. ``0`` désactive le critère.

    Raises:
        ValueError: format non reconnu.
    """
    match = re.fullmatch(r"\s*(\d+)\s*([smhdw]?)\s*", text.lower())
    if not match:
        raise ValueError(f"durée invalide : {text!r} (ex. 7d, 12h, 30m, 90s)")
    return timedelta(seconds=int(match.group(1)) * _DURATION_UNITS[match.group(2) or "s"])


class LocalDeadLetterQueue:
    """Consigne dans un fichier JSONL local les documents rejetés de façon permanente.

    Une ligne par document, format :
        {"@dlq_timestamp": "<ISO-8601 UTC>", "@dlq_index": "<index visé>",
         "@dlq_error": "<message d'erreur>", "document": {...}}

    Rotation et purge (issue #27 de RsysLogstach), toutes désactivées par
    défaut (comportement historique : un seul fichier qui grandit) :

    - ``max_bytes`` : rotation avant d'écrire une ligne qui ferait dépasser
      cette taille au fichier courant ;
    - ``max_age`` : rotation quand la plus ancienne entrée du fichier
      courant (``@dlq_timestamp`` de la 1re ligne, à défaut la date de
      modification) est plus vieille que cette durée ;
    - le fichier rotaté est renommé ``<nom>.<AAAAMMJJTHHMMSSZ>[-n]`` et
      compressé en gzip (``.gz``) si ``compress`` (défaut) ;
    - ``backups`` : nombre maximal de fichiers rotatés conservés (les plus
      anciens sont supprimés) ; ``retention`` : âge maximal d'un fichier
      rotaté. La purge suit chaque rotation et peut être lancée seule
      (``purge()``).

    Les fichiers rotatés restent des JSONL (``zcat`` pour les relire) :
    même usage de relecture/réindexation manuelle une fois la cause du
    rejet corrigée (ex. mapping de l'index ajusté). L'écriture se fait dans un thread séparé (`asyncio.to_thread`)
    pour ne jamais bloquer la boucle asyncio de l'appelant le temps d'un appel
    système disque.
    """

    @traced
    def __init__(
        self,
        path: str,
        *,
        max_bytes: int = 0,
        max_age: timedelta | None = None,
        backups: int = 0,
        retention: timedelta | None = None,
        compress: bool = True,
    ) -> None:
        """Prépare le chemin du fichier DLQ (dossier parent créé si besoin).

        Args:
            path: Chemin du fichier JSONL, créé (ou complété s'il existe
                déjà) au premier appel à `write`.
            max_bytes: Taille déclenchant la rotation (0 = jamais).
            max_age: Âge de la plus ancienne entrée déclenchant la rotation
                (None ou 0 = jamais).
            backups: Fichiers rotatés conservés au maximum (0 = tous).
            retention: Âge maximal d'un fichier rotaté (None ou 0 = illimité).
            compress: Compresser les fichiers rotatés en gzip.

        Raises:
            ValueError: valeur négative.
        """
        if max_bytes < 0 or backups < 0:
            raise ValueError("max_bytes et backups doivent être positifs ou nuls")
        for name, value in (("max_age", max_age), ("retention", retention)):
            if value is not None and value < timedelta(0):
                raise ValueError(f"{name} doit être positif ou nul")
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._max_bytes = max_bytes
        self._max_age = max_age or None
        self._backups = backups
        self._retention = retention or None
        self._compress = compress

    @traced
    async def write(self, doc: dict, *, index: str, error: str) -> None:
        """Ajoute une ligne JSON au fichier DLQ pour ce document.

        Args:
            doc: Document qui aurait dû être indexé.
            index: Nom d'index visé au moment de l'échec.
            error: Message d'erreur (`str(exception)`), à titre indicatif
                seulement — jamais reparsé, jamais utilisé pour décider quoi
                que ce soit.
        """
        entry = {
            "@dlq_timestamp": datetime.now(timezone.utc).isoformat(),
            "@dlq_index": index,
            "@dlq_error": error,
            "document": doc,
        }
        line = json.dumps(entry, ensure_ascii=False) + "\n"
        await asyncio.to_thread(self._append, line)

    @traced
    def _append(self, line: str) -> None:
        """Écriture disque proprement dite (appelée hors boucle asyncio, voir `write`)."""
        if self._should_rotate(len(line.encode("utf-8"))):
            self.rotate()
        with self._path.open("a", encoding="utf-8") as handle:
            handle.write(line)

    @traced
    def _oldest_entry_time(self) -> datetime:
        """Horodatage de la 1re ligne du fichier courant (date de modification à défaut)."""
        try:
            with self._path.open(encoding="utf-8") as handle:
                first = handle.readline()
            stamp = datetime.fromisoformat(json.loads(first)["@dlq_timestamp"])
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            return stamp
        except (ValueError, KeyError, TypeError) as exc:
            logger.debug("_oldest_entry_time : nom illisible, date de modification retenue : {}", exc)
            return datetime.fromtimestamp(self._path.stat().st_mtime, timezone.utc)

    @traced
    def _should_rotate(self, incoming: int) -> bool:
        """Vrai si le fichier courant doit être rotaté avant d'y ajouter `incoming` octets."""
        if not self._path.exists() or self._path.stat().st_size == 0:
            return False
        if self._max_bytes and self._path.stat().st_size + incoming > self._max_bytes:
            return True
        if self._max_age and datetime.now(timezone.utc) - self._oldest_entry_time() >= self._max_age:
            return True
        return False

    @traced
    def rotated_files(self) -> list[Path]:
        """Fichiers rotatés existants, du plus ancien au plus récent."""
        found = []
        for candidate in self._path.parent.glob(self._path.name + ".*"):
            match = _ROTATED_SUFFIX.fullmatch(candidate.name[len(self._path.name) :])
            if match:
                found.append((match.group(1), int(match.group(2) or 0), candidate))
        return [path for _stamp, _n, path in sorted(found)]

    @traced
    def rotate(self) -> Path | None:
        """Rotation immédiate du fichier courant puis purge.

        Returns:
            Chemin du fichier rotaté, ou None si le fichier courant est
            absent ou vide.
        """
        if not self._path.exists() or self._path.stat().st_size == 0:
            return None
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = ".gz" if self._compress else ""
        # Compteur strictement croissant dans la même seconde, même si la
        # purge a supprimé les plus anciens : l'ordre des noms reste l'ordre
        # chronologique (sur lequel s'appuie la purge par `backups`).
        same_second = [
            int(m.group(2) or 0)
            for p in self.rotated_files()
            if (m := _ROTATED_SUFFIX.fullmatch(p.name[len(self._path.name) :])) and m.group(1) == stamp
        ]
        counter = max(same_second) + 1 if same_second else 0
        name = f"{self._path.name}.{stamp}" + (f"-{counter}" if counter else "")
        target = self._path.with_name(name + suffix)
        if self._compress:
            with self._path.open("rb") as src, gzip.open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)
            self._path.unlink()
        else:
            self._path.rename(target)
        self.purge()
        return target

    @traced
    def purge(self) -> list[Path]:
        """Supprime les fichiers rotatés au-delà de `backups` ou plus vieux que `retention`.

        Returns:
            Fichiers supprimés.
        """
        rotated = self.rotated_files()
        doomed: list[Path] = []
        if self._retention:
            limit = datetime.now(timezone.utc) - self._retention
            for path in rotated:
                stamp = datetime.strptime(
                    _ROTATED_SUFFIX.fullmatch(path.name[len(self._path.name) :]).group(1), "%Y%m%dT%H%M%SZ"
                ).replace(tzinfo=timezone.utc)
                if stamp < limit:
                    doomed.append(path)
        if self._backups:
            kept = [p for p in rotated if p not in doomed]
            doomed.extend(kept[: max(0, len(kept) - self._backups)])
        for path in doomed:
            path.unlink(missing_ok=True)
        return doomed
