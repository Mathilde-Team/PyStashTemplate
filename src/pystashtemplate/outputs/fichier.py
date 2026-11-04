# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie ``file`` (issue #32) : NDJSON local avec rotation par gabarit.

Plugin Logstash ``file`` pour l'architecture de sorties agnostiques (le
``Output`` de ``base.py``) : chaque document part sur une ligne du fichier
rendu par le gabarit de chemin. La rotation est le gabarit lui-même —
``path => "/var/log/pystash/%{+YYYY-MM-dd}.ndjson"`` (Joda, contre
``@timestamp``) écrit chaque jour dans un autre fichier, à la façon de
``elasticsearch_bulk`` dont ``index_pattern`` change de valeur rendue.

Répliques des décisions de ``elasticsearch_bulk.py`` :

- les seules options reçues du bloc ``output{}`` sont la connexion
  (``path``) et la sortie d'un document (``codec``) — le DLQ éventuel
  (``dlq_path``) est ouvert à l'instanciation ;
- le chemin du gabarit est rendu contre chaque document (rendu par lot,
  ce qui est le minimum pour un chemin daté) ;
- échec d'écriture : permanent (``OSError``), donc le document part au
  DLQ s'il est configuré, sinon l'exception remonte (l'appelant décide).

Rendement : un seul fichier ouvert à la fois, le précédent est fermé au
changement de chemin rendu (la rotation ferme, pas ``flush()``).
"""

from __future__ import annotations

from pathlib import Path
from typing import IO, Any, ClassVar, Mapping

from loguru import logger

from pystashtemplate import templates
from pystashtemplate.dlq import REASON_PERMANENT, LocalDeadLetterQueue
from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, Output, enregistrer
from pystashtemplate.outputs.codecs import CODECS
from pystashtemplate.tracing import atraced, traced

__all__ = ["FileOutput"]


@enregistrer
class FileOutput(Output):
    """Sortie locale : une ligne JSON par document dans le fichier du gabarit."""

    plugin: ClassVar[str] = "file"

    capacites: ClassVar[Capacites] = Capacites(
        actions=frozenset({"index", "create"}),
        accepte_id=False,
        accuse_par_evenement=True,
        taille_max_requete=None,
    )

    @traced
    def __init__(
        self,
        options: Mapping[str, Any],
        *,
        dlq: LocalDeadLetterQueue | None = None,
    ) -> None:
        """Instancie la sortie locale.

        Args:
            options: Options du bloc ``output{}`` — ``path`` (gabarit,
                obligatoire), ``codec`` (``json_lines`` par défaut) et
                ``dlq_path`` (DLQ locale, optionnelle).
            dlq: DLQ injectée (tests) ; par défaut, ouverte depuis
                ``dlq_path`` si l'option est présente.
        """
        super().__init__(options)
        chemin = options.get("path")
        if not chemin:
            logger.debug("__init__ : option path manquante")
            raise ValueError(_("sortie file : option « path » manquante (chemin du fichier de sortie)"))
        self._chemin = str(chemin)
        codec = str(options.get("codec", "json_lines"))
        if codec not in CODECS:
            logger.debug("__init__ : codec {} inconnu", codec)
            raise ValueError(_("codec {} inconnu (codecs disponibles : json_lines, rubydebug)").format(codec))
        self._encoder = CODECS[codec]
        self._fichiers: dict[str, IO[str]] = {}
        self._dlq: LocalDeadLetterQueue | None = dlq or (
            LocalDeadLetterQueue(str(options["dlq_path"])) if "dlq_path" in options else None
        )

    @traced
    def _ecrire(self, chemin_rendu: str, document: Mapping[str, Any]) -> str:
        """Écrit le rendu du document (ligne) dans le fichier rendu.

        Le répertoire parent est créé au besoin (``mkdir -p``) ; les
        fichiers déjà ouverts dont le chemin diffèrent sont fermés — un
        seul fichier reste ouvert, le courant.
        """
        fichier = self._fichiers.get(chemin_rendu)
        if fichier is None:
            # Un seul fichier ouvert à la fois (invariant du dictionnaire) :
            # la rotation ferme le précédent.
            for ancien in self._fichiers.values():
                ancien.close()
                logger.debug("_ecrire | fermeture du fichier précédent (rotation)")
            self._fichiers.clear()
            Path(chemin_rendu).parent.mkdir(parents=True, exist_ok=True)
            # Ouvert en « a » : fermé dans close() (et à la rotation), pas de
            # gestionnaire de contexte — le fichier vit tant que la sortie vit.
            fichier = open(chemin_rendu, "a", encoding="utf-8", newline="\n")
            self._fichiers[chemin_rendu] = fichier
        texte = self._encoder(dict(document))
        fichier.write(texte + "\n")
        logger.debug("_ecrire | {} caractère(s) dans {}", len(texte), chemin_rendu)
        return chemin_rendu

    @atraced
    async def open(self) -> None:
        """Sans effet : le fichier s'ouvre à la première écriture."""
        logger.debug("FileOutput.open | ouverture différée à la première écriture")

    @atraced
    async def send(self, batch: list[Mapping[str, Any]]) -> None:
        """Écrit le lot, une ligne par document, DLQ sur échec permanent."""
        for document in batch:
            chemin_rendu = templates.render_template(self._chemin, dict(document))
            try:
                self._ecrire(chemin_rendu, document)
            except OSError as erreur:  # échec permanent : disque plein, droits…
                logger.warning("échec d'écriture sur {} : {}", chemin_rendu, erreur)
                if self._dlq is None:
                    raise
                await self._dlq.write(
                    dict(document),
                    index=chemin_rendu,
                    error=str(erreur),
                    output=self.nom,
                    reason=REASON_PERMANENT,
                )

    @atraced
    async def flush(self) -> None:
        """Vide les tampons du fichier courant (écriture déjà ligne à ligne)."""
        for fichier in self._fichiers.values():
            fichier.flush()
        logger.debug("flush | {} fichier(s) vidé(s)", len(self._fichiers))

    @atraced
    async def close(self) -> None:
        """Ferme le fichier ouvert (idempotent)."""
        for chemin, fichier in self._fichiers.items():
            fichier.close()
            logger.debug("close | {} fermé", chemin)
        self._fichiers.clear()
