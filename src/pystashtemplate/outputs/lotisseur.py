# SPDX-License-Identifier: GPL-3.0-or-later
"""Lots, contre-pression et arrêt propre des sorties (issue #6).

Entre le pipeline et chaque sortie, le `Lotisseur` constitue les lots
et amortit les écarts de vitesse :

- **lotissement** : un lot part dès qu'il atteint `batch_size` documents,
  `batch_bytes` octets (taille sérialisée NDJSON approximée) ou
  `flush_interval` secondes d'attente — le premier des trois qui arrive ;
- **contre-pression** : la file entre l'entrée et la sortie est **bornée**
  (`taille_file`) ; quand la sortie ralentit, `ajouter()` attend au lieu
  de grossir la mémoire — l'entrée est ralentie à son tour ;
- **concurrence** : `max_concurrency` lots au maximum en vol vers la
  même sortie (les autres attendent leur tour dans la file) ;
- **arrêt propre** : `arreter()` vide la file et les lots en cours, sans
  perdre un événement, avec un délai maximal (`delai_arret`) au-delà
  duquel l'arrêt est forcé et l'incident journalisé.

Un `Lotisseur` par sortie (`max_concurrency` et les tailles de lots
sont des réglages **par sortie**) ; le `MoteurSorties` route chaque
document vers les sorties retenues par les conditions `if` du bloc
`output{}` — déjà évaluées par l'interpréteur côté appelant.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.outputs.base import Document, Output
from pystashtemplate.tracing import atraced, traced

# Marqueur de fin d'entrée : un document ne peut jamais être un tuple,
# donc aucun conflit avec un vrai événement.
_SENTINELLE_FIN = object()


def _taille_document(document: Document) -> int:
    """Taille sérialisée approximative d'un document, pour `batch_bytes`.

    Args:
        document: Événement tel que traité par le pipeline.

    Returns:
        La taille en octets de sa sérialisation JSON (0 si non
        sérialisable — un document non sérialisable ne compte pas).
    """
    logger.debug("_taille_document | entrée")
    try:
        taille = len(json.dumps(document, default=str, ensure_ascii=False).encode())
        logger.debug("_taille_document | sortie | {} octet(s)", taille)
        return taille
    except (TypeError, ValueError):
        logger.debug("_taille_document | document non sérialisable : 0 octet")
        return 0


class Lotisseur:
    """File bornée + lots vers UNE sortie, avec contre-pression et arrêt propre (issue #6).

    Cycle de vie : `demarrer()` (lance la consommation), `ajouter()`
    autant de fois que nécessaire (attend si la file est pleine), puis
    `arreter()` qui vide tout sans perte — ou force après `delai_arret`.
    """

    # Valeurs par défaut alignées sur les usages d'un injecteur de
    # journaux : lots modestes, arrêt propre en moins d'une minute.
    BATCH_SIZE: int = 500
    BATCH_BYTES: int | None = None
    FLUSH_INTERVAL: float = 5.0
    TAILLE_FILE: int = 1000
    MAX_CONCURRENCY: int = 1
    DELAI_ARRET: float = 30.0

    @traced
    def __init__(self, sortie: Output, options: dict[str, Any] | None = None) -> None:
        """Configure le lotisseur depuis les options de son bloc `output{}`.

        Args:
            sortie: La sortie destination (son `send()` reçoit les lots).
            options: Options du bloc — `batch_size`, `batch_bytes`,
                `flush_interval`, `taille_file`, `max_concurrency`,
                `delai_arret` (secondes). Options inconnues ignorées.

        Raises:
            ValueError: Une option est négative ou nulle quand elle ne
                le peut pas (tailles, délais, concurrence).
        """
        options = dict(options or {})
        self.sortie = sortie
        self.batch_size = int(options.get("batch_size", self.BATCH_SIZE))
        self.batch_bytes = options.get("batch_bytes", self.BATCH_BYTES)
        if self.batch_bytes is not None:
            self.batch_bytes = int(self.batch_bytes)
        self.flush_interval = float(options.get("flush_interval", self.FLUSH_INTERVAL))
        self.taille_file = int(options.get("taille_file", self.TAILLE_FILE))
        self.max_concurrency = int(options.get("max_concurrency", self.MAX_CONCURRENCY))
        self.delai_arret = float(options.get("delai_arret", self.DELAI_ARRET))
        for nom, valeur in (
            ("batch_size", self.batch_size),
            ("flush_interval", self.flush_interval),
            ("taille_file", self.taille_file),
            ("max_concurrency", self.max_concurrency),
            ("delai_arret", self.delai_arret),
        ):
            if valeur <= 0:
                logger.error(_("option {} du lotisseur : {} (doit être strictement positif)"), nom, valeur)
                raise ValueError(_("option {} du lotisseur : {} (doit être strictement positif)").format(nom, valeur))
        if self.batch_bytes is not None and self.batch_bytes <= 0:
            logger.error(_("option batch_bytes du lotisseur : {} (doit être strictement positif)"), self.batch_bytes)
            raise ValueError(
                _("option batch_bytes du lotisseur : {} (doit être strictement positif)").format(self.batch_bytes)
            )
        self._file: asyncio.Queue[Any] = asyncio.Queue(maxsize=self.taille_file)
        self._tache: asyncio.Task[None] | None = None
        self._arrete = False
        self._en_vol: set[asyncio.Task[None]] = set()
        self._fin_vol = asyncio.Event()
        self._fin_vol.set()
        self._semaphore_envoi: asyncio.Semaphore | None = None

    # ------------------------------------------------------------------
    # Cycle de vie
    # ------------------------------------------------------------------
    @atraced
    async def demarrer(self) -> None:
        """Lance la tâche de consommation (idempotent)."""
        logger.debug("Lotisseur.demarrer | entrée")
        if self._tache is None or self._tache.done():
            self._arrete = False
            self._tache = asyncio.create_task(self._boucle(), name="lotisseur")
        logger.debug("Lotisseur.demarrer | sortie")

    @atraced
    async def ajouter(self, document: Document) -> None:
        """Ajoute un document, en attendant si la file est pleine.

        C'est la contre-pression : quand la sortie ralentit, l'appelant
        (le pipeline) est ralenti à son tour au lieu de consommer la
        mémoire.

        Args:
            document: Événement à envoyer vers la sortie.

        Raises:
            RuntimeError: Le lotisseur est arrêté — plus rien n'est
                accepté après `arreter()`.
        """
        logger.debug("Lotisseur.ajouter | entrée")
        if self._arrete:
            logger.error(_("lotisseur arrêté : plus aucun document accepté"))
            raise RuntimeError(_("lotisseur arrêté : plus aucun document accepté"))
        await self._file.put(document)  # attend si la file est pleine
        logger.debug("Lotisseur.ajouter | sortie (file : {})", self._file.qsize())

    @atraced
    async def arreter(self) -> None:
        """Arrêt propre : vide la file et les lots en cours, sans perte.

        Pousse la sentinelle de fin, attend la consommation complète
        (file vidée, lots en vol terminés), avec un délai maximal de
        `delai_arret` : au-delà, la tâche est annulée et l'incident
        journalisé — les événements encore en file sont perdus, c'est le
        seul cas où `arreter()` peut en perdre.

        Raises:
            RuntimeError: Le lotisseur n'a jamais été démarré.
        """
        logger.debug("Lotisseur.arreter | entrée")
        if self._tache is None:
            logger.error(_("arrêt demandé mais le lotisseur n'a jamais démarré"))
            raise RuntimeError(_("arrêt demandé mais le lotisseur n'a jamais démarré"))
        self._arrete = True
        await self._file.put(_SENTINELLE_FIN)
        try:
            await asyncio.wait_for(asyncio.shield(self._tache), self.delai_arret)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            logger.error(_("arrêt du lotisseur forcé après {}s : des événements peuvent être perdus"), self.delai_arret)
            self._tache.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._tache
            for tache in list(self._en_vol):  # les envois bloqués sont annulés aussi
                tache.cancel()
            await asyncio.gather(*list(self._en_vol), return_exceptions=True)
        logger.debug("Lotisseur.arreter | sortie")

    # ------------------------------------------------------------------
    # Consommation
    # ------------------------------------------------------------------
    @atraced
    async def _boucle(self) -> None:
        """Consume la file, constitue les lots et les pousse vers la sortie."""
        logger.debug("Lotisseur._boucle | entrée")
        lot: list[Document] = []
        octets = 0
        while True:
            delai = self.flush_interval if lot else None
            try:
                element = await asyncio.wait_for(self._file.get(), delai)
            except asyncio.TimeoutError:
                logger.debug("Lotisseur._boucle | flush_interval écoulé : lot partiel")
                await self._pousser(lot)
                lot, octets = [], 0
                continue
            if element is _SENTINELLE_FIN:
                logger.debug("Lotisseur._boucle | fin d'entrée : vidage du lot en cours")
                await self._pousser(lot)
                break
            assert isinstance(element, Mapping)
            lot.append(element)
            octets += _taille_document(element)
            if len(lot) >= self.batch_size or (self.batch_bytes is not None and octets >= self.batch_bytes):
                await self._pousser(lot)
                lot, octets = [], 0
        await self._attendre_en_vol()
        logger.debug("Lotisseur._boucle | sortie (file vidée, lots en vol partis)")

    @atraced
    async def _pousser(self, lot: list[Document]) -> None:
        """Envoie un lot vers la sortie, en limitant la concurrence.

        C'est ici que vit la contre-pression amont : un créneau est
        **acquis dans la boucle** avant de lancer l'envoi — quand les
        `max_concurrency` créneaux sont pris, la boucle cesse de
        consommer, la file bornée se remplit et `ajouter()` attend.

        Args:
            lot: Documents du lot (vide : sans effet, rien n'est envoyé).
        """
        logger.debug("Lotisseur._pousser | entrée | {} document(s)", len(lot))
        if not lot:
            logger.debug("Lotisseur._pousser | sortie (lot vide ignoré)")
            return
        semaphore = self._semaphore_envoi
        if semaphore is None:
            semaphore = self._semaphore_envoi = asyncio.Semaphore(self.max_concurrency)
        await semaphore.acquire()  # contre-pression : attend un créneau
        self._fin_vol.clear()
        tache = asyncio.create_task(self._envoyer_lot(lot, semaphore))
        self._en_vol.add(tache)
        tache.add_done_callback(self._en_vol.discard)
        tache.add_done_callback(lambda _: self._fin_vol.set() if not self._en_vol else None)
        logger.debug("Lotisseur._pousser | sortie (en vol : {})", len(self._en_vol))

    @atraced
    async def _envoyer_lot(self, lot: list[Document], semaphore: asyncio.Semaphore) -> None:
        """Tâche d'envoi d'un lot : `send()` de la sortie sous créneau réservé.

        Le créneau a été acquis par `_pousser` ; il est rendu ici quoi
        qu'il arrive (finally). Un échec d'envoi est journalisé sans
        interrompre la boucle — la politique de nouvelle tentative
        appartient aux sorties elles-mêmes.
        """
        logger.debug("Lotisseur._envoyer_lot | entrée | {} document(s)", len(lot))
        try:
            await self.sortie.send(lot)
            logger.debug("Lotisseur._envoyer_lot | sortie (envoyé)")
        except asyncio.CancelledError:
            logger.warning(_("envoi du lot annulé (arrêt forcé) : {} document(s) perdus"), len(lot))
            raise
        except Exception:
            logger.exception(_("échec de l'envoi du lot ({} document(s))"), len(lot))
        finally:
            semaphore.release()

    @atraced
    async def _attendre_en_vol(self) -> None:
        """Attend que tous les lots en vol soient partis."""
        logger.debug("Lotisseur._attendre_en_vol | entrée")
        if self._en_vol:
            await self._fin_vol.wait()
        logger.debug("Lotisseur._attendre_en_vol | sortie")

    @atraced
    async def __aenter__(self) -> Lotisseur:
        """`async with` : démarre le lotisseur et le renvoie."""
        logger.debug("Lotisseur.__aenter__ | entrée")
        await self.demarrer()
        logger.debug("Lotisseur.__aenter__ | sortie")
        return self

    @atraced
    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """`async with` : arrête proprement le lotisseur, même sur exception."""
        logger.debug("Lotisseur.__aexit__ | entrée (exc={})", exc_type and exc_type.__name__)
        if self._tache is not None:
            await self.arreter()
        logger.debug("Lotisseur.__aexit__ | sortie")


class MoteurSorties:
    """Un lotisseur par sortie, et le routage des documents vers leurs sorties (issue #6).

    Les conditions `if` des blocs `output{}` sont déjà évaluées par
    l'interpréteur côté appelant : `emettre()` reçoit simplement les
    indices des sorties destinataires de chaque document.
    """

    @traced
    def __init__(self, sorties: Sequence[Output], options_par_sortie: Sequence[dict[str, Any]] | None = None) -> None:
        """Construit un lotisseur par sortie.

        Args:
            sorties: Sorties compilées (`compiler_sorties`), dans
                l'ordre des blocs `output{}` du `.conf`.
            options_par_sortie: Options de lotissement de chaque bloc,
                alignées sur `sorties` (None partout si absent).
        """
        options_par_sortie = list(options_par_sortie or [{} for _ in sorties])
        self.lotisseurs = [
            Lotisseur(sortie, options) for sortie, options in zip(sorties, options_par_sortie, strict=True)
        ]

    @atraced
    async def demarrer(self) -> None:
        """Démarre tous les lotisseurs."""
        logger.debug("MoteurSorties.demarrer | entrée")
        for lotisseur in self.lotisseurs:
            await lotisseur.demarrer()
        logger.debug("MoteurSorties.demarrer | sortie | {} lotisseur(s)", len(self.lotisseurs))

    @atraced
    async def emettre(self, document: Document, indices: Sequence[int] | None = None) -> None:
        """Émet un document vers les sorties sélectionnées (toutes si None).

        Args:
            document: Événement à envoyer.
            indices: Positions des sorties destinatrices dans la liste
                du moteur ; `None` = toutes les sorties.
        """
        logger.debug("MoteurSorties.emettre | entrée")
        cibles = self.lotisseurs if indices is None else [self.lotisseurs[i] for i in indices]
        for lotisseur in cibles:
            await lotisseur.ajouter(document)
        logger.debug("MoteurSorties.emettre | sortie | {} cible(s)", len(cibles))

    @atraced
    async def arreter(self) -> None:
        """Arrête proprement tous les lotisseurs (vidage sans perte)."""
        logger.debug("MoteurSorties.arreter | entrée")
        for lotisseur in self.lotisseurs:
            await lotisseur.arreter()
        logger.debug("MoteurSorties.arreter | sortie")
