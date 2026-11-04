# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie ``stdout`` (issue #32) : les documents sur la sortie standard.

Plugin Logstash ``stdout`` pour l'architecture de sorties agnostiques (le
``Output`` de ``base.py``) : chaque document part en texte sur la sortie
standard, rendu par le codec — ``rubydebug`` par défaut (comme le plugin),
``json_lines`` pour un NDJSON pipe-able.

La sortie par défaut (``sys.stdout``) est injectable (``flux=``) — les
tests écrivent dans un tampon mémoire, le connecteur peut choisir sa
sortie (``.stderr`` d'un sous-processus). Échec d'écriture :
``OSError`` remonte (pas de DLQ pour la sortie standard, rien à
différer — le document est affiché ou l'appelant décide).
"""

from __future__ import annotations

import sys
from typing import IO, Any, ClassVar, Mapping

from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, Output, enregistrer
from pystashtemplate.outputs.codecs import CODECS
from pystashtemplate.tracing import atraced, traced

__all__ = ["StdoutOutput"]


@enregistrer
class StdoutOutput(Output):
    """Sortie standard : un document rendu par ligne (codec au choix)."""

    plugin: ClassVar[str] = "stdout"

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
        flux: IO[str] | None = None,
    ) -> None:
        """Instancie la sortie standard.

        Args:
            options: Options du bloc ``output{}`` — ``codec``
                (``rubydebug`` par défaut, comme le plugin Logstash).
            flux: Sortie texte injectable (tests, connecteur) ;
                par défaut ``sys.stdout``.
        """
        super().__init__(options)
        codec = str(options.get("codec", "rubydebug"))
        if codec not in CODECS:
            logger.debug("__init__ : codec {} inconnu", codec)
            raise ValueError(_("codec {} inconnu (codecs disponibles : json_lines, rubydebug)").format(codec))
        self._encoder = CODECS[codec]
        self._flux = flux or sys.stdout

    @atraced
    async def open(self) -> None:
        """Sans effet : la sortie standard existe déjà."""
        logger.debug("StdoutOutput.open | sans effet")

    @atraced
    async def send(self, batch: list[Mapping[str, Any]]) -> None:
        """Écrit le lot, une ligne par document, sur la sortie standard."""
        for document in batch:
            texte = self._encoder(dict(document))
            self._flux.write(texte + "\n")
            logger.debug("send | {} caractère(s) écrits", len(texte))

    @atraced
    async def flush(self) -> None:
        """Vide la sortie standard (écriture déjà ligne à ligne)."""
        self._flux.flush()
        logger.debug("flush | sortie standard vidée")

    @atraced
    async def close(self) -> None:
        """Ne ferme pas la sortie standard (idempotent, sans effet)."""
        logger.debug("close | rien à fermer (sortie standard)")
