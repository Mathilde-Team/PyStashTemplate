# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie factice `null` : accepte tout, n'envoie rien (issue #4).

Sert aux tests du socle et comme gabarit minimal d'une vraie sortie : les
quatre méthodes du cycle de vie sont présentes et journalisées, sans
aucune connexion. Le plugin Logstash éponyme (`output { null {} }`)
lui-même ne fait rien de plus.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar

from loguru import logger

from pystashtemplate.outputs import Capacites, Document, Output, enregistrer
from pystashtemplate.tracing import atraced


@enregistrer
class NullOutput(Output):
    """Sortie puits : chaque document est journalisé en debug puis oublié."""

    plugin: ClassVar[str] = "null"
    capacites: ClassVar[Capacites] = Capacites(
        actions=frozenset({"index", "create", "update", "delete"}),
        accepte_id=True,
        accuse_par_evenement=True,
        taille_max_requete=None,
    )

    @atraced
    async def open(self) -> None:
        """Sans effet : la sortie puits n'a aucune connexion à ouvrir."""
        logger.debug("NullOutput.open | sans effet")
        logger.debug("NullOutput.open | sortie")

    @atraced
    async def send(self, batch: Sequence[Document]) -> None:
        """Oublie le lot, après l'avoir compté en debug."""
        logger.debug("NullOutput.send | {} document(s) ignoré(s)", len(batch))
        logger.debug("NullOutput.send | sortie")

    @atraced
    async def flush(self) -> None:
        """Sans effet : rien n'est jamais mis en attente."""
        logger.debug("NullOutput.flush | sans effet")
        logger.debug("NullOutput.flush | sortie")

    @atraced
    async def close(self) -> None:
        """Sans effet : aucune ressource à libérer."""
        logger.debug("NullOutput.close | sans effet")
        logger.debug("NullOutput.close | sortie")
