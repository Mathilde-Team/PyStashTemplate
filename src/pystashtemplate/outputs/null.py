# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie factice `null` : accepte tout, ne livre rien (chantier #4).

Pour les tests du moteur et des pipelines : un `output { null {} }` dans
un `.conf` compile et s'exécute sans système cible. Les événements reçus
sont mémorisés dans `livres` pour être inspectés par les tests.
"""

from __future__ import annotations

from typing import Any, ClassVar

from loguru import logger

from pystashtemplate.outputs.base import ACTIONS, Capacites, Output


class NullOutput(Output):
    """Sortie de test : compte et journalise, sans rien livrer.

    `livres` accumule les événements reçus ; `ouverte` suit le cycle
    `open`/`close` pour vérifier l'arrêt propre.
    """

    nom_plugin: ClassVar[str] = "null"
    capacites: ClassVar[Capacites] = Capacites(
        actions=ACTIONS,
        accepte_id_document=True,
        accuse_par_element=True,
    )

    def __init__(self, options: dict[str, Any]) -> None:
        """Mémorise les options (aucune n'est propre à `null`) et vide l'état."""
        logger.debug("NullOutput.__init__ | null ({} option(s) ignorée(s))", len(options))
        super().__init__(options)
        self.livres: list[dict[str, Any]] = []
        self.ouverte: bool = False
        logger.debug("NullOutput.__init__ | prête")

    async def open(self) -> None:
        """Marque la sortie ouverte (rien à connecter)."""
        logger.debug("null.open |")
        self.ouverte = True
        logger.debug("null.open | ouverte")

    async def send(self, lot: list[dict[str, Any]]) -> None:
        """Mémorise le lot, sans rien livrer."""
        logger.debug("null.send | {} événement(s)", len(lot))
        self.livres.extend(lot)
        logger.debug("null.send | mémorisés (total : {})", len(self.livres))

    async def close(self) -> None:
        """Marque la sortie fermée."""
        logger.debug("null.close |")
        self.ouverte = False
        logger.debug("null.close | fermée")
