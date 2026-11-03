# SPDX-License-Identifier: GPL-3.0-or-later
"""Contrat commun des sorties (chantier « sorties agnostiques », #4).

Une sortie livre des lots d'événements au système cible (OpenSearch,
Elasticsearch, VictoriaLogs…). Le moteur — un `PipelineInterpreter` et son
connecteur, par exemple RsysLogstach — reste agnostique du système cible :
il ne connaît que ce contrat, jamais un client particulier.

Capacités déclarées par chaque classe de sortie (`Capacites`) :

- `actions` : actions supportées parmi `index`, `create`, `update`,
  `delete` (Logstash et l'API `_bulk` ES/OpenSearch) ;
- `accepte_id_document` : la sortie honore un `_id` fourni par
  l'événement (`False` : le système cible génère ses propres
  identifiants, comme Quickwit) ;
- `accuse_par_element` : accusé de réception élément par élément (réponse
  `_bulk` ES/OpenSearch) ou global (le lot entier est une unité) ;
- `taille_max_requete` : taille maximale de requête en octets (`None` :
  non contraint).

Cycle de vie : `open()` (connexion) puis `send()` par lot, `flush()` pour
vider les tampons, `close()` pour couper. Le tout est asynchrone, et
l'objet est utilisable en `async with`. Les options brutes du bloc
`output{}` du `.conf` sont passées au constructeur ; leur validation
(schéma, options inconnues) est le chantier #8.

Registre des plugins et compilation : `pystashtemplate.outputs`
(`REGISTRY`, `compiler_sorties`).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar

from loguru import logger

#: Actions comprises dans la sémantique `_bulk` (ES/OpenSearch, Logstash).
ACTIONS = frozenset({"index", "create", "update", "delete"})


@dataclass(frozen=True)
class Capacites:
    """Capacités déclarées par une sortie (voir la docstring du module).

    Args:
        actions: Actions supportées, parmi `ACTIONS` (`index` seul par
            défaut, le cas minimal d'une sortie qui écrit).
        accepte_id_document: `True` si un `_id` fourni est honoré.
        accuse_par_element: `True` si la réponse du système distingue
            chaque élément du lot.
        taille_max_requete: Taille maximale d'une requête en octets.
    """

    actions: frozenset[str] = frozenset({"index"})
    accepte_id_document: bool = True
    accuse_par_element: bool = False
    taille_max_requete: int | None = None

    def __post_init__(self) -> None:
        logger.debug("Capacites.__post_init__ | actions : {}", sorted(self.actions))
        inconnues = self.actions - ACTIONS
        if inconnues:
            logger.debug("Capacites.__post_init__ | actions inconnues : {}", sorted(inconnues))
            raise ValueError(f"actions de sortie inconnues : {sorted(inconnues)}")
        logger.debug("Capacites.__post_init__ | capacités validées")

    def supporte(self, action: str) -> bool:
        """`True` si la sortie gère cette action (test rapide, message d'erreur de #8)."""
        logger.debug("supporte | action {} : {}", action, action in self.actions)
        return action in self.actions


class Output(ABC):
    """Base des sorties : cycle `open`/`send`/`flush`/`close`, asynchrone.

    Une sous-classe déclare `nom_plugin` (nom du bloc `output{}` du
    `.conf`, par exemple `null`) et `capacites`, puis implémente
    `open()` et `send()`. `flush()` et `close()` ont un défaut sans
    effet, pour les sorties sans tampon ni connexion à couper.

    Le même objet sert de gestionnaire asynchrone :

    >>> async with NullOutput({}) as sortie:
    ...     await sortie.send([{"message": "coucou"}])  # doctest: +SKIP
    """

    #: Nom du plugin `output{}` ; défini par chaque sous-classe.
    nom_plugin: ClassVar[str]
    #: Capacités déclarées ; redéfinies par chaque sous-classe.
    capacites: ClassVar[Capacites] = Capacites()

    def __init__(self, options: dict[str, Any]) -> None:
        """Mémorise les options brutes du bloc `output{}`.

        Args:
            options: Options du plugin, telles que normalisées par
                `PipelineInterpreter.output_plugins` (validation : #8).
        """
        logger.debug("Output.__init__ | {} ({} option(s))", type(self).__name__, len(options))
        self.options: dict[str, Any] = dict(options)
        logger.debug("Output.__init__ | options mémorisées")

    @abstractmethod
    async def open(self) -> None:
        """Prépare la sortie (connexion, authentification, vérification)."""

    @abstractmethod
    async def send(self, lot: list[dict[str, Any]]) -> None:
        """Livré un lot d'événements au système cible.

        Args:
            lot: Événements rendus (gabarits `%{...}` déjà appliqués).
        """

    async def flush(self) -> None:
        """Vide les tampons de la sortie. Défaut : sans effet."""
        logger.debug("flush | {} : sans effet", self.nom_plugin)

    async def close(self) -> None:
        """Coupe la sortie. Défaut : sans effet."""
        logger.debug("close | {} : sans effet", self.nom_plugin)

    async def __aenter__(self) -> "Output":
        """`open()` puis sert de gestionnaire asynchrone."""
        logger.debug("__aenter__ | {}", self.nom_plugin)
        await self.open()
        logger.debug("__aenter__ | {} ouverte", self.nom_plugin)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """`close()` dans tous les cas (l'exception, s'il y en a une, remonte)."""
        logger.debug("__aexit__ | {} (exception : {})", self.nom_plugin, type(exc).__name__ if exc else "aucune")
        await self.close()
        logger.debug("__aexit__ | {} fermée", self.nom_plugin)
