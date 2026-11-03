# SPDX-License-Identifier: GPL-3.0-or-later
"""Socle des sorties agnostiques (issue #4) : contrat commun `Output`.

Chaque client d'ingestion (OpenSearch, Elasticsearch, VictoriaLogs…) se
branche derrière la même interface, pour que le moteur reste **agnostique**
du système cible : l'interpréteur de pipeline rend les options de chaque
bloc `output{}` (`PipelineInterpreter.output_plugins`, #125 RsysLogstach),
le registre (`pystashtemplate.outputs.REGISTRY`) fait le lien nom de
plugin → classe, et cette interface décrit le cycle de vie d'une sortie :

- `open()` : connexion (une seule fois, avant le premier envoi) ;
- `send(batch)` : envoi d'un lot de documents, la sortie gère elle-même
  ses lots internes, ses réessais et sa DLQ (issues #5 à #7) ;
- `flush()` : pousser ce qui est en attente avant un arrêt propre ;
- `close()` : libérer la connexion.

Utilisable avec `async with` : `open()` à l'entrée, `close()` à la sortie.

Les capacités sont **déclarées par la classe** (`Capacites`), pas
découvertes à l'envoi : le moteur peut valider un pipeline dès sa
compilation (actions demandées supportées, `_id` accepté…) sans ouvrir
la moindre connexion. Les méthodes asynchrones ne peuvent pas utiliser
`tracing.traced` (synchrones) : elles journalisent entrée et sortie à la
main, mêmes règles SOCLE §8 — le décorateur async dédié est l'issue #37.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Self

from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.tracing import traced

# Actions d'indexation d'un document, comme l'API `_bulk` de la famille
# Elasticsearch (issue #2 PyStashTemplate : `index`, `create`, `update`,
# `delete`).
ACTIONS = ("index", "create", "update", "delete")

# Un document d'événement : dictionnaire plat ou imbriqué, tel que
# traité par `filter{}` et rendu par les gabarits d'`output{}`.
Document = Mapping[str, Any]


@dataclass(frozen=True)
class Capacites:
    """Ce qu'une sortie accepte, déclaré par sa classe pour validation à la compilation.

    Attributs:
        actions: Actions d'indexation supportées, parmi
            ``ACTIONS`` (``index``/``create``/``update``/``delete``).
            Une classe peut déclarer un sous-ensemble (ex. Quickwit :
            ``create`` seulement, issue #2).
        accepte_id: `True` si la sortie honore un identifiant de
            document (`document_id`, action `_id` de la famille _bulk) ;
            une sortie sans `_id` rejette ce qui l'exige.
        accuse_par_evenement: `True` si la sortie rend un accusé de
            réception élément par élément (comme `_bulk` : un statut par
            ligne) ; `False` si l'accusé est global à la requête (ex.
            Quickwit, dont les erreurs ne remontent que dans les journaux
            serveur).
        taille_max_requete: Taille maximale d'une requête en octets, ou
            ``None`` si la sortie n'en publie pas (le moteur n'aura pas
            à découper le lot en conséquence).
    """

    actions: frozenset[str] = frozenset(ACTIONS)
    accepte_id: bool = True
    accuse_par_evenement: bool = True
    taille_max_requete: int | None = None

    @traced
    def verifie_action(self, action: str) -> None:
        """Lève si `action` n'est pas supportée par la sortie.

        Args:
            action: Action demandée par le pipeline (`index`, `create`…).

        Raises:
            ValueError: L'action n'est pas dans les capacités déclarées.
        """
        if action not in self.actions:
            raise ValueError(
                _("action « {} » non supportée par cette sortie (supportées : {})").format(
                    action, ", ".join(sorted(self.actions))
                )
            )


class Output(ABC):
    """Contrat commun d'une sortie de pipeline, agnostique du système cible.

    Une sous-classe déclare son nom de plugin `output{}` (attribut de
    classe `plugin`, clé du registre) et ses `Capacites`, puis implémente
    le cycle de vie asynchrone ci-dessous. `options` est le dictionnaire
    d'options du bloc `output{}`, déjà rendu contre le document (voir
    `OutputAction`).
    """

    # Nom du plugin tel qu'écrit dans un bloc `output{}` du `.conf`
    # (ex. `opensearch`, `null`). Renseigné par chaque sous-classe.
    plugin: ClassVar[str]

    # Ce que la sortie accepte (voir `Capacites`). Renseigné par chaque
    # sous-classe — les valeurs par défaut supposent une sortie complète.
    capacites: ClassVar[Capacites] = Capacites()

    @traced
    def __init__(self, options: dict[str, Any]) -> None:
        """Options du bloc `output{}` de la sortie, telles que rendues par l'interpréteur.

        Args:
            options: Options du bloc, rendues contre le document pour ce
                qui est dynamique (index, identifiant…), telles quelles
                pour la connexion (`hosts`, `user`…).
        """
        self.options: dict[str, Any] = options

    @abstractmethod
    async def open(self) -> None:
        """Établit la connexion (idempotent : sans effet si déjà ouverte)."""

    @abstractmethod
    async def send(self, batch: Sequence[Document]) -> None:
        """Envoie un lot de documents.

        Args:
            batch: Documents à indexer, dans l'ordre du pipeline. La
                sortie découpe si nécessaire (`taille_max_requete`) et
                gère ses réessais ; un échec définitif remonte au
                mécanisme de DLQ (#7).
        """

    @abstractmethod
    async def flush(self) -> None:
        """Pousse les documents en attente (arrêt propre, `SIGTERM`)."""

    @abstractmethod
    async def close(self) -> None:
        """Libère la connexion et les ressources (idempotent)."""

    async def __aenter__(self) -> Self:
        """`async with` : ouvre la sortie et la renvoie."""
        logger.debug("Output.__aenter__ | {} | ouverture", self.plugin)
        await self.open()
        logger.debug("Output.__aenter__ | {} | sortie", self.plugin)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """`async with` : ferme la sortie, même sur exception."""
        logger.debug("Output.__aexit__ | {} | fermeture (exc={})", self.plugin, exc_type and exc_type.__name__)
        await self.close()
        logger.debug("Output.__aexit__ | {} | sortie", self.plugin)
