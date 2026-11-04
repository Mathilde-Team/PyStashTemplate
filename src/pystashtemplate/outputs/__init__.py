# SPDX-License-Identifier: GPL-3.0-or-later
"""Sorties agnostiques du pipeline (issue #4) : registre des plugins `output{}`.

Le nom du plugin d'un bloc `output{}` du `.conf` (ex. `opensearch`,
`null`) désigne une sous-classe de `Output` inscrite au `REGISTRY`. La
compilation d'un pipeline instancie chaque sortie via
`compiler_sorties()` : un plugin inconnu y lève une erreur claire (et
traduite), avant tout envoi — le moteur reste agnostique du système
cible, chaque sortie porte son propre client.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.outputs.base import ACTIONS, Capacites, Document, Output
from pystashtemplate.tracing import traced

if TYPE_CHECKING:
    # Typage seul : évite un import cyclique au démarrage, la bibliothèque
    # ne dépend pas de l'interpréteur pour enregistrer des sorties.
    from pystashtemplate.interpreter import PipelineInterpreter

# Nom de plugin `output{}` → classe de sortie. Alimenté par le
# décorateur `enregistrer` au moment de l'import de chaque module de
# sortie (la sortie `null` ci-dessous l'est déjà, importée en fin de
# module pour que le registre existe au moment de son inscription).
REGISTRY: dict[str, type[Output]] = {}


@traced
def enregistrer(classe: type[Output]) -> type[Output]:
    """Décorateur : inscrit une classe de sortie dans le registre sous son `plugin`.

    Args:
        classe: Sous-classe concrète de `Output` (attribut `plugin` défini).

    Returns:
        La classe, inchangée — l'inscription est un effet de bord.

    Raises:
        ValueError: Le plugin est déjà inscrit (deux sorties ne peuvent
            pas porter le même nom).
    """
    nom = classe.plugin
    if nom in REGISTRY:
        raise ValueError(_("sortie « {} » déjà inscrite au registre").format(nom))
    REGISTRY[nom] = classe
    logger.debug("enregistrer | {} → {}", nom, classe.__qualname__)
    return classe


@traced
def compiler_sorties(pipeline: PipelineInterpreter) -> list[Output]:
    """Instancie les sorties d'un pipeline compilé, ou échoue sur un plugin inconnu.

    À appeler juste après la construction de `PipelineInterpreter` : la
    validation a lieu **à la compilation du pipeline**, avant tout envoi
    (et donc avant toute connexion). Chaque bloc `output{}` donne une
    instance, dans l'ordre du `.conf` — les options rendues contre le
    document sont portées par `OutputAction` côté appelant, les options
    de connexion viennent de `output_plugins()`.

    Args:
        pipeline: Interpréteur déjà construit (son AST est lu, jamais
            modifié).

    Returns:
        Les sorties instanciées, une par bloc `output{}` du pipeline.

    Raises:
        SortieInconnueError: Un plugin `output{}` n'est pas au registre —
            le message nomme le coupable et les sorties disponibles.
    """
    sorties: list[Output] = []
    for nom, options in pipeline.output_plugins:
        classe = REGISTRY.get(nom)
        if classe is None:
            raise SortieInconnueError(nom, sorted(REGISTRY))
        sorties.append(classe(dict(options)))
    logger.debug("compiler_sorties | {} sortie(s)", len(sorties))
    return sorties


class SortieInconnueError(ValueError):
    """Plugin `output{}` absent du registre — erreur de compilation du pipeline, traduite.

    Message en français construit à la levée (msgfmt vérifie le format
    `{}`), nommant le plugin inconnu et les sorties disponibles.
    """

    @traced
    def __init__(self, plugin: str, disponibles: list[str]) -> None:
        """Construit l'erreur traduite du plugin inconnu.

        Args:
            plugin: Nom du plugin `output{}` introuvable au registre.
            disponibles: Noms des sorties inscrites, triés.
        """
        super().__init__(_("sortie « {} » inconnue — sorties disponibles : {}").format(plugin, ", ".join(disponibles)))
        self.plugin = plugin
        self.disponibles = list(disponibles)


# Import tardif : les sorties s'inscrivent au registre ci-dessus au
# moment de leur import — le registre doit exister (définitions ci-dessus)
# avant que ce module ne soit chargé.
from pystashtemplate.outputs.elasticsearch import ElasticsearchOutput  # noqa: E402
from pystashtemplate.outputs.fichier import FileOutput  # noqa: E402
from pystashtemplate.outputs.null import NullOutput  # noqa: E402
from pystashtemplate.outputs.opensearch import OpenSearchOutput  # noqa: E402
from pystashtemplate.outputs.stdout import StdoutOutput  # noqa: E402
from pystashtemplate.outputs.victorialogs import VictoriaLogsOutput  # noqa: E402

__all__ = [
    "ACTIONS",
    "Capacites",
    "Document",
    "ElasticsearchOutput",
    "FileOutput",
    "NullOutput",
    "OpenSearchOutput",
    "Output",
    "REGISTRY",
    "SortieInconnueError",
    "StdoutOutput",
    "VictoriaLogsOutput",
    "compiler_sorties",
    "enregistrer",
]
