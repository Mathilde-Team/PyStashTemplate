# SPDX-License-Identifier: GPL-3.0-or-later
"""Sorties agnostiques : registre des plugins `output{}` (chantier #4).

`REGISTRY` associe le nom d'un plugin `output{}` d'un `.conf` — `null`
aujourd'hui, `opensearch`, `elasticsearch`, `victorialogs`… ensuite
(issues #11 et suivantes) — à sa classe `pystashtemplate.outputs.base.Output`.

`compiler_sorties()` instancie les sorties d'une liste de plugins
`(nom, options)`, telle que `PipelineInterpreter.output_plugins` la
renvoie : un plugin inconnu est refusé à la compilation du pipeline par
une `ValueError` traduite, qui nomme le plugin fautif et les sorties
disponibles. `sorties_du_pipeline()` enchaîne les deux : c'est le
branchement du registre sur l'interpréteur.

Le contrat lui-même (cycle `open`/`send`/`flush`/`close`, capacités
déclarées) est documenté dans `pystashtemplate.outputs.base`.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.outputs.base import ACTIONS, Capacites, Output
from pystashtemplate.outputs.null import NullOutput

if TYPE_CHECKING:
    from pystashtemplate.interpreter import PipelineInterpreter

__all__ = [
    "ACTIONS",
    "Capacites",
    "Output",
    "REGISTRY",
    "compiler_sorties",
    "enregistrer",
    "sorties_du_pipeline",
]

#: Nom de plugin `output{}` → classe de sortie. `null` est toujours
#: enregistrée (sortie factice des tests, #4) ; les autres sorties
#: s'ajoutent à mesure des issues du chantier « sorties agnostiques ».
REGISTRY: dict[str, type[Output]] = {NullOutput.nom_plugin: NullOutput}


def enregistrer(sortie: type[Output]) -> type[Output]:
    """Ajoute (ou remplace) une classe de sortie dans le registre.

    Sert de décorateur aux sorties définies hors du paquet, qui veulent
    être compilables depuis un `.conf` sans modifier PyStashTemplate.

    Args:
        sortie: Sous-classe de `Output` avec son `nom_plugin`.

    Returns:
        La classe, telle quelle (utilisable comme décorateur).
    """
    logger.debug("enregistrer | {}", sortie.nom_plugin)
    REGISTRY[sortie.nom_plugin] = sortie
    logger.debug("enregistrer | {} sortie(s) connue(s)", len(REGISTRY))
    return sortie


def compiler_sorties(plugins: Iterable[tuple[str, dict[str, Any]]]) -> list[Output]:
    """Instancie les sorties des plugins `(nom, options)` d'un `output{}`.

    Args:
        plugins: Plugins de sortie avec leurs options brutes, comme
            `PipelineInterpreter.output_plugins` les rend.

    Returns:
        Une sortie par plugin, dans l'ordre du `.conf`.

    Raises:
        ValueError: Un plugin de sortie est inconnu du registre (message
            traduit, avec les sorties disponibles).
    """
    logger.debug("compiler_sorties | lecture des plugins")
    # Mémorisation d'abord : `plugins` peut être un générateur.
    plugins = list(plugins)
    logger.debug("compiler_sorties | {} plugin(s)", len(plugins))
    sorties: list[Output] = []
    for nom, options in plugins:
        classe = REGISTRY.get(nom)
        if classe is None:
            disponibles = ", ".join(sorted(REGISTRY)) or "aucune"
            logger.debug("compiler_sorties | plugin inconnu : {}", nom)
            raise ValueError(_("Sortie inconnue : « {} » (disponibles : {})").format(nom, disponibles))
        logger.debug("compiler_sorties | {} → {}", nom, classe.__name__)
        sorties.append(classe(options))
    logger.debug("compiler_sorties | {} sortie(s) compilée(s)", len(sorties))
    return sorties


def sorties_du_pipeline(interpreter: PipelineInterpreter) -> list[Output]:
    """Compile les sorties des blocs `output{}` d'un pipeline interprété.

    Branchement du registre sur `PipelineInterpreter.output_plugins` : le
    `.conf` pilote les sorties, le moteur reste agnostique.

    Args:
        interpreter: Pipeline compilé (ses options de sortie sont brutes,
            rendues par événement au moment d'envoyer).

    Returns:
        Une sortie par plugin `output{}` du pipeline, branches comprises.

    Raises:
        ValueError: Un plugin de sortie du `.conf` est inconnu.
    """
    logger.debug("sorties_du_pipeline | lecture des plugins output{{}}")
    sorties = compiler_sorties(interpreter.output_plugins)
    logger.debug("sorties_du_pipeline | {} sortie(s)", len(sorties))
    return sorties
