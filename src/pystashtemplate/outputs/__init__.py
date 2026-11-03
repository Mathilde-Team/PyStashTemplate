# SPDX-License-Identifier: GPL-3.0-or-later
"""Sorties agnostiques du socle (chantier #2) : une sortie par produit.

Le nom du plugin dans un bloc `output{}` du `.conf` désigne la sortie :
`opensearch{}`, `elasticsearch{}`, `victorialogs{}`… Chaque sortie de la
famille « API _bulk Elasticsearch » n'est qu'une sous-classe de quelques
lignes de :class:`~pystashtemplate.outputs.elasticsearch_bulk.ElasticsearchBulkOutput`
(issue #11 — valeurs par défaut et capacités). Les autres familles
(Loki, ClickHouse, HEC, OTLP…) suivront dans des sous-issues.

Le registre (`SORTIES`, rempli par le décorateur `enregistrer`) sera
utilisé par le connecteur pour construire la sortie choisie dans le
`output{}` — la configuration commune (hosts, authentification, ssl,
batch, réessais, dlq) est l'objet de l'issue « schéma de configuration
commun ».
"""

from __future__ import annotations

from loguru import logger

from pystashtemplate.outputs.elasticsearch_bulk import (
    ACTIONS,
    ActionInvalideError,
    ElasticsearchBulkOutput,
    SendOutcome,
)
from pystashtemplate.tracing import traced

__all__ = [
    "ACTIONS",
    "ActionInvalideError",
    "ElasticsearchBulkOutput",
    "SendOutcome",
    "SORTIES",
    "enregistrer",
    "noms_sorties",
    "sortie_par_nom",
]

# Nom de plugin Logstash -> classe de sortie.
SORTIES: dict[str, type[ElasticsearchBulkOutput]] = {}


@traced
def enregistrer(nom: str, classe: type[ElasticsearchBulkOutput]) -> type[ElasticsearchBulkOutput]:
    """Enregistre une sortie sous le nom du plugin qui la désigne dans le `output{}`."""
    logger.debug("enregistrer | sortie {} -> {}", nom, classe.__name__)
    SORTIES[nom] = classe
    return classe


@traced
def sortie_par_nom(nom: str) -> type[ElasticsearchBulkOutput] | None:
    """Classe de sortie enregistrée sous ce nom de plugin, ou None."""
    return SORTIES.get(nom)


@traced
def noms_sorties() -> list[str]:
    """Noms de plugins enregistrés, triés."""
    return sorted(SORTIES)
