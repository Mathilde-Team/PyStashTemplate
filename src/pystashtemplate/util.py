# SPDX-License-Identifier: GPL-3.0-or-later
"""Utilitaires génériques sans logique métier propre.

Extrait d'omprog.py (session 9, réorganisation selon le squelette de
Mathilde). Ne dépend que de `tracing`.

`add_failure_tags` (session 24) : déplacée depuis `interpreter.py`
(`_add_failure_tags`, extraite en session 22 de `_date_parse_failure`,
session 19) pour être partagée avec `filters.py::GeoipFilter` (step
"geoip" de `FilterPipeline`, voir FEATURES.md) sans créer de dépendance
`filters -> interpreter` (interdite, voir PATTERNS.md, "Absence de
dépendances circulaires entre modules" — `interpreter` dépend déjà de
`filters`, l'inverse créerait un cycle). `util` reste une feuille du
graphe de dépendances (ne dépend que de `tracing`), un consommateur de
plus après `indexer`/`interpreter` depuis les sessions 17/18 — aucun
risque de cycle. Renommée sans le préfixe `_` : ancienne convention
interne à `interpreter.py`, devenue un utilitaire public partagé entre
deux modules.
"""

from __future__ import annotations

import json
import os
from typing import Any

from loguru import logger  # noqa: F401 — logger de module exigé (SOCLE §8)

from pystashtemplate.tracing import traced


@traced
def add_failure_tags(doc: dict[str, Any], raw_tags: Any) -> None:
    """Ajoute des tags à `doc["tags"]`, sans doublon — `tag_on_failure` des filtres officiels.

    Partagée entre `interpreter.py::PipelineInterpreter` (`tag_on_failure`
    de `date`/`geoip`, sessions 19/22, `--pipeline-conf`) et
    `filters.py::GeoipFilter` (`tag_on_failure` de `geoip`, session 24,
    `--filters`) — même sémantique `tag_on_failure` pour les DEUX
    mécanismes de filtrage du projet, une seule implémentation plutôt que
    plusieurs copies (même motif que `enrichment._build_geoip_record`,
    session 21, ou `filters.apply_mutate`, session 14). `raw_tags` accepte
    une chaîne unique ou une liste, comme Logstash ; les entrées non-chaîne
    d'une liste sont ignorées silencieusement.
    """
    tags_to_add = raw_tags if isinstance(raw_tags, list) else [raw_tags]
    tags = doc.get("tags")
    if not isinstance(tags, list):
        tags = []
    tags.extend(tag for tag in tags_to_add if isinstance(tag, str) and tag not in tags)
    doc["tags"] = tags


@traced
def load_json_file(path: str) -> dict:
    """Charge un fichier JSON depuis le disque.

    Args:
        path: Chemin du fichier JSON.

    Returns:
        Le contenu désérialisé.
    """
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


@traced
def opensearch_auth_from_env() -> tuple[str, str] | None:
    """Lit les identifiants OpenSearch depuis l'environnement (jamais en argument CLI, visible via `ps aux`)."""
    user = os.environ.get("OPENSEARCH_USER")
    password = os.environ.get("OPENSEARCH_PASSWORD")
    if user and password:
        return (user, password)
    return None
