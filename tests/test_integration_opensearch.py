# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité OpenSearch (issue #12) : la vraie API `_bulk`, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-opensearch.yml`
(contre un conteneur `opensearchproject/opensearch` sans plugin de
sécurité) ; localement, contre n'importe quel cluster :

    PYSTASHTEMPLATE_OPENSEARCH_URL=http://localhost:9200 uv run pytest tests/test_integration_opensearch.py

Sans la variable d'environnement, le banc est ignoré (les tests
unitaires couvrent déjà le comportement avec un faux serveur).
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

import httpx
import pytest

from pystashtemplate.outputs.opensearch import OpenSearchOutput

URL = os.environ.get("PYSTASHTEMPLATE_OPENSEARCH_URL")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_OPENSEARCH_URL non défini")


def test_le_cluster_repond_et_version_est_lue() -> None:
    sortie = OpenSearchOutput({"hosts": URL, "index": "logs-%Y.%m.%d"})
    info = asyncio.run(sortie.health())
    assert "version" in info
    asyncio.run(sortie.close())


def test_documents_indexes_sont_retrouvables() -> None:
    """Trois documents envoyés par la sortie, relus par l'API de recherche du produit."""
    index = f"conformite-{datetime.now(timezone.utc).strftime('%H%M%S')}"
    sortie = OpenSearchOutput({"hosts": URL, "index": index})
    try:
        for numero in range(3):
            outcome = asyncio.run(sortie.envoyer({"message": f"conformite-{numero}", "numero": numero}))
            assert outcome.value == "indexed", outcome
        asyncio.run(sortie.flush())
        reponse = httpx.get(f"{URL}/{index}/_search", params={"size": 10})
        reponse.raise_for_status()
        corps = reponse.json()
        assert corps["hits"]["total"]["value"] == 3
        messages = {hit["_source"]["message"] for hit in corps["hits"]["hits"]}
        assert messages == {f"conformite-{n}" for n in range(3)}
    finally:
        httpx.delete(f"{URL}/{index}")
        asyncio.run(sortie.close())


def test_document_id_deterministe_indexe_une_fois() -> None:
    """Même `_id` → même document (upsert implicite de l'API `_bulk`)."""
    index = f"conformite-id-{datetime.now(timezone.utc).strftime('%H%M%S')}"
    sortie = OpenSearchOutput({"hosts": URL, "index": index})
    try:
        for _ in range(3):
            outcome = asyncio.run(sortie.envoyer({"message": "stable"}, document_id="fixe"))
            assert outcome.value == "indexed", outcome
        reponse = httpx.get(f"{URL}/{index}/_search", params={"size": 10})
        reponse.raise_for_status()
        corps = reponse.json()
        assert corps["hits"]["total"]["value"] == 1
        assert corps["hits"]["hits"][0]["_id"] == "fixe"
    finally:
        httpx.delete(f"{URL}/{index}")
        asyncio.run(sortie.close())


def test_echec_de_mapping_va_en_dlq() -> None:
    """Un champ date reçu en texte : rejet permanent, document consigné en DLQ (format identique à RsysLogstach)."""
    index = f"conformite-dlq-{datetime.now(timezone.utc).strftime('%H%M%S')}"
    # Index avec mapping strict : date, pas de texte.
    mapping = {"mappings": {"properties": {"@timestamp": {"type": "date"}}}}
    reponse = httpx.put(f"{URL}/{index}", json=mapping)
    reponse.raise_for_status()
    try:
        sortie = OpenSearchOutput({"hosts": URL, "index": index, "dlq_path": f"/tmp/{index}-dlq.jsonl"})
        outcome = asyncio.run(sortie.envoyer({"@timestamp": "ceci-nest-pas-une-date"}))
        assert outcome.value == "dead_lettered", outcome
        ligne = json.loads(open(f"/tmp/{index}-dlq.jsonl", encoding="utf-8").read().splitlines()[0])
        assert ligne["@dlq_index"] == index
        assert "date" in ligne["@dlq_error"] or "parse" in ligne["@dlq_error"]
        assert ligne["document"] == {"@timestamp": "ceci-nest-pas-une-date"}
    finally:
        httpx.delete(f"{URL}/{index}")
        asyncio.run(sortie.close())
