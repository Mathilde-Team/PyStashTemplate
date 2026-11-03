# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité OpenSearch (issue #12) : la vraie API `_bulk`, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-opensearch.yml`
(contre un conteneur `opensearchproject/opensearch` sans plugin de
sécurité) ; localement, contre n'importe quel cluster :

    PYSTASHTEMPLATE_OPENSEARCH_URL=http://localhost:9200 uv run pytest tests/test_integration_opensearch.py

Sans la variable d'environnement, le banc est ignoré (les tests
unitaires couvrent déjà le comportement avec un faux serveur). Un seul
`asyncio.run` par test : un client httpx est lié à sa boucle d'événements
(ouvert et fermé dedans).
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


def _index(nom: str) -> str:
    """Nom d'index unique par test (nettoyé dans le `finally` du test)."""
    return f"conformite-{nom}-{datetime.now(timezone.utc).strftime('%H%M%S%f')}"


def test_le_cluster_repond_et_version_est_lue() -> None:
    async def scenario() -> None:
        sortie = OpenSearchOutput({"hosts": URL, "index": "logs-%Y.%m.%d"})
        try:
            info = await sortie.health()
            assert "version" in info
        finally:
            await sortie.close()

    asyncio.run(scenario())


def test_documents_indexes_sont_retrouvables() -> None:
    """Trois documents envoyés par la sortie, relus par l'API de recherche du produit."""
    index = _index("docs")

    async def scenario() -> None:
        sortie = OpenSearchOutput({"hosts": URL, "index": index})
        try:
            for numero in range(3):
                outcome = await sortie.envoyer({"message": f"conformite-{numero}", "numero": numero})
                assert outcome.value == "indexed", outcome
            await sortie.flush()
        finally:
            await sortie.close()

    asyncio.run(scenario())
    try:
        reponse = httpx.get(f"{URL}/{index}/_search", params={"size": 10})
        reponse.raise_for_status()
        corps = reponse.json()
        assert corps["hits"]["total"]["value"] == 3
        messages = {hit["_source"]["message"] for hit in corps["hits"]["hits"]}
        assert messages == {f"conformite-{n}" for n in range(3)}
    finally:
        httpx.delete(f"{URL}/{index}")


def test_document_id_deterministe_indexe_une_fois() -> None:
    """Même `_id` → même document (upsert implicite de l'API `_bulk`)."""
    index = _index("id")

    async def scenario() -> None:
        sortie = OpenSearchOutput({"hosts": URL, "index": index})
        try:
            for _ in range(3):
                outcome = await sortie.envoyer({"message": "stable"}, document_id="fixe")
                assert outcome.value == "indexed", outcome
        finally:
            await sortie.close()

    asyncio.run(scenario())
    try:
        reponse = httpx.get(f"{URL}/{index}/_search", params={"size": 10})
        reponse.raise_for_status()
        corps = reponse.json()
        assert corps["hits"]["total"]["value"] == 1
        assert corps["hits"]["hits"][0]["_id"] == "fixe"
    finally:
        httpx.delete(f"{URL}/{index}")


def test_echec_de_mapping_va_en_dlq() -> None:
    """Un champ date reçu en texte : rejet permanent, document consigné en DLQ (format identique à RsysLogstach)."""
    index = _index("dlq")
    # Index avec mapping strict : date, pas de texte.
    reponse = httpx.put(f"{URL}/{index}", json={"mappings": {"properties": {"@timestamp": {"type": "date"}}}})
    reponse.raise_for_status()
    try:

        async def scenario() -> None:
            sortie = OpenSearchOutput({"hosts": URL, "index": index, "dlq_path": f"/tmp/{index}-dlq.jsonl"})
            try:
                outcome = await sortie.envoyer({"@timestamp": "ceci-nest-pas-une-date"})
                assert outcome.value == "dead_lettered", outcome
            finally:
                await sortie.close()

        asyncio.run(scenario())
        ligne = json.loads(open(f"/tmp/{index}-dlq.jsonl", encoding="utf-8").read().splitlines()[0])
        assert ligne["@dlq_index"] == index
        assert "date" in ligne["@dlq_error"] or "parse" in ligne["@dlq_error"]
        assert ligne["document"] == {"@timestamp": "ceci-nest-pas-une-date"}
    finally:
        httpx.delete(f"{URL}/{index}")
