# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité Graylog GELF (issue #26) : la vraie ingestion, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-gelf.yml`
(Graylog + MongoDB + OpenSearch, entrées GELF HTTP en réception par lot,
TCP et UDP créées par l'API) ; localement :

    PYSTASHTEMPLATE_GELF_HOST=localhost \\
    PYSTASHTEMPLATE_GELF_OPENSEARCH=http://localhost:9200 \\
    uv run pytest tests/test_integration_gelf.py

Ports attendus : 12201 (HTTP), 12202 (TCP), 12203 (UDP). Sans la
variable d'environnement, le banc est ignoré. Les messages sont relus
dans les index `graylog_*` d'OpenSearch (Graylog y range le champ GELF
`_banc_id` sous `banc_id`), avec une attente bornée.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any

import httpx
import pytest

from pystashtemplate.outputs.gelf import GelfOutput

HOTE = os.environ.get("PYSTASHTEMPLATE_GELF_HOST")
OPENSEARCH = os.environ.get("PYSTASHTEMPLATE_GELF_OPENSEARCH", "http://localhost:9200")
PORTS = {"http": 12201, "tcp": 12202, "udp": 12203}
pytestmark = pytest.mark.skipif(not HOTE, reason="banc de conformité : PYSTASHTEMPLATE_GELF_HOST non défini")


def _sortie(transport: str, **options: Any) -> GelfOutput:
    """Sortie vers l'entrée Graylog du transport demandé."""
    schema = "http" if transport == "http" else transport
    return GelfOutput({"hosts": f"{schema}://{HOTE}:{PORTS[transport]}", "transport": transport, **options})


def _envoyer_tous(sortie: GelfOutput, documents: list[dict[str, Any]]) -> list[str]:
    """Envoie les documents, vide le tampon, ferme ; rend la valeur de chaque résultat."""

    async def scenario() -> list[str]:
        try:
            await sortie.open()
            resultats = [(await sortie.envoyer(doc)).value for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _chercher(banc_id: str, attendus: int) -> list[dict[str, Any]]:
    """Messages de `banc_id` dans OpenSearch, jusqu'à en avoir `attendus` (90 s au plus)."""
    sources: list[dict[str, Any]] = []
    for _ in range(180):
        reponse = httpx.post(
            f"{OPENSEARCH}/graylog_*/_search",
            json={"query": {"term": {"banc_id": banc_id}}, "size": 100},
        )
        if reponse.status_code == 200:
            sources = [hit["_source"] for hit in reponse.json()["hits"]["hits"]]
            if len(sources) >= attendus:
                return sources
        time.sleep(0.5)
    return sources


def _documents(banc_id: str, n: int) -> list[dict[str, Any]]:
    """`n` documents marqués par `banc_id`."""
    return [
        {
            "message": f"{banc_id} n{i}",
            "@timestamp": "2026-11-04T10:00:00Z",
            "host": {"name": "banc"},
            "level": "warning",
            "banc_id": banc_id,
            "n": i,
            "service": {"name": "pystash"},
            "tags": ["banc", "gelf"],
        }
        for i in range(n)
    ]


@pytest.mark.parametrize(
    ("transport", "options"),
    [
        ("http", {}),
        ("http", {"batch_size": 3}),
        ("tcp", {"batch_size": 3}),
        ("udp", {}),
        ("udp", {"compression": True}),
    ],
    ids=["http", "http-lot", "tcp", "udp", "udp-gzip"],
)
def test_messages_ingeres(transport: str, options: dict[str, Any]) -> None:
    banc_id = uuid.uuid4().hex
    resultats = _envoyer_tous(_sortie(transport, **options), _documents(banc_id, 3))
    assert "retryable" not in resultats
    messages = _chercher(banc_id, 3)
    assert sorted(m["message"] for m in messages) == [f"{banc_id} n{i}" for i in range(3)]
    for message in messages:
        assert message["source"] == "banc"
        assert message["level"] == 4
        assert message["service_name"] == "pystash"
        assert message["tags"] == "banc, gelf"
        assert message["full_message"] == message["message"]


def test_udp_message_en_morceaux() -> None:
    banc_id = uuid.uuid4().hex
    gros = {**_documents(banc_id, 1)[0], "message": f"{banc_id} " + "x" * 5000}
    assert _envoyer_tous(_sortie("udp", chunksize=1000), [gros]) == ["indexed"]
    messages = _chercher(banc_id, 1)
    assert messages[0]["message"].endswith("x" * 5000)


@pytest.mark.parametrize("transport", ["http", "tcp"])
def test_health(transport: str) -> None:
    async def scenario() -> dict[str, Any]:
        sortie = _sortie(transport)
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert asyncio.run(scenario()) == {"transport": transport, "status": "ok"}
