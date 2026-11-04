# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité GreptimeDB (issue #19) : la vraie API `_bulk`, en conteneur.

Exécuté par `.github/workflows/conformite-greptimedb.yml` (conteneur
`greptime/greptimedb`, mode standalone) ; localement :

    PYSTASHTEMPLATE_GREPTIMEDB_URL=http://localhost:4000 \\
    uv run pytest tests/test_integration_greptimedb.py

Sans la variable, le banc est ignoré. Chaque test écrit dans sa propre
table (créée à la première écriture), puis relit par l'API SQL.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.greptimedb import GreptimeDbOutput

URL = os.environ.get("PYSTASHTEMPLATE_GREPTIMEDB_URL")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_GREPTIMEDB_URL non défini")


def _messages(table: str, attendus: int) -> list[str]:
    """Messages de la table (API SQL), jusqu'à en obtenir `attendus` (30 s au plus)."""
    messages: list[str] = []
    for _ in range(60):
        reponse = httpx.post(f"{URL}/v1/sql", data={"sql": f"SELECT message FROM {table}"}, timeout=30)
        if reponse.status_code == 200 and "output" in reponse.json():
            messages = sorted(ligne[0] for ligne in reponse.json()["output"][0]["records"]["rows"])
            if len(messages) == attendus:
                break
        time.sleep(0.5)
    return messages


def _lot(sortie: GreptimeDbOutput, documents: list[dict]) -> list[SendOutcome]:
    async def scenario() -> list[SendOutcome]:
        try:
            for document in documents:
                await sortie.envoyer(document)
            return await sortie._flush()
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def test_health() -> None:
    async def scenario() -> dict:
        sortie = GreptimeDbOutput({"hosts": URL})
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert asyncio.run(scenario()) == {}


def test_lot_indexe_et_relu() -> None:
    table = f"banc_{uuid.uuid4().hex[:8]}"
    sortie = GreptimeDbOutput({"hosts": URL, "index": table, "batch_size": 100})
    assert _lot(sortie, [{"message": f"m{n}"} for n in range(3)]) == [SendOutcome.INDEXED] * 3
    assert _messages(table, 3) == ["m0", "m1", "m2"]


def test_lot_rejete_document_fautif_seul_en_dlq(tmp_path) -> None:
    table = f"banc_{uuid.uuid4().hex[:8]}"
    chemin = tmp_path / "dlq.jsonl"
    sortie = GreptimeDbOutput({"hosts": URL, "index": table, "batch_size": 100}, dlq=LocalDeadLetterQueue(str(chemin)))
    # `json.dumps` écrit NaN tel quel : ligne illisible pour GreptimeDB,
    # qui rejette alors tout le lot en 400.
    documents = [{"message": "a"}, {"message": "fautif", "valeur": float("nan")}, {"message": "c"}]
    assert _lot(sortie, documents) == [SendOutcome.INDEXED, SendOutcome.DEAD_LETTERED, SendOutcome.INDEXED]
    entrees = [json.loads(ligne) for ligne in chemin.read_text(encoding="utf-8").splitlines()]
    assert [e["document"]["message"] for e in entrees] == ["fautif"]
    assert _messages(table, 2) == ["a", "c"]
