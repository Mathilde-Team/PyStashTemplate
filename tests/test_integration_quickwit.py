# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité Quickwit (issue #15) : la vraie API `_bulk`, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-quickwit.yml`
(conteneur `quickwit/quickwit`) ; localement :

    PYSTASHTEMPLATE_QUICKWIT_URL=http://localhost:7280 uv run pytest tests/test_integration_quickwit.py

Sans la variable d'environnement, le banc est ignoré. Chaque test crée
son propre index (Quickwit ne crée pas l'index à l'ingestion), envoie
avec `refresh => "true"` (commit forcé) puis relit par l'API de
recherche, avec une attente bornée.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from datetime import datetime, timezone

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs.quickwit import QuickwitOutput

URL = os.environ.get("PYSTASHTEMPLATE_QUICKWIT_URL")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_QUICKWIT_URL non défini")


def _creer_index() -> str:
    """Crée un index dynamique éphémère (commit rapide) et rend son nom."""
    nom = f"banc-{uuid.uuid4().hex[:8]}"
    configuration = {
        "version": "0.8",
        "index_id": nom,
        "doc_mapping": {"mode": "dynamic"},
        "indexing_settings": {"commit_timeout_secs": 1},
    }
    reponse = httpx.post(f"{URL}/api/v1/indexes", json=configuration, timeout=30)
    assert reponse.status_code == 200, reponse.text
    return nom


def _envoyer_tous(sortie: QuickwitOutput, documents: list[dict]) -> list[str]:
    """Envoie les documents, vide le tampon, ferme ; rend la valeur de chaque résultat."""

    async def scenario() -> list[str]:
        try:
            resultats = [(await sortie.envoyer(doc)).value for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _chercher(index: str, attendus: int) -> list[dict]:
    """Relit l'index jusqu'à obtenir `attendus` documents (30 s au plus)."""
    hits: list[dict] = []
    for _ in range(60):
        reponse = httpx.get(f"{URL}/api/v1/{index}/search", params={"query": "*", "max_hits": 100})
        reponse.raise_for_status()
        hits = reponse.json()["hits"]
        if len(hits) >= attendus:
            break
        time.sleep(0.5)
    return hits


def test_health_rend_la_version() -> None:
    async def scenario() -> dict:
        sortie = QuickwitOutput({"hosts": URL})
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert "0.9" in str(asyncio.run(scenario()))


def test_lot_create_indexe_et_retrouvable() -> None:
    index = _creer_index()
    maintenant = datetime.now(timezone.utc).isoformat()
    documents = [{"message": f"m{n}", "@timestamp": maintenant, "n": n} for n in range(3)]
    sortie = QuickwitOutput({"hosts": URL, "index": index, "batch_size": 3, "refresh": "true"})
    assert _envoyer_tous(sortie, documents) == ["en_attente", "en_attente", "indexed"]
    hits = _chercher(index, 3)
    assert sorted(hit["message"] for hit in hits) == ["m0", "m1", "m2"]


def test_id_ignore_le_document_reste_indexe() -> None:
    index = _creer_index()
    sortie = QuickwitOutput({"hosts": URL, "index": index, "refresh": "true"})

    async def scenario() -> str:
        try:
            return (await sortie.envoyer({"message": "avec-id"}, document_id="abc")).value
        finally:
            await sortie.close()

    assert asyncio.run(scenario()) == "indexed"
    assert [hit["message"] for hit in _chercher(index, 1)] == ["avec-id"]


def test_index_inexistant_en_dlq(tmp_path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    sortie = QuickwitOutput(
        {"hosts": URL, "index": f"absent-{uuid.uuid4().hex[:8]}"}, dlq=LocalDeadLetterQueue(str(chemin))
    )
    assert _envoyer_tous(sortie, [{"message": "perdu"}]) == ["dead_lettered"]
    assert "perdu" in chemin.read_text(encoding="utf-8")
