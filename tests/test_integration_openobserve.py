# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité OpenObserve (issue #16) : la vraie API `_bulk`, en conteneur.

Exécuté par `.github/workflows/conformite-openobserve.yml` (conteneur
`openobserve/openobserve`) ; localement :

    PYSTASHTEMPLATE_OPENOBSERVE_URL=http://localhost:5080 \\
    PYSTASHTEMPLATE_OPENOBSERVE_USER=root@example.com \\
    PYSTASHTEMPLATE_OPENOBSERVE_PASSWORD=... \\
    uv run pytest tests/test_integration_openobserve.py

Sans la variable d'URL, le banc est ignoré. Un document trop ancien
(au-delà de `ZO_INGEST_ALLOWED_UPTO`, 5 h par défaut) est refusé par
OpenObserve 1.1.2 en tête des éléments, sans `originalRecord` : la
sortie renvoie alors chaque document seul ; le document refusé part seul
en DLQ, les autres sont indexés (en double : premier envoi et renvoi).
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs.openobserve import OpenObserveOutput

URL = os.environ.get("PYSTASHTEMPLATE_OPENOBSERVE_URL")
USER = os.environ.get("PYSTASHTEMPLATE_OPENOBSERVE_USER", "")
PASSWORD = os.environ.get("PYSTASHTEMPLATE_OPENOBSERVE_PASSWORD", "")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_OPENOBSERVE_URL non défini")


def _options(stream: str, **autres: object) -> dict:
    return {"hosts": URL, "index": stream, "user": USER, "password": PASSWORD, **autres}


def _envoyer_tous(sortie: OpenObserveOutput, documents: list[dict]) -> list[str]:
    async def scenario() -> list[str]:
        try:
            resultats = [(await sortie.envoyer(doc)).value for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _chercher(stream: str, attendus: int) -> list[dict]:
    """Relit le stream par l'API de recherche SQL (30 s au plus)."""
    fin = datetime.now(timezone.utc) + timedelta(minutes=5)
    debut = fin - timedelta(hours=6)
    requete = {
        "query": {
            "sql": f'SELECT * FROM "{stream}"',
            "start_time": int(debut.timestamp() * 1_000_000),
            "end_time": int(fin.timestamp() * 1_000_000),
            "from": 0,
            "size": 100,
        }
    }
    hits: list[dict] = []
    for _ in range(60):
        reponse = httpx.post(f"{URL}/api/default/_search", json=requete, auth=(USER, PASSWORD), timeout=30)
        if reponse.status_code == 200:
            hits = reponse.json().get("hits", [])
            if len(hits) >= attendus:
                break
        time.sleep(0.5)
    return hits


def test_health() -> None:
    async def scenario() -> dict:
        sortie = OpenObserveOutput(_options("sante"))
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert asyncio.run(scenario())["status"] == "ok"


def test_lot_indexe_et_retrouvable() -> None:
    stream = f"banc_{uuid.uuid4().hex[:8]}"
    documents = [{"message": f"m{n}", "n": n} for n in range(3)]
    sortie = OpenObserveOutput(_options(stream, batch_size=3))
    assert _envoyer_tous(sortie, documents) == ["en_attente", "en_attente", "indexed"]
    assert sorted(hit["message"] for hit in _chercher(stream, 3)) == ["m0", "m1", "m2"]


def test_document_trop_ancien_seul_en_dlq(tmp_path) -> None:
    stream = f"banc_{uuid.uuid4().hex[:8]}"
    ancien = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    documents = [{"message": "recent-1"}, {"message": "ancien", "_timestamp": ancien}, {"message": "recent-2"}]
    chemin = tmp_path / "dlq.jsonl"
    sortie = OpenObserveOutput(_options(stream, batch_size=3), dlq=LocalDeadLetterQueue(str(chemin)))
    _envoyer_tous(sortie, documents)
    contenu = chemin.read_text(encoding="utf-8")
    assert "ancien" in contenu
    assert "recent" not in contenu
    # Indexés au premier envoi puis au renvoi un par un (doublon assumé).
    assert sorted(hit["message"] for hit in _chercher(stream, 4)) == ["recent-1", "recent-1", "recent-2", "recent-2"]
