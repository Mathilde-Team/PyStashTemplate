# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité ZincSearch (issue #18) : la vraie API `_bulk`, en conteneur.

Exécuté par `.github/workflows/conformite-zincsearch.yml` (conteneur
`public.ecr.aws/zinclabs/zincsearch`) ; localement :

    PYSTASHTEMPLATE_ZINCSEARCH_URL=http://localhost:4080 \\
    PYSTASHTEMPLATE_ZINCSEARCH_USER=admin PYSTASHTEMPLATE_ZINCSEARCH_PASSWORD=... \\
    uv run pytest tests/test_integration_zincsearch.py

Sans la variable d'URL, le banc est ignoré. Chaque test écrit dans son
propre index (créé à l'ingestion par ZincSearch), puis relit par l'API
de recherche, avec une attente bornée.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid

import httpx
import pytest

from pystashtemplate.outputs.zincsearch import ZincSearchOutput

URL = os.environ.get("PYSTASHTEMPLATE_ZINCSEARCH_URL")
USER = os.environ.get("PYSTASHTEMPLATE_ZINCSEARCH_USER", "")
PASSWORD = os.environ.get("PYSTASHTEMPLATE_ZINCSEARCH_PASSWORD", "")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_ZINCSEARCH_URL non défini")


def _sortie(index: str, **autres: object) -> ZincSearchOutput:
    return ZincSearchOutput({"hosts": URL, "index": index, "user": USER, "password": PASSWORD, **autres})


def _executer(sortie: ZincSearchOutput, envois: list[tuple[dict, dict]]) -> list[str]:
    """Envoie chaque (document, options d'envoi), vide le tampon, ferme."""

    async def scenario() -> list[str]:
        try:
            resultats = [(await sortie.envoyer(doc, **options)).value for doc, options in envois]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _chercher(index: str, attendus: int) -> list[dict]:
    """Documents de l'index (`_source`), jusqu'à en obtenir `attendus` (30 s au plus)."""
    sources: list[dict] = []
    for _ in range(60):
        reponse = httpx.post(
            f"{URL}/api/{index}/_search",
            json={"search_type": "matchall", "max_results": 100},
            auth=(USER, PASSWORD),
            timeout=30,
        )
        if reponse.status_code == 200:
            sources = [hit["_source"] for hit in reponse.json()["hits"]["hits"]]
            if len(sources) == attendus:
                break
        time.sleep(0.5)
    return sources


def test_health_version() -> None:
    async def scenario() -> dict:
        sortie = _sortie("sante")
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert "version" in asyncio.run(scenario())


def test_lot_indexe_et_retrouvable() -> None:
    index = f"banc{uuid.uuid4().hex[:8]}"
    envois = [({"message": f"m{n}"}, {}) for n in range(3)]
    assert _executer(_sortie(index, batch_size=3), envois) == ["en_attente", "en_attente", "indexed"]
    assert sorted(doc["message"] for doc in _chercher(index, 3)) == ["m0", "m1", "m2"]


def test_id_remplace_puis_delete_supprime() -> None:
    index = f"banc{uuid.uuid4().hex[:8]}"
    envois = [
        ({"message": "v1"}, {"document_id": "a"}),
        ({"message": "v2"}, {"document_id": "a"}),
        ({"message": "garde"}, {"document_id": "b"}),
    ]
    _executer(_sortie(index, batch_size=3), envois)
    assert sorted(doc["message"] for doc in _chercher(index, 2)) == ["garde", "v2"]
    _executer(_sortie(index), [({}, {"action": "delete", "document_id": "b"})])
    assert [doc["message"] for doc in _chercher(index, 1)] == ["v2"]
