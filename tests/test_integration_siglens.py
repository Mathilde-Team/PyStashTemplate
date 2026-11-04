# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité SigLens (issue #20) : la vraie API `_bulk`, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-siglens.yml`
(conteneur `siglens/siglens`) ; localement :

    PYSTASHTEMPLATE_SIGLENS_URL=http://localhost:8081 \\
    PYSTASHTEMPLATE_SIGLENS_QUERY_URL=http://localhost:5122 \\
        uv run pytest tests/test_integration_siglens.py

Sans la variable d'environnement, le banc est ignoré. Chaque test écrit
dans son propre index (créé à l'ingestion), puis relit par l'API de
recherche du port de requête, avec une attente bornée (SigLens rend les
documents cherchables après quelques secondes).
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs.siglens import SigLensOutput

URL = os.environ.get("PYSTASHTEMPLATE_SIGLENS_URL")
URL_RECHERCHE = os.environ.get("PYSTASHTEMPLATE_SIGLENS_QUERY_URL", "http://localhost:5122")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_SIGLENS_URL non défini")


def _index() -> str:
    return f"banc-{uuid.uuid4().hex[:8]}"


def _envoyer_tous(sortie: SigLensOutput, documents: list[dict], action: str = "index") -> list[str]:
    """Envoie les documents, vide le tampon, ferme ; rend la valeur de chaque résultat."""

    async def scenario() -> list[str]:
        try:
            resultats = [(await sortie.envoyer(doc, action=action)).value for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _chercher(index: str, attendus: int) -> list[dict]:
    """Relit l'index jusqu'à obtenir `attendus` documents (60 s au plus)."""
    requete = {
        "searchText": "*",
        "indexName": index,
        "startEpoch": "now-1h",
        "endEpoch": "now",
        "queryLanguage": "Splunk QL",
        "size": 100,
    }
    records: list[dict] = []
    for _ in range(120):
        reponse = httpx.post(f"{URL_RECHERCHE}/api/search", json=requete, timeout=10)
        reponse.raise_for_status()
        records = reponse.json().get("hits", {}).get("records") or []
        if len(records) >= attendus:
            break
        time.sleep(0.5)
    return records


def test_health_rend_la_fiche_elasticsearch() -> None:
    async def scenario() -> dict:
        sortie = SigLensOutput({"hosts": URL})
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert "version" in asyncio.run(scenario())


@pytest.mark.parametrize("action", ["index", "create"])
def test_lot_indexe_et_retrouvable(action: str) -> None:
    index = _index()
    documents = [{"message": f"m{n}", "n": n} for n in range(3)]
    sortie = SigLensOutput({"hosts": URL, "index": index, "batch_size": 3})
    assert _envoyer_tous(sortie, documents, action) == ["en_attente", "en_attente", "indexed"]
    assert sorted(record["message"] for record in _chercher(index, 3)) == ["m0", "m1", "m2"]


def test_id_ignore_les_deux_documents_gardes() -> None:
    index = _index()
    sortie = SigLensOutput({"hosts": URL, "index": index, "batch_size": 10})

    async def scenario() -> list[str]:
        try:
            await sortie.envoyer({"message": "premier"}, document_id="abc")
            await sortie.envoyer({"message": "second"}, document_id="abc")
            return [r.value for r in await sortie._flush()]
        finally:
            await sortie.close()

    assert asyncio.run(scenario()) == ["indexed", "indexed"]
    assert sorted(record["message"] for record in _chercher(index, 2)) == ["premier", "second"]


def test_ligne_illisible_seule_en_dlq(tmp_path) -> None:
    """Un document non sérialisable en JSON strict (NaN) : seul refusé, les autres indexés."""
    index = _index()
    chemin = tmp_path / "dlq.jsonl"
    sortie = SigLensOutput({"hosts": URL, "index": index, "batch_size": 10}, dlq=LocalDeadLetterQueue(str(chemin)))

    async def scenario() -> list[str]:
        try:
            await sortie.envoyer({"message": "bon"}, action="create")
            await sortie.envoyer({"message": "mauvais", "v": float("nan")}, action="create")
            await sortie.envoyer({"message": "bon2"}, action="create")
            return [r.value for r in await sortie._flush()]
        finally:
            await sortie.close()

    assert asyncio.run(scenario()) == ["indexed", "dead_lettered", "indexed"]
    assert "mauvais" in chemin.read_text(encoding="utf-8")
    assert sorted(record["message"] for record in _chercher(index, 2)) == ["bon", "bon2"]
