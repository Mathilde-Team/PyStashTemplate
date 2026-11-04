# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité HTTP générique (issue #24) : la vraie ingestion, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-http.yml`
(conteneur Vector, source `http_server`) ; localement :

    PYSTASHTEMPLATE_HTTP_URL=http://localhost:8080/logs \\
    PYSTASHTEMPLATE_VECTOR_CONTAINER=vector \\
    uv run pytest tests/test_integration_http.py

Sans `PYSTASHTEMPLATE_HTTP_URL`, le banc est ignoré. Les documents
poussés sont relus dans les journaux du conteneur Vector (sink
`console`), avec une attente bornée.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
import uuid
from typing import Any

import pytest

from pystashtemplate.outputs.http_generique import HttpOutput

URL = os.environ.get("PYSTASHTEMPLATE_HTTP_URL")
CONTENEUR = os.environ.get("PYSTASHTEMPLATE_VECTOR_CONTAINER", "vector")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_HTTP_URL non défini")


def _journaux_vector() -> str:
    """Journaux du conteneur Vector (sink `console` : les événements reçus)."""
    rendu = subprocess.run(["docker", "logs", CONTENEUR], capture_output=True, text=True, check=True)
    return rendu.stdout


def _chercher(marque: str, attendus: int) -> list[dict[str, Any]]:
    """Relit les journaux jusqu'à obtenir `attendus` événements (10 s au plus)."""
    lignes: list[dict[str, Any]] = []
    for _ in range(40):
        rendu = _journaux_vector()
        lignes = [json.loads(ligne) for ligne in rendu.splitlines() if ligne.startswith("{") and marque in ligne]
        if len(lignes) >= attendus:
            return lignes
        time.sleep(0.25)
    return lignes


def _envoyer_tous(sortie: HttpOutput, documents: list[dict]) -> list[str]:
    """Envoie les documents, vide le tampon, ferme ; rend la valeur de chaque résultat."""

    async def scenario() -> list[str]:
        try:
            resultats = [(await sortie.envoyer(doc)).value for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _documents(marque: str, nombre: int) -> list[dict]:
    """Documents façon Logstash, marqués pour la recherche dans les journaux."""
    return [{"message": f"{marque} n{n}", "host": "banc", "app": marque} for n in range(nombre)]


def test_health() -> None:
    async def scenario() -> dict[str, Any]:
        sortie = HttpOutput({"url": URL})
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert asyncio.run(scenario()) == {"status": "joignable"}


def test_documents_ndjson_ingeres_par_vector() -> None:
    marque = f"conformite{uuid.uuid4().hex[:8]}"
    sortie = HttpOutput({"url": URL, "batch_size": 3})
    resultats = _envoyer_tous(sortie, _documents(marque, 3))
    assert resultats == ["en_attente", "en_attente", "indexed"]
    lignes = _chercher(marque, 3)
    # Vector rend chaque document avec son champ `message` intact.
    assert sorted(ligne.get("message") for ligne in lignes) == [f"{marque} n{n}" for n in range(3)]


def test_documents_json_batch_ingeres_par_vector() -> None:
    marque = f"conformitebatch{uuid.uuid4().hex[:8]}"
    sortie = HttpOutput({"url": URL, "format": "json_batch", "batch_size": 3})
    resultats = _envoyer_tous(sortie, _documents(marque, 3))
    assert resultats == ["en_attente", "en_attente", "indexed"]
    lignes = _chercher(marque, 3)
    assert len(lignes) == 3


def test_format_json_un_evenement_par_requete() -> None:
    marque = f"conformitejson{uuid.uuid4().hex[:8]}"
    sortie = HttpOutput({"url": URL, "format": "json"})
    resultats = _envoyer_tous(sortie, _documents(marque, 2))
    assert resultats == ["indexed"] * 2
    lignes = _chercher(marque, 2)
    assert len(lignes) == 2
