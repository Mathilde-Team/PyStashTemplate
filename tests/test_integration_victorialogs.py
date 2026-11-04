# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité VictoriaLogs (issue #14) : la vraie ingestion, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-victorialogs.yml`
(conteneur `victoriametrics/victoria-logs`) ; localement :

    PYSTASHTEMPLATE_VICTORIALOGS_URL=http://localhost:9428 uv run pytest tests/test_integration_victorialogs.py

Sans la variable d'environnement, le banc est ignoré. Les documents
envoyés sont relus par l'API LogsQL (`/select/logsql/query`), avec une
attente bornée : VictoriaLogs rend les données cherchables en quelques
secondes au plus.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone

import httpx
import pytest

from pystashtemplate.outputs.victorialogs import VictoriaLogsOutput

URL = os.environ.get("PYSTASHTEMPLATE_VICTORIALOGS_URL")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_VICTORIALOGS_URL non défini")


def _envoyer_tous(sortie: VictoriaLogsOutput, documents: list[dict]) -> list[str]:
    """Envoie les documents, vide le tampon, ferme ; rend la valeur de chaque résultat."""

    async def scenario() -> list[str]:
        try:
            resultats = [(await sortie.envoyer(doc)).value for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _chercher(requete: str, attendus: int) -> list[dict]:
    """Relit par LogsQL jusqu'à obtenir `attendus` lignes (10 s au plus)."""
    lignes: list[dict] = []
    for _ in range(40):
        reponse = httpx.get(f"{URL}/select/logsql/query", params={"query": requete})
        reponse.raise_for_status()
        lignes = [json.loads(ligne) for ligne in reponse.text.splitlines() if ligne.strip()]
        if len(lignes) >= attendus:
            break
        time.sleep(0.25)
    return lignes


def _documents(marque: str, nombre: int) -> list[dict]:
    """Documents façon Logstash (`message`, `@timestamp`), marqués pour la recherche."""
    maintenant = datetime.now(timezone.utc).isoformat()
    return [
        {"message": f"{marque} n{n}", "@timestamp": maintenant, "host": "banc", "app": marque, "n": n}
        for n in range(nombre)
    ]


def test_health() -> None:
    async def scenario() -> dict:
        sortie = VictoriaLogsOutput({"hosts": URL})
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert asyncio.run(scenario()) == {"status": "OK"}


@pytest.mark.parametrize("mode", ["bulk", "jsonline"])
def test_documents_ingeres_sont_retrouvables(mode: str) -> None:
    marque = f"conformite{mode}{uuid.uuid4().hex[:8]}"
    sortie = VictoriaLogsOutput({"hosts": URL, "mode": mode, "batch_size": 3, "stream_fields": ["host", "app"]})
    resultats = _envoyer_tous(sortie, _documents(marque, 3))
    assert resultats == ["en_attente", "en_attente", "indexed"]
    lignes = _chercher(f"app:{marque}", 3)
    assert sorted(ligne["_msg"] for ligne in lignes) == [f"{marque} n{n}" for n in range(3)]
    # Champs de flux réellement pris en compte : le flux porte host et app.
    assert all(ligne["_stream"] == f'{{app="{marque}",host="banc"}}' for ligne in lignes)


def test_time_field_est_utilise() -> None:
    marque = f"conformitetemps{uuid.uuid4().hex[:8]}"
    document = {"message": marque, "@timestamp": "2026-01-02T03:04:05Z", "app": marque}
    assert _envoyer_tous(VictoriaLogsOutput({"hosts": URL}), [document]) == ["indexed"]
    lignes = _chercher(f"app:{marque} _time:2026-01-02", 1)
    assert lignes and lignes[0]["_time"].startswith("2026-01-02T03:04:05")
