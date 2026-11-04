# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité Loki (issue #21) : la vraie ingestion, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-loki.yml`
(conteneur `grafana/loki`) ; localement :

    PYSTASHTEMPLATE_LOKI_URL=http://localhost:3100 uv run pytest tests/test_integration_loki.py

Sans la variable d'environnement, le banc est ignoré. Les documents
envoyés sont relus par l'API LogQL (`/loki/api/v1/query_range`),
avec une attente bornée.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from pystashtemplate.outputs.loki import LokiOutput

URL = os.environ.get("PYSTASHTEMPLATE_LOKI_URL")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_LOKI_URL non défini")


def _envoyer_tous(sortie: LokiOutput, documents: list[dict]) -> list[str]:
    """Envoie les documents, vide le tampon, ferme ; rend la valeur de chaque résultat."""

    async def scenario() -> list[str]:
        try:
            resultats = [(await sortie.envoyer(doc)).value for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _chercher(requete: str, attendus: int, debut_ns: int | None = None) -> list[dict]:
    """Relit par LogQL jusqu'à obtenir `attendus` flux (10 s au plus)."""
    resultats: list[dict] = []
    fin = time.time_ns()
    debut = debut_ns or fin - 60 * 60 * 1_000_000_000
    for _ in range(40):
        reponse = httpx.get(
            f"{URL}/loki/api/v1/query_range",
            params={"query": requete, "start": debut, "end": fin + 60_000_000_000, "direction": "forward"},
        )
        reponse.raise_for_status()
        corps = reponse.json()
        resultats = corps.get("data", {}).get("result", [])
        lignes = [ligne for flux in resultats for ligne in flux.get("values", [])]
        if len(lignes) >= attendus:
            return resultats
        time.sleep(0.25)
    return resultats


def _documents(marque: str, nombre: int) -> list[dict]:
    """Documents façon Logstash (`message`, `@timestamp`), marqués pour la recherche."""
    maintenant = datetime.now(timezone.utc).isoformat()
    return [
        {"message": f"{marque} n{n}", "@timestamp": maintenant, "host": "banc", "app": marque} for n in range(nombre)
    ]


def test_health() -> None:
    async def scenario() -> dict:
        sortie = LokiOutput({"hosts": URL})
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert asyncio.run(scenario()) == {"status": "ready"}


def test_documents_ingeres_sont_retrouvables_avec_leurs_labels() -> None:
    marque = f"conformite{uuid.uuid4().hex[:8]}"
    sortie = LokiOutput(
        {"hosts": URL, "batch_size": 3, "labels": {"job": "rsyslog", "host": "%{host}", "app": "%{app}"}}
    )
    resultats = _envoyer_tous(sortie, _documents(marque, 3))
    assert resultats == ["en_attente", "en_attente", "indexed"]
    resultats = _chercher(f'{{app="{marque}"}}', 3)
    lignes = [json.loads(ligne[1]) for flux in resultats for ligne in flux.get("values", [])]
    assert sorted(ligne["message"] for ligne in lignes) == [f"{marque} n{n}" for n in range(3)]
    # Le document entier part dans la ligne (JSON, sans perte).
    assert all(ligne["host"] == "banc" for ligne in lignes)
    # Labels du flux réellement pris en compte : le flux porte job et host.
    assert all(flux["stream"].get("job") == "rsyslog" for flux in resultats)
    assert all(flux["stream"].get("host") == "banc" for flux in resultats)


def test_horodatage_du_document_utilise() -> None:
    # Date passée mais dans la rétention par défaut du banc : un document
    # plus ancien serait écarté par la politique de rétention.
    marque = f"conformitedate{uuid.uuid4().hex[:8]}"
    date = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(microsecond=0)
    document = {"message": marque, "@timestamp": date.isoformat(), "app": marque}
    sortie = LokiOutput({"hosts": URL, "labels": {"app": "%{app}"}})
    assert _envoyer_tous(sortie, [document]) == ["indexed"]
    debut_ns = int(date.timestamp() * 1_000_000_000) - 60_000_000_000
    resultats = _chercher(f'{{app="{marque}"}}', 1, debut_ns=debut_ns)
    lignes = [ligne for flux in resultats for ligne in flux.get("values", [])]
    assert lignes, "le document daté n'a pas été retrouvé"
    # La ligne porte l'horodatage du document (à la seconde près).
    assert abs(int(lignes[0][0]) - int(date.timestamp() * 1_000_000_000)) < 2_000_000_000
