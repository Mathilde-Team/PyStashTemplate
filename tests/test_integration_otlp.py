# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité OTLP (issue #22) : un vrai OpenTelemetry Collector, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-otlp.yml`
(conteneur `otel/opentelemetry-collector-contrib`, récepteur OTLP/HTTP,
exportateur `file`) ; localement :

    PYSTASHTEMPLATE_OTLP_URL=http://localhost:4318 \\
    PYSTASHTEMPLATE_OTLP_FICHIER=/tmp/otel/out/logs.json \\
        uv run pytest tests/test_integration_otlp.py

Sans la variable d'environnement, le banc est ignoré. Chaque test marque
ses journaux d'un identifiant unique, puis relit le fichier écrit par
l'exportateur `file` du Collector (JSON OTLP, une requête par ligne),
avec une attente bornée.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from pystashtemplate.outputs.otlp import OtlpOutput

URL = os.environ.get("PYSTASHTEMPLATE_OTLP_URL")
FICHIER = Path(os.environ.get("PYSTASHTEMPLATE_OTLP_FICHIER", "/tmp/otel/out/logs.json"))
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_OTLP_URL non défini")


def _envoyer(options: dict[str, Any], documents: list[dict[str, Any]]) -> list[str]:
    sortie = OtlpOutput({"hosts": URL, "batch_size": len(documents) + 1, **options})

    async def scenario() -> list[str]:
        try:
            for doc in documents:
                await sortie.envoyer(doc)
            return [r.value for r in await sortie._flush()]
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _relire(marque: str, attendus: int) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """`(attributs de ressource, LogRecord)` des journaux portant `marque` (30 s au plus)."""
    trouves: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for _ in range(60):
        trouves = []
        lignes = FICHIER.read_text(encoding="utf-8").splitlines() if FICHIER.exists() else []
        for ligne in lignes:
            for rl in json.loads(ligne).get("resourceLogs", []):
                ressource = {a["key"]: a["value"] for a in rl.get("resource", {}).get("attributes", [])}
                for sl in rl.get("scopeLogs", []):
                    for journal in sl.get("logRecords", []):
                        attributs = {a["key"]: a["value"] for a in journal.get("attributes", [])}
                        if attributs.get("marque") == {"stringValue": marque}:
                            trouves.append((ressource, journal))
        if len(trouves) >= attendus:
            break
        time.sleep(0.5)
    return trouves


def test_health_requete_vide_acceptee() -> None:
    async def scenario() -> dict:
        sortie = OtlpOutput({"hosts": URL})
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert asyncio.run(scenario()) == {"status": 200}


@pytest.mark.parametrize("options", [{"encoding": "protobuf"}, {"encoding": "json"}, {"compression": True}])
def test_lot_recu_et_decode_par_le_collector(options: dict[str, Any]) -> None:
    marque = uuid.uuid4().hex
    documents = [
        {
            "message": f"m{n}",
            "@timestamp": "2026-11-04T18:00:00.250Z",
            "severity": "warning",
            "host": f"h{n % 2}",
            "marque": marque,
            "n": n,
            "detail": {"liste": [1, 2.5, True]},
            "trace_id": "0123456789abcdef0123456789abcdef",
            "span_id": "0123456789abcdef",
        }
        for n in range(3)
    ]
    options = {"resource": {"service.name": "banc"}, "fields": {"host": "resource.host.name"}, **options}
    assert _envoyer(options, documents) == ["indexed"] * 3
    trouves = _relire(marque, 3)
    assert sorted(j["body"]["stringValue"] for _, j in trouves) == ["m0", "m1", "m2"]
    for ressource, journal in trouves:
        n = int(journal["body"]["stringValue"][1:])
        assert ressource == {"service.name": {"stringValue": "banc"}, "host.name": {"stringValue": f"h{n % 2}"}}
        assert journal["timeUnixNano"] == str(1793815200 * 10**9 + 250_000_000)
        assert (journal["severityNumber"], journal["severityText"]) == (13, "warning")
        assert journal["traceId"] == "0123456789abcdef0123456789abcdef"
        assert journal["spanId"] == "0123456789abcdef"
        attributs = {a["key"]: a["value"] for a in journal["attributes"]}
        assert attributs["n"] == {"intValue": str(n)}
        assert attributs["detail"] == {
            "kvlistValue": {
                "values": [
                    {
                        "key": "liste",
                        "value": {
                            "arrayValue": {"values": [{"intValue": "1"}, {"doubleValue": 2.5}, {"boolValue": True}]}
                        },
                    }
                ]
            }
        }
