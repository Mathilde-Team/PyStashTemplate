# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité du protocole HEC (issue #28) : SigLens en conteneur.

Splunk n'étant pas open source, le banc vise un système open source qui
parle HEC : SigLens (`POST /services/collector/event` sur le port
d'ingestion 8081, santé `/services/collector/health`). Exécuté par le
workflow `.github/workflows/conformite-splunk_hec.yml` (conteneur
`siglens/siglens`) ; localement :

    PYSTASHTEMPLATE_HEC_URL=http://localhost:8081 \\
    PYSTASHTEMPLATE_SIGLENS_QUERY_URL=http://localhost:5122 \\
        uv run pytest tests/test_integration_splunk_hec.py

Sans la variable d'environnement, le banc est ignoré. Chaque test écrit
dans son propre index Splunk (`index` de l'enveloppe, créé à
l'ingestion par SigLens), puis relit par l'API de recherche du port de
requête, avec une attente bornée. SigLens range l'enveloppe entière :
le document se retrouve sous `event` (champs aplatis `event.<nom>`).
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any

import httpx
import pytest

from pystashtemplate.outputs.splunk_hec import SplunkHecOutput

URL = os.environ.get("PYSTASHTEMPLATE_HEC_URL")
URL_RECHERCHE = os.environ.get("PYSTASHTEMPLATE_SIGLENS_QUERY_URL", "http://localhost:5122")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_HEC_URL non défini")


def _index() -> str:
    return f"banc-hec-{uuid.uuid4().hex[:8]}"


def _sortie(**options: Any) -> SplunkHecOutput:
    return SplunkHecOutput({"hosts": URL, "token": "jeton-banc", **options})


def _envoyer_tous(sortie: SplunkHecOutput, documents: list[dict]) -> list[str]:
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


def _valeur(record: dict, chemin: str) -> Any:
    """Valeur d'un champ, aplati (`event.message`) ou imbriqué."""
    if chemin in record:
        return record[chemin]
    valeur: Any = record
    for morceau in chemin.split("."):
        if not isinstance(valeur, dict):
            return None
        valeur = valeur.get(morceau)
    return valeur


def test_health_collecteur_disponible() -> None:
    async def scenario() -> dict:
        sortie = _sortie()
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert asyncio.run(scenario())["status"] == "healthy"


def test_lot_indexe_et_retrouvable() -> None:
    index = _index()
    sortie = _sortie(index=index, batch_size=3, sourcetype="syslog", source="banc", fields=["severity"])
    documents = [{"message": f"hec-{n}", "severity": "info", "host": "h1"} for n in range(3)]
    assert _envoyer_tous(sortie, documents) == ["en_attente", "en_attente", "indexed"]
    records = _chercher(index, 3)
    assert sorted(_valeur(r, "event.message") for r in records) == ["hec-0", "hec-1", "hec-2"]
    for record in records:
        assert _valeur(record, "sourcetype") == "syslog"
        assert _valeur(record, "source") == "banc"
        assert _valeur(record, "host") == "h1"
        assert _valeur(record, "fields.severity") == "info"


def test_caracteres_non_ascii_preserves() -> None:
    index = _index()
    _envoyer_tous(_sortie(index=index), [{"message": "connexion refusée — échec"}])
    records = _chercher(index, 1)
    assert [_valeur(r, "event.message") for r in records] == ["connexion refusée — échec"]
