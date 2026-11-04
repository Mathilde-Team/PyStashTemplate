# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité ClickHouse (issue #23) : la vraie ingestion, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-clickhouse.yml`
(conteneur `clickhouse/clickhouse-server`) ; localement :

    PYSTASHTEMPLATE_CLICKHOUSE_URL=http://localhost:8123 uv run pytest tests/test_integration_clickhouse.py

Sans la variable d'environnement, le banc est ignoré. Les documents
envoyés par l'interface HTTP (`INSERT … FORMAT JSONEachRow`) sont relus
par `SELECT … FORMAT JSONEachRow`, avec une attente bornée.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from typing import Any

import httpx
import pytest

from pystashtemplate.outputs.clickhouse import ClickHouseOutput
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome

URL = os.environ.get("PYSTASHTEMPLATE_CLICKHOUSE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_CLICKHOUSE_URL non défini")


def _creer_table(base: str) -> None:
    """Base et table de test : colonnes `message`, `host`, `app` (String)."""
    for requete in (
        f"CREATE DATABASE IF NOT EXISTS {base}",
        (
            f"CREATE TABLE IF NOT EXISTS {base}.logs (message String, host String, app String) "
            "ENGINE = MergeTree ORDER BY tuple()"
        ),
    ):
        reponse = httpx.post(URL, params={"query": requete})
        reponse.raise_for_status()


def _selectionner(requete: str) -> list[dict[str, Any]]:
    """Lignes rendues par ClickHouse en JSON Each Row."""
    reponse = httpx.post(URL, params={"query": requete, "format": "JSONEachRow"})
    reponse.raise_for_status()
    return [json.loads(ligne) for ligne in reponse.text.splitlines() if ligne]


def _chercher(requete: str, attendus: int) -> list[dict[str, Any]]:
    """Relit jusqu'à obtenir `attendus` lignes (5 s au plus)."""
    for _ in range(20):
        lignes = _selectionner(requete)
        if len(lignes) >= attendus:
            return lignes
        time.sleep(0.25)
    return _selectionner(requete)


def _envoyer_tous(sortie: ClickHouseOutput, documents: list[dict]) -> list[str]:
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
    """Documents façon Logstash, marqués pour la recherche."""
    return [{"message": f"{marque} n{n}", "host": "banc", "app": marque} for n in range(nombre)]


def test_health() -> None:
    async def scenario() -> dict[str, Any]:
        sortie = ClickHouseOutput({"hosts": URL, "table": "logs"})
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert asyncio.run(scenario()) == {"status": "Ok."}


def test_documents_ingeres_et_retrouves_colonne_par_colonne() -> None:
    base = f"conformite{uuid.uuid4().hex[:8]}"
    _creer_table(base)
    sortie = ClickHouseOutput({"hosts": URL, "table": "logs", "database": base, "batch_size": 3})
    resultats = _envoyer_tous(sortie, _documents(base, 3))
    assert resultats == ["en_attente", "en_attente", "indexed"]
    lignes = _chercher(f"SELECT message, host, app FROM {base}.logs WHERE app = '{base}'", 3)
    # Correspondance champ → colonne : le document entier est revenu.
    assert sorted(ligne["message"] for ligne in lignes) == [f"{base} n{n}" for n in range(3)]
    assert all(ligne["host"] == "banc" for ligne in lignes)


def test_champ_sans_colonne_ignore_par_defaut() -> None:
    base = f"conformitechamp{uuid.uuid4().hex[:8]}"
    _creer_table(base)
    document = {"message": base, "host": "banc", "app": base, "champ_sans_colonne": "valeur"}
    assert _envoyer_tous(ClickHouseOutput({"hosts": URL, "table": "logs", "database": base}), [document]) == ["indexed"]
    lignes = _chercher(f"SELECT message FROM {base}.logs WHERE app = '{base}'", 1)
    assert [ligne["message"] for ligne in lignes] == [base]


def test_async_insert_visible_a_la_lecture() -> None:
    base = f"conformiteasync{uuid.uuid4().hex[:8]}"
    _creer_table(base)
    sortie = ClickHouseOutput({"hosts": URL, "table": "logs", "database": base, "async_insert": True})
    assert _envoyer_tous(sortie, _documents(base, 2)) == ["indexed"] * 2
    # Le serveur tamponne (flush rapide) : la relecture attend, bornée.
    lignes = _chercher(f"SELECT message FROM {base}.logs WHERE app = '{base}'", 2)
    assert len(lignes) == 2


def test_table_absente_lot_non_indexe() -> None:
    # Aucune table créée pour cette base : l'INSERT échoue, le lot
    # n'est jamais compté comme indexé (permanent en DLQ, ou rendu à
    # l'appelant selon le statut que ClickHouse rend).
    base = f"conformiteabsente{uuid.uuid4().hex[:8]}"
    resultats = _envoyer_tous(ClickHouseOutput({"hosts": URL, "table": "logs", "database": base}), _documents(base, 2))
    assert SendOutcome.INDEXED.value not in resultats
    lignes = _selectionner(f"SELECT count() AS n FROM system.databases WHERE name = '{base}'")
    assert lignes[0]["n"] == 0
