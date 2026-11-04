# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité Manticore (issue #17) : la vraie API `/_bulk`, en conteneur.

Exécuté par `.github/workflows/conformite-manticore.yml` (conteneur
`manticoresearch/manticore`) ; localement :

    PYSTASHTEMPLATE_MANTICORE_URL=http://localhost:9308 \\
    uv run pytest tests/test_integration_manticore.py

Sans la variable, le banc est ignoré. Chaque test crée sa propre table
(`/sql?mode=raw`), écrit par la sortie, puis relit par SQL.
"""

from __future__ import annotations

import asyncio
import os
import uuid

import httpx
import pytest

from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.manticore import ManticoreOutput

URL = os.environ.get("PYSTASHTEMPLATE_MANTICORE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_MANTICORE_URL non défini")


def _sql(requete: str) -> list[dict]:
    reponse = httpx.post(f"{URL}/sql", params={"mode": "raw"}, data={"query": requete}, timeout=30)
    reponse.raise_for_status()
    resultat = reponse.json()[0]
    assert not resultat.get("error"), resultat
    return resultat.get("data", [])


def _table() -> str:
    nom = f"banc{uuid.uuid4().hex[:8]}"
    _sql(f"CREATE TABLE {nom}(message text)")
    return nom


def _ids(table: str) -> list[int]:
    return sorted(ligne["id"] for ligne in _sql(f"SELECT id FROM {table} LIMIT 100"))


def _lot(table: str, envois: list[tuple[str, str, str]]) -> list[SendOutcome]:
    """Envoie (action, _id, message) en un seul `_bulk` et renvoie les issues."""

    async def scenario() -> list[SendOutcome]:
        sortie = ManticoreOutput({"hosts": URL, "index": table, "batch_size": 1000})
        try:
            for action, document_id, message in envois:
                await sortie.envoyer({"message": message}, action=action, document_id=document_id)
            return await sortie._flush()
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def test_health() -> None:
    async def scenario() -> dict:
        sortie = ManticoreOutput({"hosts": URL})
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert "version" in asyncio.run(scenario())


def test_lot_indexe_et_relu() -> None:
    table = _table()
    issues = _lot(table, [("index", str(n), f"m{n}") for n in (1, 2, 3)])
    assert issues == [SendOutcome.INDEXED] * 3
    assert _ids(table) == [1, 2, 3]


def test_doublon_create_compte_indexe_et_renvoi_converge() -> None:
    table = _table()
    assert _lot(table, [("create", "7", "original")]) == [SendOutcome.INDEXED]
    envois = [("create", "1", "a"), ("create", "2", "b"), ("create", "7", "doublon"), ("create", "3", "c")]
    issues = _lot(table, envois)
    assert issues[2:] == [SendOutcome.INDEXED, SendOutcome.INDEXED]
    # Les `create` rendus (perdus par Manticore, ou non) sont renvoyés,
    # comme le ferait l'appelant : le renvoi converge sans perte.
    a_renvoyer = [envoi for envoi, issue in zip(envois, issues, strict=True) if issue is SendOutcome.RETRYABLE]
    if a_renvoyer:
        assert set(_lot(table, a_renvoyer)) == {SendOutcome.INDEXED}
    assert _ids(table) == [1, 2, 3, 7]
    assert _sql(f"SELECT message FROM {table} WHERE id = 7")[0]["message"] == "original"
