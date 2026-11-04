# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie ZincSearch (issue #18), avec un faux serveur `httpx.MockTransport`.

Chemin `/api/_bulk`, authentification basic, `_id` transmis, `update`
refusée, statut HTTP global (y compris le 500 d'arrêt de lot), santé
(`/version`). Les cas communs sont rejoués par le banc de conformité.
"""

from __future__ import annotations

import asyncio
import base64
import json
from typing import Any

import httpx
import pytest

from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs import REGISTRY, compiler_sorties
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.zincsearch import ZincSearchOutput


class FauxZinc:
    """Répond comme ZincSearch : `record_count`, ou 500 `{"error": …}`."""

    def __init__(self, statut: int = 200) -> None:
        self.statut = statut
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/version":
            return httpx.Response(200, json={"version": "v0.4.10"})
        if self.statut != 200:
            return httpx.Response(self.statut, json={"error": "bulk index data format error"})
        return httpx.Response(200, json={"message": "bulk data inserted", "record_count": 1})

    def lignes(self) -> list[dict[str, Any]]:
        return [json.loads(ligne) for ligne in self.requests[0].content.decode().splitlines()]


def sortie(serveur: FauxZinc, **options: Any) -> ZincSearchOutput:
    return ZincSearchOutput(
        {"hosts": "http://zinc:4080", "index": "logs", "retry_backoff": 0, **options},
        transport=httpx.MockTransport(serveur.handler),
    )


def envoyer(s: ZincSearchOutput, doc: dict[str, Any] | None = None, **kwargs: Any) -> SendOutcome:
    async def scenario() -> SendOutcome:
        try:
            return await s.envoyer(doc if doc is not None else {"message": "m"}, **kwargs)
        finally:
            await s.close()

    return asyncio.run(scenario())


def test_inscrite_au_registre() -> None:
    assert REGISTRY["zincsearch"] is ZincSearchOutput


def test_chemin_auth_basic_et_id() -> None:
    serveur = FauxZinc()
    s = sortie(serveur, user="admin", password="secret")
    assert envoyer(s, document_id="abc") is SendOutcome.INDEXED
    requete = serveur.requests[0]
    assert requete.url.path == "/api/_bulk"
    assert requete.headers["authorization"] == "Basic " + base64.b64encode(b"admin:secret").decode()
    assert serveur.lignes() == [{"index": {"_index": "logs", "_id": "abc"}}, {"message": "m"}]


def test_delete_avec_id() -> None:
    serveur = FauxZinc()
    assert envoyer(sortie(serveur), action="delete", document_id="abc") is SendOutcome.INDEXED
    assert serveur.lignes() == [{"delete": {"_index": "logs", "_id": "abc"}}]


def test_update_refusee_a_la_compilation() -> None:
    pipeline = PipelineInterpreter('output { zincsearch { hosts => "http://zinc:4080" action => "update" } }')
    with pytest.raises(ValueError, match="action « update » non supportée"):
        compiler_sorties(pipeline)


def test_create_explicite_acceptee() -> None:
    assert sortie(FauxZinc(), action="create").options["action"] == "create"


def test_erreur_500_retentee_puis_rendue() -> None:
    serveur = FauxZinc(statut=500)
    assert envoyer(sortie(serveur, retry_max=1)) is SendOutcome.RETRYABLE
    assert len(serveur.requests) == 2


def test_health_version() -> None:
    serveur = FauxZinc()
    s = sortie(serveur)

    async def scenario() -> dict[str, Any]:
        try:
            return await s.health()
        finally:
            await s.close()

    assert asyncio.run(scenario()) == {"version": "v0.4.10"}
    assert serveur.requests[0].url.path == "/version"
