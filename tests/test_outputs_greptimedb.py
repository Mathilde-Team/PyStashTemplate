# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie GreptimeDB (issue #19), avec un faux serveur `httpx.MockTransport`.

Chemin `/v1/elasticsearch/_bulk`, insertions seules, éléments rapportés
sous `create` (relus sous l'action envoyée), lot atomique rejeté en 400
(renvoi un par un, raison du serveur en DLQ), santé `/health`. Les cas
communs sont rejoués par le banc de conformité.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs import REGISTRY, compiler_sorties
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.greptimedb import GreptimeDbOutput, raison_du_rejet


class FauxGreptime:
    """Modèle de GreptimeDB 1.3.0 : éléments sous `create`, lot rejeté en entier si une ligne est illisible."""

    def __init__(self, corps: Any = None, statut: int = 200) -> None:
        self.corps = corps
        self.statut = statut
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/health":
            return httpx.Response(200, json={})
        if self.corps is not None:
            return httpx.Response(self.statut, content=self.corps)
        donnees = request.content.decode("utf-8").splitlines()[1::2]
        try:
            for ligne in donnees:
                if json.loads(ligne).get("message") == "illisible":
                    raise ValueError(ligne)
        except ValueError:
            erreur = {"reason": "Failed to parse payload as json", "type": "illegal_argument_exception"}
            return httpx.Response(400, json={"errors": True, "items": [{"create": {"error": erreur, "status": 400}}]})
        items = [{"create": {"status": 201}} for _ in donnees]
        return httpx.Response(200, json={"errors": False, "items": items, "took": 1})

    def messages_par_requete(self) -> list[list[str]]:
        return [
            [json.loads(ligne)["message"] for ligne in r.content.decode("utf-8").splitlines()[1::2]]
            for r in self.requests
        ]


def sortie(serveur: FauxGreptime, dlq: LocalDeadLetterQueue | None = None, **options: Any) -> GreptimeDbOutput:
    return GreptimeDbOutput(
        {"hosts": "http://greptimedb:4000", "index": "logs", "retry_backoff": 0, "batch_size": 10, **options},
        dlq=dlq,
        transport=httpx.MockTransport(serveur.handler),
    )


def lot(s: GreptimeDbOutput, messages: list[str], **options: Any) -> list[SendOutcome]:
    async def scenario() -> list[SendOutcome]:
        try:
            for message in messages:
                await s.envoyer({"message": message}, **options)
            return await s._flush()
        finally:
            await s.close()

    return asyncio.run(scenario())


def _dlq(chemin: Path) -> list[dict[str, Any]]:
    if not chemin.exists():
        return []
    return [json.loads(ligne) for ligne in chemin.read_text(encoding="utf-8").splitlines()]


def test_inscrite_au_registre() -> None:
    assert REGISTRY["greptimedb"] is GreptimeDbOutput


def test_chemin_et_lot_indexe() -> None:
    serveur = FauxGreptime()
    assert lot(sortie(serveur), ["a", "b"]) == [SendOutcome.INDEXED] * 2
    assert serveur.requests[0].url.path == "/v1/elasticsearch/_bulk"


@pytest.mark.parametrize("action", ["update", "delete"])
def test_action_non_insertion_refusee_a_la_compilation(action: str) -> None:
    pipeline = PipelineInterpreter(f'output {{ greptimedb {{ hosts => "http://g:4000" action => "{action}" }} }}')
    with pytest.raises(ValueError, match=f"action « {action} » non supportée"):
        compiler_sorties(pipeline)


def test_create_acceptee() -> None:
    assert sortie(FauxGreptime(), action="create").options["action"] == "create"


def test_element_sous_create_relu_pour_index(tmp_path: Path) -> None:
    # Un `index` dont l'erreur est rapportée sous `create` : sans relecture,
    # le document serait compté indexé.
    erreur = {"create": {"status": 400, "error": {"type": "illegal_argument_exception", "reason": "type"}}}
    corps = json.dumps({"errors": True, "items": [{"create": {"status": 201}}, erreur]}).encode()
    chemin = tmp_path / "dlq.jsonl"
    s = sortie(FauxGreptime(corps=corps), dlq=LocalDeadLetterQueue(str(chemin)))
    assert lot(s, ["a", "b"]) == [SendOutcome.INDEXED, SendOutcome.DEAD_LETTERED]
    assert [e["document"]["message"] for e in _dlq(chemin)] == ["b"]


def test_lot_rejete_renvoi_un_par_un(tmp_path: Path) -> None:
    serveur = FauxGreptime()
    chemin = tmp_path / "dlq.jsonl"
    s = sortie(serveur, dlq=LocalDeadLetterQueue(str(chemin)))
    assert lot(s, ["a", "illisible", "c"]) == [SendOutcome.INDEXED, SendOutcome.DEAD_LETTERED, SendOutcome.INDEXED]
    assert serveur.messages_par_requete() == [["a", "illisible", "c"], ["a"], ["illisible"], ["c"]]
    entrees = _dlq(chemin)
    assert [e["document"]["message"] for e in entrees] == ["illisible"]
    assert entrees[0]["@dlq_error"] == "Failed to parse payload as json"


def test_document_seul_rejete_sans_raison_lisible(tmp_path: Path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    s = sortie(FauxGreptime(corps=b"pas du json", statut=400), dlq=LocalDeadLetterQueue(str(chemin)))
    assert lot(s, ["a"]) == [SendOutcome.DEAD_LETTERED]
    assert _dlq(chemin)[0]["@dlq_error"] == "statut 400"


def test_raison_du_rejet_structure_inattendue() -> None:
    assert raison_du_rejet(httpx.Response(400, json={"items": []})) == "statut 400"


@pytest.mark.parametrize("corps", [b"pas du json", b'{"took": 1}', b'{"items": []}'])
def test_reponse_non_normalisable_repli_client_commun(corps: bytes) -> None:
    assert lot(sortie(FauxGreptime(corps=corps)), ["a"]) == [SendOutcome.RETRYABLE]


def test_health() -> None:
    serveur = FauxGreptime()
    s = sortie(serveur)

    async def scenario() -> dict[str, Any]:
        try:
            return await s.health()
        finally:
            await s.close()

    assert asyncio.run(scenario()) == {}
    assert serveur.requests[0].url.path == "/health"
