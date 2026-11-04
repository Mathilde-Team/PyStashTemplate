# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie Manticore (issue #17), avec un faux serveur `httpx.MockTransport`.

Doublons `create` sous leurs deux formes (HTTP 409 + élément 400 avant
29.9.0, élément 409 depuis), `create` annulés avant un doublon rendus à
l'appelant, et repli sur le client commun. Les cas communs sont rejoués
par le banc de conformité.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.manticore import ManticoreOutput, est_doublon

CREE = {"status": 201, "result": "created"}
DOUBLON_NOUVEAU = {"status": 409, "error": {"type": "version_conflict_engine_exception", "reason": "duplicate id '1'"}}
DOUBLON_ANCIEN = {"status": 400, "error": {"type": "mapper_parsing_exception", "reason": "duplicate id '1'"}}
MAPPING = {"status": 400, "error": {"type": "mapper_parsing_exception", "reason": "unknown column: 'y'"}}


class FauxManticore:
    """Répond par la liste `elements` (une entrée par ligne d'action), sous `statut`."""

    def __init__(self, elements: list[dict[str, Any]] | None = None, statut: int = 200, corps: Any = None) -> None:
        self.elements = elements
        self.statut = statut
        self.corps = corps
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.corps is not None:
            return httpx.Response(self.statut, content=self.corps)
        lignes = [json.loads(ligne) for ligne in request.content.decode().splitlines()]
        actions = [
            next(iter(ligne)) for ligne in lignes if next(iter(ligne)) in ("index", "create", "update", "delete")
        ]
        elements = self.elements or [CREE] * len(actions)
        items = [{action: element} for action, element in zip(actions, elements, strict=True)]
        return httpx.Response(self.statut, json={"items": items, "errors": True})


def sortie(serveur: FauxManticore, dlq: LocalDeadLetterQueue | None = None, **options: Any) -> ManticoreOutput:
    return ManticoreOutput(
        {"hosts": "http://manticore:9308", "index": "logs", "retry_backoff": 0, "batch_size": 100, **options},
        dlq=dlq,
        transport=httpx.MockTransport(serveur.handler),
    )


def lot(s: ManticoreOutput, envois: list[dict[str, Any]]) -> list[SendOutcome]:
    async def scenario() -> list[SendOutcome]:
        try:
            for options in envois:
                await s.envoyer({"message": "m"}, **options)
            return await s._flush()
        finally:
            await s.close()

    return asyncio.run(scenario())


def test_inscrite_au_registre() -> None:
    assert REGISTRY["manticore"] is ManticoreOutput


@pytest.mark.parametrize("element", [DOUBLON_NOUVEAU, DOUBLON_ANCIEN])
def test_est_doublon_deux_formes(element: dict[str, Any]) -> None:
    assert est_doublon({"create": element}, "create")


def test_est_doublon_erreur_texte() -> None:
    assert est_doublon({"create": {"status": 400, "error": "duplicate id '3'"}}, "create")


@pytest.mark.parametrize(
    ("item", "action"),
    [({"create": CREE}, "create"), ({"create": MAPPING}, "create"), ({"index": DOUBLON_NOUVEAU}, "index")],
)
def test_pas_doublon(item: dict[str, Any], action: str) -> None:
    assert not est_doublon(item, action)


def test_doublon_nouveau_compte_indexe() -> None:
    serveur = FauxManticore([DOUBLON_NOUVEAU])
    assert lot(sortie(serveur), [{"action": "create", "document_id": "1"}]) == [SendOutcome.INDEXED]
    assert len(serveur.requests) == 1


def test_doublon_ancien_http_409_compte_indexe(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(tmp_path / "dlq.jsonl")
    serveur = FauxManticore([DOUBLON_ANCIEN], statut=409)
    assert lot(sortie(serveur, dlq=dlq), [{"action": "create", "document_id": "1"}]) == [SendOutcome.INDEXED]
    assert not (tmp_path / "dlq.jsonl").exists()


def test_create_precedents_rendus_suivants_gardes() -> None:
    serveur = FauxManticore([CREE, CREE, DOUBLON_NOUVEAU, CREE])
    envois = [{"action": "create", "document_id": str(n)} for n in range(4)]
    assert lot(sortie(serveur), envois) == [
        SendOutcome.RETRYABLE,
        SendOutcome.RETRYABLE,
        SendOutcome.INDEXED,
        SendOutcome.INDEXED,
    ]


def test_suite_interrompue_par_autre_action_ou_index() -> None:
    serveur = FauxManticore([CREE, CREE, CREE, DOUBLON_NOUVEAU])
    envois = [
        {"action": "create", "document_id": "a"},
        {"action": "index", "document_id": "b"},
        {"action": "create", "document_id": "c", "index": "autre"},
        {"action": "create", "document_id": "d"},
    ]
    assert lot(sortie(serveur), envois) == [SendOutcome.INDEXED] * 4


def test_doublon_et_erreur_de_mapping_dans_le_meme_lot(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(tmp_path / "dlq.jsonl")
    serveur = FauxManticore([MAPPING, DOUBLON_NOUVEAU, CREE])
    envois = [{"action": "index"}, {"action": "create", "document_id": "1"}, {"action": "create", "document_id": "2"}]
    assert lot(sortie(serveur, dlq=dlq), envois) == [
        SendOutcome.DEAD_LETTERED,
        SendOutcome.INDEXED,
        SendOutcome.INDEXED,
    ]


def test_deux_doublons_consecutifs() -> None:
    serveur = FauxManticore([CREE, DOUBLON_NOUVEAU, DOUBLON_NOUVEAU])
    envois = [{"action": "create", "document_id": str(n)} for n in range(3)]
    assert lot(sortie(serveur), envois) == [SendOutcome.RETRYABLE, SendOutcome.INDEXED, SendOutcome.INDEXED]


def test_sans_doublon_client_commun(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(tmp_path / "dlq.jsonl")
    serveur = FauxManticore([CREE, MAPPING])
    assert lot(sortie(serveur, dlq=dlq), [{}, {}]) == [SendOutcome.INDEXED, SendOutcome.DEAD_LETTERED]


def test_reponse_illisible_repli_client_commun() -> None:
    serveur = FauxManticore(corps=b"pas du json")
    assert lot(sortie(serveur), [{}]) == [SendOutcome.RETRYABLE]


def test_items_en_nombre_inattendu_repli_client_commun() -> None:
    serveur = FauxManticore(corps=json.dumps({"items": []}).encode())
    assert lot(sortie(serveur), [{}]) == [SendOutcome.RETRYABLE]


def test_statut_permanent_repli_client_commun(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(tmp_path / "dlq.jsonl")
    serveur = FauxManticore(statut=400, corps=b'{"error": "action metadata"}')
    assert lot(sortie(serveur, dlq=dlq), [{}]) == [SendOutcome.DEAD_LETTERED]


def test_create_en_erreur_avant_doublon_garde_son_issue(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(tmp_path / "dlq.jsonl")
    serveur = FauxManticore([MAPPING, DOUBLON_NOUVEAU])
    envois = [{"action": "create", "document_id": "0"}, {"action": "create", "document_id": "1"}]
    assert lot(sortie(serveur, dlq=dlq), envois) == [SendOutcome.DEAD_LETTERED, SendOutcome.INDEXED]
