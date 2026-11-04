# SPDX-License-Identifier: GPL-3.0-or-later
"""Indexation OpenSearch (`pystashtemplate.indexer`, issue #110).

Rapatrié de RsysLogstach : contrat `SendOutcome` (trois issues), DLQ,
`_id` par gabarit ou par empreinte, index du jour et actions `bulk`.
Seul substitut : le réseau (`httpx.MockTransport` branché sur la sortie).
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.fingerprint import compute_fingerprint
from pystashtemplate.indexer import ACTIONS, ActionInvalideError, OpenSearchIndexer, SendOutcome


class FauxOpenSearch:
    """Double du serveur : "ok" enregistre, "4xx" refuse (400), "transient" échoue (500)."""

    def __init__(self) -> None:
        self.appels: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
        self.requetes: list[httpx.Request] = []
        self.mode = "ok"
        self.transport = httpx.MockTransport(self._handler)

    def _handler(self, request: httpx.Request) -> httpx.Response:
        self.requetes.append(request)
        if request.method == "GET":
            if self.mode == "transient":
                return httpx.Response(500)
            return httpx.Response(200, json={"version": {"number": "2.19.0-test"}})
        lignes = [json.loads(ligne) for ligne in request.content.decode().splitlines() if ligne]
        actions = []
        i = 0
        while i < len(lignes):
            ((action, meta),) = lignes[i].items()
            actions.append(action)
            doc = lignes[i + 1] if action != "delete" else {}
            self.appels.append((action, meta, doc))
            i += 1 if action == "delete" else 2
        if self.mode == "4xx":
            return httpx.Response(400, json={"error": {"type": "parse_exception"}})
        if self.mode == "transient":
            return httpx.Response(500)
        return httpx.Response(200, json={"items": [{a: {"status": 201}} for a in actions]})


def _indexer(faux: FauxOpenSearch, **kw: Any) -> OpenSearchIndexer:
    options: dict[str, Any] = {
        "hosts": [{"host": "127.0.0.1", "port": 9200}],
        "auth": None,
        "index_pattern": "logs-%Y.%m.%d",
        "use_ssl": True,
        "verify_certs": False,
    }
    options.update(kw)
    indexer = OpenSearchIndexer(**options)
    indexer.sortie._transport = faux.transport
    return indexer


def _envoyer(indexer: OpenSearchIndexer, doc: dict, **kw: Any) -> SendOutcome:
    async def scenario() -> SendOutcome:
        try:
            return await indexer.send(doc, **kw)
        finally:
            await indexer.close()

    return asyncio.run(scenario())


def _dlq(tmp_path: Path) -> LocalDeadLetterQueue:
    return LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))


def _lignes_dlq(tmp_path: Path) -> list[dict[str, Any]]:
    fichier = tmp_path / "dlq.jsonl"
    if not fichier.exists():
        return []
    return [json.loads(ligne) for ligne in fichier.read_text(encoding="utf-8").splitlines() if ligne]


def test_index_du_jour_et_id_aleatoire():
    faux = FauxOpenSearch()
    assert _envoyer(_indexer(faux), {"message": "a"}) is SendOutcome.INDEXED
    ((action, meta, doc),) = faux.appels
    assert action == "index"
    assert meta["_index"] == datetime.now(timezone.utc).strftime("logs-%Y.%m.%d")
    assert "_id" not in meta
    assert doc == {"message": "a"}
    assert str(faux.requetes[0].url).startswith("https://127.0.0.1:9200/")


def test_id_par_gabarit_et_gabarit_vide():
    faux = FauxOpenSearch()
    indexer = _indexer(faux, document_id_template="%{[event_id]}")
    assert indexer._document_id({"event_id": "e1"}) == "e1"
    assert indexer._document_id({}) is None
    assert _envoyer(indexer, {"event_id": "e1"}) is SendOutcome.INDEXED
    assert faux.appels[0][1]["_id"] == "e1"


def test_empreinte_prioritaire_sur_le_gabarit():
    faux = FauxOpenSearch()
    indexer = _indexer(faux, document_id_template="%{[event_id]}", document_id_fingerprint_fields=["message"])
    doc = {"event_id": "e1", "message": "m"}
    assert indexer._document_id(doc) == compute_fingerprint(doc, ["message"], "sha256")


def test_surcharges_ponctuelles_index_et_id():
    faux = FauxOpenSearch()
    indexer = _indexer(faux, document_id_template="%{[event_id]}")
    assert _envoyer(indexer, {"event_id": "e1"}, index="autre", document_id="force") is SendOutcome.INDEXED
    assert faux.appels[0][1]["_index"] == "autre"
    assert faux.appels[0][1]["_id"] == "force"


def test_echec_permanent_en_dlq(tmp_path):
    faux = FauxOpenSearch()
    faux.mode = "4xx"
    assert _envoyer(_indexer(faux, dlq=_dlq(tmp_path)), {"message": "a"}) is SendOutcome.DEAD_LETTERED
    assert [ligne["document"] for ligne in _lignes_dlq(tmp_path)] == [{"message": "a"}]


def test_echec_permanent_sans_dlq_rendu_a_l_appelant():
    faux = FauxOpenSearch()
    faux.mode = "4xx"
    assert _envoyer(_indexer(faux), {"message": "a"}) is SendOutcome.RETRYABLE


def test_echec_transitoire_rendu_a_l_appelant(tmp_path):
    faux = FauxOpenSearch()
    faux.mode = "transient"
    assert _envoyer(_indexer(faux, dlq=_dlq(tmp_path)), {"message": "a"}) is SendOutcome.RETRYABLE
    assert _lignes_dlq(tmp_path) == []


def test_actions_bulk(tmp_path):
    assert ACTIONS == ("index", "create", "update", "delete")
    faux = FauxOpenSearch()
    indexer = _indexer(faux)
    assert _envoyer(indexer, {"a": 1}, document_id="x", action="update", doc_as_upsert=True) is SendOutcome.INDEXED
    action, meta, doc = faux.appels[0]
    assert action == "update"
    assert meta["_id"] == "x"
    assert doc == {"doc": {"a": 1}, "doc_as_upsert": True}
    faux2 = FauxOpenSearch()
    assert _envoyer(_indexer(faux2), {}, document_id="y", action="delete", routing="r") is SendOutcome.INDEXED
    assert faux2.appels[0][:2] == ("delete", {"_index": faux2.appels[0][1]["_index"], "_id": "y", "routing": "r"})


@pytest.mark.parametrize("avec_dlq", [True, False])
def test_action_invalide(tmp_path, avec_dlq):
    faux = FauxOpenSearch()
    indexer = _indexer(faux, dlq=_dlq(tmp_path) if avec_dlq else None)
    attendu = SendOutcome.DEAD_LETTERED if avec_dlq else SendOutcome.RETRYABLE
    assert _envoyer(indexer, {"a": 1}, action="update") is attendu
    assert faux.appels == []
    assert len(_lignes_dlq(tmp_path)) == (1 if avec_dlq else 0)
    assert issubclass(ActionInvalideError, ValueError)


def test_plusieurs_noeuds_seul_le_premier():
    faux = FauxOpenSearch()
    indexer = _indexer(
        faux,
        hosts=[{"host": "a", "port": 1}, {"host": "b", "port": 2}],
        use_ssl=False,
        auth=("u", "p"),
    )
    assert _envoyer(indexer, {"m": 1}) is SendOutcome.INDEXED
    requete = faux.requetes[0]
    assert str(requete.url).startswith("http://a:1/")
    assert requete.headers["authorization"].startswith("Basic ")


def test_options_tls_transmises():
    indexer = OpenSearchIndexer(
        hosts=[{"host": "h", "port": 9200}],
        auth=None,
        index_pattern="x",
        use_ssl=True,
        verify_certs=True,
        ca_certs="/ca.pem",
        client_cert="/c.pem",
        client_key="/k.pem",
    )
    options = indexer.sortie.options
    assert (options["cacert"], options["ssl_certificate"], options["ssl_key"]) == ("/ca.pem", "/c.pem", "/k.pem")


def test_health_et_client():
    faux = FauxOpenSearch()
    indexer = _indexer(faux)

    async def scenario() -> tuple[dict[str, Any], Any]:
        try:
            return await indexer.health(), await indexer._client()
        finally:
            await indexer.close()

    sante, client = asyncio.run(scenario())
    assert sante["version"]["number"] == "2.19.0-test"
    assert client is not None
    faux.mode = "transient"
    with pytest.raises(Exception):  # noqa: B017 - toute erreur de la sortie remonte
        asyncio.run(_indexer(faux).health())
