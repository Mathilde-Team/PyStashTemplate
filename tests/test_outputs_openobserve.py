# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie OpenObserve (issue #16), avec un faux serveur `httpx.MockTransport`.

Chemin `/api/{organization}/_bulk`, authentification basic, actions,
santé, et surtout l'appariement prudent des `items[]` dont l'ordre n'est
pas garanti (amont #15716) : par contenu (`originalRecord`), repli par
position, sinon lot rendu à l'appelant. Les cas communs sont rejoués par
le banc de conformité (`test_conformite_sorties.py`).
"""

from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs import REGISTRY, compiler_sorties
from pystashtemplate.outputs.openobserve import OpenObserveOutput

ECHEC = {"status": 422, "error": {"type": "Too old data", "reason": "Too old data"}}


class FauxOpenObserve:
    """Répond avec le corps fourni par le test (ou un succès complet)."""

    def __init__(self, corps: dict[str, Any] | None = None, statut: int = 200) -> None:
        self.corps = corps
        self.statut = statut
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/healthz":
            return httpx.Response(200, json={"status": "ok"})
        if self.corps is not None:
            return httpx.Response(self.statut, json=self.corps)
        n = len(request.content.decode("utf-8").splitlines()) // 2
        return httpx.Response(200, json={"took": 0, "errors": False, "items": [{"index": {"status": 200}}] * n})


def sortie(serveur: FauxOpenObserve, dlq: LocalDeadLetterQueue | None = None, **options: Any) -> OpenObserveOutput:
    return OpenObserveOutput(
        {"hosts": "http://openobserve:5080", "index": "logs", **options},
        transport=httpx.MockTransport(serveur.handler),
        dlq=dlq,
    )


def lot(s: OpenObserveOutput, documents: list[dict[str, Any]]) -> list[str]:
    """Envoie les documents en un lot (batch_size = len) et ferme."""

    async def scenario() -> list[str]:
        try:
            return [(await s.envoyer(dict(d))).value for d in documents]
        finally:
            await s.close()

    return asyncio.run(scenario())


DOCS = [{"message": "a"}, {"message": "b"}, {"message": "c"}]


def _dlq(chemin: Path) -> list[str]:
    if not chemin.exists():
        return []
    return [json.loads(ligne)["document"]["message"] for ligne in chemin.read_text().splitlines()]


def test_inscrite_au_registre() -> None:
    assert REGISTRY["openobserve"] is OpenObserveOutput


def test_chemin_organisation_et_auth_basic() -> None:
    serveur = FauxOpenObserve()
    s = sortie(serveur, organization="mon org", user="u", password="p", batch_size=1)
    assert lot(s, [{"message": "a"}]) == ["indexed"]
    requete = serveur.requests[0]
    assert requete.url.raw_path == b"/api/mon%20org/_bulk"
    assert requete.headers["authorization"] == "Basic " + base64.b64encode(b"u:p").decode()
    assert json.loads(requete.content.decode().splitlines()[0]) == {"index": {"_index": "logs"}}


def test_organisation_par_defaut() -> None:
    serveur = FauxOpenObserve()
    lot(sortie(serveur, batch_size=1), [{"message": "a"}])
    assert serveur.requests[0].url.path == "/api/default/_bulk"


def test_organisation_vide_refusee() -> None:
    with pytest.raises(ValueError, match="organization"):
        sortie(FauxOpenObserve(), organization="  ")


@pytest.mark.parametrize("action", ["update", "delete"])
def test_update_delete_refusees_a_la_compilation(action: str) -> None:
    pipeline = PipelineInterpreter(f'output {{ openobserve {{ hosts => "http://o:5080" action => "{action}" }} }}')
    with pytest.raises(ValueError, match=f"action « {action} » non supportée"):
        compiler_sorties(pipeline)


def test_errors_false_tout_indexe_meme_sans_items() -> None:
    # Mode ZO_BULK_API_RESPONSE_ERRORS_ONLY : aucun élément réussi renvoyé.
    serveur = FauxOpenObserve({"took": 0, "errors": False, "items": []})
    assert lot(sortie(serveur, batch_size=3), DOCS) == ["en_attente", "en_attente", "indexed"]


def test_echec_apparie_par_contenu_malgre_l_ordre(tmp_path: Path) -> None:
    # L'échec de « b » arrive en premier : la position mentirait.
    corps = {
        "errors": True,
        "items": [
            {"index": {**ECHEC, "originalRecord": {"message": "b"}}},
            {"index": {"status": 200}},
            {"index": {"status": 200}},
        ],
    }
    chemin = tmp_path / "dlq.jsonl"
    s = sortie(FauxOpenObserve(corps), dlq=LocalDeadLetterQueue(str(chemin)), batch_size=3)
    lot(s, DOCS)
    assert _dlq(chemin) == ["b"]


def test_echecs_identiques_apparies_une_seule_fois(tmp_path: Path) -> None:
    documents = [{"message": "x"}, {"message": "x"}, {"message": "y"}]
    corps = {
        "errors": True,
        "items": [{"index": {**ECHEC, "originalRecord": {"message": "x"}}}, {"index": {"status": 200}}] * 1
        + [{"index": {**ECHEC, "originalRecord": {"message": "x"}}}],
    }
    chemin = tmp_path / "dlq.jsonl"
    lot(sortie(FauxOpenObserve(corps), dlq=LocalDeadLetterQueue(str(chemin)), batch_size=3), documents)
    assert _dlq(chemin) == ["x", "x"]


def test_echec_sans_contenu_attribue_par_position(tmp_path: Path) -> None:
    corps = {"errors": True, "items": [{"index": {"status": 200}}, {"index": {"status": 400}}, {"index": {}}]}
    chemin = tmp_path / "dlq.jsonl"
    lot(sortie(FauxOpenObserve(corps), dlq=LocalDeadLetterQueue(str(chemin)), batch_size=3), DOCS)
    assert _dlq(chemin) == ["b"]
    assert "statut 400" in chemin.read_text()


def test_echec_contenu_inconnu_et_nombre_different_lot_rendu(tmp_path: Path) -> None:
    corps = {"errors": True, "items": [{"index": {**ECHEC, "originalRecord": {"message": "z"}}}]}
    chemin = tmp_path / "dlq.jsonl"
    s = sortie(FauxOpenObserve(corps), dlq=LocalDeadLetterQueue(str(chemin)), batch_size=3)
    assert lot(s, DOCS)[-1] == "retryable"
    assert _dlq(chemin) == []


def test_position_deja_attribuee_lot_rendu(tmp_path: Path) -> None:
    # « a » apparié par contenu à la position 0 ; l'échec sans contenu
    # en position 0 ne peut plus lui être attribué.
    corps = {
        "errors": True,
        "items": [{"index": {"status": 400}}, {"index": {**ECHEC, "originalRecord": {"message": "a"}}}, {"index": {}}],
    }
    chemin = tmp_path / "dlq.jsonl"
    s = sortie(FauxOpenObserve(corps), dlq=LocalDeadLetterQueue(str(chemin)), batch_size=3)
    assert lot(s, DOCS)[-1] == "retryable"


def test_element_mal_forme_ignore() -> None:
    corps = {"errors": True, "items": ["inattendu", {"index": "pas un objet"}, {"index": {"status": 200}}]}
    assert lot(sortie(FauxOpenObserve(corps), batch_size=3), DOCS)[-1] == "indexed"


def test_reponse_sans_errors_regle_commune() -> None:
    serveur = FauxOpenObserve({"error": "trop de requêtes"}, statut=429)
    assert lot(sortie(serveur, batch_size=1, retry_max=0), [{"message": "a"}]) == ["retryable"]


def test_health() -> None:
    serveur = FauxOpenObserve()
    s = sortie(serveur)

    async def scenario() -> dict[str, Any]:
        try:
            return await s.health()
        finally:
            await s.close()

    assert asyncio.run(scenario()) == {"status": "ok"}
    assert serveur.requests[0].url.path == "/healthz"
