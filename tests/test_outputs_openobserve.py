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


class FauxOpenObserve112:
    """Modèle d'OpenObserve 1.1.2 : échecs d'horodatage en tête, `originalRecord` nul.

    Les messages de `refuses` échouent (422) ; les éléments en échec sont
    listés avant les réussis, sans contenu (le serveur l'a déjà consommé).
    """

    def __init__(self, refuses: set[str]) -> None:
        self.refuses = refuses
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        documents = [json.loads(ligne) for ligne in request.content.decode("utf-8").splitlines()[1::2]]
        echecs = [d for d in documents if d["message"] in self.refuses]
        erreur = {"type": "timestamp_parsing_failed", "reason": "Too old data"}
        items = [{"index": {"status": 422, "error": erreur, "originalRecord": None}} for _ in echecs]
        items += [{"index": {"status": 200, "result": "created"}} for _ in range(len(documents) - len(echecs))]
        return httpx.Response(200, json={"took": 0, "errors": bool(echecs), "items": items})

    def messages_par_requete(self) -> list[list[str]]:
        return [
            [json.loads(ligne)["message"] for ligne in r.content.decode("utf-8").splitlines()[1::2]]
            for r in self.requests
        ]


def test_echecs_sans_contenu_en_tete_renvoi_un_par_un(tmp_path: Path) -> None:
    serveur = FauxOpenObserve112({"b"})
    chemin = tmp_path / "dlq.jsonl"
    s = sortie(serveur, dlq=LocalDeadLetterQueue(str(chemin)), batch_size=3)
    assert lot(s, DOCS)[-1] == "indexed"
    assert serveur.messages_par_requete() == [["a", "b", "c"], ["a"], ["b"], ["c"]]
    assert _dlq(chemin) == ["b"]
    assert "timestamp_parsing_failed" in chemin.read_text()


def test_tous_en_echec_sans_contenu_attribues_sans_renvoi(tmp_path: Path) -> None:
    serveur = FauxOpenObserve112({"a", "b", "c"})
    chemin = tmp_path / "dlq.jsonl"
    lot(sortie(serveur, dlq=LocalDeadLetterQueue(str(chemin)), batch_size=3), DOCS)
    assert len(serveur.requests) == 1
    assert _dlq(chemin) == ["a", "b", "c"]


def test_echec_sans_contenu_apres_appariement_par_contenu(tmp_path: Path) -> None:
    # « a » apparié par contenu ; un échec sans contenu reste pour « b »
    # et « c » : ambigu, les deux sont renvoyés seuls.
    corps = {
        "errors": True,
        "items": [{"index": {"status": 400}}, {"index": {**ECHEC, "originalRecord": {"message": "a"}}}, {"index": {}}],
    }
    serveur = FauxOpenObserve(corps)
    chemin = tmp_path / "dlq.jsonl"
    lot(sortie(serveur, dlq=LocalDeadLetterQueue(str(chemin)), batch_size=3), DOCS)
    assert len(serveur.requests) == 3
    # Le faux serveur répond toujours le même corps : renvoyés seuls,
    # « b » et « c » reçoivent l'échec sans contenu qui leur revient.
    assert _dlq(chemin) == ["a", "b", "c"]


def test_contenu_inconnu_renvoi_un_par_un() -> None:
    corps = {"errors": True, "items": [{"index": {**ECHEC, "originalRecord": {"message": "z"}}}]}
    serveur = FauxOpenObserve(corps)
    # Renvoyé seul, chaque document reçoit l'échec (sans DLQ : rendu).
    assert lot(sortie(serveur, batch_size=3), DOCS)[-1] == "retryable"
    assert len(serveur.requests) == 4


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
