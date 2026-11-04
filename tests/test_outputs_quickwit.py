# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie Quickwit (issue #15), avec un faux serveur `httpx.MockTransport`.

Chemin `_bulk` sous `/api/v1/_elastic`, action `create` imposée (refus
de toute autre action dès la construction), `_id` ignoré, paramètre
`refresh` (et délai allongé), accusé par élément rapporté sous la clé
`index` (relu sous l'action envoyée), santé (`/api/v1/version`). Les cas
communs (lot, erreur partielle, 429, coupure, arrêt) sont rejoués par le
banc de conformité (`test_conformite_sorties.py`).
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs import REGISTRY, compiler_sorties
from pystashtemplate.outputs.elasticsearch_bulk import ActionInvalideError, SendOutcome
from pystashtemplate.outputs.quickwit import TAILLE_MAX_REQUETE, TIMEOUT_REFRESH, QuickwitOutput


class FauxQuickwit:
    """Répond comme Quickwit 0.9.3 : `items[]`, chaque élément sous la clé `index`."""

    def __init__(self, statut: int = 200, elements: list[dict[str, Any]] | None = None, corps: Any = None) -> None:
        self.statut = statut
        self.elements = elements
        self.corps = corps
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/api/v1/version":
            return httpx.Response(200, json={"build": {"version": "0.9.3"}, "version": "0.9.3"})
        if self.corps is not None:
            return httpx.Response(self.statut, content=self.corps)
        nombre = len(request.content.decode("utf-8").splitlines()) // 2
        elements = self.elements or [{"status": 201, "error": None}] * nombre
        items = [{"index": {"_index": "logs", "_id": None, **element}} for element in elements]
        return httpx.Response(self.statut, json={"took": 1, "errors": False, "items": items})

    def lignes(self, i: int = 0) -> list[dict[str, Any]]:
        """Lignes NDJSON décodées de la i-ème requête."""
        return [json.loads(ligne) for ligne in self.requests[i].content.decode("utf-8").splitlines()]


def sortie(serveur: FauxQuickwit, dlq: LocalDeadLetterQueue | None = None, **options: Any) -> QuickwitOutput:
    """Sortie branchée sur le faux serveur."""
    return QuickwitOutput(
        {"hosts": "http://quickwit:7280", "index": "logs", **options},
        transport=httpx.MockTransport(serveur.handler),
        dlq=dlq,
    )


def envoyer(s: QuickwitOutput, **kwargs: Any) -> SendOutcome:
    """Envoie un document et ferme la sortie dans la même boucle."""

    async def scenario() -> SendOutcome:
        try:
            return await s.envoyer({"message": "m"}, **kwargs)
        finally:
            await s.close()

    return asyncio.run(scenario())


def test_inscrite_au_registre_sous_son_nom_de_plugin() -> None:
    assert REGISTRY["quickwit"] is QuickwitOutput


def test_capacites_create_seule_sans_id_accuse_par_element() -> None:
    capacites = QuickwitOutput.capacites
    assert capacites.actions == {"create"}
    assert not capacites.accepte_id
    assert capacites.accuse_par_evenement
    assert capacites.taille_max_requete == TAILLE_MAX_REQUETE == 10 * 1024 * 1024


def test_chemin_et_action_create_par_defaut() -> None:
    serveur = FauxQuickwit()
    assert envoyer(sortie(serveur)) is SendOutcome.INDEXED
    assert serveur.requests[0].url.path == "/api/v1/_elastic/_bulk"
    assert serveur.lignes() == [{"create": {"_index": "logs"}}, {"message": "m"}]
    assert "refresh" not in serveur.requests[0].url.params


def test_send_utilise_create_par_defaut() -> None:
    serveur = FauxQuickwit()
    s = sortie(serveur)

    async def scenario() -> None:
        await s.send([{"message": "a"}])
        await s.close()

    asyncio.run(scenario())
    assert next(iter(serveur.lignes()[0])) == "create"


def test_action_create_explicite_acceptee() -> None:
    assert sortie(FauxQuickwit(), action="create").options["action"] == "create"


@pytest.mark.parametrize("action", ["index", "update", "delete"])
def test_autre_action_refusee_a_la_construction(action: str) -> None:
    with pytest.raises(ValueError, match=f"action « {action} » non supportée par cette sortie"):
        sortie(FauxQuickwit(), action=action)


def test_autre_action_refusee_a_la_compilation_du_pipeline() -> None:
    pipeline = PipelineInterpreter('output { quickwit { hosts => "http://quickwit:7280" action => "index" } }')
    with pytest.raises(ValueError, match="supportées : create"):
        compiler_sorties(pipeline)


def test_autre_action_refusee_par_envoyer() -> None:
    with pytest.raises(ActionInvalideError):
        envoyer(sortie(FauxQuickwit()), action="index")


def test_id_ignore() -> None:
    serveur = FauxQuickwit()
    assert envoyer(sortie(serveur), document_id="abc") is SendOutcome.INDEXED
    assert serveur.lignes()[0] == {"create": {"_index": "logs"}}


@pytest.mark.parametrize("valeur", ["true", "wait_for", "false", True, "WAIT_FOR"])
def test_refresh_passe_en_parametre(valeur: Any) -> None:
    serveur = FauxQuickwit()
    envoyer(sortie(serveur, refresh=valeur))
    assert serveur.requests[0].url.params["refresh"] == str(valeur).lower()


def test_refresh_inconnu_refuse() -> None:
    with pytest.raises(ValueError, match="refresh maintenant inconnu"):
        sortie(FauxQuickwit(), refresh="maintenant")


def test_option_inconnue_refusee() -> None:
    with pytest.raises(ValueError, match="inconnue pour la sortie"):
        sortie(FauxQuickwit(), commit="auto")


def test_timeout_allonge_avec_refresh() -> None:
    assert sortie(FauxQuickwit(), refresh="true")._client_http().timeout.read == TIMEOUT_REFRESH
    assert sortie(FauxQuickwit(), refresh="wait_for", timeout=5)._client_http().timeout.read == 5
    assert sortie(FauxQuickwit())._client_http().timeout.read == 3.0


def test_element_en_erreur_sous_la_cle_index_en_dlq(tmp_path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    erreur = {"status": 404, "error": {"type": "index_not_found_exception", "reason": "no such index [logs]"}}
    serveur = FauxQuickwit(elements=[erreur])
    assert envoyer(sortie(serveur, dlq=LocalDeadLetterQueue(str(chemin)))) is SendOutcome.DEAD_LETTERED
    assert "index_not_found_exception" in chemin.read_text()


def test_elements_mixtes_par_document(tmp_path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    elements = [{"status": 201, "error": None}, {"status": 400, "error": {"type": "document_parsing_exception"}}]
    s = sortie(FauxQuickwit(elements=elements), dlq=LocalDeadLetterQueue(str(chemin)), batch_size=3)

    async def scenario() -> list[SendOutcome]:
        try:
            await s.envoyer({"message": "a"})
            await s.envoyer({"message": "b"})
            return await s._flush()
        finally:
            await s.close()

    assert asyncio.run(scenario()) == [SendOutcome.INDEXED, SendOutcome.DEAD_LETTERED]


@pytest.mark.parametrize("corps", [b"pas du json", b'{"num_docs_for_processing": 1}', b'{"items": []}'])
def test_reponse_non_normalisable_repli_client_commun(corps: bytes) -> None:
    assert envoyer(sortie(FauxQuickwit(corps=corps))) is SendOutcome.RETRYABLE


def test_statut_global_400_en_dlq(tmp_path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    serveur = FauxQuickwit(statut=400, corps=b'{"message": "requete invalide"}')
    assert envoyer(sortie(serveur, dlq=LocalDeadLetterQueue(str(chemin)))) is SendOutcome.DEAD_LETTERED
    assert json.loads(chemin.read_text())["@dlq_output"] == "quickwit"


def test_health_version() -> None:
    serveur = FauxQuickwit()
    s = sortie(serveur)

    async def scenario() -> dict[str, Any]:
        try:
            return await s.health()
        finally:
            await s.close()

    assert asyncio.run(scenario())["version"] == "0.9.3"
    assert serveur.requests[0].url.path == "/api/v1/version"
