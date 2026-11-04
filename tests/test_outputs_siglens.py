# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie SigLens (issue #20), avec un faux serveur `httpx.MockTransport`.

Chemin `_bulk` sous `/elastic`, actions `index`/`create` seules (refus
de `update`/`delete` dès la construction), `_id` ignoré, accusé par
élément rapporté sous la clé `index` avec le `status` d'un élément en
échec hors de l'objet de l'action (relu sous l'action envoyée), santé
(`/elastic/`). Les cas communs (lot, erreur partielle, 429, coupure,
arrêt) sont rejoués par le banc de conformité (`test_conformite_sorties.py`).
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
from pystashtemplate.outputs.siglens import SigLensOutput

# Élément en échec tel que rapporté par SigLens 1.0.57 (ligne illisible).
ECHEC = {"index": {"error": {"reason": "indexing request failed", "type": "mapper_parse_exception"}}, "status": 400}


class FauxSigLens:
    """Répond comme SigLens 1.0.57 : `items[]` sous la clé `index`, statut d'échec hors de l'élément."""

    def __init__(self, statut: int = 200, items: list[dict[str, Any]] | None = None, corps: Any = None) -> None:
        self.statut = statut
        self.items = items
        self.corps = corps
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/elastic/":
            return httpx.Response(200, json={"name": "siglens", "version": {"number": "7.9.3"}})
        if self.corps is not None:
            return httpx.Response(self.statut, content=self.corps)
        nombre = len(request.content.decode("utf-8").splitlines()) // 2
        items = self.items or [{"index": {"status": 201}}] * nombre
        erreurs = any("error" in next(iter(item.values())) for item in items)
        return httpx.Response(self.statut, json={"errors": erreurs, "items": items, "took": 1})

    def lignes(self, i: int = 0) -> list[dict[str, Any]]:
        """Lignes NDJSON décodées de la i-ème requête."""
        return [json.loads(ligne) for ligne in self.requests[i].content.decode("utf-8").splitlines()]


def sortie(serveur: FauxSigLens, dlq: LocalDeadLetterQueue | None = None, **options: Any) -> SigLensOutput:
    """Sortie branchée sur le faux serveur."""
    return SigLensOutput(
        {"hosts": "http://siglens:8081", "index": "logs", **options},
        transport=httpx.MockTransport(serveur.handler),
        dlq=dlq,
    )


def envoyer(s: SigLensOutput, **kwargs: Any) -> SendOutcome:
    """Envoie un document et ferme la sortie dans la même boucle."""

    async def scenario() -> SendOutcome:
        try:
            return await s.envoyer({"message": "m"}, **kwargs)
        finally:
            await s.close()

    return asyncio.run(scenario())


def lot(s: SigLensOutput, documents: int, action: str = "index") -> list[SendOutcome]:
    """Met `documents` documents en tampon puis vide le tampon."""

    async def scenario() -> list[SendOutcome]:
        try:
            for n in range(documents):
                await s.envoyer({"message": f"m{n}"}, action=action)
            return await s._flush()
        finally:
            await s.close()

    return asyncio.run(scenario())


def test_inscrite_au_registre_sous_son_nom_de_plugin() -> None:
    assert REGISTRY["siglens"] is SigLensOutput


def test_capacites_index_create_sans_id_accuse_par_element() -> None:
    capacites = SigLensOutput.capacites
    assert capacites.actions == {"index", "create"}
    assert not capacites.accepte_id
    assert capacites.accuse_par_evenement
    assert capacites.taille_max_requete is None


def test_chemin_et_action_index_par_defaut() -> None:
    serveur = FauxSigLens()
    assert envoyer(sortie(serveur)) is SendOutcome.INDEXED
    assert serveur.requests[0].url.path == "/elastic/_bulk"
    assert serveur.lignes() == [{"index": {"_index": "logs"}}, {"message": "m"}]


def test_create_accepte_et_relu_sous_create() -> None:
    serveur = FauxSigLens()
    assert envoyer(sortie(serveur), action="create") is SendOutcome.INDEXED
    assert next(iter(serveur.lignes()[0])) == "create"


@pytest.mark.parametrize("action", ["update", "delete"])
def test_update_delete_refusees_a_la_construction(action: str) -> None:
    with pytest.raises(ValueError, match=f"action « {action} » non supportée par cette sortie"):
        sortie(FauxSigLens(), action=action)


def test_delete_refusee_a_la_compilation_du_pipeline() -> None:
    pipeline = PipelineInterpreter('output { siglens { hosts => "http://siglens:8081" action => "delete" } }')
    with pytest.raises(ValueError, match="supportées : create, index"):
        compiler_sorties(pipeline)


def test_update_refusee_par_envoyer() -> None:
    with pytest.raises(ActionInvalideError):
        envoyer(sortie(FauxSigLens()), action="update")


def test_id_ignore() -> None:
    serveur = FauxSigLens()
    assert envoyer(sortie(serveur), document_id="abc") is SendOutcome.INDEXED
    assert serveur.lignes()[0] == {"index": {"_index": "logs"}}


def test_option_inconnue_refusee() -> None:
    with pytest.raises(ValueError, match="inconnue pour la sortie"):
        sortie(FauxSigLens(), query_port=5122)


@pytest.mark.parametrize("action", ["index", "create"])
def test_element_en_echec_statut_hors_element_en_dlq(tmp_path, action: str) -> None:
    chemin = tmp_path / "dlq.jsonl"
    serveur = FauxSigLens(items=[ECHEC])
    s = sortie(serveur, dlq=LocalDeadLetterQueue(str(chemin)))
    assert envoyer(s, action=action) is SendOutcome.DEAD_LETTERED
    assert "mapper_parse_exception" in chemin.read_text()


def test_elements_mixtes_par_document(tmp_path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    serveur = FauxSigLens(items=[{"index": {"status": 201}}, ECHEC, {"index": {"status": 201}}])
    s = sortie(serveur, dlq=LocalDeadLetterQueue(str(chemin)), batch_size=10)
    assert lot(s, 3, action="create") == [SendOutcome.INDEXED, SendOutcome.DEAD_LETTERED, SendOutcome.INDEXED]
    assert '"m1"' in chemin.read_text() and '"m0"' not in chemin.read_text()


@pytest.mark.parametrize("corps", [b"pas du json", b'{"took": 1}', b'{"items": []}'])
def test_reponse_non_relue_repli_client_commun(corps: bytes) -> None:
    assert envoyer(sortie(FauxSigLens(corps=corps))) is SendOutcome.RETRYABLE


def test_lot_rejete_en_400_en_dlq(tmp_path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    corps = b'{"index":{"error":{"reason":"request body is required"}},"status":400}'
    serveur = FauxSigLens(statut=400, corps=corps)
    assert envoyer(sortie(serveur, dlq=LocalDeadLetterQueue(str(chemin)))) is SendOutcome.DEAD_LETTERED
    assert json.loads(chemin.read_text())["@dlq_output"] == "siglens"


def test_health_fiche_elasticsearch() -> None:
    serveur = FauxSigLens()
    s = sortie(serveur)

    async def scenario() -> dict[str, Any]:
        try:
            return await s.health()
        finally:
            await s.close()

    assert asyncio.run(scenario())["version"]["number"] == "7.9.3"
    assert serveur.requests[0].url.path == "/elastic/"
