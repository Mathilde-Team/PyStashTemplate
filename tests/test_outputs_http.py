# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie HTTP générique (issue #24), faux serveur `httpx.MockTransport`.

URL libre découpée (hôte pour le client, chemin pour la requête),
formats `ndjson`/`json_batch`/`json`, verbe `post`/`put`, décision sur
le statut HTTP global, `url` requise.
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
from pystashtemplate.outputs.http_generique import HttpOutput


class FauxServeur:
    """Répond `statut` (200 par défaut) et garde les requêtes reçues."""

    def __init__(self, statut: int = 200) -> None:
        self.statut = statut
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.statut, text="")


def sortie(serveur: FauxServeur, **options: Any) -> HttpOutput:
    """Sortie branchée sur le faux serveur (`url` fournie par défaut)."""
    return HttpOutput(
        {"url": "http://recepteur:8080/logs", **options},
        transport=httpx.MockTransport(serveur.handler),
    )


def envoyer(s: HttpOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Envoie les documents puis ferme la sortie dans la même boucle."""

    async def scenario() -> list[SendOutcome]:
        try:
            return [await s.envoyer(doc) for doc in documents]
        finally:
            await s.close()

    return asyncio.run(scenario())


def test_inscrite_au_registre() -> None:
    assert REGISTRY["http"] is HttpOutput


def test_capacites_insertions_seules_sans_id_ni_accuse_par_element() -> None:
    capacites = HttpOutput.capacites
    assert capacites.actions == {"index"}
    assert not capacites.accepte_id
    assert not capacites.accuse_par_evenement


def test_url_decoupee_hote_et_chemin() -> None:
    serveur = FauxServeur()
    doc = {"message": "m"}
    assert envoyer(sortie(serveur), [doc]) == [SendOutcome.INDEXED]
    requete = serveur.requests[0]
    assert str(requete.url) == "http://recepteur:8080/logs"
    assert requete.method == "POST"


def test_url_avec_requete_et_sans_chemin() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, url="http://recepteur:8080?source=banc"), [{"message": "m"}])
    assert str(serveur.requests[0].url) == "http://recepteur:8080/?source=banc"


def test_url_incomplete_refusee() -> None:
    with pytest.raises(ValueError, match="url complète attendue"):
        sortie(FauxServeur(), url="recepteur:8080")


def test_url_requise() -> None:
    # Sans `url`, rien à découper : le message nomme l'option attendue.
    with pytest.raises(ValueError, match="url complète attendue"):
        HttpOutput({"hosts": "http://recepteur:8080"}, transport=httpx.MockTransport(FauxServeur().handler))


def test_format_ndjson_par_defaut() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, batch_size=2), [{"message": "a"}, {"message": "b"}])
    corps = serveur.requests[0].read().decode("utf-8")
    assert corps == '{"message": "a"}\n{"message": "b"}\n'


def test_format_json_batch_tableau() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, format="json_batch", batch_size=2), [{"message": "a"}, {"message": "b"}])
    corps = serveur.requests[0].read().decode("utf-8")
    assert json.loads(corps) == [{"message": "a"}, {"message": "b"}]


def test_format_json_un_evenement_par_requete() -> None:
    serveur = FauxServeur()
    # `batch_size` est retaillé à 1 : chaque document part seul.
    resultats = envoyer(sortie(serveur, format="json", batch_size=5), [{"message": "a"}, {"message": "b"}])
    assert resultats == [SendOutcome.INDEXED] * 2
    assert len(serveur.requests) == 2
    corps = [json.loads(r.read()) for r in serveur.requests]
    assert corps == [{"message": "a"}, {"message": "b"}]


def test_http_method_put() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, http_method="put"), [{"message": "m"}])
    assert serveur.requests[0].method == "PUT"


def test_headers_passes() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, headers={"Authorization": "Bearer jeton"}), [{"message": "m"}])
    assert serveur.requests[0].headers["Authorization"] == "Bearer jeton"


def test_option_index_refusee() -> None:
    with pytest.raises(ValueError, match="inconnue pour la sortie"):
        sortie(FauxServeur(), index="logs")


def test_statut_404_permanent_en_dlq(tmp_path: Path) -> None:
    serveur = FauxServeur(statut=404)
    s = HttpOutput(
        {"url": "http://recepteur:8080/logs"},
        transport=httpx.MockTransport(serveur.handler),
        dlq=LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl")),
    )
    assert envoyer(s, [{"message": "m"}]) == [SendOutcome.DEAD_LETTERED]


def test_429_transitoire_puis_reussite() -> None:
    etapes = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        etapes["n"] += 1
        return httpx.Response(429 if etapes["n"] == 1 else 200)

    s = HttpOutput(
        {"url": "http://recepteur:8080/logs", "retry_max": 1, "retry_backoff": 0},
        transport=httpx.MockTransport(handler),
    )
    assert envoyer(s, [{"message": "m"}]) == [SendOutcome.INDEXED]


def test_health_get_sur_le_chemin() -> None:
    requetes: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requetes.append(request)
        return httpx.Response(204)

    s = HttpOutput(
        {"url": "http://recepteur:8080/logs"},
        transport=httpx.MockTransport(handler),
    )

    async def scenario() -> dict[str, Any]:
        try:
            return await s.health()
        finally:
            await s.close()

    assert asyncio.run(scenario()) == {"status": "joignable"}
    assert str(requetes[0].url) == "http://recepteur:8080/logs"
    assert requetes[0].method == "GET"
