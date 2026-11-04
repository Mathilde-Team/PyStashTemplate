# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie Elasticsearch (issue #13), avec un faux serveur `httpx.MockTransport`.

Clé d'API et jeton (en-tête `Authorization`), exclusivité des
authentifications, data streams (action `create` imposée, nommage
type-dataset-namespace), inscription au registre. Le comportement
d'envoi lui-même est celui du client commun (tests de
`test_elasticsearch_bulk.py`).
"""

from __future__ import annotations

import asyncio
import base64
import json
from typing import Any

import httpx
import pytest

from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.elasticsearch import ElasticsearchOutput
from pystashtemplate.outputs.elasticsearch_bulk import ActionInvalideError, SendOutcome


class FauxServeur:
    """Accepte chaque `_bulk` (un item 201 par document) et garde les requêtes."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        lignes = request.read().decode("utf-8").splitlines()
        actions = [next(iter(json.loads(ligne))) for ligne in lignes[0::2]]
        return httpx.Response(200, json={"items": [{a: {"status": 201}} for a in actions]})

    def action(self, i: int = 0) -> dict[str, Any]:
        """Ligne d'action NDJSON (première ligne) de la i-ème requête."""
        return json.loads(self.requests[i].read().decode("utf-8").splitlines()[0])


def sortie(serveur: FauxServeur, **options: Any) -> ElasticsearchOutput:
    """Sortie branchée sur le faux serveur."""
    return ElasticsearchOutput(
        {"hosts": "https://elasticsearch:9200", "index": "logs-test", **options},
        transport=httpx.MockTransport(serveur.handler),
    )


def envoyer(s: ElasticsearchOutput, **kwargs: Any) -> SendOutcome:
    """Envoie un document et ferme la sortie dans la même boucle."""

    async def scenario() -> SendOutcome:
        try:
            return await s.envoyer({"message": "m"}, **kwargs)
        finally:
            await s.close()

    return asyncio.run(scenario())


def test_inscrite_au_registre_sous_son_nom_de_plugin() -> None:
    assert REGISTRY["elasticsearch"] is ElasticsearchOutput


def test_capacites_completes() -> None:
    capacites = ElasticsearchOutput.capacites
    assert capacites.actions == {"index", "create", "update", "delete"}
    assert capacites.accepte_id and capacites.accuse_par_evenement


def test_chemin_bulk_natif() -> None:
    serveur = FauxServeur()
    assert envoyer(sortie(serveur)) is SendOutcome.INDEXED
    assert serveur.requests[0].url.path == "/_bulk"
    assert "authorization" not in serveur.requests[0].headers


def test_cle_d_api_encodee_passe_telle_quelle() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, api_key="VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw=="))
    assert serveur.requests[0].headers["authorization"] == (
        "ApiKey VnVhQ2ZHY0JDZGJrUW0tZTVhT3g6dWkybHAyYXhUTm1zeWFrdzl0dk5udw=="
    )


def test_cle_d_api_id_cle_est_encodee_en_base64() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, api_key="mon-id:ma-cle"))
    attendu = base64.b64encode(b"mon-id:ma-cle").decode("ascii")
    assert serveur.requests[0].headers["authorization"] == f"ApiKey {attendu}"


def test_jeton_porteur() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, token="jeton-de-service"))
    assert serveur.requests[0].headers["authorization"] == "Bearer jeton-de-service"


def test_authentification_basique_inchangee() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, user="elastic", password="secret"))
    assert serveur.requests[0].headers["authorization"].startswith("Basic ")


@pytest.mark.parametrize(
    "auths",
    [
        {"user": "u", "password": "p", "api_key": "k"},
        {"api_key": "k", "token": "t"},
        {"user": "u", "password": "p", "token": "t"},
    ],
)
def test_une_seule_authentification(auths: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="une seule authentification"):
        sortie(FauxServeur(), **auths)


def test_option_inconnue_refusee() -> None:
    with pytest.raises(ValueError, match="options inconnues"):
        sortie(FauxServeur(), apikey="faute-de-frappe")


# --- Data streams ---------------------------------------------------------------


def test_data_stream_nommage_par_defaut_et_action_create() -> None:
    serveur = FauxServeur()
    s = ElasticsearchOutput(
        {"hosts": "https://elasticsearch:9200", "data_stream": True}, transport=httpx.MockTransport(serveur.handler)
    )
    assert envoyer(s) is SendOutcome.INDEXED
    assert serveur.action() == {"create": {"_index": "logs-generic-default"}}


def test_data_stream_nommage_personnalise() -> None:
    serveur = FauxServeur()
    s = ElasticsearchOutput(
        {
            "hosts": "https://elasticsearch:9200",
            "data_stream": True,
            "data_stream_type": "metrics",
            "data_stream_dataset": "rsyslog",
            "data_stream_namespace": "prod",
        },
        transport=httpx.MockTransport(serveur.handler),
    )
    envoyer(s)
    assert serveur.action() == {"create": {"_index": "metrics-rsyslog-prod"}}


def test_data_stream_index_explicite_conserve() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, data_stream=True, index="logs-app-default"))
    assert serveur.action() == {"create": {"_index": "logs-app-default"}}


def test_data_stream_action_index_devient_create_par_send() -> None:
    serveur = FauxServeur()
    s = sortie(serveur, data_stream=True, action="index")

    async def scenario() -> None:
        try:
            await s.send([{"message": "m"}])
        finally:
            await s.close()

    asyncio.run(scenario())
    assert next(iter(serveur.action())) == "create"


@pytest.mark.parametrize("action", ["update", "delete"])
def test_data_stream_refuse_update_et_delete_a_la_configuration(action: str) -> None:
    with pytest.raises(ValueError, match="create"):
        sortie(FauxServeur(), data_stream=True, action=action)


@pytest.mark.parametrize("action", ["update", "delete"])
def test_data_stream_refuse_update_et_delete_par_document(action: str) -> None:
    serveur = FauxServeur()
    with pytest.raises(ActionInvalideError, match="create seule"):
        envoyer(sortie(serveur, data_stream=True), action=action, document_id="x")
    assert serveur.requests == []


def test_sans_data_stream_les_actions_restent_libres() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur), action="update", document_id="x")
    assert serveur.action() == {"update": {"_index": "logs-test", "_id": "x"}}


def test_options_de_nommage_sans_data_stream_refusees() -> None:
    with pytest.raises(ValueError, match="data_stream => true"):
        sortie(FauxServeur(), data_stream_dataset="rsyslog")


def test_les_options_d_origine_ne_sont_pas_modifiees() -> None:
    options = {"hosts": "https://elasticsearch:9200", "data_stream": True}
    ElasticsearchOutput(options)
    assert options == {"hosts": "https://elasticsearch:9200", "data_stream": True}
