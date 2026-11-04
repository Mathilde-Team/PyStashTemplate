# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie ClickHouse (issue #23), faux serveur `httpx.MockTransport`.

Interface HTTP : requête INSERT en paramètre `query`, corps JSON Each
Row, réglages serveur en paramètres d'URL, décision sur le statut HTTP
global, `table` requise, options de la famille `_bulk` refusées,
`GET /ping`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.clickhouse import ClickHouseOutput
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome


class FauxServeur:
    """Répond `statut` (200 par défaut) et garde les requêtes."""

    def __init__(self, statut: int = 200) -> None:
        self.statut = statut
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.statut, text="")


def sortie(serveur: FauxServeur, **options: Any) -> ClickHouseOutput:
    """Sortie branchée sur le faux serveur (`table` fournie par défaut)."""
    return ClickHouseOutput(
        {"hosts": "http://clickhouse:8123", "table": "logs", **options},
        transport=httpx.MockTransport(serveur.handler),
    )


def envoyer(s: ClickHouseOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Envoie les documents puis ferme la sortie dans la même boucle."""

    async def scenario() -> list[SendOutcome]:
        try:
            return [await s.envoyer(doc) for doc in documents]
        finally:
            await s.close()

    return asyncio.run(scenario())


def test_inscrite_au_registre() -> None:
    assert REGISTRY["clickhouse"] is ClickHouseOutput


def test_capacites_insertions_seules_sans_id_ni_accuse_par_element() -> None:
    capacites = ClickHouseOutput.capacites
    assert capacites.actions == {"index"}
    assert not capacites.accepte_id
    assert not capacites.accuse_par_evenement


def test_requete_insert_json_each_row() -> None:
    serveur = FauxServeur()
    doc = {"message": "m", "host": "h1"}
    assert envoyer(sortie(serveur), [doc]) == [SendOutcome.INDEXED]
    requete = serveur.requests[0]
    assert requete.url.path == "/"
    assert requete.url.params["query"] == "INSERT INTO default.logs FORMAT JSONEachRow"
    # Le document part tel quel, une ligne JSON par document.
    corps = requete.read().decode("utf-8")
    assert corps == '{"message": "m", "host": "h1"}\n'


def test_database_et_table_dans_la_requete() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, database="rsyslog", table="evenements"), [{"message": "m"}])
    assert serveur.requests[0].url.params["query"] == "INSERT INTO rsyslog.evenements FORMAT JSONEachRow"


def test_table_requise() -> None:
    with pytest.raises(ValueError, match="requise"):
        ClickHouseOutput({"hosts": "http://clickhouse:8123"}, transport=httpx.MockTransport(FauxServeur().handler))


def test_reglages_par_defaut() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur), [{"message": "m"}])
    params = serveur.requests[0].url.params
    # Champs inconnus écartés, insertions synchrones.
    assert params["input_format_skip_unknown_fields"] == "1"
    assert "async_insert" not in params


def test_async_insert_active() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, async_insert=True), [{"message": "m"}])
    assert serveur.requests[0].url.params["async_insert"] == "1"


def test_champs_inconnus_erreur_sans_reglage() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, champs_inconnus="erreur"), [{"message": "m"}])
    params = serveur.requests[0].url.params
    assert "input_format_skip_unknown_fields" not in params


def test_option_index_refusee() -> None:
    # La base et la table portent les données : pas d'index.
    with pytest.raises(ValueError, match="inconnue pour la sortie"):
        sortie(FauxServeur(), index="logs")


def test_statut_400_permanent_en_dlq(tmp_path: Path) -> None:
    serveur = FauxServeur(statut=400)
    s = ClickHouseOutput(
        {"hosts": "http://clickhouse:8123", "table": "logs"},
        transport=httpx.MockTransport(serveur.handler),
        dlq=LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl")),
    )
    assert envoyer(s, [{"message": "m"}]) == [SendOutcome.DEAD_LETTERED]


def test_429_transitoire_puis_reussite() -> None:
    etapes = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        etapes["n"] += 1
        return httpx.Response(429 if etapes["n"] == 1 else 200)

    s = ClickHouseOutput(
        {"hosts": "http://clickhouse:8123", "table": "logs", "retry_max": 1, "retry_backoff": 0},
        transport=httpx.MockTransport(handler),
    )
    assert envoyer(s, [{"message": "m"}]) == [SendOutcome.INDEXED]


def test_health_ping() -> None:
    requetes: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requetes.append(request)
        if request.url.path == "/ping":
            return httpx.Response(200, text="Ok.\n")
        return httpx.Response(200)

    s = ClickHouseOutput(
        {"hosts": "http://clickhouse:8123", "table": "logs"},
        transport=httpx.MockTransport(handler),
    )

    async def scenario() -> dict[str, Any]:
        try:
            return await s.health()
        finally:
            await s.close()

    assert asyncio.run(scenario()) == {"status": "Ok."}
    assert requetes[0].url.path == "/ping"
