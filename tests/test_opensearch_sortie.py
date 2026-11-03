# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie OpenSearch (issue #12).

Unitaires avec faux serveur (`httpx.MockTransport`) : le banc de
conformité en conteneur est `tests/test_integration_opensearch.py`
(exécuté par le workflow dédié en CI). Aucune requête réseau ici.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs import REGISTRY, compiler_sorties
from pystashtemplate.outputs.opensearch import OpenSearchOutput


def index_du_jour() -> str:
    """Motif rendu par la sortie pour l'index du jour (recalculé côté test)."""
    return datetime.now(timezone.utc).strftime("logs-%Y.%m.%d")


def test_opensearch_est_inscrit_au_registre() -> None:
    """Critère d'acceptation #12 : choisie par le nom du plugin dans `output{}`."""
    assert REGISTRY["opensearch"] is OpenSearchOutput


def test_compiler_sorties_instancie_la_sortie_du_pipeline() -> None:
    conf = """
output {
  opensearch {
    hosts => ["https://opensearch:9200"]
    index  => "logs-%Y.%m.%d"
    user   => "admin"
    password => "s3cret"
    ssl    => true
  }
}
"""
    pipeline = PipelineInterpreter(conf)
    sorties = compiler_sorties(pipeline)
    assert len(sorties) == 1
    sortie = sorties[0]
    assert isinstance(sortie, OpenSearchOutput)
    assert sortie.options["hosts"] == ["https://opensearch:9200"]
    assert sortie.options["index"] == "logs-%Y.%m.%d"
    assert sortie.options["user"] == "admin"


def test_options_de_connexion_cablees_dans_le_client() -> None:
    """Parité avec l'indexer : user/password → authentification sur chaque requête."""
    requetes: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requetes.append(request)
        return httpx.Response(200, json={"items": [{"index": {"status": 201}}]})

    sortie = OpenSearchOutput(
        {"hosts": "https://localhost:9200", "index": "logs-%Y.%m.%d", "user": "bob", "password": "s3cret"},
        transport=httpx.MockTransport(handler),
    )
    assert asyncio.run(sortie.envoyer({"message": "x"})) is not None
    assert requetes[0].headers["Authorization"].startswith("Basic ")


def test_serie_complete_indexe_et_rapporte_par_element() -> None:
    reponses_restantes = [201, 201, 201]

    def handler(request: httpx.Request) -> httpx.Response:
        statut = reponses_restantes.pop(0) if reponses_restantes else 201
        return httpx.Response(200, json={"items": [{"index": {"status": statut}}]})

    sortie = OpenSearchOutput(
        {"hosts": "https://localhost:9200", "index": "logs-%Y.%m.%d"},
        transport=httpx.MockTransport(handler),
    )
    from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome

    assert asyncio.run(sortie.envoyer({"message": "1"})) is SendOutcome.INDEXED
    assert asyncio.run(sortie.envoyer({"message": "2"})) is SendOutcome.INDEXED
    assert asyncio.run(sortie.envoyer({"message": "3"})) is SendOutcome.INDEXED


def test_document_id_deterministe_dans_la_ligne_action() -> None:
    """Parité avec l'indexer : le `_id` donné par document arrive dans la ligne d'action."""

    def handler(request: httpx.Request) -> httpx.Response:
        lignes = request.read().decode("utf-8").splitlines()
        assert json.loads(lignes[0]) == {"index": {"_index": index_du_jour(), "_id": "abc"}}
        return httpx.Response(200, json={"items": [{"index": {"status": 201}}]})

    sortie = OpenSearchOutput(
        {"hosts": "https://localhost:9200", "index": "logs-%Y.%m.%d"},
        transport=httpx.MockTransport(handler),
    )
    from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome

    assert asyncio.run(sortie.envoyer({"message": "x"}, document_id="abc")) is SendOutcome.INDEXED


def test_echec_de_mapping_permanent_va_en_dlq() -> None:
    """Parité avec l'indexer : 4xx par élément → DLQ locale (format identique)."""
    chemin = Path("/tmp/test-dlq-opensearch.jsonl")
    chemin.unlink(missing_ok=True)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"items": [{"index": {"status": 400, "error": {"type": "mapper_parsing_exception"}}}]},
        )

    sortie = OpenSearchOutput(
        {"hosts": "https://localhost:9200", "index": "logs-%Y.%m.%d", "dlq_path": str(chemin)},
        transport=httpx.MockTransport(handler),
    )
    from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome

    assert asyncio.run(sortie.envoyer({"message": "x"})) is SendOutcome.DEAD_LETTERED
    ligne = json.loads(chemin.read_text(encoding="utf-8").splitlines()[0])
    assert ligne["@dlq_index"] == index_du_jour()
    assert ligne["@dlq_error"]
    assert ligne["document"] == {"message": "x"}
    chemin.unlink(missing_ok=True)


def test_health_interroge_la_racine() -> None:
    """Parité avec l'indexer : `health()` remplace `client.info()`."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        return httpx.Response(200, json={"version": {"distribution": "opensearch", "number": "2.19.0"}})

    sortie = OpenSearchOutput({"hosts": "http://localhost:9200"}, transport=httpx.MockTransport(handler))
    assert asyncio.run(sortie.health())["version"]["number"] == "2.19.0"


def test_plugin_inconnu_message_clair_a_la_compilation() -> None:
    conf = """
output {
  openseek {
    hosts => ["https://x:9200"]
  }
}
"""
    from pystashtemplate.outputs import SortieInconnueError

    with pytest.raises(SortieInconnueError, match="openseek"):
        compiler_sorties(PipelineInterpreter(conf))
