# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie VictoriaLogs (issue #14), avec un faux serveur `httpx.MockTransport`.

Modes `bulk` et `jsonline` (chemin, corps), paramètres d'ingestion
(`_stream_fields`, `_msg_field`, `_time_field`), décision sur le statut
HTTP global, `GET /health`, inscription au registre.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue, read_entries
from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.victorialogs import VictoriaLogsOutput


class FauxServeur:
    """Répond `statut` (200 par défaut, corps vide comme VictoriaLogs) et garde les requêtes."""

    def __init__(self, statut: int = 200, texte: str = "") -> None:
        self.statut = statut
        self.texte = texte
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.statut, text=self.texte)

    def lignes(self, i: int = 0) -> list[dict[str, Any]]:
        """Lignes JSON du corps de la i-ème requête."""
        return [json.loads(ligne) for ligne in self.requests[i].read().decode("utf-8").splitlines()]


def sortie(serveur: FauxServeur, dlq: LocalDeadLetterQueue | None = None, **options: Any) -> VictoriaLogsOutput:
    """Sortie branchée sur le faux serveur."""
    return VictoriaLogsOutput(
        {"hosts": "http://victorialogs:9428", **options}, transport=httpx.MockTransport(serveur.handler), dlq=dlq
    )


def envoyer(s: VictoriaLogsOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Envoie les documents puis ferme la sortie dans la même boucle."""

    async def scenario() -> list[SendOutcome]:
        try:
            return [await s.envoyer(doc) for doc in documents]
        finally:
            await s.close()

    return asyncio.run(scenario())


def test_inscrite_au_registre() -> None:
    assert REGISTRY["victorialogs"] is VictoriaLogsOutput


def test_capacites_ingestion_seule_sans_id_ni_accuse_par_element() -> None:
    capacites = VictoriaLogsOutput.capacites
    assert capacites.actions == {"index", "create"}
    assert not capacites.accepte_id
    assert not capacites.accuse_par_evenement


def test_mode_bulk_par_defaut_chemin_et_parametres_logstash() -> None:
    serveur = FauxServeur()
    assert envoyer(sortie(serveur), [{"message": "m"}]) == [SendOutcome.INDEXED]
    requete = serveur.requests[0]
    assert requete.url.path == "/insert/elasticsearch/_bulk"
    assert dict(requete.url.params) == {"_msg_field": "message", "_time_field": "@timestamp"}
    assert serveur.lignes() == [{"index": {"_index": serveur.lignes()[0]["index"]["_index"]}}, {"message": "m"}]


def test_mode_jsonline_une_ligne_par_document_sans_action() -> None:
    serveur = FauxServeur()
    s = sortie(serveur, mode="jsonline", batch_size=2)
    assert envoyer(s, [{"message": "a"}, {"message": "b"}]) == [SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
    assert serveur.requests[0].url.path == "/insert/jsonline"
    assert serveur.lignes() == [{"message": "a"}, {"message": "b"}]


@pytest.mark.parametrize(("valeur", "attendu"), [(["host", "app"], "host,app"), ("host", "host"), (("a", "b"), "a,b")])
def test_stream_fields_liste_ou_chaine(valeur: Any, attendu: str) -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, stream_fields=valeur), [{"message": "m"}])
    assert serveur.requests[0].url.params["_stream_fields"] == attendu


def test_champs_vides_retires_et_personnalises() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, msg_field="", time_field="date,ts"), [{"message": "m"}])
    assert dict(serveur.requests[0].url.params) == {"_time_field": "date,ts"}


def test_pipeline_ne_modifie_pas_les_parametres_de_la_sortie() -> None:
    serveur = FauxServeur()
    s = sortie(serveur, pipeline="p")

    async def scenario() -> None:
        try:
            await s.send([{"message": "m"}])
        finally:
            await s.close()

    asyncio.run(scenario())
    assert serveur.requests[0].url.params["pipeline"] == "p"
    assert "pipeline" not in s._parametres_requete()


def test_mode_inconnu_refuse() -> None:
    # Schéma commun (#8) : énumération validée à l'instanciation.
    with pytest.raises(ValueError, match="valeurs valides : bulk, jsonline \(reçu : loki\)"):
        sortie(FauxServeur(), mode="loki")


def test_stream_fields_de_mauvais_type_refuse() -> None:
    # Schéma commun (#8) : type validé à l'instanciation (str ou liste).
    with pytest.raises(ValueError, match="type str ou list ou tuple attendu \(reçu : int\)"):
        sortie(FauxServeur(), stream_fields=3)


def test_option_inconnue_refusee() -> None:
    # Schéma commun (#8) : option inconnue = faute de frappe, échec franc.
    with pytest.raises(ValueError, match="option « stream_field » inconnue pour la sortie « victorialogs »"):
        sortie(FauxServeur(), stream_field="host")


def test_statut_400_global_est_permanent_et_va_en_dlq(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    s = sortie(FauxServeur(400, "cannot parse"), dlq, batch_size=2)
    assert envoyer(s, [{"m": 1}, {"m": 2}]) == [SendOutcome.EN_ATTENTE, SendOutcome.DEAD_LETTERED]
    entrees = read_entries(tmp_path / "dlq.jsonl")
    assert [e["document"] for e in entrees] == [{"m": 1}, {"m": 2}]
    assert {e["@dlq_output"] for e in entrees} == {"victorialogs"}


@pytest.mark.parametrize("statut", [503, 429, 401, 302])
def test_statut_global_non_2xx_n_est_jamais_compte_indexe(statut: int) -> None:
    # Régression : le statut global seul comptait tout non-4xx-permanent comme indexé.
    assert envoyer(sortie(FauxServeur(statut)), [{"m": 1}]) == [SendOutcome.RETRYABLE]


def test_transitoire_global_epuise_va_en_dlq_avec_l_option(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    s = sortie(FauxServeur(503), dlq, dlq_transitoires=True)
    assert envoyer(s, [{"m": 1}]) == [SendOutcome.DEAD_LETTERED]
    assert read_entries(tmp_path / "dlq.jsonl")[0]["@dlq_reason"] == "transitoire_epuise"


def test_health_lit_get_health() -> None:
    serveur = FauxServeur(200, "OK\n")
    s = sortie(serveur)

    async def scenario() -> dict[str, Any]:
        try:
            return await s.health()
        finally:
            await s.close()

    assert asyncio.run(scenario()) == {"status": "OK"}
    assert serveur.requests[0].url.path == "/health"


def test_health_en_erreur_leve() -> None:
    s = sortie(FauxServeur(503))

    async def scenario() -> None:
        try:
            await s.health()
        finally:
            await s.close()

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(scenario())
