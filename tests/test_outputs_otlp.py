# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie OTLP/HTTP (issue #22), avec un faux récepteur `httpx.MockTransport`.

Chemin `/v1/logs`, encodages protobuf (défaut) et JSON, en-têtes,
compression gzip, table de correspondance champ → `LogRecord`, groupement
par ressource, statut global et `partial_success`, document impossible
à coder en JSON isolé, santé par requête vide. Les cas communs (lot,
erreur partielle, 429, coupure, arrêt) sont rejoués par le banc de
conformité (`test_conformite_sorties.py`).
"""

from __future__ import annotations

import asyncio
import gzip
import json
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs import REGISTRY, compiler_sorties
from pystashtemplate.outputs import otlp_codec as codec
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.metriques import CompteursSortie
from pystashtemplate.outputs.otlp import OtlpOutput


class FauxRecepteur:
    """Répond comme l'OpenTelemetry Collector : `partialSuccess` en JSON, ou protobuf."""

    def __init__(self, statut: int = 200, corps: bytes | None = None, type_contenu: str | None = None) -> None:
        self.statut = statut
        self.corps = corps
        self.type_contenu = type_contenu
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        json_demande = request.headers["content-type"] == "application/json"
        corps = self.corps if self.corps is not None else (b'{"partialSuccess":{}}' if json_demande else b"")
        type_contenu = self.type_contenu or request.headers["content-type"]
        return httpx.Response(self.statut, content=corps, headers={"content-type": type_contenu})

    def requete(self, i: int = 0) -> dict[str, Any]:
        """Corps JSON décodé de la i-ème requête (sortie en `encoding => "json"`)."""
        contenu = self.requests[i].content
        if self.requests[i].headers.get("content-encoding") == "gzip":
            contenu = gzip.decompress(contenu)
        return json.loads(contenu)

    def journaux(self, i: int = 0) -> list[dict[str, Any]]:
        return [r for rl in self.requete(i)["resourceLogs"] for sl in rl["scopeLogs"] for r in sl["logRecords"]]


def sortie(serveur: FauxRecepteur, dlq: LocalDeadLetterQueue | None = None, **options: Any) -> OtlpOutput:
    """Sortie branchée sur le faux récepteur (JSON par défaut pour relire les corps)."""
    return OtlpOutput(
        {"hosts": "http://collector:4318", "encoding": "json", **options},
        transport=httpx.MockTransport(serveur.handler),
        dlq=dlq,
    )


def lot(s: OtlpOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Tamponne les documents puis vide le tampon ; ferme la sortie."""

    async def scenario() -> list[SendOutcome]:
        try:
            for doc in documents:
                await s.envoyer(doc)
            return await s._flush()
        finally:
            await s.close()

    s._batch_size = len(documents) + 1
    return asyncio.run(scenario())


def test_inscrite_au_registre_et_capacites() -> None:
    assert REGISTRY["otlp"] is OtlpOutput
    capacites = OtlpOutput.capacites
    assert capacites.actions == {"index", "create"}
    assert not capacites.accepte_id
    assert not capacites.accuse_par_evenement


def test_compilation_depuis_le_pipeline() -> None:
    pipeline = PipelineInterpreter(
        'output { otlp { hosts => "http://collector:4318" encoding => "json" '
        'resource => { "service.name" => "rsyslog" } fields => { "host" => "resource.host.name" } } }'
    )
    (s,) = compiler_sorties(pipeline)
    assert isinstance(s, OtlpOutput)


def test_chemin_en_tetes_protobuf_par_defaut() -> None:
    serveur = FauxRecepteur()
    s = OtlpOutput({"hosts": "http://collector:4318"}, transport=httpx.MockTransport(serveur.handler))
    assert lot(s, [{"message": "m"}]) == [SendOutcome.INDEXED]
    requete = serveur.requests[0]
    assert requete.url.path == "/v1/logs"
    assert requete.headers["content-type"] == "application/x-protobuf"
    assert "content-encoding" not in requete.headers
    (rl,) = codec._champs(requete.content)[1]
    assert codec._champs(rl)[2]


def test_correspondance_par_defaut() -> None:
    serveur = FauxRecepteur()
    doc = {
        "message": "bonjour",
        "@timestamp": "2026-11-04T18:00:00Z",
        "severity": "error",
        "trace_id": "0123456789abcdef0123456789abcdef",
        "span_id": "0123456789abcdef",
        "host": "h1",
    }
    assert lot(sortie(serveur), [doc]) == [SendOutcome.INDEXED]
    (journal,) = serveur.journaux()
    assert journal["body"] == {"stringValue": "bonjour"}
    assert journal["timeUnixNano"] == str(1793815200 * 10**9)
    assert int(journal["observedTimeUnixNano"]) > 0
    assert (journal["severityNumber"], journal["severityText"]) == (17, "error")
    assert journal["traceId"] == "0123456789abcdef0123456789abcdef"
    assert journal["spanId"] == "0123456789abcdef"
    assert journal["attributes"] == [{"key": "host", "value": {"stringValue": "h1"}}]
    (rl,) = serveur.requete()["resourceLogs"]
    assert rl["resource"]["attributes"] == [{"key": "service.name", "value": {"stringValue": "pystashtemplate"}}]
    assert rl["scopeLogs"][0]["scope"] == {"name": "pystashtemplate"}


def test_valeurs_illisibles_gardees_en_attributs() -> None:
    serveur = FauxRecepteur()
    lot(sortie(serveur), [{"@timestamp": "hier", "trace_id": "court", "span_id": 3}])
    (journal,) = serveur.journaux()
    assert "timeUnixNano" not in journal and "traceId" not in journal and "spanId" not in journal
    assert {a["key"] for a in journal["attributes"]} == {"@timestamp", "trace_id", "span_id"}


def test_correspondance_configuree_et_groupement_par_ressource() -> None:
    serveur = FauxRecepteur()
    s = sortie(
        serveur,
        resource={"service.name": "rsyslog", "env": "banc"},
        scope="essai",
        fields={"host": "resource.host.name", "prog": "attributes.process.name", "msg": "body", "message": "drop"},
    )
    documents = [
        {"msg": "a", "host": "h1", "prog": "p", "message": "écarté"},
        {"msg": "b", "host": "h2"},
        {"msg": "c", "host": "h1"},
    ]
    assert lot(s, documents) == [SendOutcome.INDEXED] * 3
    r1, r2 = serveur.requete()["resourceLogs"]
    assert (
        codec.attributs({"service.name": "rsyslog", "env": "banc", "host.name": "h1"}) == r1["resource"]["attributes"]
    )
    assert [j["body"]["stringValue"] for j in r1["scopeLogs"][0]["logRecords"]] == ["a", "c"]
    assert [j["body"]["stringValue"] for j in r2["scopeLogs"][0]["logRecords"]] == ["b"]
    assert r1["scopeLogs"][0]["scope"] == {"name": "essai"}
    assert r1["scopeLogs"][0]["logRecords"][0]["attributes"] == [{"key": "process.name", "value": {"stringValue": "p"}}]
    assert "attributes" not in r2["scopeLogs"][0]["logRecords"][0]


def test_ressource_vide_possible() -> None:
    serveur = FauxRecepteur()
    lot(sortie(serveur, resource={}), [{"message": "m"}])
    assert serveur.requete()["resourceLogs"][0]["resource"] == {"attributes": []}


@pytest.mark.parametrize("cible", ["corps", "resource.", "attributes.", "ressource.x"])
def test_cible_inconnue_refusee(cible: str) -> None:
    with pytest.raises(ValueError, match="inconnue pour le champ « host »"):
        sortie(FauxRecepteur(), fields={"host": cible})


def test_encodage_inconnu_refuse() -> None:
    with pytest.raises(ValueError, match="encoding"):
        sortie(FauxRecepteur(), encoding="avro")


def test_option_bulk_refusee() -> None:
    with pytest.raises(ValueError, match="inconnue pour la sortie"):
        sortie(FauxRecepteur(), index="logs")


def test_compression_gzip() -> None:
    serveur = FauxRecepteur()
    assert lot(sortie(serveur, compression=True), [{"message": "m"}]) == [SendOutcome.INDEXED]
    assert serveur.requests[0].headers["content-encoding"] == "gzip"
    assert serveur.journaux()[0]["body"] == {"stringValue": "m"}


def test_reessai_reutilise_le_corps() -> None:
    etats = iter([503, 200])
    corps: list[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        corps.append(request.content)
        return httpx.Response(next(etats), json={"partialSuccess": {}})

    s = OtlpOutput(
        {"hosts": "http://collector:4318", "encoding": "json", "retry_max": 1, "retry_backoff": 0},
        transport=httpx.MockTransport(handler),
    )
    assert lot(s, [{"message": "m"}]) == [SendOutcome.INDEXED]
    assert corps[0] == corps[1]


def test_statut_400_en_dlq(tmp_path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    serveur = FauxRecepteur(statut=400, corps=b'{"code":3,"message":"illisible"}', type_contenu="application/json")
    s = sortie(serveur, dlq=LocalDeadLetterQueue(str(chemin)))
    assert lot(s, [{"message": "a"}, {"message": "b"}]) == [SendOutcome.DEAD_LETTERED] * 2
    assert json.loads(chemin.read_text().splitlines()[0])["@dlq_output"] == "otlp"


def test_statut_503_rendu_a_l_appelant() -> None:
    assert lot(sortie(FauxRecepteur(statut=503, corps=b"")), [{"message": "a"}]) == [SendOutcome.RETRYABLE]


def test_lot_entier_refuse_par_partial_success_en_dlq(tmp_path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    corps = b'{"partialSuccess":{"rejectedLogRecords":"2","errorMessage":"trop vieux"}}'
    s = sortie(FauxRecepteur(corps=corps), dlq=LocalDeadLetterQueue(str(chemin)))
    assert lot(s, [{"message": "a"}, {"message": "b"}]) == [SendOutcome.DEAD_LETTERED] * 2
    assert "trop vieux" in chemin.read_text()


def test_lot_entier_refuse_sans_message(tmp_path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    s = sortie(
        FauxRecepteur(corps=b'{"partialSuccess":{"rejectedLogRecords":"1"}}'), dlq=LocalDeadLetterQueue(str(chemin))
    )
    assert lot(s, [{"message": "a"}]) == [SendOutcome.DEAD_LETTERED]
    assert "partial_success" in chemin.read_text()


def test_refus_partiel_non_identifiable_compte_en_metrique() -> None:
    compteurs = CompteursSortie()
    corps = b'{"partialSuccess":{"rejectedLogRecords":"1","errorMessage":"un de trop"}}'
    s = sortie(FauxRecepteur(corps=corps))
    s.observateur = compteurs
    assert lot(s, [{"message": "a"}, {"message": "b"}]) == [SendOutcome.INDEXED] * 2
    assert compteurs.instantane()["otlp"]["rejected"] == 1
    assert compteurs.instantane()["otlp"]["indexed"] == 2


def test_partial_success_protobuf() -> None:
    partiel = codec._delimite(1, b"\x08" + codec._varint(1) + codec._chaine(2, "refusé"))
    serveur = FauxRecepteur(corps=partiel, type_contenu="application/x-protobuf")
    s = OtlpOutput({"hosts": "http://collector:4318"}, transport=httpx.MockTransport(serveur.handler))
    assert lot(s, [{"message": "a"}]) == [SendOutcome.RETRYABLE]  # refusé, sans DLQ


def test_reponse_illisible_lot_compte_accepte() -> None:
    serveur = FauxRecepteur(corps=b"\x0a\x09", type_contenu="application/x-protobuf")
    assert lot(sortie(serveur), [{"message": "a"}]) == [SendOutcome.INDEXED]


def test_document_non_codable_en_json_isole(tmp_path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    serveur = FauxRecepteur()
    s = sortie(serveur, dlq=LocalDeadLetterQueue(str(chemin)))
    resultats = lot(s, [{"message": "a"}, {"message": "nan", "v": float("nan")}, {"message": "c"}])
    assert resultats == [SendOutcome.INDEXED, SendOutcome.DEAD_LETTERED, SendOutcome.INDEXED]
    assert [serveur.journaux(i)[0]["body"]["stringValue"] for i in range(2)] == ["a", "c"]
    assert "nan" in chemin.read_text()


def test_nan_accepte_en_protobuf() -> None:
    serveur = FauxRecepteur()
    s = OtlpOutput({"hosts": "http://collector:4318"}, transport=httpx.MockTransport(serveur.handler))
    assert lot(s, [{"message": "nan", "v": float("nan")}]) == [SendOutcome.INDEXED]


def test_send_contrat_output() -> None:
    serveur = FauxRecepteur()
    s = sortie(serveur)

    async def scenario() -> None:
        await s.send([{"message": "a"}])
        await s.close()

    asyncio.run(scenario())
    assert serveur.journaux()[0]["body"] == {"stringValue": "a"}


@pytest.mark.parametrize(
    ("options", "attendu"),
    [({"encoding": "json"}, b"{}"), ({"encoding": "protobuf"}, b""), ({"compression": True}, gzip.compress(b"{}"))],
)
def test_health_requete_vide(options: dict[str, Any], attendu: bytes) -> None:
    serveur = FauxRecepteur()
    s = sortie(serveur, **options)

    async def scenario() -> dict[str, Any]:
        try:
            return await s.health()
        finally:
            await s.close()

    assert asyncio.run(scenario()) == {"status": 200}
    assert serveur.requests[0].url.path == "/v1/logs"
    contenu = serveur.requests[0].content
    if options.get("compression"):
        assert gzip.decompress(contenu) == b"{}"
    else:
        assert contenu == attendu


def test_health_statut_d_erreur() -> None:
    s = sortie(FauxRecepteur(statut=503, corps=b""))

    async def scenario() -> None:
        try:
            await s.health()
        finally:
            await s.close()

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(scenario())
