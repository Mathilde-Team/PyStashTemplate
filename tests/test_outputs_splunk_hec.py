# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie protocole Splunk HEC (issue #28), avec un faux serveur `httpx.MockTransport`.

Enveloppes HEC concaténées vers `/services/collector/event`, en-tête
`Authorization: Splunk <jeton>`, champs `time`/`host`/`source`/
`sourcetype`/`index`/`fields`, décision sur le statut HTTP global,
options refusées (`pipeline`, `user`…), santé `/services/collector/health`.
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
from pystashtemplate.outputs.splunk_hec import SplunkHecOutput, epoch_secondes, hote_du_document


class FauxServeur:
    """Répond comme HEC (`{"text": "Success", "code": 0}` par défaut) et garde les requêtes."""

    def __init__(self, statut: int = 200, corps: Any = None, texte: str | None = None) -> None:
        self.statut = statut
        self.corps_reponse = {"text": "Success", "code": 0} if corps is None else corps
        self.texte = texte
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.texte is not None:
            return httpx.Response(self.statut, text=self.texte)
        return httpx.Response(self.statut, json=self.corps_reponse)

    def enveloppes(self, i: int = 0) -> list[dict[str, Any]]:
        """Enveloppes HEC de la i-ème requête (une par ligne)."""
        return [json.loads(ligne) for ligne in self.requests[i].read().decode("utf-8").splitlines()]


def sortie(serveur: FauxServeur, *, dlq: LocalDeadLetterQueue | None = None, **options: Any) -> SplunkHecOutput:
    """Sortie branchée sur le faux serveur (jeton `jeton-hec` par défaut)."""
    return SplunkHecOutput(
        {"hosts": "https://splunk:8088", "token": "jeton-hec", **options},
        transport=httpx.MockTransport(serveur.handler),
        dlq=dlq,
    )


def envoyer(s: SplunkHecOutput, documents: list[dict[str, Any]], *, vider: bool = False) -> list[SendOutcome]:
    """Envoie les documents (puis vide le tampon si demandé) et ferme, dans la même boucle."""

    async def scenario() -> list[SendOutcome]:
        try:
            resultats = [await s.envoyer(doc) for doc in documents]
            if vider:
                await s.flush()
            return resultats
        finally:
            await s.close()

    return asyncio.run(scenario())


def sante(s: SplunkHecOutput) -> dict[str, Any]:
    """Appelle `health()` puis ferme, dans la même boucle."""

    async def scenario() -> dict[str, Any]:
        try:
            return await s.health()
        finally:
            await s.close()

    return asyncio.run(scenario())


def test_inscrite_au_registre() -> None:
    assert REGISTRY["splunk_hec"] is SplunkHecOutput


def test_capacites_insertions_seules_sans_id_ni_accuse_par_element() -> None:
    capacites = SplunkHecOutput.capacites
    assert capacites.actions == {"index"}
    assert not capacites.accepte_id
    assert not capacites.accuse_par_evenement
    assert capacites.taille_max_requete is None


def test_chemin_en_tetes_et_enveloppe_minimale() -> None:
    serveur = FauxServeur()
    doc = {"message": "m"}
    assert envoyer(sortie(serveur), [doc]) == [SendOutcome.INDEXED]
    requete = serveur.requests[0]
    assert requete.method == "POST"
    assert requete.url.path == "/services/collector/event"
    assert requete.headers["Authorization"] == "Splunk jeton-hec"
    assert requete.headers["Content-Type"] == "application/json"
    assert "X-Splunk-Request-Channel" not in requete.headers
    assert serveur.enveloppes() == [{"event": doc}]


def test_enveloppe_complete() -> None:
    serveur = FauxServeur()
    s = sortie(
        serveur,
        sourcetype="syslog",
        source="rsyslog",
        host="collecteur",
        index="main",
        fields=["severity", "absent", "code"],
        channel="0aeeac95-ac74-4aa9-b30d-6c4c0ac581ba",
    )
    doc = {"message": "m", "@timestamp": "2026-11-04T10:00:00.123Z", "severity": "info", "code": 42, "host": "h1"}
    envoyer(s, [doc])
    assert serveur.enveloppes() == [
        {
            "event": doc,
            "time": 1793786400.123,
            "host": "collecteur",
            "source": "rsyslog",
            "sourcetype": "syslog",
            "index": "main",
            "fields": {"severity": "info", "code": "42"},
        }
    ]
    assert serveur.requests[0].headers["X-Splunk-Request-Channel"] == "0aeeac95-ac74-4aa9-b30d-6c4c0ac581ba"


def test_hote_du_document_puis_ecs() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, batch_size=3), [{"host": "h1"}, {"host": {"name": "h2"}}, {"host": {"ip": "x"}}])
    assert [e.get("host") for e in serveur.enveloppes()] == ["h1", "h2", None]


def test_champ_horodatage_configurable() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, time_field="recu"), [{"recu": "2026-11-04T10:00:00", "@timestamp": "ignoré"}])
    assert serveur.enveloppes()[0]["time"] == 1793786400.0


def test_lot_de_plusieurs_enveloppes_une_requete() -> None:
    serveur = FauxServeur()
    documents = [{"message": f"m{n}"} for n in range(3)]
    resultats = envoyer(sortie(serveur, batch_size=3), documents)
    assert resultats == [SendOutcome.EN_ATTENTE, SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
    assert len(serveur.requests) == 1
    assert [e["event"] for e in serveur.enveloppes()] == documents


def test_tampon_vide_aucune_requete() -> None:
    serveur = FauxServeur()
    assert envoyer(sortie(serveur, batch_size=5), [], vider=True) == []
    assert serveur.requests == []


def test_caracteres_non_ascii_preserves() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur), [{"message": "connexion refusée — échec"}])
    assert "refusée — échec" in serveur.requests[0].read().decode("utf-8")


def test_en_tetes_d_options_priment() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, headers={"Authorization": "Splunk autre"}), [{"message": "m"}])
    assert serveur.requests[0].headers["Authorization"] == "Splunk autre"


@pytest.mark.parametrize("options", [{}, {"token": ""}])
def test_jeton_requis(options: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="token"):
        SplunkHecOutput({"hosts": "https://splunk:8088", **options})


def test_authentification_basique_refusee() -> None:
    with pytest.raises(ValueError, match="jeton HEC"):
        sortie(FauxServeur(), user="u", password="p")


@pytest.mark.parametrize("champs", [["ok", ""], ["ok", 3]])
def test_fields_invalides_refuses(champs: list[Any]) -> None:
    with pytest.raises(ValueError, match="fields"):
        sortie(FauxServeur(), fields=champs)


@pytest.mark.parametrize("option", ["pipeline", "routing", "doc_as_upsert"])
def test_options_bulk_refusees(option: str) -> None:
    with pytest.raises(ValueError):
        sortie(FauxServeur(), **{option: "x" if option != "doc_as_upsert" else True})


def test_action_autre_qu_index_refusee() -> None:
    with pytest.raises(ValueError):
        sortie(FauxServeur(), action="create")


def test_refus_400_en_dlq(tmp_path: Path) -> None:
    serveur = FauxServeur(400, {"text": "Invalid data format", "code": 6, "invalid-event-number": 0})
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    assert envoyer(sortie(serveur, dlq=dlq), [{"message": "m"}]) == [SendOutcome.DEAD_LETTERED]
    lignes = (tmp_path / "dlq.jsonl").read_text(encoding="utf-8").splitlines()
    assert json.loads(lignes[0])["document"] == {"message": "m"}


def test_refus_400_sans_dlq_rendu_a_l_appelant() -> None:
    serveur = FauxServeur(400, {"text": "No data", "code": 5})
    assert envoyer(sortie(serveur), [{"message": "m"}]) == [SendOutcome.RETRYABLE]


@pytest.mark.parametrize("statut", [401, 403])
def test_jeton_refuse_rendu_a_l_appelant(statut: int, tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    serveur = FauxServeur(statut, {"text": "Invalid token", "code": 4})
    assert envoyer(sortie(serveur, dlq=dlq), [{"message": "m"}]) == [SendOutcome.RETRYABLE]
    assert not (tmp_path / "dlq.jsonl").exists()


def test_serveur_occupe_retente_puis_rendu() -> None:
    serveur = FauxServeur(503, {"text": "Server is busy", "code": 9})
    assert envoyer(sortie(serveur, retry_max=2, retry_backoff=0), [{"message": "m"}]) == [SendOutcome.RETRYABLE]
    assert len(serveur.requests) == 3


def test_health_json() -> None:
    serveur = FauxServeur(200, {"text": "HEC is healthy", "code": 17})
    assert sante(sortie(serveur)) == {"status": "healthy", "reponse": {"text": "HEC is healthy", "code": 17}}
    assert [r.url.path for r in serveur.requests] == ["/services/collector/health"]


def test_health_sans_corps_json() -> None:
    assert sante(sortie(FauxServeur(200, texte="OK"))) == {"status": "healthy", "reponse": None}


def test_health_indisponible() -> None:
    with pytest.raises(httpx.HTTPStatusError):
        sante(sortie(FauxServeur(503, {"text": "HEC is unhealthy", "code": 18})))


@pytest.mark.parametrize(
    ("brut", "attendu"),
    [
        ("2026-11-04T10:00:00Z", 1793786400.0),
        ("2026-11-04T11:00:00+01:00", 1793786400.0),
        ("pas une date", None),
        (None, None),
        (1793786400, None),
    ],
)
def test_epoch_secondes(brut: Any, attendu: float | None) -> None:
    assert epoch_secondes(brut) == attendu


@pytest.mark.parametrize(
    ("doc", "attendu"),
    [({"host": "h"}, "h"), ({"host": {"name": "n"}}, "n"), ({"host": ""}, None), ({}, None), ({"host": 3}, None)],
)
def test_hote_du_document(doc: dict[str, Any], attendu: str | None) -> None:
    assert hote_du_document(doc) == attendu
