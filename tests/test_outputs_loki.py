# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie Loki (issue #21), avec un faux serveur `httpx.MockTransport`.

Push JSON (`/loki/api/v1/push`) : labels rendus par document, lignes
triées par horodatage, en-tête `X-Scope-OrgID`, décision sur le statut
HTTP global, options propres refusées (`index`), `GET /ready`.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.loki import LokiOutput


class FauxServeur:
    """Répond `statut` (204 par défaut, comme Loki) et garde les requêtes."""

    def __init__(self, statut: int = 204) -> None:
        self.statut = statut
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.statut, text="")

    def corps(self, i: int = 0) -> dict[str, Any]:
        """Corps JSON de la i-ème requête."""
        return json.loads(self.requests[i].read().decode("utf-8"))


def sortie(serveur: FauxServeur, **options: Any) -> LokiOutput:
    """Sortie branchée sur le faux serveur."""
    return LokiOutput({"hosts": "http://loki:3100", **options}, transport=httpx.MockTransport(serveur.handler))


def envoyer(s: LokiOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Envoie les documents puis ferme la sortie dans la même boucle."""

    async def scenario() -> list[SendOutcome]:
        try:
            return [await s.envoyer(doc) for doc in documents]
        finally:
            await s.close()

    return asyncio.run(scenario())


def test_inscrite_au_registre() -> None:
    assert REGISTRY["loki"] is LokiOutput


def test_capacites_insertions_seules_sans_id_ni_accuse_par_element() -> None:
    capacites = LokiOutput.capacites
    assert capacites.actions == {"index"}
    assert not capacites.accepte_id
    assert not capacites.accuse_par_evenement


def test_chemin_et_corps_push_json() -> None:
    serveur = FauxServeur()
    doc = {"message": "m", "host": "h1"}
    assert envoyer(sortie(serveur), [doc]) == [SendOutcome.INDEXED]
    requete = serveur.requests[0]
    assert requete.url.path == "/loki/api/v1/push"
    assert requete.headers["Content-Type"] == "application/json"
    corps = serveur.corps()
    assert len(corps["streams"]) == 1
    assert corps["streams"][0]["stream"] == {}
    assert len(corps["streams"][0]["values"]) == 1
    horodatage, ligne = corps["streams"][0]["values"][0]
    assert horodatage.isdigit()
    assert json.loads(ligne) == doc


def test_labels_rendus_par_document_et_regroupes_par_flux() -> None:
    serveur = FauxServeur()
    s = sortie(serveur, labels={"job": "rsyslog", "host": "%{host}"}, batch_size=3)
    docs = [{"message": "a", "host": "h1"}, {"message": "b", "host": "h2"}, {"message": "c", "host": "h1"}]
    assert envoyer(s, docs) == [SendOutcome.EN_ATTENTE] * 2 + [SendOutcome.INDEXED]
    corps = serveur.corps()
    flux = {tuple(trie.items()): valeurs for trie, valeurs in [(f["stream"], f["values"]) for f in corps["streams"]]}
    assert {k: len(v) for k, v in flux.items()} == {
        (("host", "h1"), ("job", "rsyslog")): 2,
        (("host", "h2"), ("job", "rsyslog")): 1,
    }
    # Le document entier part dans la ligne (JSON, sans perte).
    assert json.loads(flux[(("host", "h1"), ("job", "rsyslog"))][0][1])["message"] == "a"


def test_labels_vides_ecartes() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, labels={"job": "%{champ_absent}", "app": "banc"}), [{"message": "m"}])
    assert serveur.corps()["streams"][0]["stream"] == {"app": "banc"}


def test_lignes_d_un_flux_triees_par_horodatage() -> None:
    serveur = FauxServeur()
    s = sortie(serveur, batch_size=3)
    docs = [
        {"message": "apres", "@timestamp": "2026-11-04T10:00:02Z"},
        {"message": "avant", "@timestamp": "2026-11-04T10:00:01Z"},
        {"message": "milieu", "@timestamp": "2026-11-04T10:00:03Z"},
    ]
    envoyer(s, docs)
    valeurs = serveur.corps()["streams"][0]["values"]
    lignes = [json.loads(ligne) for _, ligne in valeurs]
    assert [ligne["message"] for ligne in lignes] == ["avant", "apres", "milieu"]


def test_horodatage_absent_retombe_sur_maintenant() -> None:
    serveur = FauxServeur()
    avant = time.time_ns()
    envoyer(sortie(serveur), [{"message": "m"}])
    horodatage = int(serveur.corps()["streams"][0]["values"][0][0])
    apres = time.time_ns()
    assert avant - 1_000_000_000 <= horodatage <= apres + 1_000_000_000


def test_tenant_en_tete_x_scope_orgid() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, tenant_id="locataire-42"), [{"message": "m"}])
    assert serveur.requests[0].headers["X-Scope-OrgID"] == "locataire-42"


def test_sans_tenant_pas_d_en_tete() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur), [{"message": "m"}])
    assert "X-Scope-OrgID" not in serveur.requests[0].headers


def test_option_index_refusee() -> None:
    # Loki n'a pas d'index : les flux portent les labels.
    with pytest.raises(ValueError, match="inconnue pour la sortie"):
        sortie(FauxServeur(), index="logs")


def test_action_autre_que_index_refusee() -> None:
    with pytest.raises(ValueError, match="valeurs valides : index \\(reçu : create\\)"):
        sortie(FauxServeur(), action="create")


def test_statut_global_204_indexe_400_permanent(tmp_path: Path) -> None:
    serveur = FauxServeur(statut=400)
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    s = LokiOutput(
        {"hosts": "http://loki:3100"},
        transport=httpx.MockTransport(serveur.handler),
        dlq=dlq,
    )
    assert envoyer(s, [{"message": "m"}]) == [SendOutcome.DEAD_LETTERED]


def test_429_transitoire_puis_reussite() -> None:
    etapes = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        etapes["n"] += 1
        return httpx.Response(429 if etapes["n"] == 1 else 204)

    s = LokiOutput(
        {"hosts": "http://loki:3100", "retry_max": 1, "retry_backoff": 0},
        transport=httpx.MockTransport(handler),
    )
    assert envoyer(s, [{"message": "m"}]) == [SendOutcome.INDEXED]


def test_health_pret() -> None:
    requetes: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requetes.append(request)
        if request.url.path == "/ready":
            return httpx.Response(200, text="ready")
        return httpx.Response(204)

    s = LokiOutput({"hosts": "http://loki:3100"}, transport=httpx.MockTransport(handler))

    async def scenario() -> dict[str, Any]:
        try:
            return await s.health()
        finally:
            await s.close()

    assert asyncio.run(scenario()) == {"status": "ready"}
    assert requetes[0].url.path == "/ready"


def test_horodatage_illisible_retombe_sur_maintenant() -> None:
    serveur = FauxServeur()
    avant = time.time_ns()
    envoyer(sortie(serveur), [{"message": "m", "@timestamp": "pas une date"}])
    horodatage = int(serveur.corps()["streams"][0]["values"][0][0])
    apres = time.time_ns()
    assert avant - 1_000_000_000 <= horodatage <= apres + 1_000_000_000


def test_horodatage_sans_fuseau_lu_comme_utc() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur), [{"message": "m", "@timestamp": "2026-11-04T10:00:00"}])
    horodatage = int(serveur.corps()["streams"][0]["values"][0][0])
    attendu = 1_793_786_400_000_000_000  # 2026-11-04T10:00:00Z en ns
    assert abs(horodatage - attendu) < 2_000_000_000


def test_labels_non_textels_convertis_en_chaines() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, labels={"niveau": 3, "vide": None}), [{"message": "m"}])
    # Un label vide est écarté ; une valeur non textuelle devient une chaîne.
    assert serveur.corps()["streams"][0]["stream"] == {"niveau": "3"}


# --- Cardinalité des labels (critère de #21) ---------------------------------


def _avertissements(action) -> list[str]:
    """Messages WARNING journalisés pendant `action()`."""
    from loguru import logger

    messages: list[str] = []
    sink = logger.add(lambda m: messages.append(m.record["message"]), level="WARNING")
    try:
        action()
    finally:
        logger.remove(sink)
    return messages


@pytest.mark.parametrize(
    ("modele", "attendus"),
    [
        ("%{host}", []),
        ("rsyslog", []),
        (3, []),
        ("%{+YYYY.MM.dd}", []),
        ("%{message}", ["message"]),
        ("%{@timestamp}", ["@timestamp"]),
        ("%{[client][ip]}", ["[client][ip]"]),
        ("%{source.port}", ["source.port"]),
        ("%{session_id}", ["session_id"]),
        ("%{commande_id}", ["commande_id"]),
        ("%{host}-%{pid}", ["pid"]),
        ("%{ URL }", [" URL "]),
    ],
)
def test_champs_forte_cardinalite(modele: Any, attendus: list[str]) -> None:
    from pystashtemplate.outputs.loki import champs_forte_cardinalite

    assert champs_forte_cardinalite(modele) == attendus


def test_avertit_a_la_construction_sur_un_label_a_forte_cardinalite() -> None:
    messages = _avertissements(lambda: sortie(FauxServeur(), labels={"job": "rsyslog", "client": "%{[client][ip]}"}))
    assert len(messages) == 1
    assert "client" in messages[0] and "[client][ip]" in messages[0]


def test_aucun_avertissement_pour_des_labels_sobres() -> None:
    assert _avertissements(lambda: sortie(FauxServeur(), labels={"job": "rsyslog", "host": "%{host}"})) == []


def test_avertit_une_fois_au_dela_du_seuil_de_valeurs_distinctes() -> None:
    from pystashtemplate.outputs.loki import SEUIL_CARDINALITE

    serveur = FauxServeur()
    s = sortie(serveur, labels={"job": "rsyslog", "hote": "%{hote}"})
    documents = [{"hote": f"h{i}", "message": "m"} for i in range(SEUIL_CARDINALITE + 5)]
    documents += [{"hote": "h0", "message": "m"}] * 3
    messages = _avertissements(lambda: envoyer(s, documents))
    cardinalite = [m for m in messages if "valeurs distinctes" in m]
    assert len(cardinalite) == 1
    assert "hote" in cardinalite[0] and str(SEUIL_CARDINALITE) in cardinalite[0]
    # Le label signalé n'est plus suivi : la mémoire est libérée.
    assert s._valeurs_vues["hote"] == set()
    assert len(s._valeurs_vues["job"]) == 1


def test_sous_le_seuil_aucun_avertissement_d_execution() -> None:
    from pystashtemplate.outputs.loki import SEUIL_CARDINALITE

    s = sortie(FauxServeur(), labels={"hote": "%{hote}"})
    documents = [{"hote": f"h{i % SEUIL_CARDINALITE}"} for i in range(3 * SEUIL_CARDINALITE)]
    assert [m for m in _avertissements(lambda: envoyer(s, documents)) if "valeurs distinctes" in m] == []
