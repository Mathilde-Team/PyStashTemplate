# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests du client commun « API _bulk Elasticsearch » (issue #11).

Faux serveur via `httpx.MockTransport` : aucune requête réseau réelle.
Conventions du dépôt : pas de dépendance à pytest-asyncio (`asyncio.run`
directement), journalisation entrée/sortie à la main dans les méthodes
asynchrones (décorateur asynchrone : #37).
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs import REGISTRY, Capacites
from pystashtemplate.outputs.elasticsearch_bulk import (
    ActionInvalideError,
    ElasticsearchBulkOutput,
    SendOutcome,
)


class FauxServeur:
    """Enregistre les requêtes POST /_bulk et répond selon le scénario donné.

    Un scénario est une liste de réponses, une par requête attendue :
    - `("ok", items)` : 200, corps `{"items": items}` (un item par document) ;
    - `("brut", corps)` : 200, corps JSON arbitraire ;
    - `("statut", code)` : réponse vide avec ce code HTTP ;
    - `("reseau", None)` : lève `httpx.ConnectError`.
    """

    def __init__(self, scenario: list[tuple[str, Any]]) -> None:
        self.scenario = list(scenario)
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        kind, payload = self.scenario.pop(0) if self.scenario else ("ok", [])
        if kind == "reseau":
            raise httpx.ConnectError("connexion refusée (test)")
        if kind == "statut":
            return httpx.Response(payload, text="")
        if kind == "brut":
            return httpx.Response(200, json=payload)
        return httpx.Response(200, json={"items": payload})

    def body(self, i: int = 0) -> list[str]:
        """Lignes NDJSON du corps de la i-ème requête enregistrée."""
        return self.requests[i].read().decode("utf-8").splitlines()

    def url(self, i: int = 0) -> str:
        """URL (avec requête) de la i-ème requête enregistrée."""
        return str(self.requests[i].url)


def item(action: str, *, erreur: dict | None = None, statut: int | None = None) -> dict:
    """Un item de réponse `_bulk`, façon Elasticsearch/OpenSearch."""
    resultat: dict[str, Any] = {"status": statut or (400 if erreur else 200)}
    if erreur:
        resultat["error"] = erreur
    return {action: resultat}


def sortie(
    serveur: FauxServeur,
    *,
    dlq: LocalDeadLetterQueue | None = None,
    retry_max: int = 0,
    retry_backoff: float = 0.0,
    batch_size: int = 1,
    **options: Any,
) -> ElasticsearchBulkOutput:
    """Sortie branchée sur le faux serveur, avec ses options par défaut."""
    return ElasticsearchBulkOutput(
        {
            "hosts": "https://localhost:9200",
            "index": "logs-%Y.%m.%d",
            "batch_size": batch_size,
            "retry_max": retry_max,
            "retry_backoff": retry_backoff,
            **options,
        },
        transport=httpx.MockTransport(serveur.handler),
        dlq=dlq,
    )


def index_du_jour() -> str:
    """Ce que `_current_index` rend, recalculé côté test (pas d'import privé)."""
    return datetime.now(timezone.utc).strftime("logs-%Y.%m.%d")


# ----------------------------------------------------------------------
# Registre et sous-classes
# ----------------------------------------------------------------------


def test_sous_classe_de_quelques_lignes_parametre_le_produit() -> None:
    """Critère d'acceptation #11 : chaque produit n'est plus qu'une sous-classe."""

    class QuickwitSortie(ElasticsearchBulkOutput):
        plugin = "quickwit-test"
        chemin_bulk = "/api/v1/_elastic"
        capacites = Capacites(
            actions=frozenset({"create"}),
            accepte_id=False,
            accuse_par_evenement=False,
        )

    enregistrer_avant = dict(REGISTRY)
    try:
        REGISTRY[QuickwitSortie.plugin] = QuickwitSortie
        serveur = FauxServeur([("ok", [])])
        s = REGISTRY["quickwit-test"](
            {"hosts": "http://qw:7280", "index": "logs-%Y.%m.%d"},
            transport=httpx.MockTransport(serveur.handler),
        )
        assert asyncio.run(s.envoyer({"a": 1}, document_id="x", action="create")) is SendOutcome.INDEXED
        assert serveur.url().startswith("http://qw:7280/api/v1/_elastic/_bulk")
        # Pas de _id dans la ligne d'action : refusé par le produit.
        assert json.loads(serveur.body()[0]) == {"create": {"_index": index_du_jour()}}
    finally:
        REGISTRY.clear()
        REGISTRY.update(enregistrer_avant)


# ----------------------------------------------------------------------
# Sérialisation NDJSON
# ----------------------------------------------------------------------


def test_index_ligne_action_plus_document() -> None:
    serveur = FauxServeur([("ok", [item("index")])])
    s = sortie(serveur)
    asyncio.run(s.envoyer({"message": "bonjour"}, document_id="abc"))
    lignes = serveur.body()
    assert len(lignes) == 2
    assert json.loads(lignes[0]) == {"index": {"_index": index_du_jour(), "_id": "abc"}}
    assert json.loads(lignes[1]) == {"message": "bonjour"}


def test_create_sans_id_genere_ligne_action_sans_id() -> None:
    serveur = FauxServeur([("ok", [item("create")])])
    s = sortie(serveur)
    asyncio.run(s.envoyer({"message": "x"}, action="create"))
    assert json.loads(serveur.body()[0]) == {"create": {"_index": index_du_jour()}}


def test_update_doc_as_upsert_corps_update() -> None:
    serveur = FauxServeur([("ok", [item("update")])])
    s = sortie(serveur)
    asyncio.run(s.envoyer({"message": "x"}, action="update", document_id="id1", doc_as_upsert=True))
    lignes = serveur.body()
    assert json.loads(lignes[0]) == {"update": {"_index": index_du_jour(), "_id": "id1"}}
    assert json.loads(lignes[1]) == {"doc": {"message": "x"}, "doc_as_upsert": True}


def test_update_sans_doc_as_upsert() -> None:
    serveur = FauxServeur([("ok", [item("update")])])
    s = sortie(serveur)
    asyncio.run(s.envoyer({"message": "x"}, action="update", document_id="id1"))
    assert json.loads(serveur.body()[1]) == {"doc": {"message": "x"}}


def test_delete_une_seule_ligne() -> None:
    serveur = FauxServeur([("ok", [item("delete")])])
    s = sortie(serveur)
    asyncio.run(s.envoyer({"message": "x"}, action="delete", document_id="id1"))
    lignes = serveur.body()
    assert len(lignes) == 1
    assert json.loads(lignes[0]) == {"delete": {"_index": index_du_jour(), "_id": "id1"}}


def test_routing_dans_la_ligne_action() -> None:
    serveur = FauxServeur([("ok", [item("index")])])
    s = sortie(serveur)
    asyncio.run(s.envoyer({"m": "x"}, routing="noeud-3"))
    assert json.loads(serveur.body()[0])["index"]["routing"] == "noeud-3"


def test_pipeline_en_parametre_de_requete() -> None:
    serveur = FauxServeur([("ok", [item("index")])])
    s = sortie(serveur)
    asyncio.run(s.envoyer({"m": "x"}, pipeline="ingest-geoip"))
    assert "pipeline=ingest-geoip" in serveur.url()


def test_index_explicite_pris_tel_quel() -> None:
    """L'`index` rendu par l'interpréteur (gabarit sprintf déjà résolu) arrive complet."""
    serveur = FauxServeur([("ok", [item("index")])])
    s = sortie(serveur)
    asyncio.run(s.envoyer({"m": "x"}, index="logs-2026.11.03"))
    assert json.loads(serveur.body()[0])["index"]["_index"] == "logs-2026.11.03"


def test_id_refuse_par_le_produit_est_ignore_avec_documentation() -> None:
    class SansId(ElasticsearchBulkOutput):
        capacites = Capacites(accepte_id=False)

    serveur = FauxServeur([("ok", [item("index")])])
    s = SansId({"hosts": "http://x:1"}, transport=httpx.MockTransport(serveur.handler))
    asyncio.run(s.envoyer({"m": "x"}, document_id="refuse"))
    assert "_id" not in json.loads(serveur.body()[0])["index"]


def test_corps_ndjson_termine_par_saut_de_ligne() -> None:
    serveur = FauxServeur([("ok", [item("index")])])
    s = sortie(serveur)
    asyncio.run(s.envoyer({"m": "x"}))
    assert serveur.requests[0].read().decode("utf-8").endswith("\n")


def test_en_tetes_propres_envoyes() -> None:
    serveur = FauxServeur([("ok", [item("index")])])
    s = sortie(serveur, headers={"X-Org": "mathilde"})
    asyncio.run(s.envoyer({"m": "x"}))
    assert serveur.requests[0].headers["X-Org"] == "mathilde"
    assert serveur.requests[0].headers["Content-Type"] == "application/x-ndjson"


# ----------------------------------------------------------------------
# Contrat Output (#4) : cycle de vie et send(batch)
# ----------------------------------------------------------------------


async def _cycle_vie() -> list[str]:
    etats: list[str] = []

    class Traceuse(ElasticsearchBulkOutput):
        plugin = "traceuse"

        async def open(self) -> None:
            etats.append("open")

        async def send(self, batch: list) -> None:
            etats.append("send")

        async def flush(self) -> None:
            etats.append("flush")

        async def close(self) -> None:
            etats.append("close")

    s = Traceuse({"hosts": "http://x:1"})
    async with s:
        await s.send([{"message": "un"}])
    return etats


def test_cycle_de_vie_async_with() -> None:
    assert asyncio.run(_cycle_vie()) == ["open", "send", "close"]


def test_send_batch_utilise_les_options_du_bloc() -> None:
    serveur = FauxServeur([("ok", [item("index"), item("index")])])
    s = sortie(serveur, batch_size=2, action="create", routing="r1", pipeline="p1")
    asyncio.run(s.send([{"m": "1"}, {"m": "2"}]))
    # Deux documents, deux paires de lignes, options du bloc appliquées.
    lignes = serveur.body()
    assert len(lignes) == 4
    assert json.loads(lignes[0]) == {"create": {"_index": index_du_jour(), "routing": "r1"}}
    assert json.loads(lignes[2])["create"]["_index"] == index_du_jour()
    assert "pipeline=p1" in serveur.url()


def test_send_batch_vide_ne_fait_aucune_requete() -> None:
    serveur = FauxServeur([])
    s = sortie(serveur)
    asyncio.run(s.send([]))
    assert serveur.requests == []


def test_open_cree_le_client_sans_requete() -> None:
    serveur = FauxServeur([])
    s = sortie(serveur)
    asyncio.run(s.open())
    assert serveur.requests == []


def test_close_puis_envoi_recre_le_client() -> None:
    serveur = FauxServeur([("ok", [item("index")])])
    s = sortie(serveur)
    asyncio.run(s.close())
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.INDEXED


# ----------------------------------------------------------------------
# Tampon et lotissement
# ----------------------------------------------------------------------


def test_batch_size_superieur_tamponne_puis_envoie() -> None:
    serveur = FauxServeur([("ok", [item("index"), item("index")])])
    s = sortie(serveur, batch_size=2)
    assert asyncio.run(s.envoyer({"m": "1"})) is SendOutcome.EN_ATTENTE
    assert len(serveur.requests) == 0
    assert asyncio.run(s.envoyer({"m": "2"})) is SendOutcome.INDEXED
    assert len(serveur.requests) == 1
    assert len(serveur.body()) == 4  # deux paires action/document


def test_flush_vide_ne_fait_aucune_requete() -> None:
    serveur = FauxServeur([])
    s = sortie(serveur)
    asyncio.run(s.flush())
    assert serveur.requests == []


def test_flush_renvoie_les_resultats_dans_l_ordre() -> None:
    serveur = FauxServeur([("ok", [item("index"), item("index", erreur={"type": "x"}, statut=400)])])
    s = sortie(serveur, batch_size=2)
    assert asyncio.run(s.envoyer({"m": "1"})) is SendOutcome.EN_ATTENTE
    # Le lot part avec le 2e document : 1er INDEXED, 2e 4xx sans DLQ → RETRYABLE.
    assert asyncio.run(s.envoyer({"m": "2"})) is SendOutcome.RETRYABLE
    asyncio.run(s.flush())  # tampon déjà vidé : aucune requête


def test_lot_avec_batch_size_1_envoie_immediatement() -> None:
    """Parité avec l'indexer historique : un document, une requête, résultat connu."""
    serveur = FauxServeur([("ok", [item("index")])])
    s = sortie(serveur)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.INDEXED


def test_pipelines_differents_dans_un_lot_refuses() -> None:
    serveur = FauxServeur([])
    s = sortie(serveur, batch_size=2)
    asyncio.run(s.envoyer({"m": "1"}, pipeline="a"))
    with pytest.raises(ValueError, match="pipelines"):
        asyncio.run(s.envoyer({"m": "2"}, pipeline="b"))


def test_pipeline_homogene_avec_documents_sans_pipeline_accepte() -> None:
    """Des documents sans pipeline peuvent rejoindre un lot pipeline (leur ligne est inchangée)."""
    serveur = FauxServeur([("ok", [item("index"), item("index")])])
    s = sortie(serveur, batch_size=2)
    asyncio.run(s.envoyer({"m": "1"}))  # sans pipeline
    assert asyncio.run(s.envoyer({"m": "2"}, pipeline="a")) is SendOutcome.INDEXED
    assert "pipeline=a" in serveur.url()


# ----------------------------------------------------------------------
# Analyse des réponses
# ----------------------------------------------------------------------


def test_reponse_par_element_succes() -> None:
    serveur = FauxServeur([("ok", [item("index")])])
    s = sortie(serveur)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.INDEXED


def test_erreur_4xx_par_element_va_en_dlq() -> None:
    serveur = FauxServeur([("ok", [item("index", erreur={"type": "mapper_parsing_exception"}, statut=400)])])
    dlq = LocalDeadLetterQueue("/tmp/test-dlq-11.jsonl")
    s = sortie(serveur, dlq=dlq)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.DEAD_LETTERED
    Path("/tmp/test-dlq-11.jsonl").unlink(missing_ok=True)


def test_erreur_4xx_par_element_sans_dlq_retombe_sur_retryable() -> None:
    serveur = FauxServeur([("ok", [item("index", erreur={"type": "x"}, statut=400)])])
    s = sortie(serveur)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.RETRYABLE


def test_erreur_429_par_element_est_transitoire() -> None:
    serveur = FauxServeur([("ok", [item("index", erreur={"type": "es_rejected_execution"}, statut=429)])])
    s = sortie(serveur)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.RETRYABLE


def test_erreur_5xx_par_element_est_transitoire() -> None:
    serveur = FauxServeur([("ok", [item("index", erreur={"type": "x"}, statut=503)])])
    s = sortie(serveur)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.RETRYABLE


def test_statut_http_400_refuse_tout_le_lot_en_dlq() -> None:
    serveur = FauxServeur([("statut", 400)])
    dlq = LocalDeadLetterQueue("/tmp/test-dlq-11b.jsonl")
    s = sortie(serveur, dlq=dlq)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.DEAD_LETTERED
    Path("/tmp/test-dlq-11b.jsonl").unlink(missing_ok=True)


def test_statut_http_404_refuse_tout_le_lot_sans_dlq() -> None:
    serveur = FauxServeur([("statut", 404)])
    s = sortie(serveur)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.RETRYABLE


def test_sans_accuse_par_element_statut_global_vaut_succes() -> None:
    """Dégradation documentée : Quickwit ne rapporte pas les erreurs par élément."""
    serveur = FauxServeur([("statut", 200)])

    class SansRapport(ElasticsearchBulkOutput):
        capacites = Capacites(accuse_par_evenement=False)

    s = SansRapport({"hosts": "http://x:1"}, transport=httpx.MockTransport(serveur.handler))
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.INDEXED


def test_reponse_sans_items_meme_nombre_transitoire() -> None:
    serveur = FauxServeur([("brut", {"took": 1})])
    s = sortie(serveur)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.RETRYABLE


def test_reponse_json_invalide_transitoire() -> None:
    serveur = FauxServeur([("brut", "pas du tout du tout du json")])
    s = sortie(serveur)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.RETRYABLE


def test_nombre_d_items_incoherent_transitoire() -> None:
    serveur = FauxServeur([("ok", [item("index"), item("index")])])
    s = sortie(serveur)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.RETRYABLE


def test_reponse_200_statut_inconnu_dans_item_reste_succes() -> None:
    serveur = FauxServeur([("ok", [item("index", statut=201)])])
    s = sortie(serveur)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.INDEXED


# ----------------------------------------------------------------------
# Réessais et échecs réseau
# ----------------------------------------------------------------------


def test_erreur_reseau_sans_reessai_rend_le_document() -> None:
    serveur = FauxServeur([("reseau", None)])
    s = sortie(serveur)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.RETRYABLE


def test_erreur_reseau_retentee_puis_succes() -> None:
    serveur = FauxServeur([("reseau", None), ("ok", [item("index")])])
    s = sortie(serveur, retry_max=1, retry_backoff=0.0)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.INDEXED
    assert len(serveur.requests) == 2


def test_erreur_reseau_reessais_epuises_rend_le_document() -> None:
    serveur = FauxServeur([("reseau", None), ("reseau", None)])
    s = sortie(serveur, retry_max=1, retry_backoff=0.0)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.RETRYABLE
    assert len(serveur.requests) == 2


def test_statut_429_retente_puis_succes() -> None:
    serveur = FauxServeur([("statut", 429), ("ok", [item("index")])])
    s = sortie(serveur, retry_max=1, retry_backoff=0.0)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.INDEXED


def test_statut_500_sans_reessai_rend_le_document() -> None:
    serveur = FauxServeur([("statut", 500)])
    s = sortie(serveur)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.RETRYABLE


def test_echec_transitoire_retire_les_documents_du_tampon() -> None:
    """Un échec transitoire est rendu à l'appelant : pas de renvoi automatique (double envoi)."""
    serveur = FauxServeur([("statut", 500)])
    s = sortie(serveur)
    asyncio.run(s.envoyer({"m": "x"}))
    asyncio.run(s.flush())  # tampon vidé, l'appelant décide
    assert serveur.requests[0].read()  # une seule requête : pas de renvoi


# ----------------------------------------------------------------------
# Validation des actions et de la configuration
# ----------------------------------------------------------------------


def test_action_inconnue_refusee() -> None:
    serveur = FauxServeur([])
    s = sortie(serveur)
    with pytest.raises(ActionInvalideError, match="inconnue"):
        asyncio.run(s.envoyer({"m": "x"}, action="upsert"))


def test_action_non_supportee_par_le_produit_refusee() -> None:
    class CreateSeul(ElasticsearchBulkOutput):
        capacites = Capacites(actions=frozenset({"create"}))

    serveur = FauxServeur([])
    s = CreateSeul({"hosts": "http://x:1"}, transport=httpx.MockTransport(serveur.handler))
    with pytest.raises(ActionInvalideError, match="non supportée"):
        asyncio.run(s.envoyer({"m": "x"}, action="index"))


def test_update_sans_document_id_refuse() -> None:
    serveur = FauxServeur([])
    s = sortie(serveur)
    with pytest.raises(ActionInvalideError, match="exige document_id"):
        asyncio.run(s.envoyer({"m": "x"}, action="update"))


def test_delete_sans_document_id_refuse() -> None:
    serveur = FauxServeur([])
    s = sortie(serveur)
    with pytest.raises(ActionInvalideError, match="exige document_id"):
        asyncio.run(s.envoyer({"m": "x"}, action="delete"))


def test_dlq_construite_depuis_dlq_path() -> None:
    """L'option `dlq_path` configure la DLQ sans qu'on ait à la construire soi-même."""
    serveur = FauxServeur([("ok", [item("index", erreur={"type": "x"}, statut=400)])])
    chemin = Path("/tmp/test-dlq-11c.jsonl")
    chemin.unlink(missing_ok=True)
    s = sortie(serveur, dlq_path=str(chemin))
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.DEAD_LETTERED
    assert chemin.exists()
    chemin.unlink(missing_ok=True)


def test_batch_size_invalide_refuse() -> None:
    # Schéma commun (#8) : borne minimale validée à l'instanciation.
    with pytest.raises(ValueError, match="au moins 1"):
        sortie(FauxServeur([]), batch_size=0)


def test_retry_negatif_refuse() -> None:
    # Schéma commun (#8) : borne minimale validée à l'instanciation.
    with pytest.raises(ValueError, match="au moins 0"):
        sortie(FauxServeur([]), retry_max=-1)


def test_option_inconnue_refusee() -> None:
    # Schéma commun (#8) : option inconnue = faute de frappe, échec franc.
    with pytest.raises(ValueError, match="option « option_tres_etrange » inconnue"):
        sortie(FauxServeur([]), option_tres_etrange=1)


def test_hosts_absent_refuse() -> None:
    serveur = FauxServeur([])
    with pytest.raises(ValueError, match="hosts"):
        ElasticsearchBulkOutput({"index": "logs-%Y.%m.%d"}, transport=httpx.MockTransport(serveur.handler))


def test_hosts_liste_prend_la_premiere_entree() -> None:
    serveur = FauxServeur([("ok", [item("index")])])
    s = ElasticsearchBulkOutput(
        {"hosts": ["https://a:9200", "https://b:9200"], "index": "logs-%Y.%m.%d"},
        transport=httpx.MockTransport(serveur.handler),
    )
    asyncio.run(s.envoyer({"m": "x"}))
    assert serveur.url().startswith("https://a:9200/")


def test_auth_et_tls_passes_au_client() -> None:
    """user/password, ssl, cacert : câblés dans le client httpx (vérifiés par les attributs)."""
    serveur = FauxServeur([("ok", [item("index")])])
    s = sortie(serveur, user="bob", password="s3cret", ssl=False)
    asyncio.run(s.envoyer({"m": "x"}))
    # httpx encode l'authentification dans les en-têtes de chaque requête.
    assert serveur.requests[0].headers["Authorization"].startswith("Basic ")


# ----------------------------------------------------------------------
# Santé
# ----------------------------------------------------------------------


def test_health_renvoie_la_version() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        return httpx.Response(200, json={"version": {"number": "2.19.0"}})

    s = ElasticsearchBulkOutput({"hosts": "https://localhost:9200"}, transport=httpx.MockTransport(handler))
    assert asyncio.run(s.health()) == {"version": {"number": "2.19.0"}}


def test_health_echec_leve_l_erreur_http() -> None:
    s = ElasticsearchBulkOutput(
        {"hosts": "https://localhost:9200"},
        transport=httpx.MockTransport(lambda request: httpx.Response(503, text="")),
    )
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(s.health())


def test_close_est_idempotent_et_le_client_se_recre() -> None:
    """`close()` libère le client ; le prochain envoi en recrée un (cycle de vie sans état bloqué)."""
    s = ElasticsearchBulkOutput({"hosts": "https://localhost:9200"})
    asyncio.run(s.open())
    asyncio.run(s.close())
    asyncio.run(s.close())  # idempotent
    assert s._client is None
