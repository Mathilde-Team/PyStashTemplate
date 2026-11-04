# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité commun des sorties HTTP (issue #9).

Une même suite de cas — lot simple, erreur partielle, 429, coupure
réseau, arrêt propre — est rejouée contre chaque sortie de la famille
HTTP, sans réseau : un faux serveur générique (`httpx.MockTransport`)
joue un scénario d'étapes et répond selon les capacités déclarées de la
sortie (accusé par élément `items[]` ou statut global seul).

Ajouter une sortie au banc = ajouter UNE entrée à `FIXTURES` ; un test
vérifie que toute sortie HTTP du registre y figure. Les bancs contre de
vrais systèmes en conteneurs restent des jobs CI séparés
(`.github/workflows/conformite-*.yml`) pour garder la CI principale
rapide.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.clickhouse import ClickHouseOutput
from pystashtemplate.outputs.doris import DorisOutput
from pystashtemplate.outputs.elasticsearch import ElasticsearchOutput
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput
from pystashtemplate.outputs.gelf import GelfOutput
from pystashtemplate.outputs.greptimedb import GreptimeDbOutput
from pystashtemplate.outputs.http_generique import HttpOutput
from pystashtemplate.outputs.loki import LokiOutput
from pystashtemplate.outputs.manticore import ManticoreOutput
from pystashtemplate.outputs.metriques import CompteursSortie
from pystashtemplate.outputs.openobserve import OpenObserveOutput
from pystashtemplate.outputs.opensearch import OpenSearchOutput
from pystashtemplate.outputs.otlp import OtlpOutput
from pystashtemplate.outputs.parseable import ParseableOutput
from pystashtemplate.outputs.quickwit import QuickwitOutput
from pystashtemplate.outputs.siglens import SigLensOutput
from pystashtemplate.outputs.victorialogs import VictoriaLogsOutput
from pystashtemplate.outputs.zincsearch import ZincSearchOutput


def _paires_ndjson(corps: bytes) -> int:
    """Documents d'un corps `_bulk` : une ligne d'action + une ligne de document."""
    return len(corps.decode("utf-8").splitlines()) // 2


def _journaux_otlp(corps: bytes) -> int:
    """Journaux d'un corps OTLP JSON : `logRecords` de toutes les ressources."""
    requete = json.loads(corps)
    return sum(len(sl["logRecords"]) for rl in requete["resourceLogs"] for sl in rl["scopeLogs"])


def _lignes_json(corps: bytes) -> int:
    """Documents d'un corps JSON Lines : une ligne par document."""
    return len(corps.decode("utf-8").splitlines())


def _tableau_json(corps: bytes) -> int:
    """Documents d'un corps tableau JSON (Parseable, Doris) : un élément par document."""
    return len(json.loads(corps))


def _lignes_push_loki(corps: bytes) -> int:
    """Documents d'un corps push Loki : les lignes de tous les flux."""
    streams = json.loads(corps)["streams"]
    return sum(len(flux["values"]) for flux in streams)


@dataclass(frozen=True)
class FixtureSortie:
    """Ce qu'il faut savoir d'une sortie pour lui faire passer le banc.

    Attributs:
        classe: Classe de la sortie (inscrite au registre).
        options: Options minimales propres à la sortie (en plus de
            `hosts`, `batch_size`, `retry_max`, `retry_backoff`).
        compter: Nombre de documents contenus dans le corps d'une requête.
        corps_ok: Corps d'une réponse `ok` à accusé global (`{}` ; Doris
            donne son statut dans le corps, #27).
        rejet: Statut et corps d'un lot refusé à accusé global (400 ;
            Doris répond 200 avec `Status: Fail`).
    """

    classe: type[ElasticsearchBulkOutput]
    options: dict[str, Any] = field(default_factory=dict)
    compter: Callable[[bytes], int] = _paires_ndjson
    corps_ok: dict[str, Any] = field(default_factory=dict)
    rejet: tuple[int, dict[str, Any]] = (400, {"error": "lot refusé"})

    @property
    def id(self) -> str:
        """Identifiant lisible du cas paramétré (nom de plugin + options)."""
        suffixe = ",".join(f"{k}={v}" for k, v in sorted(self.options.items()))
        return f"{self.classe.plugin}[{suffixe}]" if suffixe else self.classe.plugin


# Une entrée par sortie HTTP (ou par mode d'une sortie).
FIXTURES: list[FixtureSortie] = [
    FixtureSortie(OpenSearchOutput),
    FixtureSortie(ElasticsearchOutput),
    FixtureSortie(ElasticsearchOutput, {"data_stream": True}),
    FixtureSortie(QuickwitOutput),
    FixtureSortie(GreptimeDbOutput),
    FixtureSortie(ManticoreOutput),
    FixtureSortie(SigLensOutput),
    FixtureSortie(OpenObserveOutput),
    FixtureSortie(OtlpOutput, {"encoding": "json"}, compter=_journaux_otlp),
    FixtureSortie(VictoriaLogsOutput),
    FixtureSortie(VictoriaLogsOutput, {"mode": "jsonline"}, compter=_lignes_json),
    FixtureSortie(ZincSearchOutput),
    FixtureSortie(LokiOutput, {"labels": {"host": "%{host}"}}, compter=_lignes_push_loki),
    FixtureSortie(ClickHouseOutput, {"table": "logs"}, compter=_lignes_json),
    FixtureSortie(HttpOutput, {"url": "http://faux:9200/logs"}, compter=_lignes_json),
    FixtureSortie(ParseableOutput, {"stream": "banc"}, compter=_tableau_json),
    FixtureSortie(
        DorisOutput,
        {"database": "banc", "table": "logs"},
        compter=_tableau_json,
        corps_ok={"Status": "Success", "NumberLoadedRows": 1},
        rejet=(200, {"Status": "Fail", "Message": "[DATA_QUALITY_ERROR]too many filtered rows"}),
    ),
    FixtureSortie(GelfOutput, compter=_lignes_json),
]


class FauxServeurGenerique:
    """Faux serveur qui joue un scénario d'étapes, une par requête reçue.

    Étapes : ``ok`` (tout accepté), ``partiel`` (le 2e document refusé
    définitivement — ou tout le lot si l'accusé est global), ``429``
    (trop de requêtes), ``reseau`` (coupure : `httpx.ConnectError`).
    La dernière étape se répète une fois le scénario épuisé.
    """

    def __init__(self, fixture: FixtureSortie, *etapes: str) -> None:
        self.fixture = fixture
        self.etapes = list(etapes) or ["ok"]
        self.requetes: list[httpx.Request] = []

    @property
    def documents_recus(self) -> int:
        """Total des documents contenus dans les requêtes reçues."""
        return sum(self.fixture.compter(r.content) for r in self.requetes)

    def _items(self, corps: bytes, refuse: int | None) -> list[dict[str, Any]]:
        """`items[]` d'une réponse `_bulk` : un statut par ligne d'action."""
        lignes = corps.decode("utf-8").splitlines()
        actions = [next(iter(json.loads(ligne))) for ligne in lignes[0::2]]
        items: list[dict[str, Any]] = []
        for i, action in enumerate(actions):
            if i == refuse:
                # `originalRecord` : le document refusé, comme OpenObserve le
                # renvoie (champ ignoré par les autres produits).
                erreur = {"status": 400, "error": {"type": "mapper_parsing_exception"}}
                items.append({action: {**erreur, "originalRecord": json.loads(lignes[2 * i + 1])}})
            else:
                items.append({action: {"status": 201}})
        return items

    def handler(self, request: httpx.Request) -> httpx.Response:
        etape = self.etapes[min(len(self.requetes), len(self.etapes) - 1)]
        self.requetes.append(request)
        par_element = self.fixture.classe.capacites.accuse_par_evenement
        if etape == "reseau":
            raise httpx.ConnectError("coupure réseau simulée", request=request)
        if etape == "429":
            return httpx.Response(429, json={"error": "trop de requêtes"})
        if etape == "partiel" and not par_element:
            statut, corps = self.fixture.rejet
            return httpx.Response(statut, json=corps)
        if not par_element:
            return httpx.Response(200, json=self.fixture.corps_ok)
        refuse = 1 if etape == "partiel" else None
        return httpx.Response(200, json={"errors": refuse is not None, "items": self._items(request.content, refuse)})


def _sortie(
    fixture: FixtureSortie,
    serveur: FauxServeurGenerique,
    *,
    dlq: LocalDeadLetterQueue | None = None,
    **options: Any,
) -> ElasticsearchBulkOutput:
    """Sortie de la fixture branchée sur le faux serveur, ses métriques en mémoire (#10)."""
    sortie = fixture.classe(
        {"hosts": "http://faux:9200", "retry_backoff": 0, **fixture.options, **options},
        transport=httpx.MockTransport(serveur.handler),
        dlq=dlq,
    )
    sortie.observateur = CompteursSortie()
    return sortie


def _metriques(sortie: ElasticsearchBulkOutput) -> dict[str, object]:
    """Métriques de la sortie (vides si elle n'a rien observé)."""
    assert isinstance(sortie.observateur, CompteursSortie)
    return sortie.observateur.instantane().get(sortie.nom, {})


def _issues(sortie: ElasticsearchBulkOutput) -> dict[str, object]:
    """Compteurs d'issues non nuls de la sortie."""
    metriques = _metriques(sortie)
    return {k: metriques[k] for k in ("indexed", "dead_lettered", "retryable", "rejected") if metriques.get(k)}


def _envoyer(sortie: ElasticsearchBulkOutput, documents: list[dict[str, Any]]) -> list[str]:
    """Envoie les documents et ferme, dans une même boucle."""

    async def scenario() -> list[str]:
        try:
            return [(await sortie.envoyer(dict(doc))).value for doc in documents]
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _documents(n: int) -> list[dict[str, Any]]:
    """`n` documents distincts, horodatés (exigé par les data streams et VictoriaLogs)."""
    return [{"message": f"m{i}", "@timestamp": "2026-11-04T10:00:00Z", "app": "banc"} for i in range(n)]


def _dlq_documents(chemin: Path) -> list[dict[str, Any]]:
    """Documents écrits dans la DLQ (vide si le fichier n'existe pas)."""
    if not chemin.exists():
        return []
    return [json.loads(ligne)["document"] for ligne in chemin.read_text(encoding="utf-8").splitlines()]


parametre = pytest.mark.parametrize("fixture", FIXTURES, ids=[f.id for f in FIXTURES])


def test_toute_sortie_http_du_registre_a_sa_fixture() -> None:
    couvertes = {f.classe for f in FIXTURES}
    http = {c for c in REGISTRY.values() if issubclass(c, ElasticsearchBulkOutput)}
    assert http <= couvertes, f"sorties HTTP sans fixture de conformité : {sorted(c.plugin for c in http - couvertes)}"


@parametre
def test_lot_simple_une_requete_pour_le_lot(fixture: FixtureSortie) -> None:
    serveur = FauxServeurGenerique(fixture, "ok")
    sortie = _sortie(fixture, serveur, batch_size=3)
    resultats = _envoyer(sortie, _documents(3))
    assert resultats == ["en_attente", "en_attente", "indexed"]
    assert len(serveur.requetes) == 1
    assert serveur.documents_recus == 3
    assert _issues(sortie) == {"indexed": 3}
    assert _metriques(sortie)["requetes"] == 1
    assert _metriques(sortie)["reessais"] == 0
    assert _metriques(sortie)["file"] == 0


@parametre
def test_erreur_partielle_en_dlq_selon_l_accuse(fixture: FixtureSortie, tmp_path: Path) -> None:
    serveur = FauxServeurGenerique(fixture, "partiel")
    chemin = tmp_path / "dlq.jsonl"
    documents = _documents(3)
    sortie = _sortie(fixture, serveur, batch_size=3, dlq=LocalDeadLetterQueue(str(chemin)))
    _envoyer(sortie, documents)
    en_dlq = [d["message"] for d in _dlq_documents(chemin)]
    if fixture.classe.capacites.accuse_par_evenement:
        # Accusé par élément : seul le document refusé part en DLQ.
        assert en_dlq == ["m1"]
        assert _issues(sortie) == {"indexed": 2, "dead_lettered": 1, "rejected": 1}
    else:
        # Accusé global (dégradation documentée) : tout le lot.
        assert en_dlq == ["m0", "m1", "m2"]
        assert _issues(sortie) == {"dead_lettered": 3, "rejected": 3}


@parametre
def test_429_retente_puis_reussit(fixture: FixtureSortie) -> None:
    serveur = FauxServeurGenerique(fixture, "429", "ok")
    sortie = _sortie(fixture, serveur, retry_max=1)
    assert _envoyer(sortie, _documents(1)) == ["indexed"]
    assert len(serveur.requetes) == 2
    assert _issues(sortie) == {"indexed": 1}
    assert _metriques(sortie)["reessais"] == 1
    assert _metriques(sortie)["requetes"] == 2


@parametre
def test_429_epuise_rendu_a_l_appelant_sans_dlq(fixture: FixtureSortie, tmp_path: Path) -> None:
    serveur = FauxServeurGenerique(fixture, "429")
    chemin = tmp_path / "dlq.jsonl"
    sortie = _sortie(fixture, serveur, retry_max=1, dlq=LocalDeadLetterQueue(str(chemin)))
    assert _envoyer(sortie, _documents(1)) == ["retryable"]
    assert len(serveur.requetes) == 2
    assert _dlq_documents(chemin) == []
    assert _issues(sortie) == {"retryable": 1}
    assert _metriques(sortie)["reessais"] == 1


@parametre
def test_coupure_reseau_retente_puis_reussit(fixture: FixtureSortie) -> None:
    serveur = FauxServeurGenerique(fixture, "reseau", "ok")
    sortie = _sortie(fixture, serveur, retry_max=1)
    assert _envoyer(sortie, _documents(1)) == ["indexed"]
    assert len(serveur.requetes) == 2
    assert _issues(sortie) == {"indexed": 1}
    assert _metriques(sortie)["reessais"] == 1
    # La requête coupée compte aussi dans la latence.
    assert _metriques(sortie)["requetes"] == 2


@parametre
def test_coupure_reseau_epuisee_rendue_a_l_appelant(fixture: FixtureSortie, tmp_path: Path) -> None:
    serveur = FauxServeurGenerique(fixture, "reseau")
    chemin = tmp_path / "dlq.jsonl"
    sortie = _sortie(fixture, serveur, dlq=LocalDeadLetterQueue(str(chemin)))
    assert _envoyer(sortie, _documents(1)) == ["retryable"]
    assert _dlq_documents(chemin) == []
    assert _issues(sortie) == {"retryable": 1}
    assert _metriques(sortie)["reessais"] == 0


@parametre
def test_arret_propre_pousse_le_tampon(fixture: FixtureSortie) -> None:
    serveur = FauxServeurGenerique(fixture, "ok")
    sortie = _sortie(fixture, serveur, batch_size=10)

    async def tampon_puis_arret() -> int:
        try:
            for doc in _documents(2):
                assert (await sortie.envoyer(doc)).value == "en_attente"
            file = _metriques(sortie)["file"]
            await sortie.flush()
            return int(str(file))
        finally:
            await sortie.close()

    assert asyncio.run(tampon_puis_arret()) == 2
    assert len(serveur.requetes) == 1
    assert serveur.documents_recus == 2
    assert _issues(sortie) == {"indexed": 2}
    assert _metriques(sortie)["file"] == 0


@parametre
def test_arret_sans_tampon_ne_fait_aucune_requete_et_close_idempotent(fixture: FixtureSortie) -> None:
    serveur = FauxServeurGenerique(fixture, "ok")
    sortie = _sortie(fixture, serveur)

    async def scenario() -> None:
        await sortie.open()
        await sortie.flush()
        await sortie.close()
        await sortie.close()

    asyncio.run(scenario())
    assert serveur.requetes == []
    assert _metriques(sortie) == {}
