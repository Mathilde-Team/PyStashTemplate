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
from pystashtemplate.outputs.elasticsearch import ElasticsearchOutput
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput
from pystashtemplate.outputs.opensearch import OpenSearchOutput
from pystashtemplate.outputs.victorialogs import VictoriaLogsOutput


def _paires_ndjson(corps: bytes) -> int:
    """Documents d'un corps `_bulk` : une ligne d'action + une ligne de document."""
    return len(corps.decode("utf-8").splitlines()) // 2


def _lignes_json(corps: bytes) -> int:
    """Documents d'un corps JSON Lines : une ligne par document."""
    return len(corps.decode("utf-8").splitlines())


@dataclass(frozen=True)
class FixtureSortie:
    """Ce qu'il faut savoir d'une sortie pour lui faire passer le banc.

    Attributs:
        classe: Classe de la sortie (inscrite au registre).
        options: Options minimales propres à la sortie (en plus de
            `hosts`, `batch_size`, `retry_max`, `retry_backoff`).
        compter: Nombre de documents contenus dans le corps d'une requête.
    """

    classe: type[ElasticsearchBulkOutput]
    options: dict[str, Any] = field(default_factory=dict)
    compter: Callable[[bytes], int] = _paires_ndjson

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
    FixtureSortie(VictoriaLogsOutput),
    FixtureSortie(VictoriaLogsOutput, {"mode": "jsonline"}, compter=_lignes_json),
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
        actions = [next(iter(json.loads(ligne))) for ligne in corps.decode("utf-8").splitlines()[0::2]]
        items: list[dict[str, Any]] = []
        for i, action in enumerate(actions):
            if i == refuse:
                items.append({action: {"status": 400, "error": {"type": "mapper_parsing_exception"}}})
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
            return httpx.Response(400, json={"error": "lot refusé"})
        if not par_element:
            return httpx.Response(200, json={})
        refuse = 1 if etape == "partiel" else None
        return httpx.Response(200, json={"errors": refuse is not None, "items": self._items(request.content, refuse)})


def _sortie(
    fixture: FixtureSortie,
    serveur: FauxServeurGenerique,
    *,
    dlq: LocalDeadLetterQueue | None = None,
    **options: Any,
) -> ElasticsearchBulkOutput:
    """Sortie de la fixture branchée sur le faux serveur."""
    return fixture.classe(
        {"hosts": "http://faux:9200", "retry_backoff": 0, **fixture.options, **options},
        transport=httpx.MockTransport(serveur.handler),
        dlq=dlq,
    )


def _envoyer(sortie: ElasticsearchBulkOutput, documents: list[dict[str, Any]], *, flush: bool = False) -> list[str]:
    """Envoie les documents (puis `flush` si demandé) et ferme, dans une même boucle."""

    async def scenario() -> list[str]:
        try:
            resultats = [(await sortie.envoyer(dict(doc))).value for doc in documents]
            if flush:
                await sortie.flush()
            return resultats
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
    resultats = _envoyer(_sortie(fixture, serveur, batch_size=3), _documents(3))
    assert resultats == ["en_attente", "en_attente", "indexed"]
    assert len(serveur.requetes) == 1
    assert serveur.documents_recus == 3


@parametre
def test_erreur_partielle_en_dlq_selon_l_accuse(fixture: FixtureSortie, tmp_path: Path) -> None:
    serveur = FauxServeurGenerique(fixture, "partiel")
    chemin = tmp_path / "dlq.jsonl"
    documents = _documents(3)
    _envoyer(_sortie(fixture, serveur, batch_size=3, dlq=LocalDeadLetterQueue(str(chemin))), documents)
    en_dlq = [d["message"] for d in _dlq_documents(chemin)]
    if fixture.classe.capacites.accuse_par_evenement:
        # Accusé par élément : seul le document refusé part en DLQ.
        assert en_dlq == ["m1"]
    else:
        # Accusé global (dégradation documentée) : tout le lot.
        assert en_dlq == ["m0", "m1", "m2"]


@parametre
def test_429_retente_puis_reussit(fixture: FixtureSortie) -> None:
    serveur = FauxServeurGenerique(fixture, "429", "ok")
    assert _envoyer(_sortie(fixture, serveur, retry_max=1), _documents(1)) == ["indexed"]
    assert len(serveur.requetes) == 2


@parametre
def test_429_epuise_rendu_a_l_appelant_sans_dlq(fixture: FixtureSortie, tmp_path: Path) -> None:
    serveur = FauxServeurGenerique(fixture, "429")
    chemin = tmp_path / "dlq.jsonl"
    sortie = _sortie(fixture, serveur, retry_max=1, dlq=LocalDeadLetterQueue(str(chemin)))
    assert _envoyer(sortie, _documents(1)) == ["retryable"]
    assert len(serveur.requetes) == 2
    assert _dlq_documents(chemin) == []


@parametre
def test_coupure_reseau_retente_puis_reussit(fixture: FixtureSortie) -> None:
    serveur = FauxServeurGenerique(fixture, "reseau", "ok")
    assert _envoyer(_sortie(fixture, serveur, retry_max=1), _documents(1)) == ["indexed"]
    assert len(serveur.requetes) == 2


@parametre
def test_coupure_reseau_epuisee_rendue_a_l_appelant(fixture: FixtureSortie, tmp_path: Path) -> None:
    serveur = FauxServeurGenerique(fixture, "reseau")
    chemin = tmp_path / "dlq.jsonl"
    sortie = _sortie(fixture, serveur, dlq=LocalDeadLetterQueue(str(chemin)))
    assert _envoyer(sortie, _documents(1)) == ["retryable"]
    assert _dlq_documents(chemin) == []


@parametre
def test_arret_propre_pousse_le_tampon(fixture: FixtureSortie) -> None:
    serveur = FauxServeurGenerique(fixture, "ok")
    resultats = _envoyer(_sortie(fixture, serveur, batch_size=10), _documents(2), flush=True)
    assert resultats == ["en_attente", "en_attente"]
    assert len(serveur.requetes) == 1
    assert serveur.documents_recus == 2


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
