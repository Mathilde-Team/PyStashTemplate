# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie Apache Doris (issue #27) : Stream Load, sans réseau.

Un faux FE/BE (`httpx.MockTransport`) rejoue les réponses documentées de
Stream Load : redirection `307` du FE vers un BE, statut dans le corps
(`Success`, `Publish Timeout`, `Label Already Exists`, `Fail`), lignes
filtrées. Le banc contre un vrai Doris est
`tests/test_integration_doris.py` (`.github/workflows/conformite-doris.yml`).
"""

from __future__ import annotations

import asyncio
import base64
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.doris import AuthStreamLoad, DorisOutput, raison_du_refus
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome

OPTIONS = {"hosts": "http://fe:8030", "database": "logs", "table": "evenements", "retry_backoff": 0}
SUCCES = {"Status": "Success", "NumberLoadedRows": 1, "NumberFilteredRows": 0}


def _sortie(handler: Callable[[httpx.Request], httpx.Response], *, dlq: Any = None, **options: Any) -> DorisOutput:
    return DorisOutput({**OPTIONS, **options}, transport=httpx.MockTransport(handler), dlq=dlq)


def _envoyer(sortie: DorisOutput, documents: list[dict[str, Any]]) -> list[str]:
    async def scenario() -> list[str]:
        try:
            resultats = [(await sortie.envoyer(dict(doc))).value for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _docs(n: int) -> list[dict[str, Any]]:
    return [{"message": f"m{i}", "host": "h1"} for i in range(n)]


def _basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode("ascii")


def _dlq(chemin: Path) -> list[dict[str, Any]]:
    if not chemin.exists():
        return []
    return [json.loads(ligne) for ligne in chemin.read_text(encoding="utf-8").splitlines()]


# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------


def test_inscrite_au_registre() -> None:
    assert REGISTRY["doris"] is DorisOutput


@pytest.mark.parametrize("manquante", ["database", "table"])
def test_base_et_table_requises(manquante: str) -> None:
    options = {k: v for k, v in OPTIONS.items() if k != manquante}
    with pytest.raises(ValueError, match=manquante):
        DorisOutput(options)


@pytest.mark.parametrize("option", ["index", "action", "pipeline", "routing", "doc_as_upsert"])
def test_options_bulk_refusees(option: str) -> None:
    with pytest.raises(ValueError):
        DorisOutput({**OPTIONS, option: "x" if option != "doc_as_upsert" else True})


@pytest.mark.parametrize("prefixe", ["avec espace", "accentué", "a" * 96, "point.interdit"])
def test_label_prefix_invalide(prefixe: str) -> None:
    with pytest.raises(ValueError, match="label_prefix"):
        DorisOutput({**OPTIONS, "label_prefix": prefixe})


def test_max_filter_ratio_borne() -> None:
    with pytest.raises(ValueError, match="max_filter_ratio"):
        DorisOutput({**OPTIONS, "max_filter_ratio": 1.5})
    with pytest.raises(ValueError):
        DorisOutput({**OPTIONS, "max_filter_ratio": -0.1})
    assert DorisOutput({**OPTIONS, "max_filter_ratio": 1})._max_filter_ratio == 1.0


# ---------------------------------------------------------------------------
# Requête
# ---------------------------------------------------------------------------


def test_requete_stream_load() -> None:
    """PUT sur _stream_load, tableau JSON, en-têtes Stream Load, label préfixé."""
    requetes: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requetes.append(request)
        return httpx.Response(200, json=SUCCES)

    sortie = _sortie(handler, batch_size=2, label_prefix="banc-1", max_filter_ratio=0.25)
    assert _envoyer(sortie, _docs(2)) == ["en_attente", "indexed"]

    (requete,) = requetes
    assert requete.method == "PUT"
    assert requete.url.path == "/api/logs/evenements/_stream_load"
    assert json.loads(requete.content) == _docs(2)
    assert requete.headers["Expect"] == "100-continue"
    assert requete.headers["format"] == "json"
    assert requete.headers["strip_outer_array"] == "true"
    assert requete.headers["max_filter_ratio"] == "0.25"
    assert requete.headers["Content-Type"] == "application/json"
    prefixe, _, suffixe = requete.headers["label"].rpartition("_")
    assert prefixe == "banc-1"
    assert len(suffixe) == 32
    assert "Authorization" not in requete.headers


def test_label_stable_sur_retente_nouveau_par_lot() -> None:
    """Une retente (429) garde le label du lot ; le lot suivant en prend un autre."""
    labels: list[str] = []
    statuts = iter([429, 200, 200])

    def handler(request: httpx.Request) -> httpx.Response:
        labels.append(request.headers["label"])
        statut = next(statuts)
        return httpx.Response(statut, json=SUCCES if statut == 200 else {})

    sortie = _sortie(handler, retry_max=1)
    assert _envoyer(sortie, _docs(2)) == ["indexed", "indexed"]
    assert labels[0] == labels[1]
    assert labels[2] != labels[0]


def test_en_tetes_d_options_conserves() -> None:
    """Les `headers` du .conf partent avec chaque requête (colonnes, timezone…)."""
    vus: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        vus.append(request)
        return httpx.Response(200, json=SUCCES)

    sortie = _sortie(handler, headers={"timezone": "Europe/Paris"})
    _envoyer(sortie, _docs(1))
    assert vus[0].headers["timezone"] == "Europe/Paris"


# ---------------------------------------------------------------------------
# Redirection FE → BE
# ---------------------------------------------------------------------------


def _fe_be(sauts: int = 1, location: str = "http://be:8040/api/logs/evenements/_stream_load?x=1"):
    requetes: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requetes.append(request)
        if len(requetes) <= sauts:
            return httpx.Response(307, headers={"Location": location})
        return httpx.Response(200, json=SUCCES)

    return handler, requetes


def test_redirection_garde_l_authentification() -> None:
    handler, requetes = _fe_be()
    sortie = _sortie(handler, user="root", password="secret")

    assert _envoyer(sortie, _docs(1)) == ["indexed"]

    fe, be = requetes
    assert fe.url.host == "fe"
    assert be.url.host == "be"
    assert be.url.port == 8040
    assert be.method == "PUT"
    assert be.content == fe.content
    assert be.headers["Authorization"] == _basic("root", "secret")
    assert be.headers["label"] == fe.headers["label"]
    assert be.headers["Host"] == "be:8040"


def test_redirection_relative_sans_identifiants() -> None:
    handler, requetes = _fe_be(location="/api/logs/evenements/_stream_load")
    sortie = _sortie(handler)

    assert _envoyer(sortie, _docs(1)) == ["indexed"]
    assert requetes[1].url.host == "fe"
    assert "Authorization" not in requetes[1].headers


def test_redirections_en_boucle_rendues_a_l_appelant() -> None:
    """Au-delà de trois sauts, le 307 reste la réponse : lot rendu, jamais indexé."""
    handler, requetes = _fe_be(sauts=10)
    sortie = _sortie(handler)

    assert _envoyer(sortie, _docs(1)) == ["retryable"]
    assert len(requetes) == 4


def test_redirection_hors_http_non_suivie() -> None:
    handler, requetes = _fe_be(location="ftp://be/fichier")
    sortie = _sortie(handler)

    assert _envoyer(sortie, _docs(1)) == ["retryable"]
    assert len(requetes) == 1


def test_auth_mot_de_passe_vide() -> None:
    auth = AuthStreamLoad("root", None)
    requete = httpx.Request("PUT", "http://fe:8030/x", content=b"[]")
    signee = auth._signer(requete)
    assert signee.headers["Authorization"] == _basic("root", "")


# ---------------------------------------------------------------------------
# Statut dans le corps
# ---------------------------------------------------------------------------


def _repondre(*corps: Any, statut: int = 200):
    restants = list(corps)
    requetes: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requetes.append(request)
        reponse = restants.pop(0) if len(restants) > 1 else restants[0]
        if isinstance(reponse, httpx.Response):
            return reponse
        return httpx.Response(statut, json=reponse)

    return handler, requetes


@pytest.mark.parametrize(
    "corps",
    [
        {"Status": "Publish Timeout"},
        {"Status": "Label Already Exists", "ExistingJobStatus": "FINISHED", "Label": "l1"},
        {"Status": "Success", "NumberFilteredRows": 2, "ErrorURL": "http://be:8040/api/_load_error_log?f=x"},
    ],
)
def test_statuts_indexes(corps: dict[str, Any]) -> None:
    handler, _ = _repondre(corps)
    assert _envoyer(_sortie(handler), _docs(1)) == ["indexed"]


@pytest.mark.parametrize(
    "reponse",
    [
        {"Status": "Label Already Exists", "ExistingJobStatus": "RUNNING", "Label": "l1"},
        {"Status": "Inconnu"},
        {"pas_de_statut": True},
        ["liste"],
        httpx.Response(200, text="pas du JSON"),
        httpx.Response(401, json={"msg": "Unauthorized"}),
        httpx.Response(503, json={}),
    ],
)
def test_lot_rendu_a_l_appelant(reponse: Any, tmp_path: Path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    handler, _ = _repondre(reponse)
    sortie = _sortie(handler, dlq=LocalDeadLetterQueue(str(chemin)), retry_max=0)

    assert _envoyer(sortie, _docs(1)) == ["retryable"]
    assert _dlq(chemin) == []


def test_fail_document_seul_en_dlq_avec_la_raison(tmp_path: Path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    erreur = {"Status": "Fail", "Message": "[DATA_QUALITY_ERROR]too many filtered rows", "ErrorURL": "http://be/e"}
    handler, requetes = _repondre(erreur)
    sortie = _sortie(handler, dlq=LocalDeadLetterQueue(str(chemin)))

    assert _envoyer(sortie, _docs(1)) == ["dead_lettered"]
    assert len(requetes) == 1
    (entree,) = _dlq(chemin)
    assert entree["document"] == {"message": "m0", "host": "h1"}
    assert "too many filtered rows" in entree["@dlq_error"]
    assert "http://be/e" in entree["@dlq_error"]


def test_fail_lot_renvoye_un_par_un(tmp_path: Path) -> None:
    """Lot atomique refusé : chaque document repart seul, seul le fautif va en DLQ."""
    chemin = tmp_path / "dlq.jsonl"
    labels: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        labels.append(request.headers["label"])
        lignes = json.loads(request.content)
        if any(ligne["message"] == "m1" for ligne in lignes):
            return httpx.Response(200, json={"Status": "Fail", "Message": "[ANALYSIS_ERROR]colonne"})
        return httpx.Response(200, json=SUCCES)

    sortie = _sortie(handler, dlq=LocalDeadLetterQueue(str(chemin)), batch_size=3)

    resultats = _envoyer(sortie, _docs(3))

    assert resultats[-1] == "indexed"  # issue du dernier document du lot
    assert [e["document"]["message"] for e in _dlq(chemin)] == ["m1"]
    assert len(labels) == 4
    assert len(set(labels)) == 4


def test_statut_400_refus_definitif(tmp_path: Path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    handler, _ = _repondre(httpx.Response(400, json={"msg": "bad request"}))
    sortie = _sortie(handler, dlq=LocalDeadLetterQueue(str(chemin)))

    assert _envoyer(sortie, _docs(1)) == ["dead_lettered"]


def test_raison_du_refus() -> None:
    assert raison_du_refus({"Message": "m", "ErrorURL": "u"}) == "m (u)"
    assert raison_du_refus({"msg": "table inconnue"}) == "table inconnue"
    assert raison_du_refus({}) == "Stream Load : Fail"


# ---------------------------------------------------------------------------
# Santé
# ---------------------------------------------------------------------------


def test_health() -> None:
    vues: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        vues.append(request)
        return httpx.Response(200, json={"code": 0, "data": {"online_backend_num": 1}})

    sortie = _sortie(handler, user="root", password="")

    async def scenario() -> dict[str, Any]:
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert asyncio.run(scenario())["data"]["online_backend_num"] == 1
    assert vues[0].url.path == "/api/health"
    assert "label" not in vues[0].headers
    assert vues[0].headers["Authorization"] == _basic("root", "")


def test_health_erreur() -> None:
    sortie = _sortie(lambda request: httpx.Response(503))

    async def scenario() -> None:
        try:
            await sortie.health()
        finally:
            await sortie.close()

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(scenario())


def test_send_outcome_importe() -> None:
    # Les issues de la sortie sont celles du client commun.
    assert SendOutcome.INDEXED.value == "indexed"
