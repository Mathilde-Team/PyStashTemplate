# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie Parseable (issue #25), avec un faux serveur `httpx.MockTransport`.

Tableau JSON vers `/api/v1/ingest`, en-têtes `X-P-Stream`, `X-API-Key`,
`X-P-Tenant-ID`, `X-P-Tag-*`, décision sur le statut HTTP global,
options propres refusées (`index`), santé `readiness` + `about`.
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
from pystashtemplate.outputs.parseable import ParseableOutput


class FauxServeur:
    """Répond `statut` (200 sans corps par défaut, comme Parseable) et garde les requêtes."""

    def __init__(self, statut: int = 200, texte: str = "") -> None:
        self.statut = statut
        self.texte = texte
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/api/v1/about":
            return httpx.Response(200, json={"version": "v3.2.6", "mode": "Standalone"})
        return httpx.Response(self.statut, text=self.texte)

    def corps(self, i: int = 0) -> list[dict[str, Any]]:
        """Corps JSON de la i-ème requête."""
        return json.loads(self.requests[i].read().decode("utf-8"))


def sortie(serveur: FauxServeur, *, dlq: LocalDeadLetterQueue | None = None, **options: Any) -> ParseableOutput:
    """Sortie branchée sur le faux serveur (stream `rsyslog` par défaut)."""
    return ParseableOutput(
        {"hosts": "http://parseable:8000", "stream": "rsyslog", **options},
        transport=httpx.MockTransport(serveur.handler),
        dlq=dlq,
    )


def envoyer(s: ParseableOutput, documents: list[dict[str, Any]], *, vider: bool = False) -> list[SendOutcome]:
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


def test_inscrite_au_registre() -> None:
    assert REGISTRY["parseable"] is ParseableOutput


def test_capacites_insertions_seules_sans_id_ni_accuse_par_element() -> None:
    capacites = ParseableOutput.capacites
    assert capacites.actions == {"index"}
    assert not capacites.accepte_id
    assert not capacites.accuse_par_evenement
    assert capacites.taille_max_requete == 10 * 1024 * 1024


def test_chemin_en_tetes_et_corps_tableau_json() -> None:
    serveur = FauxServeur()
    doc = {"message": "m", "host": {"name": "h1"}}
    assert envoyer(sortie(serveur), [doc]) == [SendOutcome.INDEXED]
    requete = serveur.requests[0]
    assert requete.method == "POST"
    assert requete.url.path == "/api/v1/ingest"
    assert requete.headers["Content-Type"] == "application/json"
    assert requete.headers["X-P-Stream"] == "rsyslog"
    assert "X-API-Key" not in requete.headers
    assert "X-P-Tenant-ID" not in requete.headers
    assert serveur.corps() == [doc]


def test_lot_de_plusieurs_documents_en_un_tableau() -> None:
    serveur = FauxServeur()
    documents = [{"message": f"m{n}"} for n in range(3)]
    resultats = envoyer(sortie(serveur, batch_size=3), documents)
    assert resultats == [SendOutcome.EN_ATTENTE, SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
    assert len(serveur.requests) == 1
    assert serveur.corps() == documents


def test_tampon_vide_aucune_requete() -> None:
    # Parseable 3.2.6 panique sur un tableau vide : jamais envoyé.
    serveur = FauxServeur()
    assert envoyer(sortie(serveur, batch_size=5), [], vider=True) == []
    assert serveur.requests == []


def test_caracteres_non_ascii_preserves() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur), [{"message": "connexion refusée — échec"}])
    assert "refusée — échec" in serveur.requests[0].read().decode("utf-8")


def test_authentification_basique() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, user="banc", password="secret"), [{"message": "m"}])
    assert serveur.requests[0].headers["Authorization"].startswith("Basic ")


def test_cle_api_locataire_et_etiquettes() -> None:
    serveur = FauxServeur()
    s = sortie(serveur, api_key="px_cle", tenant_id="locataire-1", tags={"env": "prod", "site_2": "hag"})
    envoyer(s, [{"message": "m"}])
    en_tetes = serveur.requests[0].headers
    assert en_tetes["X-API-Key"] == "px_cle"
    assert en_tetes["X-P-Tenant-ID"] == "locataire-1"
    assert en_tetes["X-P-Tag-env"] == "prod"
    assert en_tetes["X-P-Tag-site_2"] == "hag"


def test_en_tetes_d_options_priment() -> None:
    serveur = FauxServeur()
    envoyer(sortie(serveur, headers={"X-P-Stream": "autre"}), [{"message": "m"}])
    assert serveur.requests[0].headers["X-P-Stream"] == "autre"


def test_stream_requis() -> None:
    with pytest.raises(ValueError, match="stream est requis"):
        ParseableOutput({"hosts": "http://parseable:8000"})
    with pytest.raises(ValueError, match="stream est requis"):
        ParseableOutput({"hosts": "http://parseable:8000", "stream": ""})


def test_nom_d_etiquette_invalide_refuse() -> None:
    with pytest.raises(ValueError, match="nom d'étiquette invalide"):
        sortie(FauxServeur(), tags={"mauvais nom": "x"})


@pytest.mark.parametrize("option", ["index", "pipeline", "routing", "doc_as_upsert"])
def test_options_bulk_refusees(option: str) -> None:
    with pytest.raises(ValueError):
        sortie(FauxServeur(), **{option: "x" if option != "doc_as_upsert" else True})


def test_action_autre_qu_index_refusee() -> None:
    with pytest.raises(ValueError):
        sortie(FauxServeur(), action="create")


def test_refus_400_en_dlq(tmp_path: Path) -> None:
    serveur = FauxServeur(400, "Invalid Request: Expected object in array of objects")
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    assert envoyer(sortie(serveur, dlq=dlq), [{"message": "m"}]) == [SendOutcome.DEAD_LETTERED]
    lignes = (tmp_path / "dlq.jsonl").read_text(encoding="utf-8").splitlines()
    assert json.loads(lignes[0])["document"] == {"message": "m"}


def test_refus_400_sans_dlq_rendu_a_l_appelant() -> None:
    serveur = FauxServeur(400, "Json deserialize error")
    assert envoyer(sortie(serveur), [{"message": "m"}]) == [SendOutcome.RETRYABLE]


@pytest.mark.parametrize("statut", [401, 403])
def test_droits_refuses_rendus_a_l_appelant(statut: int, tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    assert envoyer(sortie(FauxServeur(statut, "Invalid API key"), dlq=dlq), [{"message": "m"}]) == [
        SendOutcome.RETRYABLE
    ]
    assert not (tmp_path / "dlq.jsonl").exists()


def test_5xx_retente_puis_rendu() -> None:
    serveur = FauxServeur(503)
    assert envoyer(sortie(serveur, retry_max=2, retry_backoff=0), [{"message": "m"}]) == [SendOutcome.RETRYABLE]
    assert len(serveur.requests) == 3


def test_health_pret_et_version() -> None:
    serveur = FauxServeur()

    async def scenario() -> dict[str, Any]:
        s = sortie(serveur)
        try:
            return await s.health()
        finally:
            await s.close()

    assert asyncio.run(scenario()) == {"status": "ready", "version": "v3.2.6"}
    assert [r.url.path for r in serveur.requests] == ["/api/v1/readiness", "/api/v1/about"]


def test_health_non_pret() -> None:
    serveur = FauxServeur(503)

    async def scenario() -> dict[str, Any]:
        s = sortie(serveur)
        try:
            return await s.health()
        finally:
            await s.close()

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(scenario())
