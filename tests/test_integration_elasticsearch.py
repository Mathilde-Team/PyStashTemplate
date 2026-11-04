# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité Elasticsearch (issue #13) : la vraie API `_bulk`, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-elasticsearch.yml`
contre un conteneur `elasticsearch` avec la sécurité active (HTTP sans
TLS, utilisateur `elastic`) : la clé d'API y est réellement créée puis
utilisée. Localement, contre n'importe quel cluster :

    PYSTASHTEMPLATE_ELASTICSEARCH_URL=http://localhost:9200 \\
    PYSTASHTEMPLATE_ELASTICSEARCH_PASSWORD=<mot de passe elastic> \\
    uv run pytest tests/test_integration_elasticsearch.py

Sans `PYSTASHTEMPLATE_ELASTICSEARCH_URL`, le banc est ignoré (les tests
unitaires couvrent déjà le comportement avec un faux serveur). Un seul
`asyncio.run` par test : un client httpx est lié à sa boucle.
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

import httpx
import pytest

from pystashtemplate.outputs.elasticsearch import ElasticsearchOutput

URL = os.environ.get("PYSTASHTEMPLATE_ELASTICSEARCH_URL")
MOT_DE_PASSE = os.environ.get("PYSTASHTEMPLATE_ELASTICSEARCH_PASSWORD", "")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_ELASTICSEARCH_URL non défini")


def _admin() -> httpx.Client:
    """Client d'administration du banc (refresh, recherche, nettoyage)."""
    return httpx.Client(base_url=URL or "", auth=("elastic", MOT_DE_PASSE) if MOT_DE_PASSE else None)


def _options(**extra: object) -> dict[str, object]:
    """Options de la sortie : authentification basique si le banc a un mot de passe."""
    auth: dict[str, object] = {"user": "elastic", "password": MOT_DE_PASSE} if MOT_DE_PASSE else {}
    return {"hosts": URL, **auth, **extra}


def _index(nom: str) -> str:
    """Nom d'index unique par test (nettoyé dans le `finally` du test)."""
    return f"conformite-{nom}-{datetime.now(timezone.utc).strftime('%H%M%S%f')}"


def _envoyer_tous(sortie: ElasticsearchOutput, documents: list[dict], **kwargs: object) -> list[str]:
    """Envoie les documents puis ferme la sortie ; rend la valeur de chaque résultat."""

    async def scenario() -> list[str]:
        try:
            resultats = [(await sortie.envoyer(doc, **kwargs)).value for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _chercher(admin: httpx.Client, cible: str) -> dict:
    """Rend cherchables les documents de la cible, puis les relit."""
    admin.post(f"/{cible}/_refresh").raise_for_status()
    reponse = admin.get(f"/{cible}/_search", params={"size": 10})
    reponse.raise_for_status()
    return reponse.json()


def test_le_cluster_repond_et_version_est_lue() -> None:
    async def scenario() -> None:
        sortie = ElasticsearchOutput(_options(index="logs-%Y.%m.%d"))
        try:
            info = await sortie.health()
            assert info["version"]["number"]
        finally:
            await sortie.close()

    asyncio.run(scenario())


def test_documents_indexes_sont_retrouvables() -> None:
    index = _index("docs")
    with _admin() as admin:
        try:
            resultats = _envoyer_tous(
                ElasticsearchOutput(_options(index=index)), [{"message": f"c-{n}", "numero": n} for n in range(3)]
            )
            assert resultats == ["indexed"] * 3
            corps = _chercher(admin, index)
            assert corps["hits"]["total"]["value"] == 3
            assert {hit["_source"]["message"] for hit in corps["hits"]["hits"]} == {f"c-{n}" for n in range(3)}
        finally:
            admin.delete(f"/{index}")


def test_document_id_deterministe_indexe_une_fois() -> None:
    index = _index("id")
    with _admin() as admin:
        try:
            resultats = _envoyer_tous(
                ElasticsearchOutput(_options(index=index)), [{"message": "stable"}] * 3, document_id="fixe"
            )
            assert resultats == ["indexed"] * 3
            corps = _chercher(admin, index)
            assert corps["hits"]["total"]["value"] == 1
            assert corps["hits"]["hits"][0]["_id"] == "fixe"
        finally:
            admin.delete(f"/{index}")


@pytest.mark.skipif(not MOT_DE_PASSE, reason="clé d'API : banc sans sécurité")
def test_cle_d_api_creee_puis_utilisee() -> None:
    index = _index("apikey")
    with _admin() as admin:
        reponse = admin.post("/_security/api_key", json={"name": index})
        reponse.raise_for_status()
        cle = reponse.json()
        try:
            # Les deux formes : encodée (champ `encoded`) et `id:api_key`.
            for valeur in (cle["encoded"], f"{cle['id']}:{cle['api_key']}"):
                sortie = ElasticsearchOutput({"hosts": URL, "index": index, "api_key": valeur})
                assert _envoyer_tous(sortie, [{"message": "via api_key"}]) == ["indexed"]
            assert _chercher(admin, index)["hits"]["total"]["value"] == 2
        finally:
            admin.request("DELETE", "/_security/api_key", json={"ids": [cle["id"]]})
            admin.delete(f"/{index}")


def test_data_stream_create_dans_le_flux_par_defaut_d_elastic() -> None:
    """Le modèle d'index intégré `logs-*-*` d'Elasticsearch crée le data stream au premier `create`."""
    flux = f"logs-conformite{datetime.now(timezone.utc).strftime('%H%M%S%f')}-default"
    dataset = flux.split("-")[1]
    with _admin() as admin:
        try:
            sortie = ElasticsearchOutput(_options(data_stream=True, data_stream_dataset=dataset))
            documents = [{"@timestamp": datetime.now(timezone.utc).isoformat(), "message": "flux"}]
            assert _envoyer_tous(sortie, documents) == ["indexed"]
            assert admin.get(f"/_data_stream/{flux}").json()["data_streams"][0]["name"] == flux
            assert _chercher(admin, flux)["hits"]["total"]["value"] == 1
        finally:
            admin.delete(f"/_data_stream/{flux}")


def test_echec_de_mapping_va_en_dlq_avec_sortie_et_raison(tmp_path) -> None:  # noqa: ANN001 — fixture pytest
    index = _index("dlq")
    with _admin() as admin:
        admin.put(f"/{index}", json={"mappings": {"properties": {"@timestamp": {"type": "date"}}}}).raise_for_status()
        try:
            dlq = tmp_path / "dlq.jsonl"
            sortie = ElasticsearchOutput(_options(index=index, dlq_path=str(dlq), id="es-banc"))
            assert _envoyer_tous(sortie, [{"@timestamp": "pas-une-date"}]) == ["dead_lettered"]
            ligne = json.loads(dlq.read_text(encoding="utf-8").splitlines()[0])
            assert ligne["@dlq_index"] == index
            assert ligne["@dlq_output"] == "es-banc"
            assert ligne["@dlq_reason"] == "permanent"
            assert ligne["document"] == {"@timestamp": "pas-une-date"}
        finally:
            admin.delete(f"/{index}")
