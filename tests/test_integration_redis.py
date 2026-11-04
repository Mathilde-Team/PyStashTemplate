# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité redis (issue #33) : le vrai serveur, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-redis.yml`
(conteneur Redis) ; localement :

    PYSTASHTEMPLATE_REDIS_HOST=localhost:6379 uv run pytest tests/test_integration_redis.py

Sans `PYSTASHTEMPLATE_REDIS_HOST`, le banc est ignoré. Les messages
poussés sont relus par un client `redis` indépendant (liste, flux, canal).
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from pathlib import Path
from typing import Any

import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs.courtier_redis import RedisOutput
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome

HOTE = os.environ.get("PYSTASHTEMPLATE_REDIS_HOST")
pytestmark = pytest.mark.skipif(not HOTE, reason="banc de conformité : PYSTASHTEMPLATE_REDIS_HOST non défini")


def _client() -> Any:
    """Client de relecture, indépendant de la sortie."""
    from redis.asyncio import Redis

    nom, _, port = str(HOTE).rpartition(":")
    return Redis(host=nom, port=int(port))


async def _envoyer(sortie: RedisOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Envoie les documents, vide le tampon, ferme ; rend l'issue de chacun."""
    try:
        resultats = [(await sortie.envoyer(doc)) for doc in documents]
        await sortie.flush()
        return resultats
    finally:
        await sortie.close()


def test_liste_rpush_relue_dans_l_ordre() -> None:
    cle = f"banc-{uuid.uuid4().hex[:8]}"
    documents = [{"message": f"m{i}", "app": "banc"} for i in range(5)]
    sortie = RedisOutput({"host": HOTE, "key": cle, "batch_size": 5})

    async def scenario() -> list[bytes]:
        assert await _envoyer(sortie, documents) == [SendOutcome.EN_ATTENTE] * 4 + [SendOutcome.INDEXED]
        client = _client()
        try:
            return await client.lrange(cle, 0, -1)
        finally:
            await client.aclose()

    valeurs = [json.loads(v) for v in asyncio.run(scenario())]
    assert [v["message"] for v in valeurs] == [f"m{i}" for i in range(5)]


def test_cle_rendue_par_gabarit_par_document() -> None:
    marque = uuid.uuid4().hex[:8]
    sortie = RedisOutput({"host": ["127.0.0.1:1", HOTE], "key": f"banc-%{{app}}-{marque}", "batch_size": 2})

    async def scenario() -> tuple[int, int]:
        assert (await _envoyer(sortie, [{"app": "un"}, {"app": "deux"}]))[-1] is SendOutcome.INDEXED
        client = _client()
        try:
            return await client.llen(f"banc-un-{marque}"), await client.llen(f"banc-deux-{marque}")
        finally:
            await client.aclose()

    # le premier nœud est injoignable : le second sert
    assert asyncio.run(scenario()) == (1, 1)


def test_flux_xadd_borne() -> None:
    cle = f"banc-flux-{uuid.uuid4().hex[:8]}"
    sortie = RedisOutput({"host": HOTE, "key": cle, "data_type": "stream", "stream_maxlen": 1000})

    async def scenario() -> list[Any]:
        assert await _envoyer(sortie, [{"message": "f1"}, {"message": "f2"}]) == [SendOutcome.INDEXED] * 2
        client = _client()
        try:
            return await client.xrange(cle)
        finally:
            await client.aclose()

    entrees = asyncio.run(scenario())
    assert [json.loads(champs[b"message"])["message"] for _, champs in entrees] == ["f1", "f2"]


def test_canal_publish_recu_par_un_abonne() -> None:
    canal = f"banc-canal-{uuid.uuid4().hex[:8]}"
    sortie = RedisOutput({"host": HOTE, "key": canal, "data_type": "channel"})

    async def scenario() -> dict[str, Any]:
        client = _client()
        abonnement = client.pubsub()
        try:
            await abonnement.subscribe(canal)
            await abonnement.get_message(ignore_subscribe_messages=True, timeout=1.0)
            assert await _envoyer(sortie, [{"message": "c1"}]) == [SendOutcome.INDEXED]
            for _ in range(20):
                recu = await abonnement.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if recu:
                    return json.loads(recu["data"])
            raise AssertionError("aucun message reçu sur le canal")
        finally:
            await abonnement.aclose()
            await client.aclose()

    assert asyncio.run(scenario())["message"] == "c1"


def test_mauvais_type_de_cle_part_en_dlq(tmp_path: Path) -> None:
    cle = f"banc-chaine-{uuid.uuid4().hex[:8]}"
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    sortie = RedisOutput({"host": HOTE, "key": "%{cible}", "batch_size": 2}, dlq=dlq)

    async def scenario() -> list[SendOutcome]:
        client = _client()
        try:
            await client.set(cle, "pas une liste")
        finally:
            await client.aclose()
        return await _envoyer(sortie, [{"cible": cle}, {"cible": f"{cle}-ok"}])

    # le refus WRONGTYPE ne touche que son document
    assert asyncio.run(scenario()) == [SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
    lignes = (tmp_path / "dlq.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lignes) == 1 and "WRONGTYPE" in lignes[0]


def test_health_ping_et_version() -> None:
    sortie = RedisOutput({"host": HOTE, "key": "banc"})

    async def scenario() -> dict[str, Any]:
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    sante = asyncio.run(scenario())
    assert sante["ping"] is True and sante["version"]
