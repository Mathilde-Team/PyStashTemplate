# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité rabbitmq (issue #33) : le vrai courtier, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-rabbitmq.yml`
(conteneur RabbitMQ) ; localement :

    PYSTASHTEMPLATE_RABBITMQ_HOST=localhost:5672 uv run pytest tests/test_integration_rabbitmq.py

Sans `PYSTASHTEMPLATE_RABBITMQ_HOST`, le banc est ignoré. Une file
exclusive est liée à l'échange avant chaque envoi ; les messages sont
relus par un client `aio_pika` indépendant (clé de routage, propriétés,
corps vérifiés).
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from typing import Any

import pytest

from pystashtemplate.outputs.courtier_rabbitmq import RabbitMqOutput
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome

HOTE = os.environ.get("PYSTASHTEMPLATE_RABBITMQ_HOST")
pytestmark = pytest.mark.skipif(not HOTE, reason="banc de conformité : PYSTASHTEMPLATE_RABBITMQ_HOST non défini")


async def _connexion() -> Any:
    """Connexion de relecture, indépendante de la sortie."""
    import aio_pika

    nom, _, port = str(HOTE).rpartition(":")
    return await aio_pika.connect(host=nom, port=int(port))


async def _envoyer(sortie: RabbitMqOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Envoie les documents, vide le tampon, ferme ; rend l'issue de chacun."""
    try:
        resultats = [(await sortie.envoyer(doc)) for doc in documents]
        await sortie.flush()
        return resultats
    finally:
        await sortie.close()


async def _relire(file: Any, attendus: int) -> list[Any]:
    """Relit `attendus` messages de la file (attente bornée)."""
    messages = []
    for _ in range(50):
        message = await file.get(no_ack=True, fail=False)
        if message is None:
            await asyncio.sleep(0.1)
            continue
        messages.append(message)
        if len(messages) == attendus:
            break
    return messages


def test_publication_topic_routee_et_confirmee() -> None:
    import aio_pika

    echange = f"banc-{uuid.uuid4().hex[:8]}"
    sortie = RabbitMqOutput(
        {
            "host": HOTE,
            "exchange": echange,
            "exchange_type": "topic",
            "durable": False,
            "key": "logs.%{app}",
            "batch_size": 3,
            "message_properties": {"app_id": "pystashtemplate"},
        }
    )

    async def scenario() -> list[Any]:
        connexion = await _connexion()
        try:
            canal = await connexion.channel()
            ech = await canal.declare_exchange(echange, aio_pika.ExchangeType.TOPIC, durable=False)
            file = await canal.declare_queue(exclusive=True)
            await file.bind(ech, routing_key="logs.banc")
            documents = [{"message": f"m{i}", "app": "banc"} for i in range(3)]
            assert (await _envoyer(sortie, documents))[-1] is SendOutcome.INDEXED
            return await _relire(file, 3)
        finally:
            await connexion.close()

    messages = asyncio.run(scenario())
    assert [json.loads(m.body)["message"] for m in messages] == ["m0", "m1", "m2"]
    assert {m.routing_key for m in messages} == {"logs.banc"}
    assert messages[0].content_type == "application/json"
    assert messages[0].app_id == "pystashtemplate"
    assert messages[0].delivery_mode == aio_pika.DeliveryMode.PERSISTENT


def test_echange_declare_par_la_sortie_et_fanout() -> None:
    import aio_pika

    echange = f"banc-fanout-{uuid.uuid4().hex[:8]}"
    sortie = RabbitMqOutput(
        {"host": HOTE, "exchange": echange, "exchange_type": "fanout", "durable": False, "persistent": False}
    )

    async def scenario() -> list[Any]:
        await sortie.open()  # déclare l'échange
        connexion = await _connexion()
        try:
            canal = await connexion.channel()
            ech = await canal.declare_exchange(echange, aio_pika.ExchangeType.FANOUT, passive=True)
            file = await canal.declare_queue(exclusive=True)
            await file.bind(ech)
            assert await _envoyer(sortie, [{"message": "f"}]) == [SendOutcome.INDEXED]
            return await _relire(file, 1)
        finally:
            await connexion.close()

    messages = asyncio.run(scenario())
    assert len(messages) == 1 and messages[0].routing_key == "logstash"


def test_echange_passif_absent_rend_le_lot() -> None:
    sortie = RabbitMqOutput(
        {"host": HOTE, "exchange": f"absent-{uuid.uuid4().hex[:8]}", "exchange_type": "direct", "passive": True}
    )
    assert asyncio.run(_envoyer(sortie, [{"message": "x"}])) == [SendOutcome.RETRYABLE]


def test_mauvais_mot_de_passe_rend_le_lot() -> None:
    sortie = RabbitMqOutput(
        {"host": HOTE, "exchange": "amq.topic", "exchange_type": "topic", "password": "faux", "passive": True}
    )
    assert asyncio.run(_envoyer(sortie, [{"message": "x"}])) == [SendOutcome.RETRYABLE]


def test_health_produit_et_version() -> None:
    sortie = RabbitMqOutput({"host": HOTE, "exchange": "amq.topic", "exchange_type": "topic", "passive": True})

    async def scenario() -> dict[str, Any]:
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    sante = asyncio.run(scenario())
    assert sante["product"] == "RabbitMQ" and sante["version"]
