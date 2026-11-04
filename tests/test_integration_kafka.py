# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité kafka (issue #29) : la vraie ingestion, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-kafka.yml`
(conteneur Redpanda, protocole Kafka) ; localement :

    PYSTASHTEMPLATE_KAFKA_BOOTSTRAP=localhost:9092 \\
    PYSTASHTEMPLATE_REDPANDA_CONTAINER=redpanda \\
    uv run pytest tests/test_integration_kafka.py

Sans `PYSTASHTEMPLATE_KAFKA_BOOTSTRAP`, le banc est ignoré. Les
messages poussés sont relus par un consommateur `aiokafka` (sujet,
clé et valeur vérifiés), avec une attente bornée.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from typing import Any

import pytest

from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.kafka import KafkaOutput

BOOTSTRAP = os.environ.get("PYSTASHTEMPLATE_KAFKA_BOOTSTRAP")
CONTENEUR = os.environ.get("PYSTASHTEMPLATE_REDPANDA_CONTAINER", "redpanda")
pytestmark = pytest.mark.skipif(not BOOTSTRAP, reason="banc de conformité : PYSTASHTEMPLATE_KAFKA_BOOTSTRAP non défini")


def _consommer(sujet: str, attendus: int, timeout: float = 15.0) -> list[tuple[bytes | None, bytes]]:
    """Relit les messages du sujet jusqu'à en obtenir `attendus` (borné)."""
    from aiokafka import AIOKafkaConsumer

    async def scenario() -> list[tuple[bytes | None, bytes]]:
        consommateur = AIOKafkaConsumer(
            sujet,
            bootstrap_servers=BOOTSTRAP,
            group_id=f"banc-{uuid.uuid4()}",
            auto_offset_reset="earliest",
        )
        messages: list[tuple[bytes | None, bytes]] = []
        try:
            await consommateur.start()
            debut = time.monotonic()
            while len(messages) < attendus and time.monotonic() - debut < timeout:
                lots = await consommateur.getmany(timeout_ms=500)
                for _, partition_messages in sorted(lots.items()):
                    for message in partition_messages:
                        messages.append((message.key, message.value))
        finally:
            await consommateur.stop()
        return messages

    return asyncio.run(scenario())


def _envoyer(sortie: KafkaOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Envoie les documents, vide le tampon, ferme ; rend l'issue de chacun."""

    async def scenario() -> list[SendOutcome]:
        try:
            resultats = [(await sortie.envoyer(doc)) for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def test_envoi_indexe_sujet_cle_valeur() -> None:
    sujet = f"logs-banc-{uuid.uuid4().hex[:8]}"
    documents = [{"message": f"m{i}", "app": "banc"} for i in range(3)]
    sortie = KafkaOutput(
        {
            "bootstrap_servers": BOOTSTRAP,
            "topic_id": sujet,
            "message_key": "%{app}",
        },
    )
    assert _envoyer(sortie, documents) == [SendOutcome.INDEXED] * 3
    messages = _consommer(sujet, 3)
    assert len(messages) == 3
    cles = [cle for cle, _ in messages]
    valeurs = [json.loads(valeur.decode("utf-8")) for _, valeur in messages]
    assert cles == [b"banc"] * 3
    assert sorted(valeur["message"] for valeur in valeurs) == ["m0", "m1", "m2"]


def test_sujet_rendu_par_gabarit_par_document() -> None:
    marque = uuid.uuid4().hex[:8]
    sortie = KafkaOutput(
        {
            "bootstrap_servers": BOOTSTRAP,
            "topic_id": f"logs-%{{app}}-{marque}",
        },
    )
    documents = [{"message": "a", "app": "un"}, {"message": "b", "app": "deux"}]
    assert _envoyer(sortie, documents) == [SendOutcome.INDEXED] * 2
    for app in ("un", "deux"):
        messages = _consommer(f"logs-{app}-{marque}", 1)
        assert len(messages) == 1
        assert json.loads(messages[0][1].decode("utf-8"))["message"] in ("a", "b")


def test_health_liste_les_noeuds() -> None:
    sortie = KafkaOutput({"bootstrap_servers": BOOTSTRAP, "topic_id": "logs-banc"})

    async def scenario() -> dict[str, Any]:
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    sante = asyncio.run(scenario())
    assert sante["brokers"]
