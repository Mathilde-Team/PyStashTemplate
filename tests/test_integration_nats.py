# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité nats (issue #33) : le vrai serveur (JetStream activé), en conteneur.

Exécuté par le workflow `.github/workflows/conformite-nats.yml`
(conteneur `nats -js`) ; localement :

    PYSTASHTEMPLATE_NATS_URL=nats://localhost:4222 uv run pytest tests/test_integration_nats.py

Sans `PYSTASHTEMPLATE_NATS_URL`, le banc est ignoré. NATS cœur : un
abonné indépendant reçoit les messages ; JetStream : un flux créé pour
le banc les stocke, relus par un consommateur.
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
from pystashtemplate.outputs.courtier_nats import NatsOutput
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome

URL = os.environ.get("PYSTASHTEMPLATE_NATS_URL")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_NATS_URL non défini")


async def _envoyer(sortie: NatsOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Envoie les documents, vide le tampon, ferme ; rend l'issue de chacun."""
    try:
        resultats = [(await sortie.envoyer(doc)) for doc in documents]
        await sortie.flush()
        return resultats
    finally:
        await sortie.close()


def test_coeur_sujet_rendu_recu_par_un_abonne() -> None:
    import nats

    marque = uuid.uuid4().hex[:8]
    sortie = NatsOutput({"url": URL, "subject": f"banc.{marque}.%{{app}}", "batch_size": 3})

    async def scenario() -> list[Any]:
        abonne = await nats.connect(URL)
        try:
            abonnement = await abonne.subscribe(f"banc.{marque}.>")
            await abonne.flush()
            documents = [{"message": f"m{i}", "app": "un" if i % 2 else "deux"} for i in range(3)]
            assert (await _envoyer(sortie, documents))[-1] is SendOutcome.INDEXED
            return [await abonnement.next_msg(timeout=5) for _ in range(3)]
        finally:
            await abonne.close()

    messages = asyncio.run(scenario())
    assert [json.loads(m.data)["message"] for m in messages] == ["m0", "m1", "m2"]
    assert [m.subject for m in messages] == [f"banc.{marque}.deux", f"banc.{marque}.un", f"banc.{marque}.deux"]


def test_jetstream_accuse_et_stocke() -> None:
    import nats

    marque = uuid.uuid4().hex[:8]
    flux = f"BANC_{marque}"
    sortie = NatsOutput(
        {"url": URL, "subject": f"js.{marque}.logs", "jetstream": True, "stream": flux, "batch_size": 2}
    )

    async def scenario() -> list[Any]:
        client = await nats.connect(URL)
        try:
            js = client.jetstream()
            await js.add_stream(name=flux, subjects=[f"js.{marque}.>"])
            assert await _envoyer(sortie, [{"message": "j1"}, {"message": "j2"}]) == [
                SendOutcome.EN_ATTENTE,
                SendOutcome.INDEXED,
            ]
            abonnement = await js.pull_subscribe(f"js.{marque}.>", durable=f"banc{marque}")
            messages = await abonnement.fetch(2, timeout=5)
            await js.delete_stream(flux)
            return messages
        finally:
            await client.close()

    messages = asyncio.run(scenario())
    assert [json.loads(m.data)["message"] for m in messages] == ["j1", "j2"]


def test_jetstream_sujet_sans_flux_part_en_dlq(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    sortie = NatsOutput({"url": URL, "subject": f"sans.flux.{uuid.uuid4().hex[:8]}", "jetstream": True}, dlq=dlq)
    assert asyncio.run(_envoyer(sortie, [{"message": "x"}])) == [SendOutcome.DEAD_LETTERED]
    assert "NoStreamResponseError" in (tmp_path / "dlq.jsonl").read_text(encoding="utf-8")


def test_serveur_injoignable_rend_le_lot() -> None:
    sortie = NatsOutput({"url": "nats://127.0.0.1:1", "subject": "banc", "timeout": 1})
    assert asyncio.run(_envoyer(sortie, [{"message": "x"}])) == [SendOutcome.RETRYABLE]


def test_health_version_et_jetstream() -> None:
    sortie = NatsOutput({"url": URL, "subject": "banc", "jetstream": True})

    async def scenario() -> dict[str, Any]:
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    sante = asyncio.run(scenario())
    assert sante["version"] and "streams" in sante
