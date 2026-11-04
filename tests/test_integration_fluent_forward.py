# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité fluent_forward (issue #31) : un vrai Fluent Bit.

Exécuté par le workflow `.github/workflows/conformite-fluent_forward.yml`
(conteneur Fluent Bit, entrée `forward` ouverte sur 24224 et entrée
avec clé partagée et utilisateur sur 24225, sortie `file`) ; localement :

    PYSTASHTEMPLATE_FLUENT_OUT=/tmp/fb/out2 \\
    uv run pytest tests/test_integration_fluent_forward.py

Sans `PYSTASHTEMPLATE_FLUENT_OUT`, le banc est ignoré. Fluent Bit écrit
un fichier par étiquette, une ligne `étiquette: [temps, {document}]`
par évènement : étiquette, temps (nanosecondes) et document sont relus
et vérifiés, avec une attente bornée.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.fluent_forward import ErreurForward, FluentForwardOutput

SORTIE = os.environ.get("PYSTASHTEMPLATE_FLUENT_OUT")
HOTE = os.environ.get("PYSTASHTEMPLATE_FLUENT_HOST", "127.0.0.1")
PORT = int(os.environ.get("PYSTASHTEMPLATE_FLUENT_PORT", "24224"))
PORT_AUTH = int(os.environ.get("PYSTASHTEMPLATE_FLUENT_AUTH_PORT", "24225"))
CLE = os.environ.get("PYSTASHTEMPLATE_FLUENT_SHARED_KEY", "banc-secret")
UTILISATEUR = os.environ.get("PYSTASHTEMPLATE_FLUENT_USER", "banc")
MOT_DE_PASSE = os.environ.get("PYSTASHTEMPLATE_FLUENT_PASSWORD", "BancConformite1")
pytestmark = pytest.mark.skipif(not SORTIE, reason="banc de conformité : PYSTASHTEMPLATE_FLUENT_OUT non défini")


def _relire(tag: str, attendus: int, timeout: float = 20.0) -> list[tuple[float, dict[str, Any]]]:
    """Relit le fichier de l'étiquette jusqu'à `attendus` évènements (borné)."""
    fichier = Path(str(SORTIE)) / tag
    debut = time.monotonic()
    evenements: list[tuple[float, dict[str, Any]]] = []
    while time.monotonic() - debut < timeout:
        if fichier.exists():
            lignes = [ligne for ligne in fichier.read_text(encoding="utf-8").splitlines() if ligne.strip()]
            evenements = []
            for ligne in lignes:
                etiquette, _, reste = ligne.partition(": ")
                assert etiquette == tag
                temps, document = json.loads(reste)
                evenements.append((float(temps), document))
            if len(evenements) >= attendus:
                return evenements
        time.sleep(0.5)
    return evenements


def _envoyer(sortie: FluentForwardOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Envoie les documents, vide le tampon, ferme ; rend l'issue de chacun."""

    async def scenario() -> list[SendOutcome]:
        try:
            resultats = [(await sortie.envoyer(doc)) for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"require_ack": True},
        {"mode": "packed_forward", "require_ack": True},
        {"compression": True, "require_ack": True},
    ],
    ids=["forward", "forward-ack", "packed-ack", "gzip-ack"],
)
def test_evenements_recus_etiquette_temps_document(options: dict[str, Any]) -> None:
    marque = uuid.uuid4().hex[:8]
    documents = [{"message": f"m{i}", "app": marque, "@timestamp": f"2026-11-04T10:00:0{i}.123456Z"} for i in range(3)]
    sortie = FluentForwardOutput({"host": HOTE, "port": PORT, "tag": "banc.%{app}", "batch_size": 3, **options})
    assert _envoyer(sortie, documents)[-1] == SendOutcome.INDEXED
    evenements = _relire(f"banc.{marque}", 3)
    assert [doc["message"] for _, doc in evenements] == ["m0", "m1", "m2"]
    assert [round(temps, 6) for temps, _ in evenements] == [1793786400.123456 + i for i in range(3)]


def test_etiquette_rendue_par_document() -> None:
    marque = uuid.uuid4().hex[:8]
    sortie = FluentForwardOutput({"host": HOTE, "port": PORT, "tag": f"banc.{marque}.%{{app}}", "require_ack": True})
    assert (
        _envoyer(sortie, [{"app": "un", "message": "a"}, {"app": "deux", "message": "b"}]) == [SendOutcome.INDEXED] * 2
    )
    assert _relire(f"banc.{marque}.un", 1)[0][1]["message"] == "a"
    assert _relire(f"banc.{marque}.deux", 1)[0][1]["message"] == "b"


def test_temps_en_secondes_entieres() -> None:
    marque = uuid.uuid4().hex[:8]
    sortie = FluentForwardOutput({"host": HOTE, "port": PORT, "tag": f"banc.{marque}", "time_as_integer": True})
    assert _envoyer(sortie, [{"message": "i", "@timestamp": 1700000000}]) == [SendOutcome.INDEXED]
    assert _relire(f"banc.{marque}", 1)[0][0] == 1700000000.0


def test_cle_partagee_et_utilisateur() -> None:
    marque = uuid.uuid4().hex[:8]
    options = {
        "host": HOTE,
        "port": PORT_AUTH,
        "tag": f"banc.{marque}",
        "shared_key": CLE,
        "user": UTILISATEUR,
        "password": MOT_DE_PASSE,
        "require_ack": True,
    }
    assert _envoyer(FluentForwardOutput(options), [{"message": "k"}]) == [SendOutcome.INDEXED]
    # L'entrée authentifiée préfixe les étiquettes (Tag_Prefix auth.).
    assert _relire(f"auth.banc.{marque}", 1)[0][1] == {"message": "k"}


def test_mot_de_passe_refuse() -> None:
    options = {
        "host": HOTE,
        "port": PORT_AUTH,
        "tag": "banc.refus",
        "shared_key": CLE,
        "user": UTILISATEUR,
        "password": "faux",
    }

    async def scenario() -> None:
        sortie = FluentForwardOutput(options)
        try:
            with pytest.raises(ErreurForward, match="username/password mismatch"):
                await sortie.open()
        finally:
            await sortie.close()

    asyncio.run(scenario())


def test_cle_partagee_fausse_connexion_fermee() -> None:
    # Fluent Bit ferme la connexion sur une clé fausse (pas de PONG refusé) :
    # échec transitoire, rendu à l'appelant.
    options = {"host": HOTE, "port": PORT_AUTH, "tag": "banc.cle", "shared_key": "fausse", "require_ack": True}
    assert _envoyer(FluentForwardOutput(options), [{"message": "x"}]) == [SendOutcome.RETRYABLE]


def test_health() -> None:
    sortie = FluentForwardOutput(
        {"host": HOTE, "port": PORT_AUTH, "tag": "t", "shared_key": CLE, "user": UTILISATEUR, "password": MOT_DE_PASSE}
    )

    async def scenario() -> dict[str, Any]:
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert asyncio.run(scenario())["authentifie"] is True
