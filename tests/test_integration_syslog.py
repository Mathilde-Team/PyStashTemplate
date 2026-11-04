# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité syslog (issue #30) : le vrai récepteur, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-syslog.yml`
(conteneur Vector, sources `syslog` TCP et UDP) ; localement :

    PYSTASHTEMPLATE_SYSLOG_HOST=localhost \\
    PYSTASHTEMPLATE_SYSLOG_TCP_PORT=5140 \\
    PYSTASHTEMPLATE_SYSLOG_UDP_PORT=5141 \\
    PYSTASHTEMPLATE_VECTOR_CONTAINER=vector \\
    uv run pytest tests/test_integration_syslog.py

Sans `PYSTASHTEMPLATE_SYSLOG_HOST`, le banc est ignoré. Les messages
envoyés sont relus dans les journaux du conteneur (sink `console`) :
cadrage TCP octet-counting et LF, UDP, RFC 5424 et RFC 3164.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
import uuid
from typing import Any

import pytest

from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.syslog import SyslogOutput

HOTE = os.environ.get("PYSTASHTEMPLATE_SYSLOG_HOST")
PORT_TCP = int(os.environ.get("PYSTASHTEMPLATE_SYSLOG_TCP_PORT", "5140"))
PORT_UDP = int(os.environ.get("PYSTASHTEMPLATE_SYSLOG_UDP_PORT", "5141"))
CONTENEUR = os.environ.get("PYSTASHTEMPLATE_VECTOR_CONTAINER", "vector")
pytestmark = pytest.mark.skipif(not HOTE, reason="banc de conformité : PYSTASHTEMPLATE_SYSLOG_HOST non défini")


def _journaux_vector() -> str:
    """Journaux du conteneur Vector (sink `console` : les événements reçus)."""
    rendu = subprocess.run(["docker", "logs", CONTENEUR], capture_output=True, text=True, check=True)
    return rendu.stdout


def _chercher(marque: str, attendus: int) -> list[dict[str, Any]]:
    """Relit les journaux jusqu'à obtenir `attendus` événements (10 s au plus)."""
    lignes: list[dict[str, Any]] = []
    for _ in range(40):
        rendu = _journaux_vector()
        lignes = [json.loads(ligne) for ligne in rendu.splitlines() if ligne.startswith("{") and marque in ligne]
        if len(lignes) >= attendus:
            return lignes
        time.sleep(0.25)
    return lignes


def _envoyer(sortie: SyslogOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Envoie les documents, vide le tampon, ferme ; rend l'issue de chacun."""

    async def scenario() -> list[SendOutcome]:
        try:
            return [await sortie.envoyer(doc) for doc in documents]
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def test_tcp_octet_counting_rfc5424() -> None:
    marque = f"banc5424-{uuid.uuid4().hex[:6]}"
    documents = [{"message": f"{marque} n{n}", "host": "banc"} for n in range(3)]
    sortie = SyslogOutput(
        {
            "host": HOTE,
            "port": PORT_TCP,
            "protocol": "tcp",
            "rfc": "rfc5424",
            "appname": marque,
        },
    )
    assert _envoyer(sortie, documents) == [SendOutcome.INDEXED] * 3
    lignes = _chercher(marque, 3)
    assert len(lignes) == 3
    assert all(ligne.get("appname") == marque for ligne in lignes)
    # le MESSAGE est le document JSON : Vector le restitue tel quel
    messages = [json.loads(ligne["message"])["message"] for ligne in lignes]
    assert sorted(messages) == [f"{marque} n0", f"{marque} n1", f"{marque} n2"]


def test_tcp_lf_rfc3164() -> None:
    marque = f"banc3164-{uuid.uuid4().hex[:6]}"
    documents = [{"message": f"{marque} n{n}", "host": "banc"} for n in range(3)]
    sortie = SyslogOutput(
        {
            "host": HOTE,
            "port": PORT_TCP,
            "protocol": "tcp",
            "framing": "lf",
            "rfc": "rfc3164",
            "appname": marque,
        },
    )
    assert _envoyer(sortie, documents) == [SendOutcome.INDEXED] * 3
    lignes = _chercher(marque, 3)
    assert len(lignes) == 3


def test_udp() -> None:
    marque = f"bancudp-{uuid.uuid4().hex[:6]}"
    documents = [{"message": f"{marque} n{n}", "host": "banc"} for n in range(3)]
    sortie = SyslogOutput(
        {
            "host": HOTE,
            "port": PORT_UDP,
            "protocol": "udp",
            "rfc": "rfc5424",
            "appname": marque,
        },
    )
    assert _envoyer(sortie, documents) == [SendOutcome.INDEXED] * 3
    lignes = _chercher(marque, 3)
    assert len(lignes) == 3


def test_health_tcp() -> None:
    sortie = SyslogOutput({"host": HOTE, "port": PORT_TCP, "protocol": "tcp"})

    async def scenario() -> dict[str, Any]:
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    assert asyncio.run(scenario()) == {"status": "joignable", "protocole": "tcp"}
