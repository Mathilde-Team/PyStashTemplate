# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité Parseable (issue #25) : la vraie ingestion, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-parseable.yml`
(conteneur `parseable/parseable`) ; localement :

    PYSTASHTEMPLATE_PARSEABLE_URL=http://localhost:8000 \\
    PYSTASHTEMPLATE_PARSEABLE_USER=admin PYSTASHTEMPLATE_PARSEABLE_PASSWORD=admin \\
    uv run pytest tests/test_integration_parseable.py

Sans la variable d'environnement, le banc est ignoré. Les documents
envoyés sont relus par l'API SQL (`/api/v1/query`), avec une attente
bornée.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs.parseable import ParseableOutput

URL = os.environ.get("PYSTASHTEMPLATE_PARSEABLE_URL")
USER = os.environ.get("PYSTASHTEMPLATE_PARSEABLE_USER", "admin")
PASSWORD = os.environ.get("PYSTASHTEMPLATE_PARSEABLE_PASSWORD", "admin")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_PARSEABLE_URL non défini")


def _sortie(stream: str, **options: Any) -> ParseableOutput:
    """Sortie authentifiée vers le Parseable du banc."""
    return ParseableOutput({"hosts": URL, "stream": stream, "user": USER, "password": PASSWORD, **options})


def _envoyer_tous(sortie: ParseableOutput, documents: list[dict[str, Any]]) -> list[str]:
    """Envoie les documents, vide le tampon, ferme ; rend la valeur de chaque résultat."""

    async def scenario() -> list[str]:
        try:
            resultats = [(await sortie.envoyer(doc)).value for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _chercher(stream: str, attendus: int) -> list[dict[str, Any]]:
    """Relit le jeu de données par SQL jusqu'à obtenir `attendus` lignes (10 s au plus)."""
    lignes: list[dict[str, Any]] = []
    for _ in range(40):
        reponse = httpx.post(
            f"{URL}/api/v1/query",
            auth=(USER, PASSWORD),
            json={
                "query": f"select * from {stream}",
                "startTime": "2000-01-01T00:00:00Z",
                "endTime": "2100-01-01T00:00:00Z",
            },
        )
        if reponse.status_code == 200:
            lignes = reponse.json()
            if len(lignes) >= attendus:
                return lignes
        time.sleep(0.25)
    return lignes


def _stream(prefixe: str) -> str:
    """Nom de jeu de données unique (lettres et chiffres)."""
    return f"{prefixe}{uuid.uuid4().hex[:8]}"


def test_health() -> None:
    async def scenario() -> dict[str, Any]:
        sortie = _sortie("sante")
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    sante = asyncio.run(scenario())
    assert sante["status"] == "ready"
    assert str(sante["version"]).startswith("v")


def test_lot_ingere_et_jeu_de_donnees_cree() -> None:
    stream = _stream("conformite")
    documents = [
        {"message": f"{stream} n{n}", "@timestamp": "2026-11-04T10:00:00Z", "host": {"name": "banc"}, "n": n}
        for n in range(3)
    ]
    resultats = _envoyer_tous(_sortie(stream, batch_size=3, tags={"env": "banc"}), documents)
    assert resultats == ["en_attente", "en_attente", "indexed"]
    lignes = _chercher(stream, 3)
    assert sorted(ligne["message"] for ligne in lignes) == [f"{stream} n{n}" for n in range(3)]
    # Champs imbriqués aplatis par Parseable.
    assert all(ligne["host_name"] == "banc" for ligne in lignes)


def test_conflit_de_type_range_dans_une_colonne_suffixee() -> None:
    stream = _stream("conflit")
    resultats = _envoyer_tous(_sortie(stream), [{"message": "a", "n": 1}, {"message": "b", "n": "texte"}])
    assert resultats == ["indexed", "indexed"]
    lignes = {ligne["message"]: ligne for ligne in _chercher(stream, 2)}
    assert lignes["a"]["n"] == 1
    assert lignes["b"]["n_utf8"] == "texte"


def test_stream_invalide_en_dlq(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    sortie = ParseableOutput({"hosts": URL, "stream": "nom invalide", "user": USER, "password": PASSWORD}, dlq=dlq)
    assert _envoyer_tous(sortie, [{"message": "m"}]) == ["dead_lettered"]
    assert (tmp_path / "dlq.jsonl").read_text(encoding="utf-8").count("\n") == 1


def test_mauvais_mot_de_passe_rendu_a_l_appelant() -> None:
    sortie = ParseableOutput({"hosts": URL, "stream": _stream("droits"), "user": USER, "password": "faux"})
    assert _envoyer_tous(sortie, [{"message": "m"}]) == ["retryable"]
