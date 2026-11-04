# SPDX-License-Identifier: GPL-3.0-or-later
"""Banc de conformité Apache Doris (issue #27) : le vrai Stream Load, en conteneur.

Exécuté par le workflow `.github/workflows/conformite-doris.yml`
(conteneur `apache/doris:all-in-one`, un FE et un BE) ; localement :

    docker run -d --name doris -p 9030:9030 -p 8030:8030 -p 8040:8040 apache/doris:all-in-one-4.1.3
    PYSTASHTEMPLATE_DORIS_URL=http://localhost:8030 \\
    PYSTASHTEMPLATE_DORIS_SQL="docker exec -i doris mysql -uroot -h127.0.0.1 -P9030" \\
        uv run pytest tests/test_integration_doris.py

Sans `PYSTASHTEMPLATE_DORIS_URL`, le banc est ignoré. Les tables sont
créées et relues par le protocole MySQL (`PYSTASHTEMPLATE_DORIS_SQL` :
commande du client `mysql`, à laquelle le banc ajoute `-N -B -e <requête>`).
Les envois passent par le FE (port 8030) : la redirection `307` vers le
BE est suivie avec l'authentification.
"""

from __future__ import annotations

import asyncio
import os
import shlex
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs.doris import DorisOutput

URL = os.environ.get("PYSTASHTEMPLATE_DORIS_URL")
URL_BE = os.environ.get("PYSTASHTEMPLATE_DORIS_BE_URL")
SQL = os.environ.get("PYSTASHTEMPLATE_DORIS_SQL", "mysql -uroot -h127.0.0.1 -P9030")
UTILISATEUR = os.environ.get("PYSTASHTEMPLATE_DORIS_USER", "root")
MOT_DE_PASSE = os.environ.get("PYSTASHTEMPLATE_DORIS_PASSWORD", "")
pytestmark = pytest.mark.skipif(not URL, reason="banc de conformité : PYSTASHTEMPLATE_DORIS_URL non défini")


def _sql(requete: str) -> list[list[str]]:
    """Exécute une requête par le client `mysql` ; lignes en colonnes (TSV)."""
    sortie = subprocess.run(
        [*shlex.split(SQL), "-N", "-B", "-e", requete],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return [ligne.split("\t") for ligne in sortie.stdout.splitlines() if ligne]


def _creer_table(base: str, *, colonnes: str = "", cle: str = "message") -> None:
    """Base et table `logs` : `message`, `host`, `app` (VARCHAR), plus `colonnes`."""
    _sql(f"CREATE DATABASE IF NOT EXISTS {base}")
    _sql(
        f"CREATE TABLE IF NOT EXISTS {base}.logs "
        f"(message VARCHAR(512), host VARCHAR(128), app VARCHAR(128){colonnes}) "
        f"DUPLICATE KEY({cle}) DISTRIBUTED BY HASH({cle}) BUCKETS 1 "
        'PROPERTIES ("replication_num" = "1")'
    )


def _chercher(requete: str, attendus: int) -> list[list[str]]:
    """Relit jusqu'à obtenir `attendus` lignes (10 s au plus)."""
    for _ in range(40):
        lignes = _sql(requete)
        if len(lignes) >= attendus:
            return lignes
        time.sleep(0.25)
    return _sql(requete)


def _sortie(base: str, **options: Any) -> DorisOutput:
    dlq = options.pop("dlq", None)
    return DorisOutput(
        {
            "hosts": URL,
            "database": base,
            "table": "logs",
            "user": UTILISATEUR,
            "password": MOT_DE_PASSE,
            "timeout": 30,
            **options,
        },
        dlq=dlq,
    )


def _envoyer_tous(sortie: DorisOutput, documents: list[dict[str, Any]]) -> list[str]:
    """Envoie les documents, vide le tampon, ferme ; rend la valeur de chaque résultat."""

    async def scenario() -> list[str]:
        try:
            resultats = [(await sortie.envoyer(dict(doc))).value for doc in documents]
            await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


def _documents(marque: str, nombre: int) -> list[dict[str, Any]]:
    return [{"message": f"{marque} n{n}", "host": "banc", "app": marque} for n in range(nombre)]


def _base(nom: str) -> str:
    return f"conformite_{nom}_{uuid.uuid4().hex[:8]}"


def test_health() -> None:
    async def scenario() -> dict[str, Any]:
        sortie = _sortie("information_schema")
        try:
            return await sortie.health()
        finally:
            await sortie.close()

    corps = asyncio.run(scenario())
    assert corps["data"]["online_backend_num"] >= 1


def test_lot_charge_par_le_fe_et_relu_colonne_par_colonne() -> None:
    base = _base("lot")
    _creer_table(base)

    resultats = _envoyer_tous(_sortie(base, batch_size=3), _documents(base, 3))

    assert resultats == ["en_attente", "en_attente", "indexed"]
    lignes = _chercher(f"SELECT message, host, app FROM {base}.logs ORDER BY message", 3)
    assert [ligne[0] for ligne in lignes] == [f"{base} n{n}" for n in range(3)]
    assert all(ligne[1:] == ["banc", base] for ligne in lignes)


@pytest.mark.skipif(not URL_BE, reason="PYSTASHTEMPLATE_DORIS_BE_URL non défini")
def test_envoi_direct_au_be() -> None:
    base = _base("be")
    _creer_table(base)

    assert _envoyer_tous(_sortie(base, hosts=URL_BE), _documents(base, 2)) == ["indexed", "indexed"]
    assert len(_chercher(f"SELECT message FROM {base}.logs", 2)) == 2


def test_meme_label_rejoue_sans_doublon() -> None:
    """Un lot rejoué avec son label (réponse perdue) : indexé, aucune ligne en double."""
    base = _base("label")
    _creer_table(base)
    label = f"banc_{uuid.uuid4().hex}"
    documents = _documents(base, 2)

    for _ in range(2):
        sortie = _sortie(base, batch_size=2)
        sortie._nouveau_label = lambda: label  # type: ignore[method-assign]
        assert _envoyer_tous(sortie, documents) == ["en_attente", "indexed"]

    time.sleep(1)
    assert _sql(f"SELECT count(*) FROM {base}.logs") == [["2"]]


def test_table_absente_en_dlq(tmp_path: Path) -> None:
    base = _base("absente")
    _sql(f"CREATE DATABASE IF NOT EXISTS {base}")
    chemin = tmp_path / "dlq.jsonl"

    resultats = _envoyer_tous(_sortie(base, dlq=LocalDeadLetterQueue(str(chemin))), _documents(base, 1))

    assert resultats == ["dead_lettered"]
    assert chemin.read_text(encoding="utf-8").count("\n") == 1


def test_ligne_invalide_isolee_du_lot(tmp_path: Path) -> None:
    """Lot atomique refusé pour une ligne : les autres sont chargées, la fautive en DLQ."""
    base = _base("isolement")
    _creer_table(base, colonnes=", code INT NOT NULL")
    chemin = tmp_path / "dlq.jsonl"
    documents = [
        {"message": f"{base} ok0", "host": "banc", "app": base, "code": 1},
        {"message": f"{base} ko", "host": "banc", "app": base, "code": None},
        {"message": f"{base} ok1", "host": "banc", "app": base, "code": 2},
    ]

    _envoyer_tous(_sortie(base, batch_size=3, dlq=LocalDeadLetterQueue(str(chemin))), documents)

    lignes = _chercher(f"SELECT message FROM {base}.logs ORDER BY message", 2)
    assert [ligne[0] for ligne in lignes] == [f"{base} ok0", f"{base} ok1"]
    contenu = chemin.read_text(encoding="utf-8")
    assert contenu.count("\n") == 1
    assert f"{base} ko" in contenu
