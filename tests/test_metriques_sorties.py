# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests des métriques des sorties (issue #10).

Compteurs en mémoire et exposition Prometheus ; observateur par défaut
nul ; branchement dans la sortie `file`. Le branchement dans le client
HTTP commun est vérifié pour chaque sortie par le banc de conformité
(`test_conformite_sorties.py`).
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from pystashtemplate.outputs.base import Output
from pystashtemplate.outputs.fichier import FileOutput
from pystashtemplate.outputs.metriques import (
    ISSUES,
    OBSERVATEUR_NUL,
    CompteursSortie,
    ObservateurNul,
    ObservateurSortie,
)
from pystashtemplate.outputs.null import NullOutput


def test_observateur_par_defaut_nul_et_partage_sans_effet() -> None:
    assert Output.observateur is OBSERVATEUR_NUL
    assert NullOutput({}).observateur is OBSERVATEUR_NUL
    nul = ObservateurNul()
    nul.evenements("s", "indexed", 3)
    nul.reessai("s")
    nul.latence("s", 0.1)
    nul.file("s", 2)


def test_implementations_respectent_le_protocole() -> None:
    assert isinstance(OBSERVATEUR_NUL, ObservateurSortie)
    assert isinstance(CompteursSortie(), ObservateurSortie)


def test_compteurs_par_sortie() -> None:
    compteurs = CompteursSortie()
    compteurs.evenements("es", "indexed", 3)
    compteurs.evenements("es", "rejected")
    compteurs.evenements("es", "dead_lettered")
    compteurs.reessai("es")
    compteurs.latence("es", 0.25)
    compteurs.latence("es", 0.5)
    compteurs.file("es", 4)
    compteurs.evenements("vl", "retryable", 2)
    assert compteurs.instantane() == {
        "es": {
            "indexed": 3,
            "dead_lettered": 1,
            "retryable": 0,
            "rejected": 1,
            "reessais": 1,
            "requetes": 2,
            "duree_totale": 0.75,
            "duree_max": 0.5,
            "file": 4,
        },
        "vl": {
            "indexed": 0,
            "dead_lettered": 0,
            "retryable": 2,
            "rejected": 0,
            "reessais": 0,
            "requetes": 0,
            "duree_totale": 0.0,
            "duree_max": 0.0,
            "file": 0,
        },
    }


def test_issue_inconnue_refusee() -> None:
    with pytest.raises(ValueError, match="dropped"):
        CompteursSortie().evenements("es", "dropped")


def test_exposition_prometheus() -> None:
    compteurs = CompteursSortie()
    compteurs.evenements("es", "indexed", 3)
    compteurs.reessai("es")
    compteurs.latence("es", 0.5)
    compteurs.file("es", 2)
    texte = compteurs.exposition()
    assert texte.endswith("\n")
    lignes = texte.splitlines()
    assert "# TYPE pystashtemplate_output_events_total counter" in lignes
    assert 'pystashtemplate_output_events_total{output="es",outcome="indexed"} 3' in lignes
    for issue in ISSUES:
        assert any(f'outcome="{issue}"' in ligne for ligne in lignes)
    assert 'pystashtemplate_output_retries_total{output="es"} 1' in lignes
    assert "# TYPE pystashtemplate_output_request_duration_seconds summary" in lignes
    assert 'pystashtemplate_output_request_duration_seconds_count{output="es"} 1' in lignes
    assert 'pystashtemplate_output_request_duration_seconds_sum{output="es"} 0.5' in lignes
    assert 'pystashtemplate_output_queue_size{output="es"} 2' in lignes


def test_exposition_prefixe_et_etiquettes_echappees() -> None:
    compteurs = CompteursSortie()
    compteurs.file('a"b\\c\nd', 1)
    texte = compteurs.exposition(prefixe="connecteur")
    assert 'connecteur_queue_size{output="a\\"b\\\\c\\nd"} 1' in texte.splitlines()


def test_exposition_vide_garde_les_entetes() -> None:
    lignes = CompteursSortie().exposition().splitlines()
    assert all(ligne.startswith("#") for ligne in lignes)
    assert len(lignes) == 8


def test_file_compte_indexes_rejets_et_dlq(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sortie = FileOutput({"path": str(tmp_path / "t.ndjson"), "dlq_path": str(tmp_path / "dlq.jsonl"), "id": "fichier"})
    sortie.observateur = compteurs = CompteursSortie()
    asyncio.run(sortie.send([{"message": "a"}, {"message": "b"}]))
    ecrire = sortie._ecrire

    def _panique_sur_c(chemin_rendu: str, document: dict) -> str:
        if document["message"] == "c":
            raise OSError("disque plein")
        return ecrire(chemin_rendu, document)

    monkeypatch.setattr(sortie, "_ecrire", _panique_sur_c)
    asyncio.run(sortie.send([{"message": "c"}, {"message": "d"}]))
    asyncio.run(sortie.close())
    instantane = compteurs.instantane()["fichier"]
    assert (instantane["indexed"], instantane["rejected"], instantane["dead_lettered"]) == (3, 1, 1)


def test_file_sans_dlq_compte_le_rejet_avant_de_lever(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sortie = FileOutput({"path": str(tmp_path / "t.ndjson")})
    sortie.observateur = compteurs = CompteursSortie()

    def _panique(chemin_rendu: str, document: dict) -> str:
        raise OSError("disque plein")

    monkeypatch.setattr(sortie, "_ecrire", _panique)
    with pytest.raises(OSError):
        asyncio.run(sortie.send([{"message": "a"}]))
    assert compteurs.instantane()["file"]["rejected"] == 1
    assert compteurs.instantane()["file"]["dead_lettered"] == 0
