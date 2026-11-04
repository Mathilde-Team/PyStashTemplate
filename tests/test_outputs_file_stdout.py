# SPDX-License-Identifier: GPL-3.0-or-later
"""Sorties `file` et `stdout` (issue #32) : codecs, rotation, DLQ, registre.

Couverture : codecs de rendu (json_lines, rubydebug), écriture NDJSON avec
rotation par gabarit de chemin, DLQ sur échec permanent d'écriture, sortie
standard avec flux injecté, inscription au registre et compilation depuis
un pipeline `.conf`.
"""

from __future__ import annotations

import asyncio
import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs import REGISTRY, compiler_sorties
from pystashtemplate.outputs.codecs import encoder_json_lines, encoder_rubydebug
from pystashtemplate.outputs.fichier import FileOutput
from pystashtemplate.outputs.stdout import StdoutOutput


def _run(coro):
    return asyncio.run(coro)


def _doc(moment: str = "2026-11-04T17:00:00Z", **champs) -> dict:
    return {"@timestamp": moment, **champs}


# ----------------------------------------------------------------------
# Codecs
# ----------------------------------------------------------------------


def test_codec_json_lines_rend_une_ligne_json() -> None:
    ligne = encoder_json_lines({"a": 1, "b": "é"})
    assert json.loads(ligne) == {"a": 1, "b": "é"}
    assert "\n" not in ligne
    assert "é" in ligne  # ensure_ascii=False


def test_codec_rubydebug_rend_les_paires_indentees() -> None:
    texte = encoder_rubydebug(
        {"@timestamp": "2026-11-04T17:00:00Z", "message": "hello", "n": 1, "x": None, "tags": ["a", "b"]}
    )
    assert texte.startswith("{\n")
    assert texte.endswith("\n}")
    assert '    "message" => "hello"' in texte
    assert '    "n" => 1' in texte
    assert '    "x" => nil' in texte
    assert '    "tags" => ["a", "b"]' in texte
    # virgule en fin de ligne sauf la dernière
    lignes = texte.splitlines()
    assert all(ligne.endswith(",") for ligne in lignes[1:-2])
    assert not lignes[-2].endswith(",")


def test_codec_rubydebug_types_scalaires_et_imbriques() -> None:
    texte = encoder_rubydebug(
        {
            "vrai": True,
            "faux": False,
            "horodatage": datetime(2026, 11, 4, 17, tzinfo=timezone.utc),
            "sous": {"k": "v", "n": 0},
            "texte": 'avec "guillemets", \\ et\nsaut',
        }
    )
    assert '"vrai" => true' in texte
    assert '"faux" => false' in texte
    assert '"horodatage" => 2026-11-04T17:00:00+00:00' in texte
    assert '"sous" => {"k" => "v", "n" => 0}' in texte
    assert '"texte" => "avec \\"guillemets\\", \\\\ et\\nsaut"' in texte


def test_codec_rubydebug_type_inconnu_tombe_sur_repr() -> None:
    class Inconnu:
        def __repr__(self) -> str:
            return "<inconnu>"

    assert "<inconnu>" in encoder_rubydebug({"x": Inconnu()})


# ----------------------------------------------------------------------
# Sortie file
# ----------------------------------------------------------------------


def test_file_est_inscrit_au_registre() -> None:
    assert REGISTRY["file"] is FileOutput
    assert FileOutput.plugin == "file"
    assert FileOutput.capacites.actions == frozenset({"index", "create"})
    assert FileOutput.capacites.accepte_id is False
    assert FileOutput.capacites.accuse_par_evenement is True


def test_file_sans_path_leve_une_erreur_claire() -> None:
    with pytest.raises(ValueError, match="option « path » manquante"):
        FileOutput({})


def test_file_codec_inconnu_leve_une_erreur_claire() -> None:
    with pytest.raises(ValueError, match="codec bogus inconnu"):
        FileOutput({"path": "/tmp/x.ndjson", "codec": "bogus"})


def test_file_ecrit_une_ligne_json_par_document(tmp_path: Path) -> None:
    sortie = FileOutput({"path": str(tmp_path / "t.ndjson")})
    _run(sortie.open())
    _run(sortie.send([_doc(message="a"), _doc(message="b")]))
    _run(sortie.flush())
    lignes = (tmp_path / "t.ndjson").read_text(encoding="utf-8").splitlines()
    assert [json.loads(ligne)["message"] for ligne in lignes] == ["a", "b"]
    _run(sortie.close())


def test_file_codec_rubydebug_et_repetition_sans_double_ouverture(tmp_path: Path) -> None:
    chemin = tmp_path / "t.ndjson"
    sortie = FileOutput({"path": str(chemin), "codec": "rubydebug"})
    _run(sortie.send([_doc(message="a")]))
    _run(sortie.send([_doc(message="b")]))
    assert len(sortie._fichiers) == 1  # un seul fichier ouvert
    _run(sortie.flush())
    assert '"message" => "b"' in chemin.read_text(encoding="utf-8")


def test_file_rotation_par_gabarit_de_chemin(tmp_path: Path) -> None:
    sortie = FileOutput({"path": str(tmp_path / "%{+YYYY-MM-dd}.ndjson")})
    _run(sortie.send([_doc(moment="2026-11-03T23:59:00Z", message="hier")]))
    _run(sortie.send([_doc(moment="2026-11-04T00:01:00Z", message="aujourd'hui")]))
    _run(sortie.flush())
    hier = tmp_path / "2026-11-03.ndjson"
    aujourd_hui = tmp_path / "2026-11-04.ndjson"
    assert json.loads(hier.read_text())["message"] == "hier"
    assert json.loads(aujourd_hui.read_text())["message"] == "aujourd'hui"
    # le fichier du jour précédent est fermé à la rotation
    assert list(sortie._fichiers) == [str(aujourd_hui)]


def test_file_echec_permanent_sans_dlq_remonte(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sortie = FileOutput({"path": str(tmp_path / "t.ndjson")})

    def _panique(chemin_rendu: str, document) -> str:
        raise OSError("disque plein")

    monkeypatch.setattr(sortie, "_ecrire", _panique)
    with pytest.raises(OSError, match="disque plein"):
        _run(sortie.send([_doc(message="a")]))


def test_file_echec_permanent_avec_dlq_consigne_le_document(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sortie = FileOutput({"path": str(tmp_path / "t.ndjson"), "dlq_path": str(tmp_path / "dlq.jsonl")})

    def _panique(chemin_rendu: str, document) -> str:
        raise OSError("droits refusés")

    monkeypatch.setattr(sortie, "_ecrire", _panique)
    _run(sortie.send([_doc(message="a")]))
    entree = json.loads((tmp_path / "dlq.jsonl").read_text().splitlines()[0])
    assert entree["document"]["message"] == "a"
    assert entree["@dlq_error"] == "droits refusés"
    assert entree["@dlq_index"].endswith("t.ndjson")


def test_file_close_est_idempotent_et_ouvre_le_dossier_parent(tmp_path: Path) -> None:
    sortie = FileOutput({"path": str(tmp_path / "sous" / "dossier" / "t.ndjson")})
    _run(sortie.send([_doc(message="a")]))
    _run(sortie.close())
    _run(sortie.close())  # idempotent
    assert not sortie._fichiers


def test_compiler_sorties_instancie_file_depuis_le_pipeline(tmp_path: Path) -> None:
    chemin = str(tmp_path / "%{+YYYY-MM-dd}.ndjson")
    pipeline = PipelineInterpreter(f'output {{ file {{ path => "{chemin}" }} }}')
    sorties = compiler_sorties(pipeline)
    assert len(sorties) == 1
    assert isinstance(sorties[0], FileOutput)
    _run(sorties[0].send([_doc(message="a")]))
    assert (tmp_path / "2026-11-04.ndjson").exists()


# ----------------------------------------------------------------------
# Sortie stdout
# ----------------------------------------------------------------------


def test_stdout_est_inscrit_au_registre() -> None:
    assert REGISTRY["stdout"] is StdoutOutput
    assert StdoutOutput.plugin == "stdout"
    assert StdoutOutput.capacites.actions == frozenset({"index", "create"})
    assert StdoutOutput.capacites.accepte_id is False


def test_stdout_codec_rubydebug_par_defaut() -> None:
    flux = io.StringIO()
    sortie = StdoutOutput({}, flux=flux)
    _run(sortie.open())
    _run(sortie.send([_doc(message="hello")]))
    _run(sortie.flush())
    assert '"message" => "hello"' in flux.getvalue()
    _run(sortie.close())


def test_stdout_codec_json_lines() -> None:
    flux = io.StringIO()
    sortie = StdoutOutput({"codec": "json_lines"}, flux=flux)
    _run(sortie.send([_doc(message="a"), _doc(message="b")]))
    lignes = flux.getvalue().splitlines()
    assert [json.loads(ligne)["message"] for ligne in lignes] == ["a", "b"]


def test_stdout_codec_inconnu_leve_une_erreur_claire() -> None:
    with pytest.raises(ValueError, match="codec bogus inconnu"):
        StdoutOutput({"codec": "bogus"})


def test_stdout_par_defaut_ecrit_sur_sys_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    sortie = StdoutOutput({"codec": "json_lines"})
    _run(sortie.send([_doc(message="console")]))
    assert json.loads(capsys.readouterr().out)["message"] == "console"


def test_compiler_sorties_instancie_stdout_depuis_le_pipeline() -> None:
    pipeline = PipelineInterpreter('output { stdout { codec => "json_lines" } }')
    sorties = compiler_sorties(pipeline)
    assert len(sorties) == 1
    assert isinstance(sorties[0], StdoutOutput)
