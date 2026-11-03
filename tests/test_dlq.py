# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests pytest pour dlq.py::LocalDeadLetterQueue (session 13).

Pas de dépendance à pytest-asyncio (absente de requirements-dev.txt,
même convention que test_indexer.py) : `write` est piloté via
`asyncio.run()` directement.
"""

from __future__ import annotations

import asyncio
import json

from pystashtemplate.dlq import LocalDeadLetterQueue


class TestWrite:
    def test_creates_parent_directory_if_missing(self, tmp_path) -> None:
        path = tmp_path / "nested" / "dir" / "dlq.jsonl"
        LocalDeadLetterQueue(str(path))  # ne doit pas lever, même si "nested/dir" n'existe pas encore
        assert path.parent.is_dir()
        assert not path.exists()  # le fichier lui-même n'est créé qu'au premier write

    def test_writes_one_json_line_per_call(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        dlq = LocalDeadLetterQueue(str(path))
        asyncio.run(dlq.write({"message": "un"}, index="idx-1", error="boom"))
        asyncio.run(dlq.write({"message": "deux"}, index="idx-1", error="boom encore"))

        lines = path.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0])["document"] == {"message": "un"}
        assert json.loads(lines[1])["document"] == {"message": "deux"}

    def test_entry_contains_expected_keys(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        dlq = LocalDeadLetterQueue(str(path))
        asyncio.run(dlq.write({"message": "hello"}, index="switch-logs-2026.09.14", error="mapper_parsing_exception"))

        entry = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
        assert entry["@dlq_index"] == "switch-logs-2026.09.14"
        assert entry["@dlq_error"] == "mapper_parsing_exception"
        assert entry["document"] == {"message": "hello"}
        assert "@dlq_timestamp" in entry

    def test_timestamp_is_iso8601_utc(self, tmp_path) -> None:
        from datetime import datetime

        path = tmp_path / "dlq.jsonl"
        dlq = LocalDeadLetterQueue(str(path))
        asyncio.run(dlq.write({"message": "hello"}, index="idx-1", error="boom"))

        entry = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
        parsed = datetime.fromisoformat(entry["@dlq_timestamp"])
        assert parsed.tzinfo is not None  # timezone-aware, pas un timestamp naïf

    def test_non_ascii_content_preserved(self, tmp_path) -> None:
        """`ensure_ascii=False` : un champ accentué reste lisible tel quel dans le fichier."""
        path = tmp_path / "dlq.jsonl"
        dlq = LocalDeadLetterQueue(str(path))
        asyncio.run(dlq.write({"message": "événement réseau"}, index="idx-1", error="erreur"))

        raw = path.read_text(encoding="utf-8")
        assert "événement réseau" in raw

    def test_appends_without_truncating_existing_content(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        path.write_text(json.dumps({"pre_existing": True}) + "\n", encoding="utf-8")
        dlq = LocalDeadLetterQueue(str(path))
        asyncio.run(dlq.write({"message": "nouveau"}, index="idx-1", error="boom"))

        lines = path.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"pre_existing": True}
        assert json.loads(lines[1])["document"] == {"message": "nouveau"}


# --------------------------------------------------------------------------- #
# Rotation et purge (issue #27)
# --------------------------------------------------------------------------- #

import gzip  # noqa: E402
import os  # noqa: E402
import time  # noqa: E402
from datetime import datetime, timedelta, timezone  # noqa: E402

import pytest  # noqa: E402

from pystashtemplate.dlq import parse_duration, parse_size  # noqa: E402


def _write(dlq: LocalDeadLetterQueue, message: str = "x") -> None:
    asyncio.run(dlq.write({"message": message}, index="idx", error="boom"))


def _lines(path) -> list[dict]:
    opener = gzip.open if str(path).endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


class TestParse:
    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("0", 0),
            ("1048576", 1048576),
            ("100M", 100 * 1024**2),
            ("100mb", 100 * 1024**2),
            ("512k", 512 * 1024),
            ("1G", 1024**3),
            ("1.5k", 1536),
            (" 2 KB ", 2048),
        ],
    )
    def test_parse_size(self, text, expected) -> None:
        assert parse_size(text) == expected

    @pytest.mark.parametrize("text", ["", "-1", "10x", "M", "1 T"])
    def test_parse_size_invalide(self, text) -> None:
        with pytest.raises(ValueError, match="taille invalide"):
            parse_size(text)

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("0", timedelta(0)),
            ("90", timedelta(seconds=90)),
            ("90s", timedelta(seconds=90)),
            ("30m", timedelta(minutes=30)),
            ("12h", timedelta(hours=12)),
            ("7d", timedelta(days=7)),
            ("2W", timedelta(weeks=2)),
        ],
    )
    def test_parse_duration(self, text, expected) -> None:
        assert parse_duration(text) == expected

    @pytest.mark.parametrize("text", ["", "7j", "-1d", "1.5h"])
    def test_parse_duration_invalide(self, text) -> None:
        with pytest.raises(ValueError, match="durée invalide"):
            parse_duration(text)


class TestRotation:
    def test_desactivee_par_defaut(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        dlq = LocalDeadLetterQueue(str(path))
        for i in range(50):
            _write(dlq, "m" * 200 + str(i))
        assert len(_lines(path)) == 50
        assert dlq.rotated_files() == []

    def test_valeurs_negatives_refusees(self, tmp_path) -> None:
        for kwargs in (
            {"max_bytes": -1},
            {"backups": -1},
            {"max_age": timedelta(seconds=-1)},
            {"retention": timedelta(days=-1)},
        ):
            with pytest.raises(ValueError):
                LocalDeadLetterQueue(str(tmp_path / "d.jsonl"), **kwargs)

    def test_rotation_par_taille_compressee(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        dlq = LocalDeadLetterQueue(str(path), max_bytes=400)
        for i in range(6):
            _write(dlq, f"message {i} " + "x" * 100)
        rotated = dlq.rotated_files()
        assert rotated and all(p.name.endswith(".gz") for p in rotated)
        assert path.stat().st_size <= 400
        messages = [e["document"]["message"] for p in rotated for e in _lines(p)] + [
            e["document"]["message"] for e in _lines(path)
        ]
        # aucune perte, ordre conservé (rotation avant l'écriture qui dépasserait)
        assert [m.split()[1] for m in messages] == [str(i) for i in range(6)]

    def test_une_ligne_plus_grosse_que_le_seuil_reste_ecrite(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        dlq = LocalDeadLetterQueue(str(path), max_bytes=10)
        _write(dlq, "long" * 20)
        assert len(_lines(path)) == 1 and dlq.rotated_files() == []
        _write(dlq, "suivant")
        assert len(dlq.rotated_files()) == 1 and len(_lines(path)) == 1

    def test_rotation_sans_compression(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        dlq = LocalDeadLetterQueue(str(path), compress=False)
        _write(dlq)
        target = dlq.rotate()
        assert target is not None and not target.name.endswith(".gz")
        assert _lines(target)[0]["document"] == {"message": "x"}
        assert not path.exists()

    def test_rotate_sur_fichier_absent_ou_vide(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        dlq = LocalDeadLetterQueue(str(path))
        assert dlq.rotate() is None
        path.write_text("", encoding="utf-8")
        assert dlq.rotate() is None

    def test_rotations_dans_la_meme_seconde_sans_ecrasement(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        dlq = LocalDeadLetterQueue(str(path))
        names = set()
        for i in range(3):
            _write(dlq, str(i))
            names.add(dlq.rotate().name)
        assert len(names) == 3
        assert [e["document"]["message"] for p in dlq.rotated_files() for e in _lines(p)] == ["0", "1", "2"]

    def test_rotation_par_age_de_la_plus_ancienne_entree(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        old = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        path.write_text(json.dumps({"@dlq_timestamp": old, "document": {"message": "vieux"}}) + "\n", encoding="utf-8")
        dlq = LocalDeadLetterQueue(str(path), max_age=timedelta(days=7))
        _write(dlq, "neuf")
        assert [e["document"]["message"] for e in _lines(path)] == ["neuf"]
        assert _lines(dlq.rotated_files()[0])[0]["document"] == {"message": "vieux"}

    def test_pas_de_rotation_si_entree_recente(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        dlq = LocalDeadLetterQueue(str(path), max_age=timedelta(days=7))
        _write(dlq, "a")
        _write(dlq, "b")
        assert len(_lines(path)) == 2 and dlq.rotated_files() == []

    def test_age_par_date_de_modification_si_premiere_ligne_illisible(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        path.write_text("pas du json\n", encoding="utf-8")
        old = time.time() - 3600
        os.utime(path, (old, old))
        dlq = LocalDeadLetterQueue(str(path), max_age=timedelta(minutes=30))
        _write(dlq, "neuf")
        assert len(dlq.rotated_files()) == 1

    def test_horodatage_naif_considere_utc(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        naive = (datetime.now(timezone.utc) - timedelta(hours=2)).replace(tzinfo=None).isoformat()
        path.write_text(json.dumps({"@dlq_timestamp": naive}) + "\n", encoding="utf-8")
        dlq = LocalDeadLetterQueue(str(path), max_age=timedelta(hours=1))
        _write(dlq)
        assert len(dlq.rotated_files()) == 1


class TestPurge:
    def _rotated(self, tmp_path, *stamps: str) -> LocalDeadLetterQueue:
        for stamp in stamps:
            (tmp_path / f"dlq.jsonl.{stamp}.gz").write_bytes(gzip.compress(b"{}\n"))
        return tmp_path / "dlq.jsonl"

    def test_backups_garde_les_plus_recents(self, tmp_path) -> None:
        path = self._rotated(tmp_path, "20260101T000000Z", "20260102T000000Z", "20260103T000000Z")
        dlq = LocalDeadLetterQueue(str(path), backups=2)
        removed = dlq.purge()
        assert [p.name for p in removed] == ["dlq.jsonl.20260101T000000Z.gz"]
        assert [p.name for p in dlq.rotated_files()] == [
            "dlq.jsonl.20260102T000000Z.gz",
            "dlq.jsonl.20260103T000000Z.gz",
        ]

    def test_retention_par_age(self, tmp_path) -> None:
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%dT%H%M%SZ")
        path = self._rotated(tmp_path, "20200101T000000Z", recent)
        dlq = LocalDeadLetterQueue(str(path), retention=timedelta(days=7))
        assert [p.name for p in dlq.purge()] == ["dlq.jsonl.20200101T000000Z.gz"]
        assert [p.name for p in dlq.rotated_files()] == [f"dlq.jsonl.{recent}.gz"]

    def test_purge_apres_rotation(self, tmp_path) -> None:
        path = tmp_path / "dlq.jsonl"
        dlq = LocalDeadLetterQueue(str(path), backups=2)
        for i in range(5):
            _write(dlq, str(i))
            dlq.rotate()
        kept = dlq.rotated_files()
        assert len(kept) == 2
        assert [e["document"]["message"] for p in kept for e in _lines(p)] == ["3", "4"]

    def test_fichiers_etrangers_ignores(self, tmp_path) -> None:
        path = self._rotated(tmp_path, "20200101T000000Z")
        (tmp_path / "dlq.jsonl.bak").write_text("garde", encoding="utf-8")
        (tmp_path / "autre.jsonl.20200101T000000Z.gz").write_bytes(b"")
        dlq = LocalDeadLetterQueue(str(path), backups=1, retention=timedelta(days=1))
        assert [p.name for p in dlq.purge()] == ["dlq.jsonl.20200101T000000Z.gz"]
        assert (tmp_path / "dlq.jsonl.bak").exists() and (tmp_path / "autre.jsonl.20200101T000000Z.gz").exists()

    def test_sans_reglage_rien_n_est_purge(self, tmp_path) -> None:
        path = self._rotated(tmp_path, "20200101T000000Z", "20200102T000000Z")
        dlq = LocalDeadLetterQueue(str(path))
        assert dlq.purge() == []
        assert len(dlq.rotated_files()) == 2
