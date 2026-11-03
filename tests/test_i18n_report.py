# SPDX-License-Identifier: GPL-3.0-or-later
"""Rapport de traduction ``scripts/i18n_report.py`` (issue #38)."""

from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _charger():
    spec = importlib.util.spec_from_file_location("i18n_report", ROOT / "scripts" / "i18n_report.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["i18n_report"] = module
    spec.loader.exec_module(module)
    return module


report = _charger()

PO = """# commentaire
msgid ""
msgstr ""
"Language: xx\\n"

#: src/a.py
msgid "un"
msgstr "one"

#, fuzzy
msgid "deux"
msgstr "two"

msgid "trois"
msgstr ""

msgctxt "menu"
msgid "quatre"
msgid_plural "quatres"
msgstr[0] "four"
msgstr[1] "fours"

msgid ""
"cinq "
"suite"
msgstr ""
"five "
"more"

#~ msgid "obsolète"
#~ msgstr "obsolete"
"""


@pytest.fixture
def lang(tmp_path):
    """Copie de lang/ réduite à en_US et fr_FR."""
    (tmp_path / "LINGUAS").write_text("# commentaire\nen_US\nfr_FR  # source\n", encoding="utf-8")
    for loc in ("en_US", "fr_FR"):
        shutil.copy(ROOT / "lang" / f"{loc}.po", tmp_path / f"{loc}.po")
        shutil.copytree(ROOT / "lang" / "locale" / loc, tmp_path / "locale" / loc)
    return tmp_path


def test_parse_po() -> None:
    entrees = report.parse_po(PO)
    assert [e[0] for e in entrees] == ["", "un", "deux", "trois", "quatre", "cinq suite"]
    assert entrees[2] == ("deux", ["two"], True)
    assert entrees[4][1] == ["four", "fours"]
    assert entrees[5][1] == ["five more"]


def test_catalog_stats() -> None:
    stats = report.catalog_stats("xx", PO)
    assert (stats.translated, stats.fuzzy, stats.untranslated, stats.total) == (3, 1, 1, 5)
    assert stats.percent == 60.0
    assert report.catalog_stats("xx", 'msgid ""\nmsgstr ""\n').percent == 0.0


def test_collect_et_rendu(lang) -> None:
    stats, erreurs = report.collect(lang)
    assert not erreurs
    assert [s.locale for s in stats] == ["en_US", "fr_FR"]
    assert stats[0].percent == 100.0 and stats[0].compiled
    texte = report.render(stats, erreurs, markdown=False)
    assert "Langues :" in texte and "Catalogues compilés :" in texte and "2/2" in texte
    markdown = report.render(stats, erreurs, markdown=True)
    assert markdown.startswith("## Internationalisation")
    assert "| `en_US` |" in markdown


def test_totaux_hors_langue_source(lang) -> None:
    stats, erreurs = report.collect(lang)
    synthese = dict(report.summary_lines(stats, erreurs))
    # fr_FR (source) ne compte pas comme « non traduite ».
    assert synthese["Non traduites"] == "0"
    assert synthese["Traduites"] == synthese["Chaînes par catalogue"]
    assert report.summary_lines([], [])[1] == ("Chaînes par catalogue", "0")


def test_catalogue_manquant_ou_mo_casse(lang, capsys, monkeypatch, tmp_path) -> None:
    (lang / "LINGUAS").write_text("en_US\nfr_FR\nde_DE\n", encoding="utf-8")
    (lang / "locale" / "fr_FR" / "LC_MESSAGES" / "rsyslogstach.mo").write_bytes(b"pas un mo")
    resume = tmp_path / "resume.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(resume))
    assert report.main(["--lang-dir", str(lang)]) == 2
    sortie = capsys.readouterr()
    assert "::error::lang/de_DE.po" in sortie.err
    sortie_compacte = " ".join(sortie.out.split())
    assert "Locales OK : 2/3" in sortie_compacte
    assert "Catalogues compilés : 1/3" in sortie_compacte
    assert "## Internationalisation" in resume.read_text(encoding="utf-8")


def test_main_markdown(lang, capsys, monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    assert report.main(["--markdown", "--lang-dir", str(lang)]) == 0
    assert "| Langues | 2 |" in capsys.readouterr().out
