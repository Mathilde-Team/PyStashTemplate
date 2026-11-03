# SPDX-License-Identifier: GPL-3.0-or-later
"""Internationalisation (issue #38) : module ``i18n``, catalogues de ``lang/``
et marquage des chaînes destinées à l'utilisateur.

Le contrôle « .pot et .po à jour avec les sources » demande gettext
(xgettext, msgmerge) : il est fait par ``scripts/i18n-update.sh --check``
dans le job i18n de la CI. Ici, uniquement Python : les catalogues suivis
par git sont lus avec le module ``gettext`` de la bibliothèque standard.
"""

from __future__ import annotations

import ast
import gettext
import importlib
import os
import re
import shutil
import string
import subprocess
import sys
from pathlib import Path

import pytest

import pystashtemplate.i18n as i18n

ROOT = Path(__file__).resolve().parents[1]
LANG_DIR = ROOT / "lang"
SRC = ROOT / "src" / "pystashtemplate"


def linguas() -> list[str]:
    lignes = (
        ligne.split("#", 1)[0].strip() for ligne in (LANG_DIR / "LINGUAS").read_text(encoding="utf-8").splitlines()
    )
    return [ligne for ligne in lignes if ligne]


def entrees_po(chemin: Path) -> list[dict]:
    """Entrées d'un .po écrit sans coupure de ligne (``--no-wrap``)."""
    entrees, courante = [], None
    for ligne in chemin.read_text(encoding="utf-8").splitlines():
        if ligne.startswith("#~"):
            continue
        if ligne.startswith("#:") or ligne.startswith("#,") or ligne.startswith("msgid "):
            if courante is None or "msgstr" in courante:
                courante = {"refs": [], "flags": []}
                entrees.append(courante)
        if ligne.startswith("#:"):
            courante["refs"] += ligne[2:].split()
        elif ligne.startswith("#,"):
            courante["flags"] += [f.strip() for f in ligne[2:].split(",")]
        elif ligne.startswith("msgid "):
            courante["msgid"] = ast.literal_eval(ligne[6:])
        elif ligne.startswith("msgstr "):
            courante["msgstr"] = ast.literal_eval(ligne[7:])
    return [e for e in entrees if e.get("msgid")]


@pytest.fixture
def recharge(monkeypatch):
    """Recharge ``i18n`` après avoir changé l'environnement, puis le remet en français."""

    def _recharger():
        return importlib.reload(i18n)

    yield _recharger
    monkeypatch.undo()
    os.environ["LANGUAGE"] = "fr_FR"
    os.environ.pop(i18n.ENV_LOCALEDIR, None)
    importlib.reload(i18n)


class TestRechercheDesCatalogues:
    def test_ordre_depot_prefix_systeme_defaut(self, monkeypatch) -> None:
        monkeypatch.delenv(i18n.ENV_LOCALEDIR, raising=False)
        dossiers = i18n.localedirs()
        assert dossiers[0] == str(LANG_DIR / "locale")
        assert dossiers[1] == str(Path(sys.prefix) / "share" / "locale")
        assert dossiers[-2:] == [i18n.SYSTEM_LOCALEDIR, None]

    def test_variable_d_environnement_en_premier(self, monkeypatch, tmp_path) -> None:
        monkeypatch.setenv(i18n.ENV_LOCALEDIR, str(tmp_path))
        assert i18n.localedirs()[:2] == [str(tmp_path), str(LANG_DIR / "locale")]

    def test_doublons_retires(self, monkeypatch) -> None:
        monkeypatch.setenv(i18n.ENV_LOCALEDIR, str(LANG_DIR / "locale"))
        dossiers = i18n.localedirs()
        assert dossiers.count(str(LANG_DIR / "locale")) == 1

    def test_roue_installee_sans_dossier_lang(self, monkeypatch, tmp_path) -> None:
        # Module installé dans site-packages : pas de lang/ à côté.
        monkeypatch.delenv(i18n.ENV_LOCALEDIR, raising=False)
        monkeypatch.setattr(i18n, "REPO_LANG_DIR", tmp_path / "absent")
        assert str(LANG_DIR / "locale") not in i18n.localedirs()


class TestChargement:
    def test_anglais(self, monkeypatch, recharge) -> None:
        monkeypatch.setenv("LANGUAGE", "en_US")
        module = recharge()
        assert module._("Arrêt du pont messages={count}") == "Bridge stopped messages={count}"
        assert module.ngettext("un", "plusieurs", 2) == "plusieurs"

    def test_variable_de_langue_courte(self, monkeypatch, recharge) -> None:
        # Valeur usuelle de LANG/LANGUAGE : le jeu de caractères est ignoré.
        monkeypatch.setenv("LANGUAGE", "en_US.UTF-8")
        assert recharge()._("enrichissement") == "enrichment"

    def test_francais_langue_source(self, recharge) -> None:
        # fr_FR.po n'a aucune traduction : les msgid (français) sont rendus.
        module = recharge()
        assert isinstance(module.translation, gettext.GNUTranslations)
        assert module._("enrichissement") == "enrichissement"

    def test_langue_sans_catalogue(self, monkeypatch, recharge) -> None:
        monkeypatch.setenv("LANGUAGE", "xx_XX")
        monkeypatch.setenv("LC_ALL", "C")
        module = recharge()
        assert type(module.translation) is gettext.NullTranslations
        assert module._("enrichissement") == "enrichissement"

    def test_dossier_de_l_environnement(self, monkeypatch, recharge, tmp_path) -> None:
        # Catalogue « maison » placé hors du dépôt (installation sur mesure).
        cible = tmp_path / "de_DE" / "LC_MESSAGES"
        cible.mkdir(parents=True)
        shutil.copy(LANG_DIR / "locale" / "en_US" / "LC_MESSAGES" / f"{i18n.DOMAIN}.mo", cible / f"{i18n.DOMAIN}.mo")
        monkeypatch.setenv(i18n.ENV_LOCALEDIR, str(tmp_path))
        monkeypatch.setenv("LANGUAGE", "de_DE")
        module = recharge()
        assert module._("enrichissement") == "enrichment"


class TestCatalogues:
    def test_29_langues_declarees(self) -> None:
        langues = linguas()
        assert len(langues) == 29
        assert len(set(langues)) == 29
        assert {"en_US", "fr_FR"} <= set(langues)

    def test_chaque_langue_a_son_po_et_son_mo(self) -> None:
        for loc in linguas():
            assert (LANG_DIR / f"{loc}.po").is_file(), loc
            mo = LANG_DIR / "locale" / loc / "LC_MESSAGES" / f"{i18n.DOMAIN}.mo"
            with mo.open("rb") as fichier:
                catalogue = gettext.GNUTranslations(fichier)
            assert catalogue.info()["language"] == loc

    def test_aucun_po_hors_linguas(self) -> None:
        assert {p.stem for p in LANG_DIR.glob("*.po")} == set(linguas())

    def test_po_synchronises_avec_le_pot(self) -> None:
        attendus = {e["msgid"] for e in entrees_po(LANG_DIR / "messages.pot")}
        assert attendus
        for loc in linguas():
            assert {e["msgid"] for e in entrees_po(LANG_DIR / f"{loc}.po")} == attendus, loc

    def test_references_vers_des_fichiers_existants(self) -> None:
        for chemin in [LANG_DIR / "messages.pot", *LANG_DIR.glob("*.po")]:
            for entree in entrees_po(chemin):
                for ref in entree["refs"]:
                    assert (ROOT / ref.split(":")[0]).is_file(), f"{chemin.name} : {ref}"

    def test_aucune_traduction_approximative(self) -> None:
        # « fuzzy » n'est pas utilisé à l'exécution : à relire avant fusion.
        for loc in linguas():
            assert not [e["msgid"] for e in entrees_po(LANG_DIR / f"{loc}.po") if "fuzzy" in e["flags"]], loc

    def test_anglais_complet_et_champs_conserves(self) -> None:
        for entree in entrees_po(LANG_DIR / "en_US.po"):
            assert entree["msgstr"], entree["msgid"]
            if "python-brace-format" in entree["flags"]:
                champs = [f for _, f, _, _ in string.Formatter().parse(entree["msgid"]) if f is not None]
                traduits = [f for _, f, _, _ in string.Formatter().parse(entree["msgstr"]) if f is not None]
                assert sorted(champs) == sorted(traduits), entree["msgid"]
            assert entree["msgid"].count("%%") == entree["msgstr"].count("%%"), entree["msgid"]

    def test_francais_sans_traduction(self) -> None:
        # Langue source : un msgstr recopié deviendrait faux au premier changement de msgid.
        assert not [e["msgid"] for e in entrees_po(LANG_DIR / "fr_FR.po") if e["msgstr"]]


def _est_marquee(noeud: ast.AST) -> bool:
    """``_("...")``, éventuellement suivi de ``.format(...)``."""
    if isinstance(noeud, ast.Call) and isinstance(noeud.func, ast.Attribute) and noeud.func.attr == "format":
        noeud = noeud.func.value
    return (
        isinstance(noeud, ast.Call)
        and isinstance(noeud.func, ast.Name)
        and noeud.func.id == "_"
        and len(noeud.args) == 1
        and isinstance(noeud.args[0], ast.Constant)
        and isinstance(noeud.args[0].value, str)
    )


def _chaines_utilisateur(arbre: ast.Module):
    """(ligne, nœud) des messages vus par l'utilisateur : journaux info et
    au-dessus, aide, description et titres de groupes argparse, metavar en
    toutes lettres."""
    for noeud in ast.walk(arbre):
        if not isinstance(noeud, ast.Call) or not isinstance(noeud.func, ast.Attribute):
            continue
        attr = noeud.func.attr
        if (
            attr in {"info", "success", "warning", "error", "exception", "critical"}
            and isinstance(noeud.func.value, ast.Name)
            and noeud.func.value.id == "logger"
            and noeud.args
        ):
            yield noeud.lineno, noeud.args[0]
        if attr == "add_argument_group" and noeud.args:
            yield noeud.lineno, noeud.args[0]
        for mot in noeud.keywords:
            if mot.arg in {"help", "description", "epilog"}:
                yield noeud.lineno, mot.value
            if mot.arg == "metavar" and not (
                isinstance(mot.value, ast.Constant) and re.fullmatch(r"JSONL?|MMDB|PEM|URL", str(mot.value.value))
            ):
                yield noeud.lineno, mot.value


def test_chaines_utilisateur_passent_par_gettext() -> None:
    oublis = []
    for chemin in sorted(SRC.glob("*.py")):
        arbre = ast.parse(chemin.read_text(encoding="utf-8"))
        oublis += [f"{chemin.name}:{ligne}" for ligne, noeud in _chaines_utilisateur(arbre) if not _est_marquee(noeud)]
    assert not oublis, f"chaînes non marquées avec _() : {oublis}"


def test_detecteur_de_chaines_non_marquees() -> None:
    # Le test précédent ne doit pas être vide de sens : il voit bien un oubli.
    arbre = ast.parse(
        'logger.warning("brut")\nlogger.info(_("ok"))\np.add_argument("-x", help="brut", metavar="CHEMIN")\n'
        'p.add_argument("-y", help=_("a {b}").format(b=1), metavar="URL")\nlogger.debug("trace")\n'
    )
    marques = [(ligne, _est_marquee(noeud)) for ligne, noeud in _chaines_utilisateur(arbre)]
    assert marques == [(1, False), (2, True), (3, False), (3, False), (4, True)]

