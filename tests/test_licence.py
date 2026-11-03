# SPDX-License-Identifier: GPL-3.0-or-later
"""Licence du dépôt : GPL-3.0-or-later (SOCLE §1, issue #91)."""

import subprocess
import tomllib
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent
EN_TETE = "# SPDX-License-Identifier: GPL-3.0-or-later"


def _fichiers_de_code() -> list[str]:
    """Liste les fichiers Python et shell suivis par git."""
    sortie = subprocess.run(
        ["git", "ls-files", "*.py", "*.sh"],
        cwd=RACINE,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return sortie.split()


def test_licence_est_le_texte_gplv3() -> None:
    """LICENSE contient le texte officiel de la GPLv3."""
    texte = (RACINE / "LICENSE").read_text(encoding="utf-8")
    assert texte.lstrip().startswith("GNU GENERAL PUBLIC LICENSE")
    assert "Version 3, 29 June 2007" in texte


def test_pyproject_declare_gpl_or_later() -> None:
    """pyproject.toml déclare GPL-3.0-or-later."""
    donnees = tomllib.loads((RACINE / "pyproject.toml").read_text(encoding="utf-8"))
    assert donnees["project"]["license"] == "GPL-3.0-or-later"


def test_chaque_fichier_de_code_porte_l_en_tete_spdx() -> None:
    """Chaque fichier .py et .sh porte l'en-tête SPDX dans ses trois premières lignes."""
    fichiers = _fichiers_de_code()
    assert fichiers
    manquants = [f for f in fichiers if EN_TETE not in (RACINE / f).read_text(encoding="utf-8").splitlines()[:3]]
    assert manquants == []


def test_readme_annonce_la_licence() -> None:
    """Le README annonce GPL-3.0-or-later et plus la MIT."""
    texte = (RACINE / "README.md").read_text(encoding="utf-8")
    assert "GPL-3.0-or-later" in texte
    assert "License: MIT" not in texte
