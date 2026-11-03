# SPDX-License-Identifier: GPL-3.0-or-later
"""Packaging installable : contrats du pyproject.toml (découpage #111).

La roue expose le paquet `pystashtemplate` en agencement src/ (PEP 517,
hatchling) : `pip install pystashtemplate` fournit `from pystashtemplate
import ...`. Les catalogues compilés sont des données partagées installées
dans `<prefix>/share/locale/` (domaine gettext « pystashtemplate »).
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pystashtemplate

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def _pyproject() -> dict:
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))


class TestPyproject:
    def test_backend_de_build_present(self) -> None:
        # Sans backend PEP 517, pas de `pip install` possible.
        build = _pyproject()["build-system"]
        assert build["requires"] == ["hatchling"]
        assert build["build-backend"] == "hatchling.build"

    def test_roue_agencement_src(self) -> None:
        # `sources = ["src"]` installe le paquet pystashtemplate/ (agencement
        # src/ classique) : les imports deviennent `from pystashtemplate.X import`.
        wheel = _pyproject()["tool"]["hatch"]["build"]["targets"]["wheel"]
        assert wheel["only-include"] == ["src/pystashtemplate"]
        assert wheel["sources"] == ["src"]

    def test_uv_package_true(self) -> None:
        # uv doit installer le projet lui-même (pas seulement ses dépendances).
        assert _pyproject()["tool"]["uv"]["package"] is True

    def test_le_paquet_s_importe(self) -> None:
        # Le paquet s'importe depuis les sources (conftest ajoute src/ au path).
        assert pystashtemplate.__doc__

    def test_modules_de_la_bibliotheque_dans_le_paquet(self) -> None:
        # Garde-fou sur la DISPOSITION : les modules de la bibliothèque vivent
        # dans src/pystashtemplate/ (et pas le connecteur, resté dans
        # RsysLogstach — voir test_no_cross_imports).
        src = Path(__file__).resolve().parents[1] / "src" / "pystashtemplate"
        modules = sorted(p.name for p in src.glob("*.py"))
        for attendu in ("filters.py", "interpreter.py", "templates.py", "__init__.py"):
            assert attendu in modules, f"{attendu} manque dans src/pystashtemplate/"
        for interdit in ("omprog.py", "indexer.py", "metrics.py", "opensearchconf.py"):
            assert interdit not in modules, f"{interdit} (connecteur) ne doit pas être dans la bibliothèque"
        # `dlq.py` est passé dans la bibliothèque (chantier sorties #2) :
        # la DLQ fait partie du socle, le connecteur la réexporte.
        assert "dlq.py" in modules, "dlq.py (socle sorties) manque dans src/pystashtemplate/"
        for attendu in ("outputs",):
            assert (src / attendu).is_dir(), f"{attendu}/ (socle sorties #11) manque"


class TestTraductions:
    """Issue #38 (héritée) : les catalogues compilés font partie de la roue."""

    def test_catalogues_en_donnees_partagees(self) -> None:
        # Tout lang/locale/ (un dossier par langue de lang/LINGUAS) devient
        # <prefix>/share/locale/ : ajouter une langue ne touche pas au pyproject.
        wheel = _pyproject()["tool"]["hatch"]["build"]["targets"]["wheel"]
        assert wheel["shared-data"] == {"lang/locale": "share/locale"}
