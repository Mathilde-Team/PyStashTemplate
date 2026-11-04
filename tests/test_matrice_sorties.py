# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la matrice de compatibilité des sorties (issue #9)."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import ClassVar

import pytest

from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.base import Capacites, Output
from pystashtemplate.outputs.matrice import DEBUT, ENTETE, FIN, ligne, remplacer, tableau

ROOT = Path(__file__).resolve().parent.parent


def _script() -> ModuleType:
    """Charge `scripts/matrice_sorties.py` comme module."""
    spec = importlib.util.spec_from_file_location("matrice_sorties", ROOT / "scripts" / "matrice_sorties.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SortieRestreinte(Output):
    """Sortie factice (jamais instanciée) : `create` seule, sans `_id`, accusé global, requête bornée."""

    plugin = "restreinte"
    capacites = Capacites(
        actions=frozenset({"create"}), accepte_id=False, accuse_par_evenement=False, taille_max_requete=1024
    )
    OPTIONS_CONNUES: ClassVar[frozenset[str]] = frozenset({"retry_max"})


def test_documentation_a_jour() -> None:
    assert _script().main(["--check"]) == 0, "lancer : uv run python scripts/matrice_sorties.py"


def test_une_ligne_par_sortie_du_registre() -> None:
    lignes = tableau().splitlines()
    assert tuple(lignes[:2]) == ENTETE
    assert [ligne.split(" | ")[0] for ligne in lignes[2:]] == [f"| `{nom}`" for nom in sorted(REGISTRY)]


def test_ligne_lit_les_capacites_declarees() -> None:
    assert ligne(SortieRestreinte, banc=False) == ("| `restreinte` | `create` | non | non | 1024 o | non | oui | non |")


def test_ligne_sortie_complete_avec_banc() -> None:
    assert ligne(REGISTRY["opensearch"], banc=True) == "| `opensearch` | toutes | oui | oui | — | oui | oui | oui |"


def test_remplacer_entre_les_marqueurs_seulement() -> None:
    document = f"avant\n{DEBUT}\nvieux tableau\n{FIN}\naprès\n"
    resultat = remplacer(document, ["opensearch"])
    assert resultat.startswith(f"avant\n{DEBUT}\n| Plugin")
    assert resultat.endswith(f"\n{FIN}\naprès\n")
    assert "vieux tableau" not in resultat
    assert remplacer(resultat, ["opensearch"]) == resultat


@pytest.mark.parametrize("document", ["sans marqueurs", f"{FIN}\n{DEBUT}"])
def test_remplacer_sans_marqueurs_leve(document: str) -> None:
    with pytest.raises(ValueError, match="marqueurs"):
        remplacer(document)


def test_bancs_lus_dans_les_workflows(tmp_path: Path) -> None:
    (tmp_path / "conformite-zeta.yml").touch()
    (tmp_path / "conformite-alpha.yml").touch()
    (tmp_path / "ci.yml").touch()
    assert _script().bancs_en_conteneur(tmp_path) == ["alpha", "zeta"]


def test_check_echoue_si_obsolete_puis_ecriture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = _script()
    doc = tmp_path / "sorties.md"
    doc.write_text(f"# Sorties\n{DEBUT}\nobsolète\n{FIN}\n", encoding="utf-8")
    monkeypatch.setattr(script, "DOC", doc)
    assert script.main(["--check"]) == 1
    assert script.main([]) == 0
    assert script.main(["--check"]) == 0
    assert "obsolète" not in doc.read_text(encoding="utf-8")
