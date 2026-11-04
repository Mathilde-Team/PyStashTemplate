# SPDX-License-Identifier: GPL-3.0-or-later
"""Chaque sortie du registre a sa page dans `docs/sorties.md` (critère commun des issues #12 à #33).

Une page = un titre `## Sortie(s) …` qui nomme le plugin entre accents
graves, et au moins un gabarit `output { <plugin> { … } }` dans la
section. Garde-fou ajouté après l'audit du 04/11 (pages manquantes pour
greptimedb #19, siglens #20, otlp #22, parseable #25).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from pystashtemplate.outputs import REGISTRY

DOC = Path(__file__).resolve().parents[1] / "docs" / "sorties.md"
# `null` est la sortie de référence du socle (#4), sans page dédiée.
SANS_PAGE = {"null"}


def _sections() -> dict[str, str]:
    """Titre `## …` → texte de la section (sous-sections `###` comprises)."""
    texte = DOC.read_text(encoding="utf-8")
    morceaux = re.split(r"^(## .*)$", texte, flags=re.M)
    return {morceaux[i]: morceaux[i + 1] for i in range(1, len(morceaux) - 1, 2)}


def _section_du_plugin(plugin: str) -> tuple[str, str] | None:
    for titre, corps in _sections().items():
        if titre.startswith("## Sortie") and f"`{plugin}`" in titre:
            return titre, corps
    return None


@pytest.mark.parametrize("plugin", sorted(set(REGISTRY) - SANS_PAGE))
def test_page_et_gabarit_pour_chaque_sortie(plugin: str) -> None:
    trouvee = _section_du_plugin(plugin)
    assert trouvee is not None, f"aucune section « ## Sortie `{plugin}` » dans docs/sorties.md"
    _, corps = trouvee
    assert re.search(rf"output\s*\{{\s*{re.escape(plugin)}\s*\{{", corps), (
        f"pas de gabarit output {{ {plugin} {{ … }} }}"
    )


def test_section_introuvable_pour_un_plugin_inconnu() -> None:
    assert _section_du_plugin("plugin_qui_n_existe_pas") is None
