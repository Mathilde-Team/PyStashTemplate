#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""Régénère la matrice de compatibilité des sorties dans `docs/sorties.md` (issue #9).

La matrice (système × fonctionnalité) est lue dans le registre des
sorties (`pystashtemplate.outputs.matrice`) ; la colonne « banc en
conteneur » vient des workflows `.github/workflows/conformite-<plugin>.yml`.

Usage :

    scripts/matrice_sorties.py           # réécrit docs/sorties.md
    scripts/matrice_sorties.py --check   # échoue (code 1) si la matrice n'est pas à jour
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loguru import logger

from pystashtemplate.outputs.matrice import remplacer

RACINE = Path(__file__).resolve().parent.parent
DOC = RACINE / "docs" / "sorties.md"
WORKFLOWS = RACINE / ".github" / "workflows"


def bancs_en_conteneur(workflows: Path = WORKFLOWS) -> list[str]:
    """Plugins disposant d'un banc contre un vrai système (`conformite-<plugin>.yml`)."""
    logger.debug("bancs_en_conteneur | entrée")
    noms = sorted(f.stem.removeprefix("conformite-") for f in workflows.glob("conformite-*.yml"))
    logger.debug("bancs_en_conteneur | sortie ({})", len(noms))
    return noms


def main(argv: list[str] | None = None) -> int:
    """Réécrit (ou vérifie avec `--check`) la matrice de `docs/sorties.md`."""
    logger.debug("main | entrée")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="vérifie sans écrire")
    args = parser.parse_args(argv)
    actuel = DOC.read_text(encoding="utf-8")
    attendu = remplacer(actuel, bancs_en_conteneur())
    if args.check:
        a_jour = actuel == attendu
        if not a_jour:
            print("docs/sorties.md : matrice des sorties obsolète — lancer scripts/matrice_sorties.py", file=sys.stderr)
        logger.debug("main | sortie (check)")
        return 0 if a_jour else 1
    DOC.write_text(attendu, encoding="utf-8")
    logger.debug("main | sortie (écrit)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
