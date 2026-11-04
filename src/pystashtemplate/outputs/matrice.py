# SPDX-License-Identifier: GPL-3.0-or-later
"""Matrice de compatibilité des sorties (issue #9), générée depuis le registre.

Une ligne par sortie inscrite (`REGISTRY`), une colonne par
fonctionnalité, lue dans ce que chaque classe déclare : capacités
(`Capacites` : actions, `_id`, accusé par élément, taille maximale de
requête), DLQ (paramètre `dlq` du constructeur), réessais (option
`retry_max` connue) et banc contre un vrai système en conteneur (fourni
par l'appelant, qui connaît les workflows du dépôt).

`scripts/matrice_sorties.py` insère le tableau dans `docs/sorties.md`
entre `DEBUT` et `FIN` ; un test vérifie qu'il est à jour.
"""

from __future__ import annotations

import inspect
from collections.abc import Iterable

from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.base import ACTIONS, Output

DEBUT = "<!-- matrice-sorties:debut (générée par scripts/matrice_sorties.py, ne pas éditer) -->"
FIN = "<!-- matrice-sorties:fin -->"

ENTETE = (
    "| Plugin | Actions | `_id` | Accusé par élément | Taille max. de requête | DLQ | Réessais | Banc en conteneur |",
    "|---|---|---|---|---|---|---|---|",
)


MIO = 1024 * 1024

# Cellule « oui » / « non » d'une fonctionnalité booléenne.
OUI_NON = {True: "oui", False: "non"}


def ligne(classe: type[Output], *, banc: bool) -> str:
    """Ligne Markdown d'une sortie : ses fonctionnalités déclarées.

    Args:
        classe: Classe de sortie inscrite au registre.
        banc: `True` si un banc contre un vrai système existe pour elle.

    Returns:
        La ligne du tableau (sans saut de ligne final).
    """
    logger.debug("ligne | entrée ({})", classe.plugin)
    capacites = classe.capacites
    if capacites.actions == frozenset(ACTIONS):
        actions = "toutes"
    else:
        actions = ", ".join(f"`{a}`" for a in ACTIONS if a in capacites.actions)
    taille_max = capacites.taille_max_requete
    if taille_max is None:
        taille = "—"
    elif taille_max % MIO == 0:
        taille = f"{taille_max // MIO} Mio"
    else:
        taille = f"{taille_max} o"
    dlq = "dlq" in inspect.signature(classe.__init__).parameters
    reessais = "retry_max" in getattr(classe, "OPTIONS_CONNUES", frozenset())
    cellules = [
        f"`{classe.plugin}`",
        actions,
        OUI_NON[capacites.accepte_id],
        OUI_NON[capacites.accuse_par_evenement],
        taille,
        OUI_NON[dlq],
        OUI_NON[reessais],
        OUI_NON[banc],
    ]
    logger.debug("ligne | sortie")
    return "| " + " | ".join(cellules) + " |"


def tableau(bancs: Iterable[str] = ()) -> str:
    """Tableau Markdown complet, une ligne par sortie du registre (ordre alphabétique).

    Args:
        bancs: Noms de plugin disposant d'un banc en conteneur.

    Returns:
        Le tableau, lignes séparées par des sauts de ligne, sans saut final.
    """
    logger.debug("tableau | entrée")
    avec_banc = set(bancs)
    lignes = [*ENTETE, *(ligne(REGISTRY[nom], banc=nom in avec_banc) for nom in sorted(REGISTRY))]
    logger.debug("tableau | sortie ({} sortie(s))", len(REGISTRY))
    return "\n".join(lignes)


def remplacer(document: str, bancs: Iterable[str] = ()) -> str:
    """Remplace la matrice entre les marqueurs `DEBUT`/`FIN` d'un document.

    Args:
        document: Contenu Markdown contenant les deux marqueurs.
        bancs: Noms de plugin disposant d'un banc en conteneur.

    Returns:
        Le document avec la matrice régénérée.

    Raises:
        ValueError: Marqueurs absents ou dans le désordre.
    """
    logger.debug("remplacer | entrée")
    debut = document.find(DEBUT)
    fin = document.find(FIN)
    if debut < 0 or fin < debut:
        logger.warning(_("matrice des sorties : marqueurs introuvables debut={debut} fin={fin}"), debut=debut, fin=fin)
        raise ValueError(_("marqueurs de la matrice des sorties absents ou dans le désordre"))
    avant = document[: debut + len(DEBUT)]
    apres = document[fin:]
    resultat = f"{avant}\n{tableau(bancs)}\n{apres}"
    logger.debug("remplacer | sortie")
    return resultat
