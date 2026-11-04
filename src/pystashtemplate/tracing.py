# SPDX-License-Identifier: GPL-3.0-or-later
"""Traçabilité systématique des fonctions : un `logger.debug` en entrée,
un `logger.debug` à CHAQUE sortie (retour normal ou exception).

Règle du projet (pas héritée de switch-capture — nouvelle convention
demandée explicitement) : toute fonction du code applicatif (`src/`, hors
`tests/`) doit être tracée à l'entrée et à la sortie. Voir PATTERNS.md,
« Traçabilité systématique des fonctions », pour la justification
complète.

Implémenté une seule fois ici, sous forme de décorateur, plutôt que
dupliqué à la main dans chaque fonction (un `logger.debug()` copié avant
chaque `return` est facile à oublier sur un chemin de sortie ajouté plus
tard — un nouveau `return` précoce, une exception qui se propage). Le
décorateur enveloppe l'appel entier : il ne peut structurellement pas
rater un chemin de sortie, contrairement à une convention appliquée à la
main.

Module intentionnellement sans AUCUNE dépendance interne au projet (ni
`app_core`, ni `credentials`, ni `app_cli`/`app_gtk`) : c'est le seul
moyen pour que tous les autres modules (y compris `credentials.py` et
`app_core.py`, qui doivent eux-mêmes rester indépendants l'un de l'autre)
puissent importer `traced` sans introduire de dépendance croisée entre
eux. Voir `tests/test_no_cross_imports.py`, qui vérifie mécaniquement
(via `ast`, pas de simple relecture) que le graphe de dépendances entre
modules de `src/` reste acyclique.
"""

from __future__ import annotations

import functools
import re
from typing import Any, Callable, Coroutine, TypeVar

from loguru import logger

F = TypeVar("F", bound=Callable)
A = TypeVar("A", bound=Callable[..., Coroutine[Any, Any, Any]])


# Identifiants d'une URL (`schéma://utilisateur:secret@hôte`) : partie
# avant `@` de l'autorité, jusqu'au premier `/`, espace ou guillemet.
_IDENTIFIANTS_URL = re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)[^\s/@'\"<>]+@")


def masquer_identifiants(texte: object) -> str:
    """Texte de `texte` où les identifiants d'URL sont remplacés par `***`.

    `http://user:secret@hote:9200/x` devient `http://***@hote:9200/x` :
    un message d'exception (httpx cite l'URL de la requête) ou une URL
    journalisée ne laisse ainsi jamais passer un mot de passe écrit dans
    `hosts` (#5).
    """
    return _IDENTIFIANTS_URL.sub(r"\1***@", str(texte))


def traced(func: F) -> F:
    """Journalise l'entrée et la sortie (normale ou par exception) de `func`.

    N'enregistre QUE le nom qualifié de la fonction (`__qualname__`) —
    jamais les arguments ni la valeur de retour : `credentials.py`
    manipule des secrets, et un décorateur générique appliqué à TOUTES les
    fonctions du projet ne doit jamais risquer d'en faire fuiter un dans
    les logs. Une fonction qui a besoin de journaliser un détail précis et
    non sensible (ex. la commande envoyée, voir
    `app_core.py::log_outbound_command`) le fait via son propre
    `logger.debug()` additionnel dans son corps — ce décorateur pose le
    marqueur d'entrée/sortie systématique, pas le détail métier.

    Préserve `__name__`/`__doc__`/`__wrapped__` (`functools.wraps`) : la
    fonction décorée reste introspectable normalement (aide, docs,
    `inspect.signature`, et — vérifié empiriquement, voir CLAUDE.md/
    PATTERNS.md — compatible avec les méthodes de classes GObject/GTK4
    telles que `__init__`/`do_activate`/les gestionnaires de signaux).

    Args:
        func: fonction (ou méthode) à tracer.

    Returns:
        La fonction décorée, de même signature apparente que `func`.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.debug("{} | entrée", func.__qualname__)
        try:
            result = func(*args, **kwargs)
        except BaseException as exc:
            logger.debug(
                "{} | sortie (exception {}) : {}", func.__qualname__, type(exc).__name__, masquer_identifiants(exc)
            )
            raise
        logger.debug("{} | sortie", func.__qualname__)
        return result

    return wrapper


def atraced(func: A) -> A:
    """Équivalent asynchrone de `traced` (#37) : entrée et sortie journalisées.

    Contrairement à `@traced` appliqué à une fonction `async def` — qui
    journalise la « sortie » dès la création de la coroutine, AVANT que
    l'appelant ne l'attende — ce décorateur enveloppe le `await` : la
    sortie n'est journalisée qu'une fois la coroutine réellement terminée
    (retour normal ou exception propagée). Mêmes règles loguru que
    `traced` : uniquement le nom qualifié (`__qualname__`), jamais les
    arguments ni la valeur de retour.

    Préserve `__name__`/`__doc__`/`__wrapped__` (`functools.wraps`) ;
    la fonction décorée reste une coroutine function, `await`able
    normalement.

    Args:
        func: fonction (ou méthode) asynchrone à tracer.

    Returns:
        La fonction décorée, de même signature apparente que `func`.
    """

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        logger.debug("{} | entrée", func.__qualname__)
        try:
            result = await func(*args, **kwargs)
        except BaseException as exc:
            logger.debug(
                "{} | sortie (exception {}) : {}", func.__qualname__, type(exc).__name__, masquer_identifiants(exc)
            )
            raise
        logger.debug("{} | sortie", func.__qualname__)
        return result

    return wrapper
