# SPDX-License-Identifier: GPL-3.0-or-later
"""Traduction des messages destinés à l'utilisateur (issue #38).

Domaine gettext : ``rsyslogstach``. Langue source : le français (les
``msgid`` sont les messages d'origine). La langue est choisie par
l'environnement, comme pour tout programme gettext (``LANGUAGE``,
``LC_ALL``, ``LC_MESSAGES``, ``LANG``) : aucune langue n'est codée ici, la
seule liste est ``lang/LINGUAS``.

Catalogues compilés cherchés, dans cet ordre, au premier qui contient un
``.mo`` pour la langue demandée :

1. ``$PYSTASHTEMPLATE_LOCALEDIR`` (installation sur mesure) ;
2. ``lang/locale/`` du dépôt (exécution depuis une extraction, ``src/``
   dans le chemin : rsyslog, tests) ;
3. ``<sys.prefix>/share/locale`` (roue installée : ``lang/locale/`` y est
   posé comme données partagées, voir ``pyproject.toml``) ;
4. ``/usr/share/locale`` puis l'emplacement par défaut de gettext.

Sans catalogue, les messages restent en français.

Les journaux ``debug``/``trace`` (traces de développement, SOCLE §8) ne
sont pas traduits ; ``info`` et au-dessus, l'aide de la ligne de commande
le sont. Les messages loguru gardent leurs champs ``{...}`` : la
traduction doit les conserver à l'identique (``msgfmt --check`` vérifie la
cohérence des formats ``python-brace-format``).

Module sans dépendance interne autre que ``tracing`` (voir
``tests/test_no_cross_imports.py``).
"""

from __future__ import annotations

import gettext
import os
import sys
from pathlib import Path

from loguru import logger

from pystashtemplate.tracing import traced

DOMAIN = "pystashtemplate"
ENV_LOCALEDIR = "PYSTASHTEMPLATE_LOCALEDIR"
# Compatibilité : l'ancien nom (avant le découpage #111) reste lu en repli.
ENV_LOCALEDIR_ANCIEN = "RSYSLOGSTACH_LOCALEDIR"
REPO_LANG_DIR = Path(__file__).resolve().parents[2] / "lang" / "locale"
SYSTEM_LOCALEDIR = "/usr/share/locale"


@traced
def localedirs() -> list[str | None]:
    """Dossiers de catalogues, dans l'ordre de recherche (``None`` : défaut gettext)."""
    candidates: list[str | None] = []
    env = os.environ.get(ENV_LOCALEDIR) or os.environ.get(ENV_LOCALEDIR_ANCIEN)
    if env:
        candidates.append(env)
    if REPO_LANG_DIR.is_dir():
        candidates.append(str(REPO_LANG_DIR))
    candidates += [str(Path(sys.prefix) / "share" / "locale"), SYSTEM_LOCALEDIR, None]
    unique: list[str | None] = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    logger.debug("dossiers de catalogues : {}", unique)
    return unique


@traced
def load_translation() -> gettext.NullTranslations:
    """Catalogue de la langue de l'environnement, ou passe-plat (français)."""
    for localedir in localedirs():
        if gettext.find(DOMAIN, localedir):
            logger.debug("catalogue {} trouvé dans {}", DOMAIN, localedir)
            return gettext.translation(DOMAIN, localedir)
    logger.debug("aucun catalogue {} pour cette langue : messages en français", DOMAIN)
    return gettext.NullTranslations()


# Choisi une fois, à l'import : la langue d'un processus ne change pas en
# cours de route. Fonctions liées (pas de ``def``) : pas de trace debug à
# chaque message traduit.
translation = load_translation()
_ = translation.gettext
ngettext = translation.ngettext
