# SPDX-License-Identifier: GPL-3.0-or-later
"""Codecs de sortie locale (issue #32) : rendu d'un document en texte.

Deux codecs, comme les plugins Logstash `stdout`/`file` :

- ``json_lines`` : une ligne JSON (``json.dumps``, UTF-8, sans ASCII
  d'échappement) par document — format NDJSON, celui de la DLQ ;
- ``rubydebug`` : le rendu « inspect Ruby » du plugin éponyme, lisible
  en console — paires ``"clé" => valeur`` indentées, une par ligne,
  virgule en fin de ligne sauf la dernière (simplifié : tableaux et
  sous-hachages restent sur une ligne, à la façon de ``inspect``).

Portée volontairement réduite (même discipline que ``fingerprint.py``,
voir FEATURES.md) : les types inconnus tombent sur ``repr``.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from loguru import logger

from pystashtemplate.tracing import traced

__all__ = [
    "CODECS",
    "encoder_json_lines",
    "encoder_rubydebug",
]


@traced
def encoder_json_lines(doc: Any) -> str:
    """Rend un document en une ligne JSON (codec ``json_lines``).

    Args:
        doc: Document événement (dictionnaire plat ou imbriqué).

    Returns:
        La ligne JSON, sans retour à la ligne final — l'appelant
        l'ajoute (une ligne par document).
    """
    texte = json.dumps(doc, ensure_ascii=False, default=str)
    logger.debug("encoder_json_lines | sortie | {} caractère(s)", len(texte))
    return texte


@traced
def _valeur_ruby(valeur: Any) -> str:
    """Rend une valeur isolée à la façon de ``inspect`` de Ruby (rubydebug).

    ``nil`` pour ``None``, ``true``/``false``, chaînes entre guillemets
    doubles (échappement Ruby : ``\\"`` ``\\\\`` ``\\n`` ``\\t``), nombres
    tels quels, horodatages ISO8601 sans guillemets (comme le plugin,
    qui affiche ``@timestamp`` nu), tableaux et hachages imbriqués sur
    une ligne.
    """
    if valeur is None:
        return "nil"
    if isinstance(valeur, bool):
        return "true" if valeur else "false"
    if isinstance(valeur, datetime):
        return valeur.isoformat()
    if isinstance(valeur, str):
        echappee = valeur.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\t", "\\t")
        return f'"{echappee}"'
    if isinstance(valeur, (int, float)):
        return str(valeur)
    if isinstance(valeur, list):
        return "[" + ", ".join(_valeur_ruby(x) for x in valeur) + "]"
    if isinstance(valeur, dict):
        return "{" + ", ".join(f"{_valeur_ruby(str(k))} => {_valeur_ruby(x)}" for k, x in valeur.items()) + "}"
    return repr(valeur)


@traced
def encoder_rubydebug(doc: Any) -> str:
    """Rend un document à la façon du codec ``rubydebug`` de Logstash.

    Args:
        doc: Document événement (dictionnaire plat ou imbriqué).

    Returns:
        Le texte multi-lignes ``{\\n    "clé" => valeur,\\n    …\\n}``,
        chaque paire sur sa ligne, virgule en fin de ligne sauf la
        dernière — sans retour à la ligne final.
    """
    paires = [f"    {_valeur_ruby(str(k))} => {_valeur_ruby(v)}" for k, v in doc.items()]
    texte = "{\n" + ",\n".join(paires) + "\n}"
    logger.debug("encoder_rubydebug | sortie | {} caractère(s)", len(texte))
    return texte


# Nom de codec → fonction de rendu. La validation du nom se fait côté
# sorties (message traduit), ce dictionnaire n'est pas user-facing.
CODECS = {
    "json_lines": encoder_json_lines,
    "rubydebug": encoder_rubydebug,
}
