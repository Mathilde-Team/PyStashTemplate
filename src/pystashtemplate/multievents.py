# SPDX-License-Identifier: GPL-3.0-or-later
"""Filtres `clone` et `split` façon Logstash : plusieurs événements par message (issue #141).

`clone { clones => ["copie"] }` reprend logstash-filter-clone (`clone.rb`) :
l'événement d'origine continue tel quel, suivi d'une copie par entrée de
`clones` ; chaque copie reçoit le nom en `type` si
`ecs_compatibility => disabled`, sinon il est ajouté à `tags` (défaut
Logstash 8, `v8`). Les options communes (`add_field`, `add_tag`…) ne
s'appliquent qu'aux copies, comme `filter_matched(clone)`.

`split { field => "message" terminator => "\\n" target => "ligne" }` reprend
logstash-filter-split (`split.rb`) : une chaîne est découpée sur
`terminator` (chaîne littérale, `"\\n"` par défaut, découpage Ruby
`String#split(terminator, -1)`), une liste est parcourue élément par
élément ; chaque morceau non vide devient un nouvel événement, copie de
l'original avec le morceau dans `target` (ou dans `field`), et l'original
disparaît. Une chaîne qui ne contient pas le séparateur laisse l'événement
inchangé ; un champ absent ou d'un autre type ajoute le tag
`_splitparsefailure`.

Les nouveaux événements poursuivent le pipeline à partir du filtre suivant
(voir `interpreter.PipelineInterpreter.apply_events`).
"""

from __future__ import annotations

import copy
import re
from typing import Any

from loguru import logger

from pystashtemplate.templates import _MISSING, assign_field, parse_field_path, remove_field_path, resolve_field
from pystashtemplate.tracing import traced

SPLIT_FAILURE_TAG = "_splitparsefailure"


class CloneFilter:
    """Filtre `clone` compilé une fois par bloc."""

    @traced
    def __init__(self, options: dict[str, Any]) -> None:
        """Valide `clones` et `ecs_compatibility`.

        Raises:
            ValueError: `clones` absent ou qui n'est pas une liste de chaînes.
        """
        if "clones" not in options:
            raise ValueError("clone : option 'clones' obligatoire")
        clones = options["clones"]
        if isinstance(clones, str):
            clones = [clones]
        if not isinstance(clones, list) or not all(isinstance(c, str) for c in clones):
            raise ValueError("clone : 'clones' attend une liste de chaînes")
        self.clones: list[str] = clones
        self.ecs = str(options.get("ecs_compatibility", "v8")).strip().lower() != "disabled"
        logger.debug("CloneFilter | {} copie(s), ecs={}", len(self.clones), self.ecs)

    @traced
    def apply(self, doc: dict[str, Any]) -> list[dict[str, Any]]:
        """Copies de `doc` (sans l'original), une par entrée de `clones`."""
        copies: list[dict[str, Any]] = []
        for name in self.clones:
            clone = copy.deepcopy(doc)
            if self.ecs:
                tags = clone.get("tags")
                tags = [] if tags is None else list(tags) if isinstance(tags, list) else [tags]
                tags.append(name)
                clone["tags"] = tags
            else:
                clone["type"] = name
            copies.append(clone)
        logger.debug("CloneFilter.apply | {} copie(s)", len(copies))
        return copies


class SplitFilter:
    """Filtre `split` compilé une fois par bloc."""

    @traced
    def __init__(self, options: dict[str, Any]) -> None:
        """Valide `field`, `terminator` et `target`.

        Raises:
            ValueError: option qui n'est pas une chaîne, ou `field` vide.
        """
        for option in ("field", "terminator", "target"):
            if option in options and not isinstance(options[option], str):
                raise ValueError(f"split : {option!r} attend une chaîne")
        field = options.get("field", "message")
        if not field:
            raise ValueError("split : 'field' vide")
        self.field = parse_field_path(field)
        self.terminator: str = options.get("terminator", "\n")
        target = options.get("target")
        self.target = parse_field_path(target) if target else None
        logger.debug("SplitFilter | field={} target={}", field, target)

    @traced
    def apply(self, doc: dict[str, Any]) -> list[dict[str, Any]] | None:
        """Événements issus du découpage, ou None si `doc` reste seul (inchangé ou tagué).

        Une liste vide signifie que l'original disparaît sans remplaçant
        (chaîne vide, liste vide ou morceaux tous vides), comme Logstash.
        """
        value = resolve_field(doc, self.field)
        if isinstance(value, list):
            splits: list[Any] = value
            if self.target is None:
                remove_field_path(doc, self.field)
        elif isinstance(value, str):
            splits = _ruby_split(value, self.terminator)
            if len(splits) == 1:
                logger.debug("SplitFilter.apply | un seul morceau, événement inchangé")
                return None
        else:
            kind = "absent" if value is _MISSING else type(value).__name__
            logger.debug("SplitFilter.apply | champ {} non découpable ({})", self.field, kind)
            tags = doc.get("tags")
            tags = [] if tags is None else list(tags) if isinstance(tags, list) else [tags]
            if SPLIT_FAILURE_TAG not in tags:
                tags.append(SPLIT_FAILURE_TAG)
            doc["tags"] = tags
            return None
        target = self.target or self.field
        events: list[dict[str, Any]] = []
        for part in splits:
            if part is None or part == "":
                continue
            event = copy.deepcopy(doc)
            assign_field(event, target, part)
            events.append(event)
        logger.debug("SplitFilter.apply | {} événement(s)", len(events))
        return events


@traced
def _ruby_split(text: str, terminator: str) -> list[str]:
    """`text.split(terminator, -1)` de Ruby : chaîne vide → [], `" "` → découpage sur les blancs."""
    if text == "":
        logger.debug("_ruby_split | chaîne vide")
        return []
    if terminator == " ":
        stripped = text.lstrip(" \t\n\v\f\r")
        parts = re.split(r"[ \t\n\v\f\r]+", stripped) if stripped else []
    elif terminator == "":
        parts = [*text, ""]
    else:
        parts = text.split(terminator)
    logger.debug("_ruby_split | {} morceau(x)", len(parts))
    return parts
