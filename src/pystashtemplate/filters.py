# SPDX-License-Identifier: GPL-3.0-or-later
"""Filtres d'extraction façon Logstash filter{} (dissect, regex, kv, drop, mutate, geoip, translate, date).

Extrait d'omprog.py (session 9, réorganisation selon le squelette de
Mathilde) pour respecter la séparation en modules à responsabilité unique
du squelette — voir PATTERNS.md, "Absence de dépendances circulaires
entre modules". Dépend de `tracing` et, depuis la session 14, de
`templates` (pour le rendu des gabarits sprintf de `mutate.add_field`,
voir `apply_mutate`/`MutateFilter` plus bas) ; jamais de `indexer`.

Depuis la session 24, dépend aussi de `enrichment` (`GeoIPLookup`, pour le
step "geoip", voir `GeoipFilter` plus bas) et de `util`
(`add_failure_tags`, partagée avec `interpreter.py::PipelineInterpreter`
pour le `tag_on_failure` de `date`/`geoip`) — les deux restent des feuilles
du graphe de dépendances (ne dépendent que de `tracing`), aucun risque de
cycle : voir PATTERNS.md pour le détail de cette nouvelle arête.

Depuis la session 25, `enrichment` sert aussi à `TranslateDictionary` (step
"translate", voir `TranslateFilter` plus bas) — aucune arête supplémentaire
dans le graphe de dépendances, `filters -> enrichment` existe déjà depuis
la session 24 pour `GeoIPLookup`.

Voir filters.example.json pour un exemple bâti sur le format de message
typique des switches HPE Comware ("%%10MODULE/sévérité/ÉVÉNEMENT: détail"),
et blocklist.example.txt pour le format attendu par `load_word_list`.
"""

from __future__ import annotations

import csv
import json
import re
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import maxminddb
import yaml
from loguru import logger

from pystashtemplate.dnslookup import DnsFilter
from pystashtemplate.enrichment import GeoIPLookup, TranslateDictionary
from pystashtemplate.i18n import _
from pystashtemplate.templates import (
    _MISSING,
    assign_field,
    parse_field_path,
    remove_field_path,
    render_template,
    resolve_field,
)
from pystashtemplate.tracing import atraced, traced
from pystashtemplate.util import add_failure_tags


@dataclass
class _Field:
    """Un champ extrait par un patron dissect, avec ses modificateurs."""

    name: str
    append: bool = False
    skip: bool = False
    skip_repeated_delimiter: bool = False


class Dissector:
    """Extraction de champs à partir d'un patron positionnel (façon filtre dissect de Logstash).

    Contrairement à un grok (regex), un patron dissect découpe une chaîne sur
    des délimiteurs littéraux entre marqueurs %{champ} : plus rapide qu'une
    regex, mais suppose une structure de délimiteurs fixe. Modificateurs pris
    en charge : %{+champ} (concatène à un champ du même nom déjà extrait),
    %{?champ} ou %{} (extrait sans conserver dans le résultat), %{champ->}
    (ignore les répétitions du délimiteur qui suit, utile pour des espaces
    multiples). %{&champ} (référence indirecte, une fonctionnalité plus rare
    du dissect Logstash) n'est pas pris en charge.
    """

    @traced
    def __init__(self, pattern: str) -> None:
        """Compile un patron dissect.

        Args:
            pattern: Patron du type "%{a}/%{b}: %{c}".

        Raises:
            ValueError: Patron sans aucun champ %{...}.
        """
        matches = list(re.finditer(r"%\{([^}]*)\}", pattern))
        if not matches:
            raise ValueError(f"patron dissect sans aucun champ %{{...}}: {pattern!r}")
        self._leading = pattern[: matches[0].start()]
        self._fields = [self._parse_token(match.group(1)) for match in matches]
        self._delims = [pattern[matches[i].end() : matches[i + 1].start()] for i in range(len(matches) - 1)]
        self._trailing = pattern[matches[-1].end() :]

    @staticmethod
    @traced
    def _parse_token(token: str) -> _Field:
        """Interprète un token %{...} et ses modificateurs."""
        skip_repeated_delimiter = token.endswith("->")
        if skip_repeated_delimiter:
            token = token[: -len("->")]
        append = token.startswith("+")
        skip = token.startswith("?") or token == ""
        name = token.lstrip("+?")
        return _Field(name, append=append, skip=skip, skip_repeated_delimiter=skip_repeated_delimiter)

    @traced
    def apply(self, source: str) -> dict[str, str] | None:
        """Applique le patron à une chaîne source.

        Args:
            source: Chaîne à découper.

        Returns:
            Le dictionnaire des champs extraits, ou None si un délimiteur du
            patron est introuvable dans la source (structure inattendue).
        """
        if not source.startswith(self._leading):
            return None
        pos = len(self._leading)
        result: dict[str, str] = {}
        for index, field in enumerate(self._fields):
            is_last = index == len(self._fields) - 1
            delim = self._trailing if is_last else self._delims[index]
            if delim:
                delim_pos = source.find(delim, pos)
                if delim_pos == -1:
                    return None
                value = source[pos:delim_pos]
                pos = delim_pos + len(delim)
                if field.skip_repeated_delimiter:
                    while source.startswith(delim, pos):
                        pos += len(delim)
            else:
                value = source[pos:]
                pos = len(source)
            if not field.skip:
                if field.append and field.name in result:
                    result[field.name] = result[field.name] + value
                else:
                    result[field.name] = value
        return result


class RegexFilter:
    """Extraction de champs par expression régulière à groupes nommés (grok simplifié).

    Complète Dissector pour les structures qu'un patron positionnel ne peut
    pas exprimer, typiquement un préfixe collé sans délimiteur (ex. le "10"
    et "IFNET" de "%%10IFNET/3/LINK_UPDOWN" chez les switches HPE Comware).
    """

    @traced
    def __init__(self, pattern: str) -> None:
        """Compile l'expression régulière.

        Args:
            pattern: Regex Python à groupes nommés, ex.
                r"%%\\d+(?P<module>[A-Z_]+)/(?P<severity_code>\\d+)/(?P<event_name>[A-Z_]+): ?(?P<detail>.*)".
        """
        self._regex = re.compile(pattern)

    @traced
    def apply(self, source: str) -> dict[str, str] | None:
        """Applique la regex à `source`.

        Args:
            source: Chaîne à analyser.

        Returns:
            Les groupes nommés capturés, ou None si la regex ne correspond pas.
        """
        match = self._regex.match(source)
        if match is None:
            return None
        return match.groupdict()


class KeyValueFilter:
    """Extraction de paires clé/valeur (façon filtre kv de Logstash).

    Options du plugin officiel reprises (issue #113) : une clé répétée
    dans la source produit une LISTE de valeurs (comme Logstash, qui
    stocke un tableau par clé) ; `allow_duplicate_values => false`
    déduplique ces listes en gardant l'ordre.
    """

    @traced
    def __init__(
        self,
        field_split: str = " ",
        value_split: str = "=",
        prefix: str = "",
        *,
        include_keys: list[str] | None = None,
        exclude_keys: list[str] | None = None,
        trim_key: str = "",
        trim_value: str = "",
        allow_duplicate_values: bool = True,
    ) -> None:
        """Initialise l'extracteur.

        Args:
            field_split: Séparateur entre paires (espace par défaut).
            value_split: Séparateur entre clé et valeur ("=" par défaut).
            prefix: Préfixe ajouté à chaque nom de champ extrait, pour
                éviter les collisions avec des champs existants.
            include_keys: Si fournie, ne garder QUE ces clés (filtrage
                avant préfixage, comme le plugin officiel).
            exclude_keys: Si fournie, écarter ces clés.
            trim_key: Caractères à retirer aux deux extrémités de chaque
                clé (après `strip()` usuel).
            trim_value: Caractères à retirer aux deux extrémités de
                chaque valeur.
            allow_duplicate_values: Si False, une clé répétée avec la
                même valeur ne la garde qu'une fois (dédup, ordre préservé).
        """
        self._field_split = field_split
        self._value_split = value_split
        self._prefix = prefix
        self._include_keys = include_keys
        self._exclude_keys = exclude_keys
        self._trim_key = trim_key
        self._trim_value = trim_value
        self._allow_duplicate_values = allow_duplicate_values

    @traced
    def apply(self, source: str) -> dict[str, str | list[str]]:
        """Découpe `source` en paires clé/valeur.

        Args:
            source: Chaîne à analyser, ex. "user=admin action=login result=ok".

        Returns:
            Les paires trouvées (clés préfixées par `prefix`) ; les segments
            sans séparateur value_split sont ignorés. Une clé apparue une
            seule fois vaut la chaîne trouvée ; une clé répétée vaut la
            liste de ses valeurs (liste dédupliquée si
            `allow_duplicate_values` vaut False).
        """
        paires: dict[str, list[str]] = {}
        for token in source.split(self._field_split):
            if self._value_split not in token:
                continue
            key, _sep, value = token.partition(self._value_split)
            key = key.strip()
            if self._trim_key:
                key = key.strip(self._trim_key)
            value = value.strip()
            if self._trim_value:
                value = value.strip(self._trim_value)
            if not key:
                continue
            if self._include_keys is not None and key not in self._include_keys:
                continue
            if self._exclude_keys is not None and key in self._exclude_keys:
                continue
            paires.setdefault(key, []).append(value)
        resultat: dict[str, str | list[str]] = {}
        for key, values in paires.items():
            if not self._allow_duplicate_values:
                values = list(dict.fromkeys(values))
            resultat[self._prefix + key] = values[0] if len(values) == 1 else values
        return resultat


class DropFilter:
    """Bloque (drop) le document si la valeur testée correspond à un mot d'une liste.

    Équivalent d'un filtre drop{} conditionnel de Logstash : liste de mots
    chargée depuis un fichier texte (load_word_list) ou fournie en ligne.
    Comparaison insensible à la casse par défaut.
    """

    @traced
    def __init__(self, words: list[str], mode: str = "contains", case_sensitive: bool = False) -> None:
        """Compile la liste de blocage.

        Args:
            words: Motifs à comparer.
            mode: "equals" (égalité stricte du champ entier) ou "contains"
                (le champ contient un des motifs comme sous-chaîne).
            case_sensitive: Comparaison sensible à la casse si True.

        Raises:
            ValueError: mode inconnu.
        """
        if mode not in ("equals", "contains"):
            raise ValueError(f"mode de drop inconnu: {mode!r} (attendu 'equals' ou 'contains')")
        self._mode = mode
        self._case_sensitive = case_sensitive
        self._words = words if case_sensitive else [word.lower() for word in words]

    @traced
    def matches(self, value: str) -> bool:
        """Indique si `value` correspond à un des mots de la liste (donc si le document doit être bloqué).

        Args:
            value: Valeur du champ testé.

        Returns:
            True si le document doit être bloqué.
        """
        haystack = value if self._case_sensitive else value.lower()
        if self._mode == "equals":
            return haystack in self._words
        return any(word in haystack for word in self._words)


MUTATE_HANDLED_KEYS = (
    "coerce",
    "rename",
    "update",
    "replace",
    "convert",
    "gsub",
    "uppercase",
    "capitalize",
    "lowercase",
    "strip",
    "split",
    "join",
    "merge",
    "copy",
    "add_field",
    "add_tag",
    "remove_field",
    "remove_tag",
    "tag_on_failure",
)


_BOOLEENS_VRAIS = ("true", "t", "yes", "y", "1", "1.0")
_BOOLEENS_FAUX = ("false", "f", "no", "n", "0", "0.0", "")


@traced
def _texte_vers_decimal(texte: str, *, eu: bool) -> Decimal:
    """Chaîne numérique avec séparateurs de milliers → `Decimal` exact.

    Format anglo-saxon (`integer`/`float`) : virgule = milliers, point =
    décimales (« 1,000.5 »). Format européen (`*_eu`) : point = milliers,
    virgule = décimales (« 1.000,5 »).
    """
    texte = texte.strip()
    texte = texte.replace(".", "").replace(",", ".") if eu else texte.replace(",", "")
    nombre = Decimal(texte)
    if not nombre.is_finite():
        raise ValueError(f"nombre non fini : {texte!r}")
    return nombre


@traced
def convert_value(value: Any, target_type: str, field_name: str) -> Any:
    """Caste `value` selon `target_type` (`mutate.convert`), comme Logstash.

    Règles de la documentation officielle du filtre `mutate` (option
    `convert`, issue #73) :
        - tableau : chaque élément converti ; hash : aucune action ;
        - `integer` : booléen → 1/0, nombre décimal tronqué (`3.99` → 3,
          `-2.7` → -2), chaîne lue avec virgules de milliers (« 1,000 »)
          et partie décimale tronquée (« 1000.5 » → 1000) ;
        - `integer_eu` : idem, points de milliers et virgule décimale
          (« 1.000 » → 1000) ;
        - `float` : booléen → 1.0/0.0, entier → flottant, chaîne avec
          virgules de milliers et point décimal (« 1,000.5 ») ;
        - `float_eu` : idem, « 1.000,5 » → 1000.5 ;
        - `string` : texte (booléens en `true`/`false`, comme Ruby) ;
        - `boolean` : 0/1 (entiers ou flottants) et chaînes « true », « t »,
          « yes », « y », « 1 », « 1.0 » / « false », « f », « no », « n »,
          « 0 », « 0.0 », chaîne vide (sans tenir compte de la casse ni des
          espaces) ; toute autre valeur passe inchangée.

    Écart assumé : une chaîne qui n'est pas un nombre (ex. « abc »)
    laisse `value` inchangée (log en DEBUG) au lieu du `to_i` de Ruby
    (« abc » → 0), plutôt que d'inventer une valeur : même philosophie que
    `FilterPipeline.apply` pour un filtre sans correspondance. `int` est
    accepté comme alias d'`integer`, `str` de `string`, `bool` de
    `boolean`.
    """
    if isinstance(value, list):
        return [convert_value(item, target_type, field_name) for item in value]
    if isinstance(value, dict):
        return value
    try:
        if target_type in ("integer", "integer_eu", "int"):
            if isinstance(value, bool):
                return int(value)
            if isinstance(value, (int, float)):
                return int(value)
            return int(_texte_vers_decimal(str(value), eu=target_type == "integer_eu"))
        if target_type in ("float", "float_eu"):
            if isinstance(value, (bool, int, float)):
                return float(value)
            return float(_texte_vers_decimal(str(value), eu=target_type == "float_eu"))
        if target_type in ("string", "str"):
            if isinstance(value, bool):
                return "true" if value else "false"
            return str(value)
        if target_type in ("boolean", "bool"):
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return {0: False, 1: True}.get(value, value)
            texte = str(value).strip().lower()
            if texte in _BOOLEENS_VRAIS:
                return True
            if texte in _BOOLEENS_FAUX:
                return False
            logger.debug("conversion booléenne impossible, champ inchangé : {}", field_name)
            return value
    except (TypeError, ValueError, ArithmeticError, OverflowError):
        logger.debug("conversion mutate impossible, champ inchangé : {}={!r} -> {}", field_name, value, target_type)
        return value
    logger.debug("type de conversion mutate inconnu, champ inchangé : {}", target_type)
    return value


@traced
def apply_gsub(doc: dict[str, Any], gsub: list) -> None:
    """`mutate { gsub => ["champ", "motif_regex", "remplacement", "champ2", ...] }` (triplets).

    "champ" accepte un chemin imbriqué façon `%{...}` (ex.
    `"[host][domain]"`) depuis la session 15, comme les autres options
    `mutate` — voir `apply_mutate`.
    """
    for i in range(0, len(gsub) - 2, 3):
        field_ref, pattern, replacement = gsub[i], gsub[i + 1], gsub[i + 2]
        path = parse_field_path(field_ref)
        value = resolve_field(doc, path)
        if isinstance(value, str) and isinstance(pattern, str):
            try:
                assign_field(doc, path, re.sub(pattern, replacement, value))
            except re.error:
                logger.debug("motif gsub invalide, ignoré : champ={} motif={!r}", field_ref, pattern)


@traced
def _assign_mutate_field(
    doc: dict[str, Any], key: str, raw_value: Any, *, now: datetime | None, on_missing: str
) -> None:
    """Rend (si besoin) puis écrit une valeur à son chemin de champ.

    Partagée par `add_field`/`replace` (session 15) et, depuis la
    session 20, par `update`/`coerce` : les quatre options écrivent une
    valeur (littérale ou gabarit sprintf) à un chemin de champ de la même
    façon, seule la CONDITION préalable diffère (voir `apply_mutate` —
    `add_field`/`replace` inconditionnels, `update` seulement si le champ
    existe déjà, `coerce` seulement si le champ existe et vaut `null`).
    `add_field`/`replace` ont le même effet dans cette implémentation
    volontairement simplifiée (voir "Limites assumées" dans la docstring
    du module `filters.py` concerné — pas de logique d'ajout à un tableau
    existant façon Logstash pour `add_field` sur un champ déjà présent).
    """
    rendered = (
        render_template(raw_value, doc, now=now, on_missing=on_missing) if isinstance(raw_value, str) else raw_value
    )
    assign_field(doc, parse_field_path(key), rendered)


@traced
def apply_common_options(
    doc: dict[str, Any], options: dict[str, Any], *, now: datetime | None = None, on_missing: str = "keep"
) -> None:
    """Options communes à tout filtre Logstash : `add_field`, `add_tag`, `remove_field`, `remove_tag` (#116).

    Appliquées dans cet ordre, comme `filter_matched` de Logstash, et
    seulement après un filtre RÉUSSI : `mutate` (via `apply_mutate`) et,
    depuis #116, les autres plugins natifs de `interpreter` (`dissect`,
    `date`, `geoip`, `translate`, `mmdb`…). Les valeurs d'`add_field` sont
    des gabarits sprintf ; `add_tag`/`remove_tag` sont des listes plates.
    """
    logger.debug("apply_common_options() called | options={}", sorted(set(options) & set(COMMON_OPTION_KEYS)))
    add_fields = options.get("add_field", {})
    if isinstance(add_fields, dict):
        for key, raw_value in add_fields.items():
            _assign_mutate_field(doc, key, raw_value, now=now, on_missing=on_missing)
    add_tags = options.get("add_tag")
    if isinstance(add_tags, list):
        tags = doc.setdefault("tags", [])
        tags.extend(tag for tag in add_tags if tag not in tags)
    remove_fields = options.get("remove_field", [])
    if isinstance(remove_fields, list):
        for field_ref in remove_fields:
            remove_field_path(doc, parse_field_path(field_ref))
    remove_tags = options.get("remove_tag")
    if isinstance(remove_tags, list) and isinstance(doc.get("tags"), list):
        doc["tags"] = [tag for tag in doc["tags"] if tag not in remove_tags]
    logger.debug("apply_common_options() returning | tags={}", doc.get("tags"))


#: Options communes à tout filtre (voir `apply_common_options`, #116).
COMMON_OPTION_KEYS = ("add_field", "add_tag", "remove_field", "remove_tag")


@traced
def _apply_string_case(doc: dict[str, Any], field_ref: str, transform: Any) -> None:
    """Applique `transform` (`str.upper`/`str.lower`/`str.capitalize`) à un champ texte, en place.

    Partagée par `uppercase`/`capitalize`/`lowercase` (session 20) : les
    trois options n'ont que la fonction de transformation qui change,
    tout le reste (résolution du chemin, tolérance à un champ absent/non
    textuel) est identique. Champ absent ou non textuel : ignoré
    silencieusement, même tolérance que `strip` (qui ne s'applique lui
    aussi qu'à une valeur `str`) — cohérent avec le comportement officiel
    documenté ("Only works on string fields" pour `split`, même principe
    retenu ici bien que non explicitement écrit pour ces trois options).
    """
    path = parse_field_path(field_ref)
    value = resolve_field(doc, path)
    if isinstance(value, str):
        assign_field(doc, path, transform(value))


@traced
def _apply_merge(doc: dict[str, Any], dest_ref: str, source_ref: str) -> None:
    """`mutate.merge` (session 20) : fusionne `source_ref` DANS `dest_ref`, en place.

    Reproduit la matrice documentée par Logstash lui-même : deux hash
    fusionnent (`dict.update`, la source l'emporte sur des clés
    communes) ; une chaîne se comporte comme une liste à un seul élément
    pour la fusion (conversion automatique — `array + string` fonctionne,
    `string + string` produit un tableau à deux éléments dans
    `dest_field`) ; `array`/`string` et `hash` ne se combinent JAMAIS
    ("array et hash ne fonctionnent pas ensemble", doc officielle) —
    journalisé en DEBUG et ignoré plutôt que de deviner une fusion
    arbitraire. Un `dest_ref` absent reçoit directement la valeur de
    `source_ref` telle quelle (cas non documenté explicitement par
    Logstash mais sans ambiguïté : rien à fusionner encore).
    """
    dest_path = parse_field_path(dest_ref)
    source_path = parse_field_path(source_ref)
    dest_value = resolve_field(doc, dest_path)
    source_value = resolve_field(doc, source_path)
    if source_value is _MISSING:
        return
    if dest_value is _MISSING:
        assign_field(doc, dest_path, source_value)
        return
    if isinstance(dest_value, dict) and isinstance(source_value, dict):
        merged = dict(dest_value)
        merged.update(source_value)
        assign_field(doc, dest_path, merged)
    elif isinstance(dest_value, dict) or isinstance(source_value, dict):
        logger.debug(
            "merge impossible (hash incompatible avec un tableau/une chaîne), ignoré : {} <- {}",
            dest_ref,
            source_ref,
        )
    else:
        dest_list = dest_value if isinstance(dest_value, list) else [dest_value]
        source_list = source_value if isinstance(source_value, list) else [source_value]
        assign_field(doc, dest_path, dest_list + source_list)


@traced
def apply_mutate(
    doc: dict[str, Any], options: dict[str, Any], *, now: datetime | None = None, on_missing: str = "keep"
) -> None:
    """Applique un ensemble d'options `mutate` façon Logstash à `doc`, en place.

    Fonction pure, unique implémentation partagée par `MutateFilter`
    (ce module, étape "mutate" de `FilterPipeline`/`--filters`, session 14)
    ET `interpreter.PipelineInterpreter._apply_mutate` (`filter { mutate
    { ... } }` d'un pipeline `.conf`, session 11) — un seul endroit qui
    connaît la sémantique de chaque option, aucune duplication entre les
    deux mécanismes de filtrage du projet (voir PATTERNS.md).

    Chemins de champs imbriqués (session 15) : chaque nom de champ
    manipulé ici (hors `add_tag`/`remove_tag`, toujours plats) accepte la
    même syntaxe qu'une référence `%{...}` — `"hostname"` (plat) ou
    `"[syslog][name]"` (imbriqué), via `templates.parse_field_path`/
    `assign_field`/`resolve_field`/`remove_field_path`. Avant la
    session 15, ces options ne traitaient chaque clé QUE comme une clé
    plate littérale : un `add_field => { "[syslog][name]" => ... }`
    écrivait une clé au nom `"[syslog][name]"` au premier niveau du
    document, au lieu de `doc["syslog"]["name"]` — confirmé en rejouant un
    pipeline de production réel (voir CLAUDE.md, session 15).

    Ordre d'application (session 20) : reproduit l'ordre RÉEL documenté
    par le plugin `mutate` officiel de Logstash ("Mutations in a config
    file are executed in this order") — `coerce`, `rename`, `update`,
    `replace`, `convert`, `gsub`, `uppercase`, `capitalize`, `lowercase`,
    `strip`, `split`, `join`, `merge`, `copy` — puis les quatre options
    COMMUNES à TOUT filtre Logstash (`add_field`, `add_tag`,
    `remove_field`, `remove_tag`, dans cet ordre précis — reproduit
    `LogStash::Filters::Base#filter_matched`, pas la liste "mutate"
    ci-dessus, dont elles sont d'ailleurs absentes), toujours après les
    mutations propres à `mutate` elle-même. Avant cette session, l'ordre
    utilisé ici était arbitraire (`rename`/`convert`/`gsub`/`strip`/
    `add_field`/`replace`/`remove_field`/`add_tag`/`remove_tag`) — un
    écart de comportement réel si un même champ est touché par deux
    options d'un seul bloc (ex. `convert` puis `strip` ne rend pas le
    même résultat que l'inverse sur une valeur avec des espaces
    superflus), resté sans conséquence observable jusqu'ici car aucun
    test/pipeline réel de ce projet ne combinait deux options sur un même
    champ dans un seul bloc `mutate` (vérifié en relisant chaque
    combinaison existante avant ce changement, voir CLAUDE.md session 20).

    `tag_on_failure` (session 23, chaîne unique, défaut `"_mutate_error"`
    — DIFFÉRENT de `tag_on_failure` de `grok`/`date`/`geoip`, validé par
    Logstash comme une LISTE de tags, jamais une chaîne unique ; vérifié
    via le code source réel du plugin officiel, pas supposé par analogie
    avec les autres) : TOUTE la séquence ci-dessus (mutations propres à
    `mutate` PUIS options communes) est protégée par un seul bloc
    try/except, reproduisant `filter(event) ... rescue => ex ...
    event.tag(@tag_on_failure)` du plugin RÉEL — si une opération lève une
    exception, les opérations déjà exécutées avant elle restent
    appliquées (aucun rollback, comme Logstash), celles qui suivaient dans
    l'ordre ci-dessus, `add_field`/`add_tag`/`remove_field`/`remove_tag`
    INCLUS, sont purement et simplement SAUTÉES (jamais exécutées), et le
    tag est ajouté à `doc["tags"]` (sans doublon) à la place. Une valeur
    de `tag_on_failure` qui n'est pas une chaîne (ex. une liste, par
    analogie erronée avec `grok`/`date`/`geoip`) retombe sur le défaut
    `"_mutate_error"` plutôt que d'être silencieusement ignorée — cohérent
    avec la validation stricte `:validate => :string` du plugin officiel
    (une configuration qui s'attend à ce comportement de LISTE échouerait
    au démarrage sur un vrai Logstash ; ce projet, plus tolérant, retombe
    sur le défaut au lieu de faire échouer tout le chargement du
    pipeline).

    Concrètement, deux cas RÉELS (pas seulement théoriques) lèvent
    aujourd'hui une exception dans ce module, jusqu'ici NON interceptée :
    `split => { champ => "" }` (séparateur vide, `str.split("")` lève
    `ValueError` en Python — cas qui, avant cette session, faisait
    planter tout le traitement du document plutôt que d'être toléré comme
    les autres erreurs déjà gérées individuellement, ex. `merge`
    hash/tableau incompatible) et un chemin de champ imbriqué mal formé
    dans N'IMPORTE laquelle des options ci-dessus (ex.
    `rename => { "[host" => "renamed" }`, crochet non refermé —
    `templates.parse_field_path` lève déjà `ValueError` pour ce cas,
    documenté depuis la session 15, mais jamais intercepté par
    `apply_mutate` lui-même avant cette session).

    Args:
        doc: Document à modifier en place.
        options: Options `mutate` au format JSON — dict {champ: valeur}
            pour `coerce`/`rename`/`update`/`replace`/`convert`/`split`/
            `join`/`merge`/`copy` ; `gsub` en liste plate de triplets
            [champ, motif, remplacement, champ2, ...] ; listes de noms de
            champs/tags pour `uppercase`/`capitalize`/`lowercase`/
            `strip`/`remove_field`/`add_tag`/`remove_tag` — mêmes formes
            que la syntaxe `mutate` officielle de Logstash (hash => dict
            JSON, array => liste JSON), qu'il s'agisse d'un step JSON
            `--filters` ou d'un bloc `.conf` déjà normalisé par
            `interpreter._normalize_attrs`. `add_tag`/`remove_tag`
            restent des noms de tags plats (jamais des chemins imbriqués),
            comme dans Logstash. `tag_on_failure` : chaîne unique, voir
            plus haut.
        now: Horodatage pour le rendu des gabarits sprintf de `add_field`/
            `replace`/`update`/`coerce` (voir `templates.render_template`)
            ; `None` = horodatage réel.
        on_missing: Comportement de ces mêmes gabarits pour un champ
            référencé absent ("keep" ou "empty", voir `templates.py`).
    """
    try:
        for field_ref, raw_value in options.get("coerce", {}).items():
            path = parse_field_path(field_ref)
            if resolve_field(doc, path) is None:
                _assign_mutate_field(doc, field_ref, raw_value, now=now, on_missing=on_missing)
        for old_ref, new_ref in options.get("rename", {}).items():
            old_path = parse_field_path(old_ref)
            value = resolve_field(doc, old_path)
            if value is not _MISSING:
                remove_field_path(doc, old_path)
                assign_field(doc, parse_field_path(new_ref), value)
        for field_ref, raw_value in options.get("update", {}).items():
            path = parse_field_path(field_ref)
            if resolve_field(doc, path) is not _MISSING:
                _assign_mutate_field(doc, field_ref, raw_value, now=now, on_missing=on_missing)
        for key, raw_value in options.get("replace", {}).items():
            _assign_mutate_field(doc, key, raw_value, now=now, on_missing=on_missing)
        for field_ref, target_type in options.get("convert", {}).items():
            path = parse_field_path(field_ref)
            value = resolve_field(doc, path)
            if value is not _MISSING:
                assign_field(doc, path, convert_value(value, target_type, field_ref))
        gsub = options.get("gsub")
        if isinstance(gsub, list):
            apply_gsub(doc, gsub)
        for field_ref in options.get("uppercase", []):
            _apply_string_case(doc, field_ref, str.upper)
        for field_ref in options.get("capitalize", []):
            _apply_string_case(doc, field_ref, str.capitalize)
        for field_ref in options.get("lowercase", []):
            _apply_string_case(doc, field_ref, str.lower)
        for field_ref in options.get("strip", []):
            path = parse_field_path(field_ref)
            value = resolve_field(doc, path)
            if isinstance(value, str):
                assign_field(doc, path, value.strip())
        for field_ref, separator in options.get("split", {}).items():
            path = parse_field_path(field_ref)
            value = resolve_field(doc, path)
            if isinstance(value, str) and isinstance(separator, str):
                assign_field(doc, path, value.split(separator))
            elif value is not _MISSING:
                logger.debug("split impossible (champ non textuel), ignoré : {}", field_ref)
        for field_ref, separator in options.get("join", {}).items():
            path = parse_field_path(field_ref)
            value = resolve_field(doc, path)
            if isinstance(value, list) and isinstance(separator, str):
                assign_field(doc, path, separator.join(str(item) for item in value))
            elif value is not _MISSING:
                logger.debug("join impossible (champ non tableau), ignoré : {}", field_ref)
        for dest_ref, source_ref in options.get("merge", {}).items():
            _apply_merge(doc, dest_ref, source_ref)
        for source_ref, dest_ref in options.get("copy", {}).items():
            path = parse_field_path(source_ref)
            value = resolve_field(doc, path)
            if value is not _MISSING:
                assign_field(doc, parse_field_path(dest_ref), deepcopy(value))

        # Options communes à tout filtre Logstash (add_field/add_tag/
        # remove_field/remove_tag) — voir la note d'ordre ci-dessus.
        # DANS le même bloc try que les mutations ci-dessus (comme
        # `filter_matched`, appelé par le VRAI plugin dans le même begin
        # que ses propres mutations) : sautées elles aussi si une
        # exception a déjà été levée plus haut, jamais appliquées "à
        # moitié" (session 23).
        apply_common_options(doc, options, now=now, on_missing=on_missing)
    except Exception as exc:  # reproduit le `rescue => ex` générique du plugin réel
        logger.warning(_("exception pendant l'application du filtre mutate, opérations restantes ignorées : {}"), exc)
        tag_on_failure = options.get("tag_on_failure", "_mutate_error")
        if not isinstance(tag_on_failure, str):
            tag_on_failure = "_mutate_error"
        tags = doc.setdefault("tags", [])
        if tag_on_failure not in tags:
            tags.append(tag_on_failure)

    unhandled = sorted(set(options) - set(MUTATE_HANDLED_KEYS))
    if unhandled:
        logger.debug("options mutate non interprétées, ignorées : {}", unhandled)


class MutateFilter:
    """`mutate` façon Logstash (coerce/rename/update/replace/convert/gsub/uppercase/
    capitalize/lowercase/strip/split/join/merge/copy/add_field/remove_field/add_tag/remove_tag/
    tag_on_failure depuis la session 23).

    Contrairement à `Dissector`/`RegexFilter`/`KeyValueFilter`, n'attend
    aucun champ "source" textuel à extraire : opère sur le document
    ENTIER, en place — voir `FilterPipeline._compile_step`/`apply`, qui le
    reconnaît spécifiquement (comme `DropFilter`) plutôt que de le faire
    passer par le chemin source/target générique des filtres d'extraction.
    Toute la logique vit dans `apply_mutate` (ce module) — cette classe
    n'est qu'un wrapper compilé une fois par `FilterPipeline`, cohérent
    avec `Dissector`/`RegexFilter` (compilés une fois, appliqués plusieurs
    fois). Chemins de champs imbriqués façon `%{[a][b]}` acceptés partout
    depuis la session 15 — voir `apply_mutate`.
    """

    @traced
    def __init__(self, options: dict[str, Any]) -> None:
        """Mémorise les options `mutate` de cette étape (voir `apply_mutate` pour leur format)."""
        self._options = options

    @traced
    def apply(self, doc: dict[str, Any], *, now: datetime | None = None, on_missing: str = "keep") -> None:
        """Applique les options mémorisées à `doc`, en place (voir `apply_mutate`)."""
        apply_mutate(doc, self._options, now=now, on_missing=on_missing)


class GeoipFilter:
    """`geoip` façon Logstash (source/database/target/fields/tag_on_failure), pour `--filters`.

    Répond à l'item resté ouvert depuis la session 21 (voir FEATURES.md,
    "Non fait délibérément" des sessions 21-23) : `geoip` n'était implémenté
    que pour `--pipeline-conf`
    (`interpreter.py::PipelineInterpreter._apply_geoip`), pas pour
    `--filters`/`FilterPipeline`, contrairement à `mutate` (partagé entre
    les deux mécanismes depuis la session 14). Réutilise le même format de
    sortie que `_apply_geoip`/`GeoIPEnricher` (`enrichment._build_geoip_record`,
    via `enrichment.GeoIPLookup`) : UNE SEULE implémentation du schéma
    GeoIP2 pour les TROIS mécanismes de géolocalisation du projet
    (`--geoip-db`, `--pipeline-conf`, `--filters`), jamais deux qui
    pourraient diverger — même motif que `apply_mutate` (session 14) et
    `_build_geoip_record` (session 21) elle-même.

    Comme `MutateFilter`, opère sur le document ENTIER en place (résout
    lui-même `source` via `templates.resolve_field`) : `FilterPipeline`
    le reconnaît spécifiquement dans `apply()`, il ne passe donc pas par
    le chemin source/target générique des filtres d'extraction
    (`Dissector`/`RegexFilter`/`KeyValueFilter`).

    Différence assumée avec `--pipeline-conf` : `interpreter.py` ouvre sa
    base GeoIP2 PARESSEUSEMENT (au premier message qui atteint le bloc
    `geoip {}`), une base illisible étant alors journalisée en DEBUG et
    ignorée pour ce step (le fichier `.conf` reste, lui, du texte parsé
    une fois, jamais "exécuté" avant le premier message) — voir sa
    docstring. `FilterPipeline`, elle, connaît déjà TOUTES ses étapes dès
    sa construction JSON : cette classe ouvre donc la base à la
    compilation du step (`__init__`, comme `Dissector`/`RegexFilter`
    compilent leur patron), et une base `.mmdb` illisible ou un step mal
    formé (`source`/`database` absents ou non textuels) font ÉCHOUER
    `FilterPipeline(...)` tout de suite (`ValueError`) plutôt qu'au
    premier message concerné — cohérent avec le contrat déjà documenté de
    `FilterPipeline.__init__` ("Raises: ValueError: ... patron/config
    invalide") et avec `--geoip-db`/`GeoIPEnricher`, elle aussi ouverte
    eagerly au démarrage d'`omprog`. `target` prend `"geoip"` par défaut
    si absent (comme Logstash, comme `_apply_geoip`) ; `database` reste,
    lui, toujours requis explicitement (aucune base GeoLite2 embarquée
    dans ce projet, même choix que les deux autres mécanismes). Résultat
    écrit à `target` comme un REMPLACEMENT, pas une fusion (même choix
    que `_apply_geoip` — le schéma GeoIP2 officiel est un sous-objet
    autonome).
    """

    @traced
    def __init__(self, options: dict[str, Any], *, lookup_factory: Any = GeoIPLookup) -> None:
        """Résout les options du step et ouvre la base geoip.

        Args:
            options: Options du step ("source"/"database" requis,
                "target"/"fields"/"tag_on_failure" optionnels — voir la
                docstring de la classe et `interpreter.py::_apply_geoip`
                pour leur format exact, identique ici).
            lookup_factory: Callable construisant la `GeoIPLookup` à
                partir du chemin de base ; injectable pour les tests
                (double de test), comme
                `PipelineInterpreter._geoip_lookup_factory`.

        Raises:
            ValueError: "source"/"database"/"target" absents ou non
                textuels, ou base `.mmdb` illisible/invalide.
        """
        source = options.get("source")
        database = options.get("database")
        target = options.get("target", "geoip")
        if not (isinstance(source, str) and isinstance(database, str) and isinstance(target, str)):
            raise ValueError("step geoip mal formé : 'source' et 'database' (chaînes) sont requis")
        try:
            self._lookup = lookup_factory(database)
        except (OSError, maxminddb.InvalidDatabaseError) as exc:
            logger.error(_("base geoip illisible : {} ({})"), database, exc)
            raise ValueError(f"base geoip illisible : {database} ({exc})") from exc
        self._source = source
        self._target = target
        self._fields = options.get("fields")
        self._tag_on_failure = options.get("tag_on_failure", ["_geoip_lookup_failure"])

    @traced
    def apply(self, doc: dict[str, Any]) -> None:
        """Géolocalise `doc[source]` et écrit le résultat sous `target`, en place.

        `tag_on_failure` (défaut `["_geoip_lookup_failure"]`, comme
        Logstash) signale par un tag dans `doc["tags"]` une IP absente/non
        textuelle ou sans correspondance dans la base — `target` reste
        alors inchangé, exactement comme `interpreter.py::_apply_geoip`.
        """
        ip = resolve_field(doc, parse_field_path(self._source))
        if not isinstance(ip, str):
            add_failure_tags(doc, self._tag_on_failure)
            return
        record = self._lookup.lookup(ip)
        if not record:
            add_failure_tags(doc, self._tag_on_failure)
            return
        fields = self._fields
        if isinstance(fields, list) and fields:
            record = {key: value for key, value in record.items() if key in fields}
        assign_field(doc, parse_field_path(self._target), record)

    @traced
    def close(self) -> None:
        """Ferme la base geoip. Voir `FilterPipeline.close`."""
        self._lookup.close()


@traced
def _texte_ruby(valeur: Any) -> str:
    """Conversion en texte façon Ruby `to_s` (clé de recherche `translate`, #119)."""
    if valeur is None:
        texte = ""
    elif isinstance(valeur, bool):
        texte = "true" if valeur else "false"
    elif isinstance(valeur, (dict, list)):
        texte = json.dumps(valeur, ensure_ascii=False, separators=(",", ":"))
    else:
        texte = str(valeur)
    logger.debug("_texte_ruby() | {!r}", texte)
    return texte


@traced
def _option_booleenne(valeur: Any, defaut: bool | None) -> bool | None:
    """Option booléenne Logstash : `true`/`false` natifs ou en texte, sinon `defaut`."""
    if isinstance(valeur, bool):
        resultat: bool | None = valeur
    elif isinstance(valeur, str) and valeur.strip().lower() in {"true", "false"}:
        resultat = valeur.strip().lower() == "true"
    else:
        resultat = defaut
    logger.debug("_option_booleenne() | {}", resultat)
    return resultat


class TranslateFilter:
    """Plugin `translate` façon Logstash, partagé par `--filters` et `--pipeline-conf` (#119).

    Reproduit `logstash-filter-translate` (`translate.rb`,
    `single_value_update.rb`, `array_of_values_update.rb`,
    `array_of_maps_value_update.rb`, `fetch_strategy/memory.rb`) :

    - `source` (ou `field`, ancien nom) : champ cherché, converti en texte
      comme Ruby `to_s` (tableau : premier élément). Champ absent : rien.
    - `target` (ou `destination`) : défaut `source` (traduction en place,
      `ecs_compatibility` v1/v8 de Logstash 8), ou `translation` si
      `ecs_compatibility => disabled`.
    - `override` : faux par défaut, vrai pour une traduction en place ; un
      `target` déjà présent n'est alors pas réécrit. `override => false`
      en place est refusé, comme le plugin.
    - `dictionary` (hash en ligne, ou tableau de paires) ou
      `dictionary_path` (`.yml`/`.yaml`/`.json`/`.csv`, voir
      `enrichment.TranslateDictionary`) ; les deux ensemble sont refusés.
      Les clés sont comparées en texte.
    - `exact` (vrai) et `regex` (faux) : correspondance exacte ; clés vues
      comme regex (premier motif qui trouve, dans l'ordre du dictionnaire) ;
      ou, avec `exact => false`, remplacement de chaque clé trouvée dans la
      valeur (`"foofing"` -> `"barfing"`).
    - `fallback` : gabarit sprintf écrit sans correspondance (une valeur
      non textuelle est écrite telle quelle).
    - `iterate_on` : égal à `source`, traduit chaque élément d'un tableau
      vers un tableau `target` de même taille (`fallback` ou `None` sans
      correspondance) ; sinon, tableau d'objets : `source`/`target` sont
      relatifs à chaque objet, et `override` n'est pas consulté.
    - `refresh_interval` (300 s) et `refresh_behaviour` (`merge` ou
      `replace`) : le fichier est relu au premier document après
      l'intervalle s'il a changé (pas de minuterie de fond ; 0 désactive).

    `yaml_dictionary_code_point_limit` et `yaml_load_strategy` sont acceptés
    sans effet. Comme le plugin, les options communes s'appliquent si une
    traduction a eu lieu ou si la traduction est en place.

    Le dictionnaire fichier est chargé à la construction : un fichier
    introuvable ou malformé, ou une option incohérente, lève `ValueError`
    (refus de `FilterPipeline(...)` ; bloc ignoré par `interpreter.py`).
    """

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        dictionary_factory: Any = TranslateDictionary,
        clock: Any = time.monotonic,
    ) -> None:
        """Valide les options et charge le dictionnaire.

        Args:
            options: Options du plugin (voir la docstring de la classe).
            dictionary_factory: Callable `chemin -> TranslateDictionary`,
                injectable pour les tests et pour le cache par chemin de
                `PipelineInterpreter`.
            clock: Horloge monotone (secondes) de `refresh_interval`.

        Raises:
            ValueError: Options incohérentes, ou dictionnaire illisible.
        """
        self._source = self._choisir(options, "source", "field")
        if self._source is None:
            raise ValueError("step translate mal formé : 'source' (chaîne) est requis")
        target = self._choisir(options, "target", "destination")
        if target is None:
            ecs = str(options.get("ecs_compatibility", "v8")).strip().lower()
            target = "translation" if ecs == "disabled" else self._source
        self._target = target
        override = _option_booleenne(options.get("override"), None)
        if self._source == self._target:
            if override is False:
                raise ValueError("step translate mal formé : 'override => false' sans effet en traduction en place")
            override = True
        self._override = bool(override)
        self._exact = bool(_option_booleenne(options.get("exact"), True))
        self._regex = bool(_option_booleenne(options.get("regex"), False))
        self._fallback = options.get("fallback")
        iterate_on = options.get("iterate_on")
        self._iterate_on = iterate_on if isinstance(iterate_on, str) and iterate_on else None
        self._comportement = str(options.get("refresh_behaviour", "merge")).strip().lower()
        if self._comportement not in {"merge", "replace"}:
            raise ValueError(f"step translate mal formé : refresh_behaviour inconnu {self._comportement!r}")
        try:
            self._intervalle = float(options.get("refresh_interval", 300))
        except (TypeError, ValueError) as exc:
            logger.debug("translate : refresh_interval invalide : {}", exc)
            intervalle = options.get("refresh_interval")
            raise ValueError(f"step translate mal formé : refresh_interval {intervalle!r}") from exc
        self._chemin_source = parse_field_path(self._source)
        self._chemin_target = parse_field_path(self._target)
        self._chemin_iteration = parse_field_path(self._iterate_on) if self._iterate_on else []
        self._dictionary: Any = None
        self._entrees = self._entrees_en_ligne(options.get("dictionary"))
        dictionary_path = options.get("dictionary_path")
        if dictionary_path is not None:
            if not isinstance(dictionary_path, str):
                raise ValueError("step translate mal formé : 'dictionary_path' doit être une chaîne")
            if self._entrees:
                raise ValueError("step translate mal formé : 'dictionary' et 'dictionary_path' sont exclusifs")
            try:
                self._dictionary = dictionary_factory(dictionary_path)
            except (OSError, yaml.YAMLError, ValueError, csv.Error) as exc:
                logger.error(_("dictionnaire translate illisible : {} ({})"), dictionary_path, exc)
                raise ValueError(f"dictionnaire translate illisible : {dictionary_path} ({exc})") from exc
            self._entrees = dict(self._dictionary.mapping)
        self._clock = clock
        self._dernier_rafraichissement = clock()
        self._version = getattr(self._dictionary, "version", 0)
        self._preparer()

    @staticmethod
    @traced
    def _choisir(options: dict[str, Any], nom: str, ancien: str) -> str | None:
        """Option `nom` ou son ancien nom `ancien` (les deux ensemble : refus, comme le plugin)."""
        valeur, valeur_ancienne = options.get(nom), options.get(ancien)
        if valeur is not None and valeur_ancienne is not None:
            raise ValueError(f"step translate mal formé : '{ancien}' et '{nom}' sont exclusifs")
        choisie = valeur if valeur is not None else valeur_ancienne
        if choisie is not None and not (isinstance(choisie, str) and choisie):
            raise ValueError(f"step translate mal formé : '{nom}' doit être une chaîne non vide")
        return choisie

    @staticmethod
    @traced
    def _entrees_en_ligne(dictionary: Any) -> dict[Any, Any]:
        """`dictionary` en ligne : hash, ou tableau `[clé, valeur, clé, valeur…]`."""
        if dictionary is None:
            return {}
        if isinstance(dictionary, dict):
            return dict(dictionary)
        if isinstance(dictionary, list) and len(dictionary) % 2 == 0:
            return dict(zip(dictionary[::2], dictionary[1::2], strict=True))
        raise ValueError("step translate mal formé : 'dictionary' doit être un hash ou une liste de paires")

    @traced
    def _preparer(self) -> None:
        """Construit la table en texte et les motifs de recherche (après chargement ou rechargement)."""
        self._table = {_texte_ruby(cle): valeur for cle, valeur in self._entrees.items()}
        self._motifs: list[tuple[str, re.Pattern[str]]] = []
        if self._exact and self._regex:
            for cle in self._table:
                try:
                    self._motifs.append((cle, re.compile(cle)))
                except re.error as exc:
                    logger.debug("translate : clé regex invalide ignorée {!r} ({})", cle, exc)
        cles = list(self._table)
        self._union = re.compile("|".join(re.escape(cle) for cle in cles)) if cles else None
        logger.debug("TranslateFilter._preparer : {} entrée(s), {} motif(s)", len(self._table), len(self._motifs))

    @traced
    def _rafraichir(self) -> None:
        """Relit le fichier après `refresh_interval` (s'il a changé) et reconstruit la table."""
        if self._dictionary is None or self._intervalle <= 0:
            return
        maintenant = self._clock()
        if maintenant - self._dernier_rafraichissement < self._intervalle:
            return
        self._dernier_rafraichissement = maintenant
        self._dictionary.recharger(self._comportement)
        if self._dictionary.version != self._version:
            self._version = self._dictionary.version
            self._entrees = dict(self._dictionary.mapping)
            self._preparer()
        logger.debug("TranslateFilter._rafraichir : version {}", self._version)

    @traced
    def _chercher(self, texte: str) -> tuple[bool, Any]:
        """Recherche `texte` selon `exact`/`regex` ; renvoie `(trouvé, valeur copiée)`."""
        if self._exact and not self._regex:
            if texte in self._table:
                return True, deepcopy(self._table[texte])
            return False, None
        if self._exact:
            for cle, motif in self._motifs:
                if motif.search(texte):
                    return True, deepcopy(self._table[cle])
            return False, None
        if self._union is None:
            return False, None
        remplace = self._union.sub(lambda trouve: _texte_ruby(self._table[trouve.group(0)]), texte)
        return (True, remplace) if remplace != texte else (False, None)

    @traced
    def _repli(self, doc: dict[str, Any], now: datetime | None, on_missing: str) -> Any:
        """Valeur de `fallback` : gabarit sprintf rendu, ou valeur non textuelle telle quelle."""
        if isinstance(self._fallback, str):
            return render_template(self._fallback, doc, now=now, on_missing=on_missing)
        return self._fallback

    @traced
    def apply(self, doc: dict[str, Any], *, now: datetime | None = None, on_missing: str = "keep") -> bool:
        """Traduit le document en place.

        Returns:
            True si les options communes doivent suivre (traduction, repli
            ou traduction en place), comme `filter_matched` du plugin.
        """
        self._rafraichir()
        if self._iterate_on is None:
            reussi = self._valeur_simple(doc, now, on_missing)
        elif self._iterate_on == self._source:
            reussi = self._tableau_de_valeurs(doc, now, on_missing)
        else:
            reussi = self._tableau_d_objets(doc, now, on_missing)
        resultat = bool(reussi) or (reussi is not None and self._source == self._target)
        logger.debug("TranslateFilter.apply : {}", resultat)
        return resultat

    @traced
    def _cible_libre(self, doc: dict[str, Any]) -> bool:
        """Faux si `target` existe déjà et que `override` est faux."""
        return self._override or resolve_field(doc, self._chemin_target) is _MISSING

    @traced
    def _valeur_simple(self, doc: dict[str, Any], now: datetime | None, on_missing: str) -> bool | None:
        """`SingleValueUpdate` : None si le filtre ne s'applique pas (cible occupée, source absente)."""
        valeur = resolve_field(doc, self._chemin_source)
        if not self._cible_libre(doc) or valeur is _MISSING:
            return None
        if isinstance(valeur, list):
            valeur = valeur[0] if valeur else None
        trouve, traduction = self._chercher(_texte_ruby(valeur))
        if trouve:
            assign_field(doc, self._chemin_target, traduction)
        elif self._fallback is not None:
            assign_field(doc, self._chemin_target, self._repli(doc, now, on_missing))
            trouve = True
        return trouve

    @traced
    def _tableau_de_valeurs(self, doc: dict[str, Any], now: datetime | None, on_missing: str) -> bool | None:
        """`ArrayOfValuesUpdate` : `iterate_on == source`, tableau traduit élément par élément."""
        valeurs = resolve_field(doc, self._chemin_iteration)
        if not self._cible_libre(doc) or valeurs is _MISSING:
            return None
        if not isinstance(valeurs, list):
            valeurs = [] if valeurs is None else [valeurs]
        repli = self._repli(doc, now, on_missing) if self._fallback is not None else None
        traductions = []
        for element in valeurs:
            trouve, traduction = self._chercher(_texte_ruby(element))
            traductions.append(traduction if trouve else repli)
        assign_field(doc, self._chemin_target, traductions)
        return any(traductions)

    @traced
    def _tableau_d_objets(self, doc: dict[str, Any], now: datetime | None, on_missing: str) -> bool | None:
        """`ArrayOfMapsValueUpdate` : `source`/`target` relatifs à chaque objet de `iterate_on`."""
        objets = resolve_field(doc, self._chemin_iteration)
        if objets is _MISSING:
            return None
        if not isinstance(objets, list):
            objets = [] if objets is None else [objets]
        reussi = False
        for objet in objets:
            if not isinstance(objet, dict):
                continue
            valeur = resolve_field(objet, self._chemin_source)
            if valeur is _MISSING or valeur is None:
                continue
            trouve, traduction = self._chercher(_texte_ruby(valeur))
            if trouve:
                assign_field(objet, self._chemin_target, traduction)
            elif self._fallback is not None:
                assign_field(objet, self._chemin_target, self._repli(doc, now, on_missing))
            else:
                continue
            reussi = True
        return reussi


# ----------------------------------------------------------------------
# Filtre `date` (session 19, déplacé d'interpreter.py en #34) : motifs
# Java/Joda -> `strptime`, portée réduite, partagé par `--pipeline-conf`
# (`PipelineInterpreter._apply_date`) et `--filters` (step "date")
# ----------------------------------------------------------------------

# Jetons Java/Joda reconnus par `translate_date_pattern`, traduits en
# motif `strptime` de la bibliothèque standard. Portée délibérément
# réduite (voir la docstring de `translate_date_pattern`) : ni les
# lettres ambigues restantes de Java Time (E, W, D, h…, voir #123), ni
# les fractions autres que `SSS`. `SSS` (millisecondes Java) devient
# `%f`, qui met à l'échelle pareil : "123" → 123000 microsecondes, soit
# exactement 123 millisecondes. `z`/`zz`/`zzz`/`ZZZ` (nom de fuseau
# textuel, ex. "CET", "Europe/Paris") deviennent `%Z` — le nom est
# remplacé par "UTC" dans le texte avant analyse (voir
# `parse_date_value`), `%Z` ne connaissant que UTC/GMT.
DATE_PATTERN_TOKENS: dict[str, str] = {
    "yyyy": "%Y",
    "yy": "%y",
    "MMMM": "%B",
    "MMM": "%b",
    "MM": "%m",
    "dd": "%d",
    "HH": "%H",
    "hh": "%I",
    "mm": "%M",
    "ss": "%S",
    "SSS": "%f",
    "EEEE": "%A",
    "EEE": "%a",
    "a": "%p",
    "XXX": "%z",
    "XX": "%z",
    "X": "%z",
    "Z": "%z",
    "ZZZ": "%Z",
    "zzz": "%Z",
    "zz": "%Z",
    "z": "%Z",
}

# Abréviations de fuseaux textuels (jetons `z`/`ZZZ`, #118) : portée
# réduite aux abréviations courantes des zones européennes et
# nord-américaines, en heures d'été comme d'hiver — un nom IANA complet
# (« Europe/Paris ») passe par `ZoneInfo`, pas par cette table.
ZONE_ABBREVIATIONS: dict[str, int] = {
    "UTC": 0,
    "GMT": 0,
    "WET": 0,
    "CET": 1,
    "WEST": 1,
    "CEST": 2,
    "EET": 2,
    "EEST": 3,
    "EST": -5,
    "EDT": -4,
    "CST": -6,
    "CDT": -5,
    "MST": -7,
    "MDT": -6,
    "PST": -8,
    "PDT": -7,
}

# Noms de mois/jours locaux pour l'option `locale` du plugin `date`
# (#118) : portée réduite à `fr` (l'anglais est le comportement par
# défaut de strptime), traduits vers les noms anglais attendus par
# `%b`/`%B`/`%a`/`%A` (insensibles à la casse). Le remplacement se fait
# dans le TEXTE avant analyse — pas de `setlocale` global (non
# réentrant, indisponible selon le système).
DATE_LOCALE_NAMES: dict[str, dict[str, dict[str, str]]] = {
    "fr": {
        "abbr": {
            "janv.": "Jan",
            "févr.": "Feb",
            "mars": "Mar",
            "avr.": "Apr",
            "mai": "May",
            "juin": "Jun",
            "juil.": "Jul",
            "août": "Aug",
            "sept.": "Sep",
            "oct.": "Oct",
            "nov.": "Nov",
            "déc.": "Dec",
            "lun.": "Mon",
            "mar.": "Tue",
            "mer.": "Wed",
            "jeu.": "Thu",
            "ven.": "Fri",
            "sam.": "Sat",
            "dim.": "Sun",
        },
        "full": {
            "janvier": "January",
            "février": "February",
            "mars": "March",
            "avril": "April",
            "mai": "May",
            "juin": "June",
            "juillet": "July",
            "août": "August",
            "septembre": "September",
            "octobre": "October",
            "novembre": "November",
            "décembre": "December",
            "lundi": "Monday",
            "mardi": "Tuesday",
            "mercredi": "Wednesday",
            "jeudi": "Thursday",
            "vendredi": "Friday",
            "samedi": "Saturday",
            "dimanche": "Sunday",
        },
    }
}

# Jeton de nom de fuseau (`z`, `zz`, `zzz`, `ZZZ`) dans un motif
# Java/Joda : lettres seules, pas au milieu d'un autre jeton.
_ZONE_NAME_TOKEN_RE = re.compile(r"(?<![A-Za-z])(?:ZZZ|zzz|zz|z)(?![A-Za-z])")
# Nom IANA (« Europe/Paris ») ou abréviation en majuscules (« CET »).
_ZONE_ID_RE = re.compile(r"[A-Za-z][A-Za-z0-9_+-]*(?:/[A-Za-z0-9_+-]+)+")
_ZONE_ABBREV_RE = re.compile(r"\b[A-Z]{2,5}\b")


@traced
def _find_zone_name(text: str) -> str | None:
    """Trouve un nom de fuseau textuel dans `text` (jetons `z`/`ZZZ`).

    Un ID IANA (« Europe/Paris ») est retenu tel quel s'il est reconnu
    par `ZoneInfo` ; sinon la première abréviation en majuscules
    connue de `ZONE_ABBREVIATIONS` l'emporte (« CET »). `None` si le
    texte n'en contient aucun — `parse_date_value` traite ça comme un
    échec de correspondance du motif, comme une valeur qui ne collerait
    pas au reste du motif.
    """
    id_match = _ZONE_ID_RE.search(text)
    if id_match:
        try:
            ZoneInfo(id_match.group(0))
            return id_match.group(0)
        except (ZoneInfoNotFoundError, ValueError):
            logger.debug("date : ID de fuseau inconnu : {}", id_match.group(0))
    for abbrev_match in _ZONE_ABBREV_RE.finditer(text):
        if abbrev_match.group(0) in ZONE_ABBREVIATIONS:
            return abbrev_match.group(0)
    return None


@traced
def _apply_zone_name(dt: datetime, name: str) -> datetime:
    """Applique le fuseau `name` (abréviation ou ID IANA) à `dt`, naïf."""
    if "/" in name:
        try:
            return dt.replace(tzinfo=ZoneInfo(name))
        except (ZoneInfoNotFoundError, ValueError) as exc:
            # Défensif : déjà validé par `_find_zone_name`.
            logger.warning(_("date : fuseau {name} devenu inconnu, UTC retenu erreur={error}"), name=name, error=exc)
            return dt.replace(tzinfo=timezone.utc)
    return dt.replace(tzinfo=timezone(timedelta(hours=ZONE_ABBREVIATIONS[name])))


@traced
def _replace_locale_names(text: str, mapping: dict[str, str]) -> str:
    """Remplace dans `text` les noms locaux par leurs équivalents anglais.

    Insensible à la casse (comme `%b`/`%a` de strptime), aux bords de
    mots, noms les plus longs d'abord (« juillet » avant « juil. »).
    """
    for key in sorted(mapping, key=len, reverse=True):
        text = re.sub(rf"(?<!\w){re.escape(key)}(?!\w)", mapping[key], text, flags=re.IGNORECASE)
    return text


@traced
def translate_date_pattern(pattern: str) -> str | None:
    """Traduit un motif de date Java/Joda (option `match` du plugin `date`) en motif `strptime`.

    Portée volontairement réduite à `DATE_PATTERN_TOKENS` ci-dessus — la
    même discipline que `fingerprint.py` pour le filtre `fingerprint`
    officiel (voir FEATURES.md, "Limites connues") : couvrir un sous-
    ensemble utile et sans ambiguïté plutôt que réimplémenter tout
    `java.time`/Joda. Renvoie `None` si le motif contient un jeton non
    reconnu ou une quote non refermée — `parse_date_value` traite alors
    ce motif comme un simple échec de correspondance, exactement comme un
    motif Logstash qui ne correspond pas à la valeur : `match => [champ,
    motif1, motif2, ...]` peut encore essayer le motif suivant, ce n'est
    pas une erreur qui interrompt le filtre `date` tout entier.

    Littéraux entre quotes simples (convention Java/Joda, ex. `"'T'"` ->
    le caractère `T` littéral, `"''"` -> un guillemet simple littéral, y
    compris à l'intérieur d'un littéral : `"'o''clock'"` -> `o'clock`) et
    texte non alphabétique (`-`, `:`, `.`...) recopiés tels quels (un `%`
    littéral est échappé en `%%` pour `strptime`), comme
    `templates._split_date_pattern` pour le SENS INVERSE (formatage) —
    non réutilisée ici : privée à `templates.py`, un module ne doit pas
    puiser dans les membres privés d'un autre (voir PATTERNS.md).
    """
    result: list[str] = []
    i, n = 0, len(pattern)
    while i < n:
        ch = pattern[i]
        if ch == "'":
            if pattern.startswith("''", i):  # `''` hors littéral : une quote
                result.append("'")
                i += 2
                continue
            literal: list[str] = []
            j = i + 1
            while True:
                k = pattern.find("'", j)
                if k == -1:
                    return None
                literal.append(pattern[j:k])
                if not pattern.startswith("''", k):
                    break
                literal.append("'")  # `''` DANS un littéral : une quote (`'o''clock'`)
                j = k + 2
            result.append("".join(literal).replace("%", "%%"))
            i = k + 1
            continue
        if ch.isalpha():
            j = i
            while j < n and pattern[j] == ch:
                j += 1
            token = DATE_PATTERN_TOKENS.get(pattern[i:j])
            if token is None:
                return None
            result.append(token)
            i = j
            continue
        result.append("%%" if ch == "%" else ch)  # `%` littéral, pas une directive strptime
        i += 1
    return "".join(result)


@traced
def parse_date_value(text: str, pattern: str, tz_name: str | None, locale: str | None = None) -> datetime | None:
    """Tente de parser `text` selon `pattern` (une entrée de `match` du plugin `date`).

    Trois formes reconnues, comme Logstash : `"ISO8601"`
    (`datetime.fromisoformat`, `Z` accepté), `"UNIX"`/`"UNIX_MS"` (epoch
    secondes/millisecondes), ou un motif Java/Joda traduit par
    `translate_date_pattern`. `None` en cas d'échec — quelle qu'en soit
    la cause (motif non reconnu, valeur qui ne correspond pas au motif,
    nombre invalide pour `UNIX`/`UNIX_MS`, horodatage hors limites) —
    pour que l'appelant (`DateFilter.apply`) puisse
    essayer le motif suivant de `match` sans distinguer ces cas.

    Un résultat SANS fuseau (motif sans `Z`/`X`/`z`) reçoit `tz_name` (nom
    IANA, `zoneinfo.ZoneInfo`) s'il est fourni et reconnu, sinon UTC —
    comme Logstash. Un `ZoneInfoNotFoundError` (nom de fuseau inconnu, ex.
    faute de frappe dans le `.conf`) retombe silencieusement sur UTC
    plutôt que de faire échouer le parsing entier pour une erreur de
    configuration distincte.

    `SSS` (millisecondes) et les noms de fuseaux textuels `z`/`ZZZ`
    (« CET », « Europe/Paris ») sont reconnus (#118) : le nom trouvé
    dans le texte est remplacé par « UTC » avant `strptime` (`%Z` ne
    connaît que UTC/GMT) puis réappliqué comme fuseau — il prime sur
    `timezone`, comme un fuseau explicite dans la valeur. `locale`
    (option du plugin officiel, `fr` prise en charge) traduit d'abord les
    noms de mois/jours locaux en anglais — les seuls que `strptime`
    sache lire — sans `setlocale` global (non réentrant).
    """
    zone_name: str | None = None
    try:
        if pattern == "ISO8601":
            normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
            dt = datetime.fromisoformat(normalized)
        elif pattern in ("UNIX", "UNIX_MS"):
            dt = datetime.fromtimestamp(float(text) / (1000.0 if pattern == "UNIX_MS" else 1.0), tz=timezone.utc)
        else:
            strptime_pattern = translate_date_pattern(pattern)
            if strptime_pattern is None:
                return None
            if _ZONE_NAME_TOKEN_RE.search(pattern):
                zone_name = _find_zone_name(text)
                if zone_name is None:
                    logger.debug("date : motif {} attend un nom de fuseau, aucun trouvé dans {!r}", pattern, text)
                    return None
                text = text.replace(zone_name, "UTC", 1)
            if locale and locale.lower() != "en":
                table = DATE_LOCALE_NAMES.get(locale.lower())
                if table is None:
                    logger.debug("date : locale {} non prise en charge, noms anglais retenus", locale)
                else:
                    if "%b" in strptime_pattern or "%a" in strptime_pattern:
                        text = _replace_locale_names(text, table["abbr"])
                    if "%B" in strptime_pattern or "%A" in strptime_pattern:
                        text = _replace_locale_names(text, table["full"])
            dt = datetime.strptime(text, strptime_pattern)
            if zone_name is not None and dt.tzinfo is None:
                dt = _apply_zone_name(dt, zone_name)
    except (ValueError, OverflowError, OSError) as exc:
        logger.debug("date : valeur non analysable ({!r}) : {}", text, exc)
        return None
    if dt.tzinfo is None:
        if tz_name:
            try:
                dt = dt.replace(tzinfo=ZoneInfo(tz_name))
            except (ZoneInfoNotFoundError, ValueError) as exc:
                logger.debug("fuseau {} inconnu, UTC retenu : {}", tz_name, exc)
                dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
    return dt


class DateFilter:
    """`date` façon Logstash (match/target/timezone/tag_on_failure), pour `--filters` et `--pipeline-conf`.

    Répond à #34 : `date` n'existait que pour `--pipeline-conf`
    (`interpreter.py::PipelineInterpreter._apply_date`, session 19). Comme
    `apply_mutate` (session 14), `GeoipFilter` (session 24) et
    `TranslateFilter` (session 25), la logique vit désormais ICI, une seule
    fois : `_apply_date` ne fait plus que déléguer à cette classe, et le
    step `"date"` de `FilterPipeline` l'utilise aussi. Aucune nouvelle arête
    de dépendances (`interpreter -> filters` existe déjà).

    Options (mêmes noms que le plugin officiel) :

    - `match` : `[champ, motif1, motif2, ...]`, motifs essayés dans
      l'ordre — `"ISO8601"`, `"UNIX"`, `"UNIX_MS"` ou motif Java/Joda
      (voir `translate_date_pattern` pour la portée réduite) ;
    - `target` : champ écrit, `"@timestamp"` par défaut, toujours en
      chaîne ISO8601 UTC (document sérialisable en JSON tel quel) ;
    - `timezone` : nom IANA appliqué à une date sans fuseau ;
    - `locale` (#118) : langue des noms de mois/jours du motif (`fr`
      pris en charge, anglais par défaut — pas de `setlocale` global) ;
    - `tag_on_failure` : tags ajoutés si rien ne correspond (champ absent,
      `None`, ou aucun motif), `["_dateparsefailure"]` par défaut.

    Un `match` mal formé (pas une liste d'au moins deux éléments dont le
    premier est un nom de champ) rend le filtre inactif (log DEBUG) :
    `FilterPipeline` le refuse dès la compilation (`ValueError`, voir
    `match_valide`), alors que `--pipeline-conf` l'ignore pour ne pas
    faire planter un `.conf` imparfait, comme avant #34.
    """

    @traced
    def __init__(self, options: dict[str, Any]) -> None:
        """Mémorise les options `date` de cette étape (voir la docstring de la classe)."""
        self._options = options

    @staticmethod
    @traced
    def match_valide(match: Any) -> bool:
        """Vrai si `match` a la forme `[champ, motif, ...]` attendue par le plugin `date`."""
        return isinstance(match, list) and len(match) >= 2 and isinstance(match[0], str)

    @traced
    def apply(self, doc: dict[str, Any]) -> bool:
        """Analyse la date désignée par `match` et l'écrit dans `target`, en place.

        Returns:
            True si une date a été écrite ; False sinon (match mal formé,
            ou échec signalé par `tag_on_failure`).
        """
        match = self._options.get("match")
        if not self.match_valide(match):
            logger.debug("plugin date sans 'match' valide, ignoré")
            return False
        value = resolve_field(doc, parse_field_path(match[0]))
        if value is _MISSING or value is None:
            self._echec(doc)
            return False
        text = str(value)
        tz_name = self._options.get("timezone")
        locale = self._options.get("locale")
        parsed: datetime | None = None
        for pattern in match[1:]:
            if not isinstance(pattern, str):
                continue
            parsed = parse_date_value(text, pattern, tz_name, locale=locale)
            if parsed is not None:
                break
        if parsed is None:
            self._echec(doc)
            return False
        target = self._options.get("target", "@timestamp")
        assign_field(doc, parse_field_path(target), parsed.astimezone(timezone.utc).isoformat())
        return True

    @traced
    def _echec(self, doc: dict[str, Any]) -> None:
        """`tag_on_failure` du plugin `date` (défaut `["_dateparsefailure"]`, comme Logstash)."""
        add_failure_tags(doc, self._options.get("tag_on_failure", ["_dateparsefailure"]))


@traced
def load_word_list(path: str) -> list[str]:
    """Charge une liste de mots depuis un fichier texte.

    Args:
        path: Chemin du fichier, un motif par ligne ; lignes vides et lignes
            commençant par "#" ignorées.

    Returns:
        La liste des motifs, dans l'ordre du fichier.
    """
    words: list[str] = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                words.append(stripped)
    return words


class FilterPipeline:
    """Chaîne de filtres façon Logstash (dissect, regex, kv, drop, mutate, geoip, translate, date, dns), en séquence.

    `apply` est asynchrone depuis #36 (chantier full async #1) : les steps
    réseau (`dns`) sont attendus ; les filtres CPU (dissect, regex, kv,
    mutate…) restent exécutés en ligne.
    """

    @traced
    def __init__(
        self,
        steps: list[dict],
        *,
        geoip_lookup_factory: Any = GeoIPLookup,
        translate_dictionary_factory: Any = TranslateDictionary,
    ) -> None:
        """Compile la chaîne de filtres.

        Args:
            steps: Liste ordonnée de définitions de filtre, chacune avec au
                moins "type" ("dissect", "regex", "kv", "drop", "mutate"
                (depuis la session 14), "geoip" (depuis la session 24) ou
                "translate" (depuis la session 25) et, sauf pour "mutate"/
                "geoip"/"translate", "source" (champ du document à
                analyser). "dissect"/"regex"/"kv" acceptent en plus "target"
                (sous-objet dans lequel placer les champs extraits ; à
                défaut, fusionnés au premier niveau). "drop" attend
                "words_file" (fichier texte, un motif par ligne) ou "words"
                (liste inline), et accepte "mode" ("equals"/"contains",
                "contains" par défaut) et "case_sensitive" (false par
                défaut). "mutate" n'attend PAS de "source"/"target" (il
                opère sur le document entier) : voir `apply_mutate` pour le
                format de ses propres clés ("rename"/"convert"/"gsub"/
                "strip"/"add_field"/"remove_field"/"add_tag"/"remove_tag").
                "geoip" attend "source"/"database" (requis) et
                "target"/"fields"/"tag_on_failure" (optionnels) : voir
                `GeoipFilter` pour le détail, identique à `interpreter.py::
                _apply_geoip`. "translate" attend "dictionary_path"/
                "source"/"target" (requis) et "fallback" (optionnel) : voir
                `TranslateFilter` pour le détail, identique à
                `interpreter.py::_apply_translate`. "date" (depuis #34)
                attend "match" ([champ, motif, ...], requis) et
                "target"/"timezone"/"tag_on_failure" (optionnels) : voir
                `DateFilter`, identique au plugin `date` de
                `--pipeline-conf`. "dns" (depuis #36) reprend les options
                du plugin `dns` du `.conf` (voir `dnslookup.DnsFilter`) :
                résolution réseau asynchrone, `await`ée par `apply`. Voir
                filters.example.json et blocklist.example.txt.
            geoip_lookup_factory: Callable construisant une
                `enrichment.GeoIPLookup` à partir d'un chemin de base,
                utilisée par tout step "geoip" de `steps` ; injectable pour
                les tests (double de test), comme
                `PipelineInterpreter._geoip_lookup_factory`.
            translate_dictionary_factory: Callable construisant un
                `enrichment.TranslateDictionary` à partir d'un chemin de
                fichier YAML, utilisée par tout step "translate" de
                `steps` ; injectable pour les tests, comme
                `PipelineInterpreter._translate_dictionary_factory`.

        Raises:
            ValueError: Type de filtre inconnu, patron/config invalide, ou
                (depuis la session 24) step "geoip" mal formé/base `.mmdb`
                illisible, ou (depuis la session 25) step "translate" mal
                formé/dictionnaire YAML illisible.
        """
        self._geoip_lookup_factory = geoip_lookup_factory
        self._translate_dictionary_factory = translate_dictionary_factory
        self._steps = [self._compile_step(step) for step in steps]

    @traced
    def _compile_step(self, step: dict) -> tuple[str | None, object, str | None]:
        """Précompile une étape (champ source, filtre compilé, champ cible).

        "mutate" (session 14), "geoip" (session 24) et "translate" (session
        25) sont des cas à part : ils ne lisent ni "source" ni "target" au
        sens générique (contrairement à dissect/regex/kv, qui extraient
        TOUJOURS d'un champ texte unique vers un champ/sous-objet cible —
        "geoip"/"translate" résolvent eux-mêmes leur "source", à un chemin
        imbriqué possible, voir `GeoipFilter.apply`/`TranslateFilter.apply`),
        donc leur "source" renvoyé est `None` — `apply()` les reconnaît par
        type (`isinstance(compiled, ...)`) avant même de regarder `source`,
        comme il le fait déjà pour `DropFilter`.
        """
        filter_type = step["type"]
        if filter_type == "mutate":
            # "_comment" est une convention documentaire du projet (voir
            # filters.example.json), pas une option mutate : exclue comme
            # "type", pour ne pas polluer le log "options mutate non
            # interprétées" d'apply_mutate avec une fausse alerte.
            options = {key: value for key, value in step.items() if key not in ("type", "_comment")}
            return None, MutateFilter(options), None
        if filter_type == "geoip":
            options = {key: value for key, value in step.items() if key not in ("type", "_comment")}
            return None, GeoipFilter(options, lookup_factory=self._geoip_lookup_factory), None
        if filter_type == "translate":
            options = {key: value for key, value in step.items() if key not in ("type", "_comment")}
            return None, TranslateFilter(options, dictionary_factory=self._translate_dictionary_factory), None
        if filter_type == "date":
            options = {key: value for key, value in step.items() if key not in ("type", "_comment")}
            if not DateFilter.match_valide(options.get("match")):
                raise ValueError("filtre date : 'match' doit valoir [champ, motif, ...]")
            return None, DateFilter(options), None
        if filter_type == "dns":
            options = {key: value for key, value in step.items() if key not in ("type", "_comment")}
            return None, DnsFilter(options), None
        source = step["source"]
        target = step.get("target")
        if filter_type == "dissect":
            compiled: object = Dissector(step["pattern"])
        elif filter_type == "regex":
            compiled = RegexFilter(step["pattern"])
        elif filter_type == "kv":
            compiled = KeyValueFilter(
                field_split=step.get("field_split", " "),
                value_split=step.get("value_split", "="),
                prefix=step.get("prefix", ""),
                include_keys=step.get("include_keys"),
                exclude_keys=step.get("exclude_keys"),
                trim_key=step.get("trim_key", ""),
                trim_value=step.get("trim_value", ""),
                allow_duplicate_values=step.get("allow_duplicate_values", True),
            )
        elif filter_type == "drop":
            if "words_file" in step:
                words = load_word_list(step["words_file"])
            elif "words" in step:
                words = step["words"]
            else:
                raise ValueError("filtre drop sans 'words_file' ni 'words'")
            compiled = DropFilter(
                words=words,
                mode=step.get("mode", "contains"),
                case_sensitive=step.get("case_sensitive", False),
            )
        else:
            raise ValueError(f"type de filtre inconnu: {filter_type!r}")
        return source, compiled, target

    @atraced
    async def apply(self, doc: dict, *, now: datetime | None = None, on_missing: str = "keep") -> bool:
        """Applique chaque filtre de la chaîne à doc, en place.

        Asynchrone depuis #36 (chantier full async #1) : les steps réseau
        (`dns`) sont attendus (`await`) ; les filtres CPU (dissect, regex,
        kv, mutate, geoip, translate, date, drop) restent exécutés en
        ligne, l'ordre de la chaîne est inchangé. Comme le plugin `dns`
        du `.conf`, un step `dns` qui échoue (délai dépassé, valeur
        refusée) n'écarte pas le document : la chaîne continue, sans
        options communes à appliquer ici.

        Un filtre "dissect"/"regex"/"kv" dont le champ source est absent, non
        textuel, ou dont le patron ne correspond pas est ignoré pour ce
        message (log en DEBUG) plutôt que de faire échouer tout le document.
        Un filtre "drop" qui correspond arrête immédiatement la chaîne. Un
        filtre "mutate" (session 14), "geoip" (session 24) ou "translate"
        (session 25) s'applique inconditionnellement au document entier (pas
        de notion de "correspondance" au sens de dissect/regex/kv — "geoip"/
        "translate" ont chacun leur propre notion d'échec, gérée par
        `tag_on_failure`/`fallback`, voir `GeoipFilter.apply`/
        `TranslateFilter.apply`).

        Args:
            doc: Document à enrichir, modifié en place.
            now: Horodatage transmis à un éventuel step "mutate"/"translate"
                pour le rendu des gabarits sprintf de son option
                "add_field"/"fallback" (voir `apply_mutate`/
                `TranslateFilter.apply`) ; `None` = horodatage réel.
            on_missing: Idem, comportement pour un champ référencé absent
                dans un gabarit "add_field"/"fallback" ("keep" ou "empty").

        Returns:
            False si un filtre "drop" a demandé l'abandon du document
            (aucun traitement ultérieur ne doit avoir lieu), True sinon.
        """
        for source, compiled, target in self._steps:
            if isinstance(compiled, DnsFilter):
                await compiled.apply(doc)
                continue
            if isinstance(compiled, MutateFilter):
                compiled.apply(doc, now=now, on_missing=on_missing)
                continue
            if isinstance(compiled, GeoipFilter):
                compiled.apply(doc)
                continue
            if isinstance(compiled, TranslateFilter):
                compiled.apply(doc, now=now, on_missing=on_missing)  # options communes : non lues par --filters
                continue
            if isinstance(compiled, DateFilter):
                compiled.apply(doc)
                continue
            value = doc.get(source)
            if not isinstance(value, str):
                continue
            if isinstance(compiled, DropFilter):
                if compiled.matches(value):
                    return False
                continue
            extracted = compiled.apply(value)
            if extracted is None:
                logger.debug("filtre sans correspondance source={source!r}", source=source)
                continue
            if target:
                doc.setdefault(target, {}).update(extracted)
            else:
                doc.update(extracted)
        return True

    @traced
    def close(self) -> None:
        """Ferme toute base geoip ouverte par un step "geoip" de cette chaîne (session 24).

        Sans effet si la chaîne ne contient aucun step "geoip" (aucune
        `GeoIPLookup` créée). Symétrique de
        `PipelineInterpreter.close_geoip_lookups` (`--pipeline-conf`) et de
        `GeoIPEnricher.close` (`--geoip-db`) — les TROIS mécanismes de
        géolocalisation du projet exposent désormais un `close()`.
        """
        for _source, compiled, _target in self._steps:
            if isinstance(compiled, GeoipFilter):
                compiled.close()
