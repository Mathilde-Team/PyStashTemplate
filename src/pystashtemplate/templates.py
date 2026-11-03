# SPDX-License-Identifier: GPL-3.0-or-later
"""Lecture et interprétation des templates Logstash (syntaxe "sprintf", %{...}).

Module autonome et réutilisable : ne connaît rien d'omprog, de rsyslog ni
d'OpenSearch — juste comment un gabarit texte façon Logstash se compile et
se rend contre un document (dict) événement. Prévu pour être importé par
n'importe quel module qui a besoin de produire dynamiquement un index, un
document_id, un chemin de fichier ou un champ texte comme le ferait
Logstash (`indexer.py` l'utilise pour `--document-id-template`).

Grammaire couverte (voir le document de référence transmis par Mathilde,
sections 1-6 et 9-12 ; la section 7, conditions `if [champ] == ...`,
appartient au DSL des blocs filter{}/output{}, pas à la syntaxe "template"
au sens sprintf, et n'est délibérément pas traitée ici) :

    %{champ}                référence de champ simple
    %{[objet][champ]}       référence de champ imbriqué (un niveau par
                             paire de crochets)
    %{[@metadata][champ]}   référence dans @metadata (mêmes règles de
                             chemin ; @metadata est un champ racine comme
                             un autre du point de vue du gabarit)
    %{[tableau][0]}         indexation d'un tableau (segment de chemin
                             numérique)
    %{+FORMAT}               date basée sur @timestamp, ancienne syntaxe
                             (ex. %{+YYYY.MM.dd})
    %{+%s}                   secondes depuis l'epoch de @timestamp
    %{{FORMAT}}               date basée sur @timestamp, syntaxe Java Time
                             recommandée (ex. %{{yyyy.MM.dd}})
    %{{TIME_NOW}}             heure du traitement (pas @timestamp)
    texte fixe autour        conservé tel quel ; plusieurs références
                             possibles dans un même gabarit

Champ absent (section 11 du document) : Logstash ne garantit pas un
comportement unique ("peut conserver littéralement ... ou générer une
valeur vide") — `render()` expose ce choix via `on_missing` ("keep" par
défaut, conserve le "%{...}" littéral ; "empty" pour une chaîne vide)
plutôt que de trancher silencieusement à la place de l'appelant. Un champ
présent mais explicitement `null` se rend toujours en chaîne vide (comme
un `to_s` Ruby de `nil`), indépendamment de `on_missing`.

Limites assumées (voir FEATURES.md) :
    - Modes d'échappement de noms de champs contenant eux-mêmes des
      crochets (section 12 : none/percent/ampersand) non implémentés —
      cas non rencontré dans les logs HPE Comware qui motivent ce projet.
    - Le format Java Time n'est pas réimplémenté dans sa généralité (pas
      d'équivalent Python de `java.time.DateTimeFormatter`) : les lettres
      de `_format_letter_run` sont traduites (#123), avec semaines ISO et
      textes en anglais quelle que soit la locale. Une lettre hors de cette
      liste (ex. `F`, `B`) est recopiée telle quelle plutôt que de lever
      une exception : repli sans surprise, pas un échec silencieux qui
      tronquerait le résultat.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger

from pystashtemplate.tracing import traced

_MISSING = object()  # sentinelle : distingue "champ absent" de "champ présent valant None"

# Issue #124 : `field_reference.escape_style` de Logstash — séquences qui
# désignent un crochet littéral dans un nom de champ. Réglage global au
# processus (comme logstash.yml), choisi par `--field-reference-escape-style`.
ESCAPE_STYLES = ("none", "percent", "ampersand")
_ECHAPPEMENTS: dict[str, tuple[tuple[str, str], ...]] = {
    "none": (),
    "percent": (("%5B", "["), ("%5D", "]")),
    "ampersand": (("&#91;", "["), ("&#93;", "]")),
}
_REGLAGES = {"escape_style": "none"}


@dataclass(frozen=True)
class _Token:
    """Un fragment de gabarit déjà classifié (littéral, référence, ou date)."""

    kind: str  # "literal" | "field" | "olddate" | "newdate"
    value: str


@dataclass(frozen=True)
class CompiledTemplate:
    """Un gabarit Logstash déjà découpé en tokens, prêt à être rendu plusieurs fois sans reparser.

    Réutiliser une même `CompiledTemplate` pour rendre de nombreux
    documents (ex. un `document_id` par message reçu) évite de
    retokeniser la chaîne de gabarit à chaque appel.
    """

    source: str
    tokens: tuple[_Token, ...]


@traced
def compile_template(source: str) -> CompiledTemplate:
    """Découpe un gabarit Logstash en tokens.

    Args:
        source: Le gabarit brut, ex. "logs-%{[service][name]}-%{+YYYY.MM.dd}".

    Returns:
        Le gabarit compilé, à passer à `render()`.
    """
    return CompiledTemplate(source, tuple(_tokenize(source)))


@traced
def render(
    compiled: CompiledTemplate,
    event: dict[str, Any],
    *,
    now: datetime | None = None,
    on_missing: str = "keep",
) -> str:
    """Rend un gabarit déjà compilé contre un document événement.

    Args:
        compiled: Résultat de `compile_template`.
        event: Document événement (dict), ex. {"client_ip": "1.2.3.4", ...}.
        now: Horodatage à utiliser pour %{{TIME_NOW}} et comme repli pour
            %{+FORMAT}/%{{FORMAT}} si `event` n'a pas de "@timestamp"
            exploitable. Par défaut `datetime.now(timezone.utc)`.
        on_missing: "keep" (défaut, conserve "%{...}" littéral) ou "empty"
            (chaîne vide) pour une référence de champ dont le chemin ne
            résout à rien dans `event` (section 11 du document de
            référence).

    Returns:
        Le texte rendu.

    Raises:
        ValueError: `on_missing` autre que "keep"/"empty".
    """
    if on_missing not in ("keep", "empty"):
        raise ValueError(f"on_missing inconnu: {on_missing!r} (attendu 'keep' ou 'empty')")
    moment = now if now is not None else datetime.now(timezone.utc)
    parts: list[str] = []
    for token in compiled.tokens:
        if token.kind == "literal":
            parts.append(token.value)
        elif token.kind == "field":
            parts.append(_render_field(token.value, event, on_missing))
        elif token.kind == "olddate":
            if token.value == "%s":
                # `%{+%s}` : secondes depuis l'epoch, cas à part de Logstash
                # (StringInterpolation.java), pas un motif Joda (issue #102).
                parts.append(str(int(_event_timestamp(event, moment).timestamp())))
            else:
                parts.append(_format_timestamp(token.value, _event_timestamp(event, moment), java=False))
        else:  # "newdate" : seul type restant produit par _tokenize
            if token.value == "TIME_NOW":
                parts.append(_format_timestamp("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", moment))
            else:
                parts.append(_format_timestamp(token.value, _event_timestamp(event, moment)))
    return "".join(parts)


@traced
def render_template(
    source: str,
    event: dict[str, Any],
    *,
    now: datetime | None = None,
    on_missing: str = "keep",
) -> str:
    """Raccourci `compile_template` + `render` pour un gabarit utilisé une seule fois.

    Pour un gabarit réutilisé sur de nombreux documents (ex. dans une
    boucle de traitement), préférer `compile_template` une fois puis
    `render` à chaque document : évite de retokeniser à chaque appel.
    """
    return render(compile_template(source), event, now=now, on_missing=on_missing)


@traced
def set_escape_style(style: str) -> None:
    """Choisit le style d'échappement des crochets dans les noms de champs (#124).

    Args:
        style: `none` (défaut), `percent` (`%5B`, `%5D`) ou `ampersand`
            (`&#91;`, `&#93;`), comme `field_reference.escape_style`.

    Raises:
        ValueError: Style inconnu.
    """
    if style not in ESCAPE_STYLES:
        raise ValueError(f"field_reference.escape_style inconnu : {style!r}")
    _REGLAGES["escape_style"] = style
    logger.debug("set_escape_style: {}", style)


@traced
def _desechapper(segment: str) -> str:
    """Remplace les crochets échappés d'un segment selon le style courant."""
    for sequence, crochet in _ECHAPPEMENTS[_REGLAGES["escape_style"]]:
        segment = segment.replace(sequence, crochet)
    return segment


@traced
def parse_field_path(expr: str) -> list[str]:
    """Interprète l'intérieur d'une référence %{...} en chemin de champ.

    Args:
        expr: Contenu entre "%{" et "}", ex. "client_ip" ou
            "[service][name]" ou "[tags][0]".

    Returns:
        Le chemin sous forme de liste de segments, ex. ["service", "name"].
        Une référence simple (sans crochets) donne une liste à un élément.

    Raises:
        ValueError: Référence vide, ou chemin entre crochets mal formé
            (crochet non refermé, ou texte parasite entre deux groupes).
    """
    expr = expr.strip()
    if not expr:
        raise ValueError("référence de champ vide: %{}")
    if not expr.startswith("["):
        # Issue #49 : comme Logstash (FieldReference strict), un nom simple
        # ne contient aucun crochet — « a[b] » ou « a] » est une référence
        # invalide, pas un nom de champ littéral.
        if "[" in expr or "]" in expr:
            raise ValueError(f"chemin de champ mal formé (crochet dans un nom simple): {expr!r}")
        return [_desechapper(expr)]
    segments: list[str] = []
    rest = expr
    while rest:
        if not rest.startswith("["):
            raise ValueError(f"chemin de champ mal formé (texte hors crochets): {expr!r}")
        end = rest.find("]")
        if end == -1:
            raise ValueError(f"crochet non refermé dans le chemin de champ: {expr!r}")
        segment = rest[1:end]
        if not segment or "[" in segment:
            # « [] » (segment vide) et « [a[b]] » (crochets imbriqués) : refusés par Logstash.
            raise ValueError(f"chemin de champ mal formé (segment vide ou imbriqué): {expr!r}")
        segments.append(_desechapper(segment))
        rest = rest[end + 1 :]
    return segments


@traced
def resolve_field(event: Any, path: list[str]) -> Any:
    """Suit un chemin de champ dans un document (dict imbriqué et/ou listes).

    Args:
        event: Document événement (ou toute valeur imbriquée en cours de
            parcours — dict, liste, ou scalaire).
        path: Chemin tel que renvoyé par `parse_field_path`.

    Returns:
        La valeur trouvée (qui peut être `None` si le champ vaut
        explicitement `null`), ou la sentinelle interne `_MISSING` si le
        chemin ne résout à rien (clé absente, indice hors limites, ou
        chemin qui continue sur un scalaire).
    """
    current: Any = event
    for step in path:
        if isinstance(current, dict):
            if step not in current:
                return _MISSING
            current = current[step]
        elif isinstance(current, (list, tuple)):
            if not step.lstrip("-").isdigit():
                return _MISSING
            index = int(step)
            if -len(current) <= index < len(current):
                current = current[index]
            else:
                return _MISSING
        else:
            return _MISSING
    return current


@traced
def assign_field(event: dict[str, Any], path: list[str], value: Any) -> None:
    """Écrit `value` au chemin `path` dans `event`, en créant les dictionnaires imbriqués manquants.

    Nouveauté session 15, en réponse à un pipeline `.conf` de production
    réel transmis par Mathilde : chaque bloc `mutate { add_field => {
    "[syslog][name]" => ... } }` de ce pipeline cible un champ imbriqué,
    jamais une clé plate — sans cette fonction, `apply_mutate`
    (`filters.py`) écrivait littéralement une clé `"[syslog][name]"` au
    premier niveau du document au lieu de `doc["syslog"]["name"]`.

    Suit le même chemin que `resolve_field`, mais en ÉCRITURE : un segment
    intermédiaire absent ou qui ne vaut ni dict ni liste devient un
    nouveau dict (jamais une liste — créer une liste de la bonne taille à
    partir d'un simple indice numérique serait deviner une intention que
    rien dans un `%{...}`/mutate ne précise). Un chemin qui traverse un
    indice de liste hors limites est abandonné (log en DEBUG) plutôt que
    d'agrandir la liste ou de lever une exception : même philosophie que
    `FilterPipeline.apply` pour un filtre sans correspondance.

    Args:
        event: Document à modifier en place.
        path: Chemin tel que renvoyé par `parse_field_path`. Un chemin
            vide ne fait rien (rien à nommer).
        value: Valeur à écrire au segment final.
    """
    if not path:
        return
    current: Any = event
    for step in path[:-1]:
        if isinstance(current, dict):
            nxt = current.get(step)
            if not isinstance(nxt, (dict, list)):
                nxt = {}
                current[step] = nxt
            current = nxt
        elif isinstance(current, list):
            if not step.lstrip("-").isdigit() or not (-len(current) <= int(step) < len(current)):
                logger.debug("chemin de champ hors limites, écriture abandonnée : {}", path)
                return
            current = current[int(step)]
        else:
            logger.debug("chemin de champ traverse un scalaire, écriture abandonnée : {}", path)
            return
    last = path[-1]
    if isinstance(current, dict):
        current[last] = value
    elif isinstance(current, list):
        if last.lstrip("-").isdigit() and -len(current) <= int(last) < len(current):
            current[int(last)] = value
        else:
            logger.debug("indice de liste hors limites, écriture abandonnée : {}", path)
    else:
        logger.debug("chemin de champ traverse un scalaire, écriture abandonnée : {}", path)


@traced
def remove_field_path(event: dict[str, Any], path: list[str]) -> None:
    """Retire le champ situé à `path` dans `event`, sans erreur s'il est déjà absent.

    Nouveauté session 15 (voir `assign_field`) : équivalent imbriqué de
    `doc.pop(field_name, None)`, pour `mutate.remove_field` sur un champ
    comme `"[hostTable][index]"`. Seul le dict parent immédiat peut perdre
    une clé — retirer un élément d'une LISTE parente décalerait les
    indices suivants de façon surprenante pour l'appelant, donc ce cas est
    ignoré (log en DEBUG) plutôt que traité.

    Args:
        event: Document à modifier en place.
        path: Chemin tel que renvoyé par `parse_field_path`. Un chemin
            vide ne fait rien.
    """
    if not path:
        return
    parent = event if len(path) == 1 else resolve_field(event, path[:-1])
    if isinstance(parent, dict):
        parent.pop(path[-1], None)
    elif parent is not _MISSING and not isinstance(parent, dict):
        logger.debug("parent du champ n'est pas un dict, suppression ignorée : {}", path)


@traced
def _render_field(inner: str, event: dict[str, Any], on_missing: str) -> str:
    """Rend une seule référence %{...} (hors dates)."""
    path = parse_field_path(inner)
    value = resolve_field(event, path)
    if value is _MISSING:
        return f"%{{{inner}}}" if on_missing == "keep" else ""
    if value is None:
        return ""
    return _logstash_string(value)


@traced
def _logstash_string(value: Any) -> str:
    """Convertit une valeur de champ en texte comme `StringInterpolation` de Logstash 8.

    Issue #49, d'après la source de Logstash (logstash-core,
    `StringInterpolation.evaluate` et `KeyNode.join`) :
        - liste : éléments joints par « , » ; élément `null` → « » ; liste
          imbriquée aplatie avec le même séparateur ;
        - objet (dict) : JSON compact (Jackson : pas d'espace) ;
        - booléen : `true` / `false` (Java `Boolean.toString`) ;
        - autre : `str()`.
    Écart assumé : un objet DANS une liste est rendu en JSON (Logstash
    appelle le `toString()` Java de la map, « {ip=1.2.3.4} », qui n'est
    ni du JSON ni stable).
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return ",".join("" if item is None else _logstash_string(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    return str(value)


@traced
def _tokenize(source: str) -> list[_Token]:
    """Découpe un gabarit brut en tokens (littéral / field / olddate / newdate).

    Distingue %{{...}} (syntaxe Java Time / TIME_NOW, cherché en premier
    pour ne pas être pris pour un simple %{...}) de %{...} (référence de
    champ, ou date ancienne syntaxe si le contenu commence par "+"). Un
    "%{" ou "%{{" jamais refermé est recopié tel quel comme texte littéral
    plutôt que de lever une exception : un gabarit légèrement malformé ne
    doit pas faire planter tout l'appelant.
    """
    tokens: list[_Token] = []
    i = 0
    n = len(source)
    literal_start = 0
    while i < n:
        if source[i] == "%" and i + 1 < n and source[i + 1] == "{":
            if i > literal_start:
                tokens.append(_Token("literal", source[literal_start:i]))
            if i + 2 < n and source[i + 2] == "{":
                end = source.find("}}", i + 3)
                if end == -1:
                    tokens.append(_Token("literal", source[i:]))
                    i = n
                    literal_start = n
                    continue
                tokens.append(_Token("newdate", source[i + 3 : end]))
                i = end + 2
                literal_start = i
                continue
            end = source.find("}", i + 2)
            if end == -1:
                tokens.append(_Token("literal", source[i:]))
                i = n
                literal_start = n
                continue
            inner = source[i + 2 : end]
            if inner.startswith("+"):
                tokens.append(_Token("olddate", inner[1:]))
            else:
                tokens.append(_Token("field", inner))
            i = end + 1
            literal_start = i
            continue
        i += 1
    if literal_start < n:
        tokens.append(_Token("literal", source[literal_start:n]))
    return tokens


@traced
def _event_timestamp(event: dict[str, Any], fallback: datetime) -> datetime:
    """Extrait "@timestamp" de `event` comme datetime aware, ou renvoie `fallback`."""
    raw = event.get("@timestamp") if isinstance(event, dict) else None
    if not raw:
        return fallback
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, str):
        text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            logger.debug("date de gabarit non analysable, valeur de repli : {}", exc)
            return fallback
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return fallback


@traced
def _format_timestamp(pattern: str, dt: datetime, java: bool = True) -> str:
    """Formate `dt` selon un motif de date Java Time (`java`) ou Joda (voir `_split_date_pattern`)."""
    return "".join(
        text if kind == "literal" else _format_letter_run(text, dt, java) for kind, text in _split_date_pattern(pattern)
    )


@traced
def _split_date_pattern(pattern: str) -> list[tuple[str, str]]:
    """Découpe un motif de date en runs de lettres identiques et texte littéral.

    Gère les littéraux entre quotes simples de la convention Joda/Java
    Time ('T' -> le caractère T tel quel, '' -> un guillemet simple
    littéral) en plus du texte déjà non-alphabétique (ex. "-", ":", ".").
    """
    tokens: list[tuple[str, str]] = []
    i = 0
    n = len(pattern)
    while i < n:
        ch = pattern[i]
        if ch == "'":
            j = i + 1
            buf: list[str] = []
            closed = False
            while j < n:
                if pattern[j] == "'":
                    if j + 1 < n and pattern[j + 1] == "'":
                        buf.append("'")
                        j += 2
                        continue
                    j += 1
                    closed = True
                    break
                buf.append(pattern[j])
                j += 1
            tokens.append(("literal", "".join(buf) if buf else "'"))
            i = j if closed else n
        elif ch.isalpha():
            j = i
            while j < n and pattern[j] == ch:
                j += 1
            tokens.append(("field", pattern[i:j]))
            i = j
        else:
            j = i
            while j < n and pattern[j] != "'" and not pattern[j].isalpha():
                j += 1
            tokens.append(("literal", pattern[i:j]))
            i = j
    return tokens


_MOIS = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)
_JOURS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
_TRIMESTRES = ("1st quarter", "2nd quarter", "3rd quarter", "4th quarter")


@traced
def _texte_java(texte: str, length: int, *, java: bool) -> str:
    """Texte selon le nombre de lettres : court (1-3), complet (4), étroit (5, Java Time)."""
    if java and length == 5:
        return texte[0]
    return texte if length >= 4 else texte[:3]


@traced
def _semaine_du_mois(dt: datetime) -> int:
    """Semaine ISO dans le mois (`W`) : semaine 1 = première semaine d'au moins 4 jours du mois."""
    decalage = dt.replace(day=1).weekday()
    semaine = (dt.day + decalage - 1) // 7
    return semaine + 1 if 7 - decalage >= 4 else semaine


@traced
def _format_letter_run(run: str, dt: datetime, java: bool = True) -> str:
    """Formate un run de lettres identiques (ex. "yyyy", "SSS") pour `dt`.

    `java` choisit la syntaxe : Java Time (`%{{…}}`) ou Joda (`%{+…}`),
    qui diffèrent pour `Y`, `x`, `e`, `C` et les lettres propres à Java
    Time (`u`, `Q`, `q`, `L`, `c`, `n`, `N`, `A`, `V`, `O`). Les semaines
    (`Y`, `w`, `W`, `e`, `c`, et `x` de Joda) suivent la norme ISO
    (lundi, semaine 1 d'au moins 4 jours), les textes sont en anglais.
    """
    letter = run[0]
    length = len(run)
    annee_iso, semaine_iso, jour_iso = dt.isocalendar()
    nombres: dict[str, int] = {
        "M": dt.month,
        "L": dt.month,
        "d": dt.day,
        "D": dt.timetuple().tm_yday,
        "H": dt.hour,
        "k": dt.hour or 24,
        "K": dt.hour % 12,
        "h": dt.hour % 12 or 12,
        "m": dt.minute,
        "s": dt.second,
        "w": semaine_iso,
    }
    if java:
        nombres.update(
            {
                "W": _semaine_du_mois(dt),
                "e": jour_iso,
                "c": jour_iso,
                "Q": (dt.month - 1) // 3 + 1,
                "q": (dt.month - 1) // 3 + 1,
                "n": dt.microsecond * 1000,
                "A": ((dt.hour * 60 + dt.minute) * 60 + dt.second) * 1000 + dt.microsecond // 1000,
                "N": (((dt.hour * 60 + dt.minute) * 60 + dt.second) * 1_000_000 + dt.microsecond) * 1000,
            }
        )
    else:
        nombres.update({"e": jour_iso, "C": dt.year // 100})
    annees = {"y": dt.year, "u": dt.year, "Y": annee_iso} if java else {"y": dt.year, "Y": dt.year, "x": annee_iso}
    if letter in annees:
        # Joda et Java Time : seul `yy` est réduit à deux chiffres ; `y`,
        # `yyy`, `yyyy` donnent l'année complète (« 2026 »), complétée par des
        # zéros jusqu'au nombre de lettres (`yyyyy` → « 02026 »).
        annee = annees[letter]
        return f"{annee % 100:02d}" if length == 2 else f"{annee:0{length}d}"
    if letter in ("M", "L") and length >= 3:
        return _texte_java(_MOIS[dt.month - 1], length, java=java)
    if letter == "E" or (java and letter in ("e", "c") and length >= 3):
        return _texte_java(_JOURS[dt.weekday()], length, java=java)
    if java and letter in ("Q", "q") and length >= 3:
        trimestre = (dt.month - 1) // 3 + 1
        return {3: f"Q{trimestre}", 4: _TRIMESTRES[trimestre - 1]}.get(length, str(trimestre))
    if letter == "G":
        return "Anno Domini" if java and length == 4 else ("A" if java and length == 5 else "AD")
    if letter == "a":
        return "AM" if dt.hour < 12 else "PM"
    if letter in nombres:
        return f"{nombres[letter]:0{length}d}"
    if letter == "S":
        return f"{dt.microsecond:06d}".ljust(length, "0")[:length]
    if letter in ("X", "x", "Z", "z"):
        return _format_offset(dt, letter, length)
    if java and letter == "V" and length == 2:
        return _identifiant_fuseau(dt)
    if java and letter == "O" and length in (1, 4):
        return _decalage_gmt(dt, longue=length == 4)
    # Lettre non gérée (ex. F, B) : recopiée telle quelle — voir la
    # docstring du module, "Limites assumées".
    logger.debug("_format_letter_run : lettre non gérée {!r}", run)
    return run


@traced
def _identifiant_fuseau(dt: datetime) -> str:
    """`VV` : identifiant IANA du fuseau (`Europe/Paris`), sinon décalage (`Z`, `+02:00`)."""
    cle = getattr(dt.tzinfo, "key", None)
    if cle:
        return cle
    return _format_offset(dt, "X", 3)


@traced
def _decalage_gmt(dt: datetime, *, longue: bool) -> str:
    """`O` (`GMT+2`, `GMT+5:30`) et `OOOO` (`GMT+02:00`) ; `GMT` pour un décalage nul."""
    minutes = int((dt.utcoffset() or timedelta(0)).total_seconds() // 60)
    if minutes == 0:
        return "GMT"
    signe = "+" if minutes > 0 else "-"
    heures, reste = divmod(abs(minutes), 60)
    if longue:
        return f"GMT{signe}{heures:02d}:{reste:02d}"
    return f"GMT{signe}{heures}" + (f":{reste:02d}" if reste else "")


@traced
def _format_offset(dt: datetime, letter: str, length: int) -> str:
    """Formate le décalage UTC de `dt` façon Java Time X/Z (voir la docstring du module)."""
    offset = dt.utcoffset() or timedelta(0)
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    total_minutes = abs(total_minutes)
    hours, minutes = divmod(total_minutes, 60)
    is_zero = hours == 0 and minutes == 0
    if letter.upper() == "X":
        if is_zero:
            return "Z"
        if length == 1:
            return f"{sign}{hours:02d}"
        if length == 2:
            return f"{sign}{hours:02d}{minutes:02d}"
        return f"{sign}{hours:02d}:{minutes:02d}"
    # Z/z (style RFC 822) : jamais "Z" littéral pour un décalage nul.
    if length >= 4:
        return f"{sign}{hours:02d}:{minutes:02d}"
    return f"{sign}{hours:02d}{minutes:02d}"
