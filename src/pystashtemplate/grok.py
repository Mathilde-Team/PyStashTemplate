# SPDX-License-Identifier: GPL-3.0-or-later
# Motifs de base : Copyright 2012-2015 Elasticsearch, Apache-2.0 (voir LICENSES/).
"""Filtre `grok` façon Logstash pour les pipelines `.conf` (issue #112).

`filter { grok { match => { "message" => "%{SYNTAXE:champ}" } } }` :
chaque motif `%{SYNTAXE}`, `%{SYNTAXE:champ}` ou `%{SYNTAXE:champ:type}` est
remplacé par l'expression de la bibliothèque, récursivement ; les champs
nommés deviennent des groupes de capture et sont écrits dans le document.

Options lues (mêmes noms et mêmes défauts que le plugin officiel
logstash-filter-grok) :

- `match` : `{ champ => motif }` ou `{ champ => [motif1, motif2] }`, ou la
  forme tableau historique `[champ, motif, champ2, motif2]` ;
- `break_on_match` (vrai par défaut) : arrêt au premier motif qui
  correspond, tous champs confondus ;
- `overwrite` : champs remplacés ; sinon une capture vers un champ texte
  déjà présent en fait un tableau `[ancien, nouveau]` (ajout à un tableau
  existant), comme `handle()` du plugin officiel ;
- `tag_on_failure` (`["_grokparsefailure"]` par défaut) : tags ajoutés si
  aucun motif ne correspond (champ absent compris) ;
- `patterns_dir` (+ `patterns_files_glob`, `"*"` par défaut) : fichiers de
  motifs `NOM expression` ; `pattern_definitions` : motifs en ligne,
  prioritaires ;
- `keep_empty_captures` (faux par défaut) : captures vides conservées ;
- `target` : captures écrites sous ce champ plutôt qu'à la racine.

Conversions `:int` et `:float` : mêmes règles que Ruby `to_i`/`to_f`
utilisées par le plugin (préfixe numérique, `0` sinon).

Différences connues, documentées dans le README : moteur `re` de Python
(3.11+, groupes atomiques compris) au lieu d'Oniguruma — `(?<nom>...)`,
classes POSIX `[:alnum:]` et `\\z` sont traduits ; `named_captures_only`
est toujours vrai ; `timeout_millis` n'est pas appliqué.

Bibliothèque de base : `patterns/legacy/grok-patterns` de
logstash-patterns-core (https://github.com/logstash-plugins/logstash-patterns-core),
Copyright 2012-2015 Elasticsearch, licence Apache-2.0 (compatible
GPL-3.0, texte dans `LICENSES/Apache-2.0.txt`), recopiée sans
modification.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger

from pystashtemplate.templates import _MISSING, assign_field, parse_field_path, resolve_field
from pystashtemplate.tracing import traced
from pystashtemplate.util import add_failure_tags

BASE_PATTERNS: dict[str, str] = {
    "USERNAME": "[a-zA-Z0-9._-]+",
    "USER": "%{USERNAME}",
    "EMAILLOCALPART": "[a-zA-Z][a-zA-Z0-9_.+-=:]+",
    "EMAILADDRESS": "%{EMAILLOCALPART}@%{HOSTNAME}",
    "INT": "(?:[+-]?(?:[0-9]+))",
    "BASE10NUM": "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))",
    "NUMBER": "(?:%{BASE10NUM})",
    "BASE16NUM": "(?<![0-9A-Fa-f])(?:[+-]?(?:0x)?(?:[0-9A-Fa-f]+))",
    "BASE16FLOAT": "\\b(?<![0-9A-Fa-f.])(?:[+-]?(?:0x)?(?:(?:[0-9A-Fa-f]+(?:\\.[0-9A-Fa-f]*)?)|(?:\\.[0-9A-Fa-f]+)))\\b",  # noqa: E501 — motif officiel recopié tel quel
    "POSINT": "\\b(?:[1-9][0-9]*)\\b",
    "NONNEGINT": "\\b(?:[0-9]+)\\b",
    "WORD": "\\b\\w+\\b",
    "NOTSPACE": "\\S+",
    "SPACE": "\\s*",
    "DATA": ".*?",
    "GREEDYDATA": ".*",
    "QUOTEDSTRING": "(?>(?<!\\\\)(?>\"(?>\\\\.|[^\\\\\"]+)+\"|\"\"|(?>'(?>\\\\.|[^\\\\']+)+')|''|(?>`(?>\\\\.|[^\\\\`]+)+`)|``))",  # noqa: E501 — motif officiel recopié tel quel
    "UUID": "[A-Fa-f0-9]{8}-(?:[A-Fa-f0-9]{4}-){3}[A-Fa-f0-9]{12}",
    "URN": "urn:[0-9A-Za-z][0-9A-Za-z-]{0,31}:(?:%[0-9a-fA-F]{2}|[0-9A-Za-z()+,.:=@;$_!*'/?#-])+",
    "MAC": "(?:%{CISCOMAC}|%{WINDOWSMAC}|%{COMMONMAC})",
    "CISCOMAC": "(?:(?:[A-Fa-f0-9]{4}\\.){2}[A-Fa-f0-9]{4})",
    "WINDOWSMAC": "(?:(?:[A-Fa-f0-9]{2}-){5}[A-Fa-f0-9]{2})",
    "COMMONMAC": "(?:(?:[A-Fa-f0-9]{2}:){5}[A-Fa-f0-9]{2})",
    "IPV6": "((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:)))(%.+)?",  # noqa: E501 — motif officiel recopié tel quel
    "IPV4": "(?<![0-9])(?:(?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5]))(?![0-9])",  # noqa: E501 — motif officiel recopié tel quel
    "IP": "(?:%{IPV6}|%{IPV4})",
    "HOSTNAME": "\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b)",
    "IPORHOST": "(?:%{IP}|%{HOSTNAME})",
    "HOSTPORT": "%{IPORHOST}:%{POSINT}",
    "PATH": "(?:%{UNIXPATH}|%{WINPATH})",
    "UNIXPATH": "(/[[[:alnum:]]_%!$@:.,+~-]*)+",
    "TTY": "(?:/dev/(pts|tty([pq])?)(\\w+)?/?(?:[0-9]+))",
    "WINPATH": "(?>[A-Za-z]+:|\\\\)(?:\\\\[^\\\\?*]*)+",
    "URIPROTO": "[A-Za-z]([A-Za-z0-9+\\-.]+)+",
    "URIHOST": "%{IPORHOST}(?::%{POSINT:port})?",
    "URIPATH": "(?:/[A-Za-z0-9$.+!*'(){},~:;=@#%&_\\-]*)+",
    "URIPARAM": "\\?[A-Za-z0-9$.+!*'|(){},~@#%&/=:;_?\\-\\[\\]<>]*",
    "URIPATHPARAM": "%{URIPATH}(?:%{URIPARAM})?",
    "URI": "%{URIPROTO}://(?:%{USER}(?::[^@]*)?@)?(?:%{URIHOST})?(?:%{URIPATHPARAM})?",
    "MONTH": "\\b(?:[Jj]an(?:uary|uar)?|[Ff]eb(?:ruary|ruar)?|[Mm](?:a|ä)?r(?:ch|z)?|[Aa]pr(?:il)?|[Mm]a(?:y|i)?|[Jj]un(?:e|i)?|[Jj]ul(?:y|i)?|[Aa]ug(?:ust)?|[Ss]ep(?:tember)?|[Oo](?:c|k)?t(?:ober)?|[Nn]ov(?:ember)?|[Dd]e(?:c|z)(?:ember)?)\\b",  # noqa: E501 — motif officiel recopié tel quel
    "MONTHNUM": "(?:0?[1-9]|1[0-2])",
    "MONTHNUM2": "(?:0[1-9]|1[0-2])",
    "MONTHDAY": "(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])",
    "DAY": "(?:Mon(?:day)?|Tue(?:sday)?|Wed(?:nesday)?|Thu(?:rsday)?|Fri(?:day)?|Sat(?:urday)?|Sun(?:day)?)",
    "YEAR": "(?>\\d\\d){1,2}",
    "HOUR": "(?:2[0123]|[01]?[0-9])",
    "MINUTE": "(?:[0-5][0-9])",
    "SECOND": "(?:(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?)",
    "TIME": "(?!<[0-9])%{HOUR}:%{MINUTE}(?::%{SECOND})(?![0-9])",
    "DATE_US": "%{MONTHNUM}[/-]%{MONTHDAY}[/-]%{YEAR}",
    "DATE_EU": "%{MONTHDAY}[./-]%{MONTHNUM}[./-]%{YEAR}",
    "ISO8601_TIMEZONE": "(?:Z|[+-]%{HOUR}(?::?%{MINUTE}))",
    "ISO8601_SECOND": "(?:%{SECOND}|60)",
    "TIMESTAMP_ISO8601": "%{YEAR}-%{MONTHNUM}-%{MONTHDAY}[T ]%{HOUR}:?%{MINUTE}(?::?%{SECOND})?%{ISO8601_TIMEZONE}?",
    "DATE": "%{DATE_US}|%{DATE_EU}",
    "DATESTAMP": "%{DATE}[- ]%{TIME}",
    "TZ": "(?:[APMCE][SD]T|UTC)",
    "DATESTAMP_RFC822": "%{DAY} %{MONTH} %{MONTHDAY} %{YEAR} %{TIME} %{TZ}",
    "DATESTAMP_RFC2822": "%{DAY}, %{MONTHDAY} %{MONTH} %{YEAR} %{TIME} %{ISO8601_TIMEZONE}",
    "DATESTAMP_OTHER": "%{DAY} %{MONTH} %{MONTHDAY} %{TIME} %{TZ} %{YEAR}",
    "DATESTAMP_EVENTLOG": "%{YEAR}%{MONTHNUM2}%{MONTHDAY}%{HOUR}%{MINUTE}%{SECOND}",
    "SYSLOGTIMESTAMP": "%{MONTH} +%{MONTHDAY} %{TIME}",
    "PROG": "[\\x21-\\x5a\\x5c\\x5e-\\x7e]+",
    "SYSLOGPROG": "%{PROG:program}(?:\\[%{POSINT:pid}\\])?",
    "SYSLOGHOST": "%{IPORHOST}",
    "SYSLOGFACILITY": "<%{NONNEGINT:facility}.%{NONNEGINT:priority}>",
    "HTTPDATE": "%{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{INT}",
    "QS": "%{QUOTEDSTRING}",
    "SYSLOGBASE": "%{SYSLOGTIMESTAMP:timestamp} (?:%{SYSLOGFACILITY} )?%{SYSLOGHOST:logsource} %{SYSLOGPROG}:",
    "LOGLEVEL": "([Aa]lert|ALERT|[Tt]race|TRACE|[Dd]ebug|DEBUG|[Nn]otice|NOTICE|[Ii]nfo?(?:rmation)?|INFO?(?:RMATION)?|[Ww]arn?(?:ing)?|WARN?(?:ING)?|[Ee]rr?(?:or)?|ERR?(?:OR)?|[Cc]rit?(?:ical)?|CRIT?(?:ICAL)?|[Ff]atal|FATAL|[Ss]evere|SEVERE|EMERG(?:ENCY)?|[Ee]merg(?:ency)?)",  # noqa: E501 — motif officiel recopié tel quel
}

# %{NOM}, %{NOM:champ} ou %{NOM:champ:type} (champ façon référence Logstash).
_REFERENCE = re.compile(r"%\{(?P<nom>[A-Za-z0-9_]+)(?::(?P<champ>[^}]+))?\}")
# Groupe nommé Oniguruma `(?<nom>...)`, sans toucher aux `(?<=` / `(?<!`.
_GROUPE_NOMME = re.compile(r"\(\?<(?P<nom>[^>=!][^>]*)>")
_CLASSES_POSIX = {
    "[:alnum:]": "a-zA-Z0-9",
    "[:alpha:]": "a-zA-Z",
    "[:digit:]": "0-9",
    "[:xdigit:]": "0-9A-Fa-f",
    "[:lower:]": "a-z",
    "[:upper:]": "A-Z",
    "[:space:]": r"\s",
    "[:word:]": r"\w",
    "[:punct:]": r"!-/:-@\[-`{-~",
}
_FICHIER_MOTIF = re.compile(r"^(?P<nom>[A-Za-z0-9_]+)\s+(?P<motif>.*)$")
_PROFONDEUR_MAX = 64
_TYPES = {"int", "float"}
_ENTIER_RUBY = re.compile(r"\s*[+-]?\d+")
_REEL_RUBY = re.compile(r"\s*[+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?")


@dataclass(frozen=True)
class _Capture:
    """Un groupe de capture du motif compilé : chemin du champ et conversion."""

    champ: str
    chemin: tuple[str, ...]
    type: str | None


@dataclass(frozen=True)
class MotifCompile:
    """Un motif grok développé en expression régulière Python."""

    source: str
    regex: re.Pattern[str]
    captures: dict[str, _Capture]


@traced
def traduire_oniguruma(expression: str) -> str:
    """Traduit les tournures Oniguruma (Ruby) absentes de `re` : classes POSIX et `\\z`.

    Une classe POSIX imbriquée dans un ensemble (`[[[:alnum:]]_-]`, motif
    `UNIXPATH`) perd ses crochets intérieurs (`[a-zA-Z0-9_-]`) : `re` ne
    connaît pas les ensembles imbriqués. Seule dans son ensemble
    (`[[:alnum:]]`), elle est remplacée par ses plages (`[a-zA-Z0-9]`).
    """
    for posix, python in _CLASSES_POSIX.items():
        expression = expression.replace(f"[[{posix}]", f"[{python}").replace(posix, python)
    expression = expression.replace(r"\z", r"\Z")
    logger.debug("traduire_oniguruma() | {} caractère(s)", len(expression))
    return expression


@traced
def convertir(valeur: str, type_: str | None) -> Any:
    """Conversion `:int`/`:float` du plugin (Ruby `to_i`/`to_f`) ; autre type : texte inchangé."""
    if type_ == "int":
        trouve = _ENTIER_RUBY.match(valeur)
        resultat: Any = int(trouve.group()) if trouve else 0
    elif type_ == "float":
        trouve = _REEL_RUBY.match(valeur)
        resultat = float(trouve.group()) if trouve else 0.0
    else:
        resultat = valeur
    logger.debug("convertir() | type={}", type_)
    return resultat


@traced
def lire_dossiers_de_motifs(dossiers: list[str], glob: str = "*") -> dict[str, str]:
    """Motifs `NOM expression` des fichiers de `patterns_dir` (lignes vides et `#` ignorées).

    Un dossier absent ou un fichier illisible est ignoré (journalisé) :
    un `.conf` imparfait ne doit pas faire planter le traitement.
    """
    motifs: dict[str, str] = {}
    for dossier in dossiers:
        racine = Path(dossier)
        if not racine.is_dir():
            logger.debug("grok : patterns_dir absent, ignoré : {}", dossier)
            continue
        for fichier in sorted(racine.glob(glob)):
            if not fichier.is_file():
                continue
            try:
                lignes = fichier.read_text(encoding="utf-8").splitlines()
            except (OSError, UnicodeDecodeError) as erreur:
                logger.debug("grok : fichier de motifs illisible, ignoré : {} ({})", fichier, erreur)
                continue
            for ligne in lignes:
                trouve = _FICHIER_MOTIF.match(ligne.strip())
                if trouve and not ligne.lstrip().startswith("#"):
                    motifs[trouve.group("nom")] = trouve.group("motif")
    logger.debug("lire_dossiers_de_motifs() | {} motif(s)", len(motifs))
    return motifs


class Bibliotheque:
    """Motifs nommés disponibles (base, puis `patterns_dir`, puis `pattern_definitions`)."""

    @traced
    def __init__(self, supplements: dict[str, str] | None = None) -> None:
        """Bibliothèque de base complétée par `supplements` (prioritaires)."""
        self._motifs = {**BASE_PATTERNS, **(supplements or {})}

    @traced
    def compiler(self, motif: str) -> MotifCompile:
        """Développe `motif` et le compile.

        Raises:
            ValueError: Motif inconnu, récursion sans fin, champ mal formé
                ou expression invalide pour `re`.
        """
        captures: dict[str, _Capture] = {}
        expression = self._developper(motif, captures, 0)
        try:
            regex = re.compile(expression)
        except re.error as erreur:
            logger.debug("grok : expression invalide pour {!r} : {}", motif, erreur)
            raise ValueError(f"expression grok invalide : {erreur}") from erreur
        logger.debug("compiler() | {} capture(s)", len(captures))
        return MotifCompile(source=motif, regex=regex, captures=captures)

    @traced
    def _developper(self, motif: str, captures: dict[str, _Capture], profondeur: int) -> str:
        """Remplace récursivement les `%{...}` et les groupes nommés Oniguruma."""
        if profondeur > _PROFONDEUR_MAX:
            raise ValueError("motif grok récursif")
        morceaux: list[str] = []
        position = 0
        for trouve in _REFERENCE.finditer(motif):
            morceaux.append(self._groupes_nommes(motif[position : trouve.start()], captures))
            nom, champ = trouve.group("nom"), trouve.group("champ")
            if nom not in self._motifs:
                raise ValueError(f"motif grok inconnu : {nom}")
            interieur = self._developper(traduire_oniguruma(self._motifs[nom]), captures, profondeur + 1)
            if champ is None:
                morceaux.append(f"(?:{interieur})")
            else:
                groupe = self._nouvelle_capture(champ, captures)
                morceaux.append(f"(?P<{groupe}>{interieur})")
            position = trouve.end()
        morceaux.append(self._groupes_nommes(motif[position:], captures))
        resultat = "".join(morceaux)
        logger.debug("_developper() | profondeur={}", profondeur)
        return resultat

    @traced
    def _groupes_nommes(self, texte: str, captures: dict[str, _Capture]) -> str:
        """`(?<champ>...)` (Oniguruma) devient un groupe Python à nom généré."""
        resultat = _GROUPE_NOMME.sub(lambda t: f"(?P<{self._nouvelle_capture(t.group('nom'), captures)}>", texte)
        logger.debug("_groupes_nommes() | {} capture(s)", len(captures))
        return resultat

    @staticmethod
    @traced
    def _nouvelle_capture(reference: str, captures: dict[str, _Capture]) -> str:
        """Enregistre `champ[:type]` et renvoie le nom de groupe Python généré."""
        champ, type_ = reference, None
        if ":" in reference:
            avant, apres = reference.rsplit(":", 1)
            if apres in _TYPES or apres.isalpha():
                champ, type_ = avant, apres
        chemin = tuple(parse_field_path(champ))
        groupe = f"g{len(captures)}"
        captures[groupe] = _Capture(champ=champ, chemin=chemin, type=type_)
        logger.debug("_nouvelle_capture() | {} -> {}", champ, groupe)
        return groupe


@traced
def _liste(valeur: Any) -> list:
    """Option scalaire ou tableau ramenée à une liste (`None` : liste vide)."""
    resultat = [] if valeur is None else valeur if isinstance(valeur, list) else [valeur]
    logger.debug("_liste() | {} élément(s)", len(resultat))
    return resultat


@traced
def _booleen(valeur: Any, defaut: bool) -> bool:
    """Booléen d'option Logstash : `true`/`false` natifs ou en texte."""
    if isinstance(valeur, bool):
        resultat = valeur
    elif isinstance(valeur, str) and valeur.lower() in {"true", "false"}:
        resultat = valeur.lower() == "true"
    else:
        resultat = defaut
    logger.debug("_booleen() | {}", resultat)
    return resultat


@traced
def _paires_match(match: Any) -> list[tuple[str, list[str]]]:
    """`match` en paires `(champ, [motifs])` : forme hash ou tableau historique."""
    if isinstance(match, dict):
        paires = [(champ, [m for m in _liste(motifs) if isinstance(m, str)]) for champ, motifs in match.items()]
    elif isinstance(match, list) and len(match) % 2 == 0:
        paires = [(match[i], [match[i + 1]]) for i in range(0, len(match), 2) if isinstance(match[i + 1], str)]
    else:
        paires = []
    resultat = [(champ, motifs) for champ, motifs in paires if isinstance(champ, str) and motifs]
    logger.debug("_paires_match() | {} champ(s)", len(resultat))
    return resultat


@traced
def _texte(valeur: Any) -> str:
    """Valeur analysée en texte, booléens à la manière de Ruby (`true`/`false`)."""
    resultat = ("true" if valeur else "false") if isinstance(valeur, bool) else str(valeur)
    logger.debug("_texte() | {} caractère(s)", len(resultat))
    return resultat


class GrokFilter:
    """Plugin `grok` d'un `.conf`, compilé une fois, appliqué à chaque document."""

    @traced
    def __init__(self, options: dict[str, Any]) -> None:
        """Compile les motifs de `match` ; un motif invalide est ignoré (journalisé)."""
        supplements = lire_dossiers_de_motifs(
            [d for d in _liste(options.get("patterns_dir")) if isinstance(d, str)],
            options.get("patterns_files_glob") if isinstance(options.get("patterns_files_glob"), str) else "*",
        )
        definitions = options.get("pattern_definitions")
        if isinstance(definitions, dict):
            supplements.update({k: v for k, v in definitions.items() if isinstance(v, str)})
        bibliotheque = Bibliotheque(supplements)
        self._match: list[tuple[list[str], list[MotifCompile]]] = []
        for champ, motifs in _paires_match(options.get("match")):
            compiles = []
            for motif in motifs:
                try:
                    compiles.append(bibliotheque.compiler(motif))
                except ValueError as erreur:
                    logger.debug("grok : motif ignoré {!r} : {}", motif, erreur)
            try:
                chemin = parse_field_path(champ)
            except ValueError as erreur:
                logger.debug("grok : champ source mal formé, ignoré {!r} : {}", champ, erreur)
                continue
            if compiles:
                self._match.append((chemin, compiles))
        self._break_on_match = _booleen(options.get("break_on_match"), True)
        self._keep_empty = _booleen(options.get("keep_empty_captures"), False)
        self._overwrite = {c for c in _liste(options.get("overwrite")) if isinstance(c, str)}
        tags = options.get("tag_on_failure", ["_grokparsefailure"])
        self._tag_on_failure = [t for t in _liste(tags) if isinstance(t, str)]
        cible = options.get("target")
        self._cible = parse_field_path(cible) if isinstance(cible, str) and cible else []

    @property
    def actif(self) -> bool:
        """Vrai si au moins un motif valide a été compilé."""
        return bool(self._match)

    @traced
    def apply(self, doc: dict[str, Any]) -> bool:
        """Analyse les champs de `match`, écrit les captures en place.

        Returns:
            True si un motif a correspondu ; False sinon (tags
            `tag_on_failure` ajoutés), ou si le filtre est inactif.
        """
        if not self.actif:
            logger.debug("apply() | grok inactif (aucun motif valide)")
            return False
        reussi = False
        for chemin, motifs in self._match:
            valeur = resolve_field(doc, chemin)
            if valeur is _MISSING or valeur is None:
                continue
            if self._analyser(doc, _liste(valeur), motifs):
                reussi = True
                if self._break_on_match:
                    break
        if not reussi and self._tag_on_failure:
            add_failure_tags(doc, self._tag_on_failure)
        logger.debug("apply() | correspondance={}", reussi)
        return reussi

    @traced
    def _analyser(self, doc: dict[str, Any], valeurs: list, motifs: list[MotifCompile]) -> bool:
        """Essaie les motifs sur chaque valeur du champ (tableau compris)."""
        reussi = False
        for valeur in valeurs:
            texte = _texte(valeur)
            for motif in motifs:
                trouve = motif.regex.search(texte)
                if trouve is None:
                    continue
                reussi = True
                self._ecrire(doc, motif, trouve)
                if self._break_on_match:
                    break
        logger.debug("_analyser() | {}", reussi)
        return reussi

    @traced
    def _ecrire(self, doc: dict[str, Any], motif: MotifCompile, trouve: re.Match[str]) -> None:
        """Écrit chaque capture selon `handle()` du plugin officiel."""
        for groupe, capture in motif.captures.items():
            valeur = trouve.group(groupe)
            if valeur is None or (valeur == "" and not self._keep_empty):
                continue
            converti = convertir(valeur, capture.type)
            chemin = [*self._cible, *capture.chemin]
            existant = resolve_field(doc, chemin)
            if capture.champ in self._overwrite or existant is _MISSING or existant is None:
                assign_field(doc, chemin, converti)
            elif isinstance(existant, list):
                existant.append(converti)
            elif isinstance(existant, str):
                assign_field(doc, chemin, [existant, converti])
        logger.debug("_ecrire() | {} capture(s) possibles", len(motif.captures))
