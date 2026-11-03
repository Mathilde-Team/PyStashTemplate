# SPDX-License-Identifier: GPL-3.0-or-later
"""Plugins `filter{}` simples façon Logstash pour les pipelines `.conf` (issue #120).

Plugins sans dépendance externe, options principales et défauts du plugin
officiel correspondant :

- `csv` : `source` (`message`), `target`, `separator` (`,`), `quote_char`
  (`"`), `columns`, `autogenerate_column_names` (vrai), `skip_empty_columns`,
  `skip_header`, `convert` (`integer`, `float`, `boolean`), `tag_on_failure`
  (`_csvparsefailure`) ;
- `cidr` : `address` (gabarits `%{champ}`), `network` (CIDR) et
  `network_path` (un CIDR par ligne) — succès si une adresse est dans un
  réseau ;
- `prune` : `whitelist_names`, `blacklist_names` (défaut `%{[^}]+}`),
  `whitelist_values`, `blacklist_values` (expressions régulières), champs de
  premier niveau seulement, comme le plugin ;
- `urldecode` : `field` (`message`), `all_fields`, `tag_on_failure`
  (`_urldecodefailure`) ;
- `uuid` : `target` (requis), `overwrite` ;
- `syslog_pri` : `syslog_pri_field_name` (`syslog_pri`, défaut 13),
  `use_labels` (vrai), `facility_labels`, `severity_labels` ;
- `truncate` : `length_bytes` (requis), `fields` (sinon tous les champs de
  premier niveau), coupure sur une frontière de caractère UTF-8.

Chaque fonction renvoie True si le filtre a réussi (les options communes
#116 sont alors appliquées par l'interpréteur).
"""

from __future__ import annotations

import csv
import ipaddress
import re
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.templates import _MISSING, assign_field, parse_field_path, render_template, resolve_field
from pystashtemplate.tracing import traced
from pystashtemplate.util import add_failure_tags

FACILITES = [
    "kernel",
    "user-level",
    "mail",
    "daemon",
    "security/authorization",
    "syslogd",
    "line printer",
    "network news",
    "uucp",
    "clock",
    "security/authorization",
    "ftp",
    "ntp",
    "log audit",
    "log alert",
    "clock",
    *(f"local{i}" for i in range(8)),
]
SEVERITES = ["emergency", "alert", "critical", "error", "warning", "notice", "informational", "debug"]


@traced
def _liste(valeur: Any) -> list[Any]:
    """Option scalaire ou tableau → liste (absente → liste vide)."""
    if valeur is None:
        return []
    return valeur if isinstance(valeur, list) else [valeur]


@traced
def _convertir_csv(texte: str, type_cible: Any) -> Any:
    """`convert` de csv : `integer`, `float`, `boolean` ; texte inchangé sinon."""
    if type_cible == "integer":
        return int(texte)
    if type_cible == "float":
        return float(texte)
    if type_cible == "boolean":
        bas = texte.strip().lower()
        if bas in ("true", "t", "yes", "y", "1"):
            return True
        if bas in ("false", "f", "no", "n", "0"):
            return False
        raise ValueError(texte)
    logger.warning(_("csv : type {} inconnu pour convert"), type_cible)
    return texte


@traced
def appliquer_csv(doc: dict[str, Any], options: dict[str, Any]) -> bool:
    """`csv { … }` : découpe une ligne CSV en colonnes."""
    source = options.get("source", "message")
    texte = resolve_field(doc, parse_field_path(source))
    if texte is _MISSING or texte is None:
        return False
    echec = options.get("tag_on_failure", ["_csvparsefailure"])
    try:
        if not isinstance(texte, str):
            raise TypeError(type(texte).__name__)
        lignes = list(
            csv.reader(
                [texte],
                delimiter=options.get("separator", ","),
                quotechar=options.get("quote_char", '"'),
                strict=True,
            )
        )
    except (csv.Error, TypeError) as exc:
        add_failure_tags(doc, echec)
        logger.warning(_("csv : analyse impossible du champ {} : {}"), source, exc)
        return False
    valeurs = lignes[0] if lignes else []
    colonnes = [str(c) for c in _liste(options.get("columns"))]
    if options.get("skip_header", False) is True and colonnes and valeurs == colonnes:
        return False
    conversions = options.get("convert") if isinstance(options.get("convert"), dict) else {}
    cible = options.get("target")
    for index, valeur in enumerate(valeurs):
        if index < len(colonnes):
            nom = colonnes[index]
        elif options.get("autogenerate_column_names", True) is True:
            nom = f"column{index + 1}"
        else:
            continue
        if valeur == "" and options.get("skip_empty_columns", False) is True:
            continue
        resultat: Any = valeur
        if nom in conversions:
            try:
                resultat = _convertir_csv(valeur, conversions[nom])
            except ValueError:
                logger.warning(_("csv : conversion impossible de {} en {}"), nom, conversions[nom])
        chemin = parse_field_path(nom)
        assign_field(doc, [*parse_field_path(cible), *chemin] if isinstance(cible, str) else chemin, resultat)
    return True


@traced
def reseaux_cidr(options: dict[str, Any]) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    """Réseaux de `network` et `network_path` ; entrées invalides journalisées et ignorées."""
    textes = [str(n) for n in _liste(options.get("network"))]
    chemin = options.get("network_path")
    if isinstance(chemin, str):
        try:
            textes += [ligne.strip() for ligne in Path(chemin).read_text(encoding="utf-8").splitlines()]
        except OSError as exc:
            logger.warning(_("cidr : lecture impossible de {} : {}"), chemin, exc)
    reseaux = []
    for texte in textes:
        if not texte or texte.startswith("#"):
            continue
        try:
            reseaux.append(ipaddress.ip_network(texte, strict=False))
        except ValueError:
            logger.warning(_("cidr : réseau invalide ignoré : {}"), texte)
    return reseaux


@traced
def appliquer_cidr(doc: dict[str, Any], options: dict[str, Any], reseaux: list[Any]) -> bool:
    """`cidr { address network }` : vrai si une adresse appartient à un réseau."""
    for gabarit in _liste(options.get("address")):
        texte = render_template(str(gabarit), doc)
        try:
            adresse = ipaddress.ip_address(texte)
        except ValueError:
            logger.debug("cidr : adresse invalide ignorée : {}", texte)
            continue
        if any(adresse.version == r.version and adresse in r for r in reseaux):
            return True
    return False


@traced
def _correspond(motifs: list[Any], texte: str) -> bool:
    """Vrai si une des expressions régulières trouve `texte`."""
    return any(re.search(str(m), texte) for m in motifs)


@traced
def _valeurs_retenues(valeur: Any, motif: Any, *, garder: bool) -> Any:
    """Filtre une valeur (ou les éléments d'un tableau) selon `motif` ; _MISSING si plus rien."""
    motifs = _liste(motif)
    if isinstance(valeur, list):
        restes = [v for v in valeur if _correspond(motifs, str(v)) is garder]
        return restes or _MISSING
    return valeur if _correspond(motifs, str(valeur)) is garder else _MISSING


@traced
def appliquer_prune(doc: dict[str, Any], options: dict[str, Any]) -> bool:
    """`prune { … }` : retire les champs de premier niveau selon leurs noms et valeurs."""
    blancs = _liste(options.get("whitelist_names"))
    noirs = _liste(options.get("blacklist_names", [r"%\{[^}]+\}"]))
    valeurs_blanches = options.get("whitelist_values") if isinstance(options.get("whitelist_values"), dict) else {}
    valeurs_noires = options.get("blacklist_values") if isinstance(options.get("blacklist_values"), dict) else {}
    for nom in list(doc):
        if (blancs and not _correspond(blancs, nom)) or _correspond(noirs, nom):
            del doc[nom]
            continue
        valeur = doc[nom]
        if nom in valeurs_blanches:
            valeur = _valeurs_retenues(valeur, valeurs_blanches[nom], garder=True)
        if nom in valeurs_noires and valeur is not _MISSING:
            valeur = _valeurs_retenues(valeur, valeurs_noires[nom], garder=False)
        if valeur is _MISSING:
            del doc[nom]
        else:
            doc[nom] = valeur
    return True


@traced
def _decoder(valeur: Any) -> Any:
    """Décodage URL d'une chaîne ou des chaînes d'un tableau ; le reste inchangé."""
    if isinstance(valeur, str):
        return unquote(valeur, errors="strict")
    if isinstance(valeur, list):
        return [_decoder(v) for v in valeur]
    return valeur


@traced
def appliquer_urldecode(doc: dict[str, Any], options: dict[str, Any]) -> bool:
    """`urldecode { field all_fields }`."""
    if options.get("all_fields", False) is True:
        chemins = [[nom] for nom in doc]
    else:
        chemins = [parse_field_path(options.get("field", "message"))]
    try:
        for chemin in chemins:
            valeur = resolve_field(doc, chemin)
            if valeur is not _MISSING:
                assign_field(doc, chemin, _decoder(valeur))
    except UnicodeDecodeError as exc:
        add_failure_tags(doc, options.get("tag_on_failure", ["_urldecodefailure"]))
        logger.warning(_("urldecode : décodage impossible : {}"), exc)
        return False
    return True


@traced
def appliquer_uuid(doc: dict[str, Any], options: dict[str, Any]) -> bool:
    """`uuid { target overwrite }` : UUID v4 aléatoire."""
    cible = options.get("target")
    if not isinstance(cible, str):
        logger.warning(_("uuid ignoré : option target manquante"))
        return False
    chemin = parse_field_path(cible)
    if options.get("overwrite", False) is not True and resolve_field(doc, chemin) is not _MISSING:
        return True
    assign_field(doc, chemin, str(uuid.uuid4()))
    return True


@traced
def appliquer_syslog_pri(doc: dict[str, Any], options: dict[str, Any]) -> bool:
    """`syslog_pri { … }` : code et libellé de facility et de severity."""
    brut = resolve_field(doc, parse_field_path(options.get("syslog_pri_field_name", "syslog_pri")))
    if isinstance(brut, list):
        brut = brut[0] if brut else _MISSING
    try:
        pri = 13 if brut is _MISSING or brut is None else int(str(brut))
    except ValueError:
        logger.warning(_("syslog_pri : valeur invalide {}"), brut)
        pri = 0  # Ruby `to_i` d'un texte non numérique
    facilite, severite = pri >> 3, pri & 7
    doc["syslog_facility_code"] = facilite
    doc["syslog_severity_code"] = severite
    if options.get("use_labels", True) is True:
        libelles_f = _liste(options.get("facility_labels")) or FACILITES
        libelles_s = _liste(options.get("severity_labels")) or SEVERITES
        if facilite < len(libelles_f):
            doc["syslog_facility"] = libelles_f[facilite]
        if severite < len(libelles_s):
            doc["syslog_severity"] = libelles_s[severite]
    return True


@traced
def _tronquer(valeur: Any, longueur: int) -> Any:
    """Coupe une chaîne (ou les chaînes d'un tableau) à `longueur` octets UTF-8."""
    if isinstance(valeur, str):
        return valeur.encode("utf-8")[:longueur].decode("utf-8", errors="ignore")
    if isinstance(valeur, list):
        return [_tronquer(v, longueur) for v in valeur]
    return valeur


@traced
def appliquer_truncate(doc: dict[str, Any], options: dict[str, Any]) -> bool:
    """`truncate { length_bytes fields }`."""
    longueur = options.get("length_bytes")
    if not isinstance(longueur, int) or isinstance(longueur, bool) or longueur < 1:
        logger.warning(_("truncate ignoré : length_bytes doit être un entier positif"))
        return False
    champs = _liste(options.get("fields")) or list(doc)
    for champ in champs:
        chemin = parse_field_path(str(champ))
        valeur = resolve_field(doc, chemin)
        if valeur is not _MISSING:
            assign_field(doc, chemin, _tronquer(valeur, longueur))
    return True


PLUGINS: dict[str, Callable[[dict[str, Any], dict[str, Any]], bool]] = {
    "csv": appliquer_csv,
    "prune": appliquer_prune,
    "urldecode": appliquer_urldecode,
    "uuid": appliquer_uuid,
    "syslog_pri": appliquer_syslog_pri,
    "truncate": appliquer_truncate,
}
