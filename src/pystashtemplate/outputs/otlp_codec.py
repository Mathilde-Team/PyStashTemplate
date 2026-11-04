# SPDX-License-Identifier: GPL-3.0-or-later
"""Codage OTLP des journaux (issue #22) : JSON et protobuf, sans dépendance.

La requête `ExportLogsServiceRequest` est d'abord construite sous sa
forme JSON OTLP (noms en camelCase, entiers 64 bits en chaînes, comme le
prévoit la spécification OTLP/HTTP JSON), puis :

- sérialisée telle quelle (`application/json`) ;
- ou codée en protobuf (`application/x-protobuf`) par un encodeur écrit
  à la main : le schéma `opentelemetry/proto/logs/v1` n'utilise que des
  messages, des chaînes, des entiers (varint, fixed32/64) et des
  `double`, soit quelques dizaines de lignes — pas besoin d'ajouter
  `protobuf` ni `opentelemetry-proto` aux dépendances.

Le décodage se limite à la réponse `ExportLogsServiceResponse`
(`partial_success` : nombre de journaux refusés et message).

Fonctions pures : aucune entrée/sortie.
"""

from __future__ import annotations

import json
import math
import struct
from datetime import datetime, timezone
from typing import Any

from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.tracing import traced

# Niveaux de sévérité OTLP (`SeverityNumber`) par nom usuel — syslog
# (RFC 5424) et niveaux courants des bibliothèques de journalisation.
SEVERITES: dict[str, int] = {
    "trace": 1,
    "debug": 5,
    "info": 9,
    "informational": 9,
    "notice": 10,
    "warn": 13,
    "warning": 13,
    "err": 17,
    "error": 17,
    "crit": 18,
    "critical": 18,
    "alert": 19,
    "emerg": 21,
    "emergency": 21,
    "fatal": 21,
    "panic": 21,
}

# Sévérité syslog numérique (0 = emerg … 7 = debug) → `SeverityNumber`.
SEVERITES_SYSLOG: tuple[int, ...] = (21, 19, 18, 17, 13, 10, 9, 5)


class ErreurCodageOtlp(ValueError):
    """Valeur impossible à coder en OTLP (ex. flottant non fini en JSON)."""


@traced
def valeur(v: Any) -> dict[str, Any]:
    """`AnyValue` OTLP (forme JSON) d'une valeur Python.

    Les entiers 64 bits sont des chaînes en JSON OTLP ; un entier hors de
    l'intervalle int64 devient une chaîne de caractères ; un dictionnaire
    devient une `kvlistValue`, une liste ou un tuple une `arrayValue`,
    tout autre objet (octets compris) sa représentation `str`.
    """
    if v is None:
        return {}
    if isinstance(v, bool):
        return {"boolValue": v}
    if isinstance(v, int):
        if -(2**63) <= v < 2**63:
            return {"intValue": str(v)}
        return {"stringValue": str(v)}
    if isinstance(v, float):
        return {"doubleValue": v}
    if isinstance(v, str):
        return {"stringValue": v}
    if isinstance(v, dict):
        return {"kvlistValue": {"values": attributs(v)}}
    if isinstance(v, (list, tuple)):
        return {"arrayValue": {"values": [valeur(e) for e in v]}}
    return {"stringValue": str(v)}


@traced
def attributs(champs: dict[str, Any]) -> list[dict[str, Any]]:
    """Liste de `KeyValue` OTLP (forme JSON) d'un dictionnaire."""
    return [{"key": str(cle), "value": valeur(v)} for cle, v in champs.items()]


@traced
def horodatage_ns(v: Any) -> int | None:
    """Horodatage en nanosecondes depuis l'époque Unix, ou `None` si illisible.

    Accepte une date ISO 8601 (`2026-11-04T18:00:00Z`, sans fuseau : UTC),
    un `datetime`, ou un nombre — secondes, millisecondes, microsecondes ou
    nanosecondes, devinées à l'ordre de grandeur.
    """
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, datetime):
        moment = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        return int(moment.timestamp()) * 10**9 + moment.microsecond * 1000
    if isinstance(v, (int, float)):
        if not math.isfinite(v) or v < 0:
            return None
        for seuil, facteur in ((1e11, 10**9), (1e14, 10**6), (1e17, 10**3)):
            if v < seuil:
                entier = int(v)
                return entier * facteur + round((v - entier) * facteur)
        return int(v)
    if isinstance(v, str):
        try:
            return horodatage_ns(datetime.fromisoformat(v.replace("Z", "+00:00")))
        except ValueError:
            logger.debug("horodatage_ns | date illisible {!r}", v)
            return None
    return None


@traced
def severite(v: Any) -> tuple[int, str]:
    """`(SeverityNumber, severity_text)` d'un niveau (nom usuel ou code syslog 0–7).

    Un nom inconnu garde son texte avec le niveau 0 (non précisé).
    """
    if isinstance(v, bool) or v is None:
        return 0, ""
    if isinstance(v, int):
        if 0 <= v < len(SEVERITES_SYSLOG):
            return SEVERITES_SYSLOG[v], str(v)
        return 0, str(v)
    texte = str(v)
    return SEVERITES.get(texte.strip().lower(), 0), texte


@traced
def identifiant_hex(v: Any, octets: int) -> str | None:
    """Identifiant de trace (16 octets) ou de span (8 octets) en hexadécimal, ou `None`."""
    if not isinstance(v, str):
        return None
    texte = v.strip().lower()
    if len(texte) != 2 * octets:
        return None
    try:
        brut = bytes.fromhex(texte)
    except ValueError:
        logger.debug("identifiant_hex | identifiant non hexadécimal {!r}", v)
        return None
    return texte if any(brut) else None


@traced
def corps_json(requete: dict[str, Any]) -> bytes:
    """Requête OTLP sérialisée en JSON strict (UTF-8).

    Raises:
        ErreurCodageOtlp: flottant non fini (NaN, infini), interdit en JSON.
    """
    try:
        return json.dumps(requete, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except ValueError as exc:
        logger.warning(_("codage OTLP JSON impossible erreur={error}"), error=exc)
        raise ErreurCodageOtlp(str(exc)) from exc


# --- protobuf ---------------------------------------------------------------


@traced
def _varint(n: int) -> bytes:
    """Entier non signé en varint (les négatifs int64 sur 10 octets, complément à deux)."""
    n &= (1 << 64) - 1
    sortie = bytearray()
    while True:
        octet = n & 0x7F
        n >>= 7
        if n:
            sortie.append(octet | 0x80)
        else:
            sortie.append(octet)
            return bytes(sortie)


@traced
def _cle(numero: int, type_fil: int) -> bytes:
    return _varint((numero << 3) | type_fil)


@traced
def _delimite(numero: int, contenu: bytes) -> bytes:
    return _cle(numero, 2) + _varint(len(contenu)) + contenu


@traced
def _chaine(numero: int, texte: str) -> bytes:
    return _delimite(numero, texte.encode("utf-8")) if texte else b""


@traced
def _pb_valeur(v: dict[str, Any]) -> bytes:
    """`AnyValue` (champ du oneof présent, même vide : une chaîne vide reste une chaîne)."""
    if "stringValue" in v:
        return _delimite(1, v["stringValue"].encode("utf-8"))
    if "boolValue" in v:
        return _cle(2, 0) + _varint(int(v["boolValue"]))
    if "intValue" in v:
        return _cle(3, 0) + _varint(int(v["intValue"]))
    if "doubleValue" in v:
        return _cle(4, 1) + struct.pack("<d", v["doubleValue"])
    if "arrayValue" in v:
        return _delimite(5, b"".join(_delimite(1, _pb_valeur(e)) for e in v["arrayValue"]["values"]))
    if "kvlistValue" in v:
        return _delimite(6, b"".join(_delimite(1, _pb_kv(kv)) for kv in v["kvlistValue"]["values"]))
    return b""


@traced
def _pb_kv(kv: dict[str, Any]) -> bytes:
    return _chaine(1, kv["key"]) + _delimite(2, _pb_valeur(kv["value"]))


@traced
def _pb_attributs(numero: int, kvs: list[dict[str, Any]]) -> bytes:
    return b"".join(_delimite(numero, _pb_kv(kv)) for kv in kvs)


@traced
def _pb_journal(r: dict[str, Any]) -> bytes:
    """`LogRecord` (champs 1, 2, 3, 5, 6, 9, 10, 11)."""
    sortie = bytearray()
    if r.get("timeUnixNano"):
        sortie += _cle(1, 1) + struct.pack("<Q", int(r["timeUnixNano"]))
    if r.get("severityNumber"):
        sortie += _cle(2, 0) + _varint(int(r["severityNumber"]))
    sortie += _chaine(3, r.get("severityText", ""))
    if "body" in r:
        sortie += _delimite(5, _pb_valeur(r["body"]))
    sortie += _pb_attributs(6, r.get("attributes", []))
    if r.get("traceId"):
        sortie += _delimite(9, bytes.fromhex(r["traceId"]))
    if r.get("spanId"):
        sortie += _delimite(10, bytes.fromhex(r["spanId"]))
    if r.get("observedTimeUnixNano"):
        sortie += _cle(11, 1) + struct.pack("<Q", int(r["observedTimeUnixNano"]))
    return bytes(sortie)


@traced
def corps_protobuf(requete: dict[str, Any]) -> bytes:
    """Requête OTLP (forme JSON) codée en protobuf `ExportLogsServiceRequest`."""
    sortie = bytearray()
    for rl in requete.get("resourceLogs", []):
        ressource = _pb_attributs(1, rl.get("resource", {}).get("attributes", []))
        contenu = _delimite(1, ressource)
        for sl in rl.get("scopeLogs", []):
            portee = sl.get("scope", {})
            scope = _chaine(1, portee.get("name", "")) + _chaine(2, portee.get("version", ""))
            journaux = b"".join(_delimite(2, _pb_journal(r)) for r in sl.get("logRecords", []))
            contenu += _delimite(2, _delimite(1, scope) + journaux)
        sortie += _delimite(1, contenu)
    return bytes(sortie)


@traced
def _lire_varint(donnees: bytes, pos: int) -> tuple[int, int]:
    resultat = decalage = 0
    while True:
        if pos >= len(donnees) or decalage > 63:
            logger.warning(_("réponse OTLP protobuf : varint tronqué"))
            raise ValueError("varint tronqué")
        octet = donnees[pos]
        pos += 1
        resultat |= (octet & 0x7F) << decalage
        decalage += 7
        if not octet & 0x80:
            return resultat, pos


@traced
def _champs(donnees: bytes) -> dict[int, list[Any]]:
    """Champs d'un message protobuf : varints en entiers, délimités en octets (64/32 bits ignorés)."""
    champs: dict[int, list[Any]] = {}
    pos = 0
    while pos < len(donnees):
        cle, pos = _lire_varint(donnees, pos)
        numero, type_fil = cle >> 3, cle & 7
        if type_fil == 0:
            v, pos = _lire_varint(donnees, pos)
        elif type_fil == 2:
            longueur, pos = _lire_varint(donnees, pos)
            if pos + longueur > len(donnees):
                logger.warning(_("réponse OTLP protobuf : champ tronqué"))
                raise ValueError("champ tronqué")
            v, pos = donnees[pos : pos + longueur], pos + longueur
        elif type_fil in (1, 5):
            pos += 8 if type_fil == 1 else 4
            continue
        else:
            logger.warning(_("réponse OTLP protobuf : type de fil {type_fil} non pris en charge"), type_fil=type_fil)
            raise ValueError(f"type de fil {type_fil} non pris en charge")
        champs.setdefault(numero, []).append(v)
    return champs


@traced
def succes_partiel_protobuf(donnees: bytes) -> tuple[int, str]:
    """`(rejected_log_records, error_message)` d'une réponse protobuf.

    Raises:
        ValueError: réponse illisible.
    """
    reponse = _champs(donnees)
    if not reponse.get(1):
        return 0, ""
    partiel = _champs(reponse[1][-1])
    rejetes = partiel.get(1, [0])[-1]
    if rejetes >= 2**63:
        rejetes -= 2**64
    message = partiel.get(2, [b""])[-1]
    return int(rejetes), message.decode("utf-8", "replace")


@traced
def succes_partiel_json(corps: Any) -> tuple[int, str]:
    """`(rejectedLogRecords, errorMessage)` d'une réponse JSON (décodée).

    Raises:
        ValueError: réponse qui n'est pas un objet, ou nombre illisible.
    """
    if not isinstance(corps, dict):
        logger.warning(_("réponse OTLP JSON : objet attendu"))
        raise ValueError("réponse OTLP JSON : objet attendu")
    partiel = corps.get("partialSuccess") or {}
    if not isinstance(partiel, dict):
        logger.warning(_("réponse OTLP JSON : partialSuccess n'est pas un objet"))
        raise ValueError("partialSuccess : objet attendu")
    return int(partiel.get("rejectedLogRecords") or 0), str(partiel.get("errorMessage") or "")
