# SPDX-License-Identifier: GPL-3.0-or-later
"""Décodeur sFlow v5 (RFC 3176) : l'entrée réseau du service (#97).

Un datagramme sFlow porte des **échantillons** — de flux (un paquet
rencontré, éventuellement sous-échantillonné) ou de compteurs (les
compteurs d'une interface, comme SNMP) — émis par un agent (routeur,
commutateur, hôte) vers le collecteur. Le service autonome (#39)
écoute ces datagrammes en UDP et transforme chaque échantillon en
document pour le pipeline `filter{}`/`output{}` :

- **entête du datagramme** : `agent`, `sub_agent_id`, `sequence`,
  `uptime_ms` — rapportés à chaque échantillon, avec `type`
  (`flux`/`compteur`) ;
- **échantillon de flux** : `sampling_rate`, `sample_pool`, `drops`,
  `input_interface`/`output_interface`, et l'enregistrement « en-tête
  de paquet brut » (format 1) décodé jusqu'aux ports : `src_mac`/
  `dst_mac`, `ethertype`, `src_ip`/`dst_ip`, `ip_protocol`, `ip_ttl`,
  `src_port`/`dst_port`, `packet_size` ;
- **échantillon de compteurs** : enregistrement « compteurs génériques
  d'interface » (format 1) : `ifindex`, `iftype`, `ifspeed`,
  `in_octets`, `in_packets`, `in_errors`, `out_octets`,
  `out_packets`, `out_errors`, `in_discards`, `out_discards`,
  `ifpromiscuous_mode`.

Tout ce qui n'est pas décodé (type d'échantillon inconnu, format
d'enregistrement inconnu, datagramme tronqué) est écarté avec un
journal — un collecteur doit tolérer le bruit, pas tomber. Les
échantillons « étendus » (`expanded`, types 3 et 4, index 64 bits)
attendent d'être utiles : personne ne les envoie par défaut.
"""

from __future__ import annotations

import struct
from typing import Any

from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.tracing import traced

__all__ = ["analyser_sflow"]


class _Lecteur:
    """Lecteur gros-boutien avec curseur — lève si la trame est trop courte."""

    def __init__(self, donnees: bytes) -> None:
        logger.debug("_Lecteur.__init__ | entrée")
        self._donnees = donnees
        self._position = 0
        logger.debug("_Lecteur.__init__ | sortie")

    @traced
    def u32(self) -> int:
        """Lit un entier non signé sur 4 octets."""
        return struct.unpack_from(">I", self._donnees, self._curseur(4))[0]

    @traced
    def u64(self) -> int:
        """Lit un entier non signé sur 8 octets."""
        return struct.unpack_from(">Q", self._donnees, self._curseur(8))[0]

    @traced
    def octets(self, nombre: int) -> bytes:
        """Lit `nombre` octets tels quels."""
        debut = self._curseur(nombre)
        return self._donnees[debut : debut + nombre]

    @traced
    def _curseur(self, besoin: int) -> int:
        """Avance le curseur de `besoin` octets ou lève (trame tronquée)."""
        if self._position + besoin > len(self._donnees):
            logger.debug("_Lecteur._curseur | trame tronquée à l'octet {}", self._position)
            raise ValueError("trame sflow tronquée")
        position = self._position
        self._position += besoin
        return position


@traced
def analyser_sflow(trame: bytes) -> list[dict[str, Any]]:
    """Décode un datagramme sFlow v5 en documents, un par échantillon.

    Args:
        trame: Le datagramme UDP brut.

    Returns:
        Les échantillons décodés (peut être vide : version non 5,
        aucun échantillon, ou datagramme tronqué — écarté avec un
        journal, le collecteur reste debout).
    """
    try:
        lecteur = _Lecteur(trame)
        version = lecteur.u32()
        if version != 5:
            logger.debug("analyser_sflow | version {} non supportée, écarté", version)
            return []
        agent = _adresse_agent(lecteur)
        entete: dict[str, Any] = {
            "agent": agent,
            "sub_agent_id": lecteur.u32(),
            "sequence": lecteur.u32(),
            "uptime_ms": lecteur.u32(),
        }
        documents: list[dict[str, Any]] = []
        for _index in range(lecteur.u32()):
            type_echantillon = lecteur.u32()
            longueur = lecteur.u32()
            corps = lecteur.octets(longueur)
            if type_echantillon == 1:
                documents.append(_decoder_echantillon_flux(corps, entete))
            elif type_echantillon == 2:
                documents.append(_decoder_echantillon_compteurs(corps, entete))
            else:
                logger.debug("analyser_sflow | type d'échantillon {} inconnu, écarté", type_echantillon)
        return documents
    except (ValueError, struct.error):
        logger.warning(_("sflow : datagramme tronqué écarté"))
        return []


@traced
def _adresse_agent(lecteur: _Lecteur) -> str:
    """Lit l'adresse de l'agent sFlow (IPv4, IPv6 ou MAC)."""
    type_adresse = lecteur.u32()
    if type_adresse == 1:
        return ".".join(str(o) for o in lecteur.octets(4))
    if type_adresse == 2:
        octets = lecteur.octets(16)
        groupes = (octets[i : i + 2].hex() for i in range(0, 16, 2))
        return ":".join(groupes)
    if type_adresse == 3:
        return lecteur.octets(6).hex(":")
    logger.debug("_adresse_agent | type d'adresse {} inconnu, 0.0.0.0", type_adresse)
    return "0.0.0.0"


@traced
def _decoder_echantillon_flux(corps: bytes, entete: dict[str, Any]) -> dict[str, Any]:
    """Décode un échantillon de flux : en-tête + enregistrements."""
    lecteur = _Lecteur(corps)
    document: dict[str, Any] = dict(entete)
    document["type"] = "flux"
    document["sequence_echantillon"] = lecteur.u32()
    document["source_id"] = lecteur.u32()
    document["sampling_rate"] = lecteur.u32()
    document["sample_pool"] = lecteur.u32()
    document["drops"] = lecteur.u32()
    document["input_interface"] = lecteur.u32()
    document["output_interface"] = lecteur.u32()
    for _index in range(lecteur.u32()):
        format_enregistrement = lecteur.u32()
        longueur = lecteur.u32()
        enregistrement = lecteur.octets(longueur)
        if format_enregistrement == 1:
            document.update(_decoder_en_tete_paquet(enregistrement))
        else:
            logger.debug(
                "_decoder_echantillon_flux | format d'enregistrement {} inconnu, écarté",
                format_enregistrement,
            )
    return document


@traced
def _decoder_echantillon_compteurs(corps: bytes, entete: dict[str, Any]) -> dict[str, Any]:
    """Décode un échantillon de compteurs : en-tête + enregistrements."""
    lecteur = _Lecteur(corps)
    document: dict[str, Any] = dict(entete)
    document["type"] = "compteur"
    document["sequence_echantillon"] = lecteur.u32()
    document["source_id"] = lecteur.u32()
    for _index in range(lecteur.u32()):
        format_enregistrement = lecteur.u32()
        longueur = lecteur.u32()
        enregistrement = lecteur.octets(longueur)
        if format_enregistrement == 1:
            document.update(_decoder_compteurs_interface(enregistrement))
        else:
            logger.debug(
                "_decoder_echantillon_compteurs | format d'enregistrement {} inconnu, écarté",
                format_enregistrement,
            )
    return document


@traced
def _decoder_en_tete_paquet(enregistrement: bytes) -> dict[str, Any]:
    """Décode l'enregistrement « en-tête de paquet brut » (format 1).

    Ethernet puis IPv4 puis TCP/UDP quand c'est ce que contient le
    paquet échantillonné — les adresses MAC, IP et ports du flux.
    """
    lecteur = _Lecteur(enregistrement)
    document: dict[str, Any] = {}
    protocole_entete = lecteur.u32()
    document["packet_size"] = lecteur.u32()
    lecteur.u32()  # octets retirés par l'agent avant capture
    longueur_entete = lecteur.u32()
    entete = lecteur.octets(longueur_entete)
    if protocole_entete == 1 and len(entete) >= 14:  # Ethernet
        document["dst_mac"] = entete[0:6].hex(":")
        document["src_mac"] = entete[6:12].hex(":")
        ethertype = struct.unpack_from(">H", entete, 12)[0]
        document["ethertype"] = f"0x{ethertype:04x}"
        if ethertype == 0x0800 and len(entete) >= 34:  # IPv4
            document.update(_decoder_ipv4(entete[14:]))
    else:
        logger.debug("_decoder_en_tete_paquet | protocole d'en-tête {} non décodé", protocole_entete)
    return document


@traced
def _decoder_ipv4(entete: bytes) -> dict[str, Any]:
    """Décode un en-tête IPv4 (puis TCP/UDP : les ports du flux)."""
    document: dict[str, Any] = {}
    longueur_entete_ip = (entete[0] & 0x0F) * 4
    document["ip_protocol"] = entete[9]
    document["src_ip"] = ".".join(str(o) for o in entete[12:16])
    document["dst_ip"] = ".".join(str(o) for o in entete[16:20])
    document["ip_ttl"] = entete[8]
    charge = entete[longueur_entete_ip:]
    if document["ip_protocol"] in (6, 17) and len(charge) >= 4:  # TCP, UDP
        document["src_port"] = struct.unpack_from(">H", charge, 0)[0]
        document["dst_port"] = struct.unpack_from(">H", charge, 2)[0]
    else:
        logger.debug("_decoder_ipv4 | protocole {} sans ports décodés", document["ip_protocol"])
    return document


@traced
def _decoder_compteurs_interface(enregistrement: bytes) -> dict[str, Any]:
    """Décode l'enregistrement « compteurs génériques d'interface » (format 1)."""
    lecteur = _Lecteur(enregistrement)
    return {
        "ifindex": lecteur.u32(),
        "iftype": lecteur.u32(),
        "ifspeed": lecteur.u64(),
        "ifdirection": lecteur.u32(),
        "ifstatus": lecteur.u32(),
        "in_octets": lecteur.u64(),
        "in_packets": lecteur.u32(),
        "in_errors": lecteur.u32(),
        "in_discards": lecteur.u32(),
        "out_octets": lecteur.u64(),
        "out_packets": lecteur.u32(),
        "out_errors": lecteur.u32(),
        "out_discards": lecteur.u32(),
        "ifpromiscuous_mode": lecteur.u32(),
    }
