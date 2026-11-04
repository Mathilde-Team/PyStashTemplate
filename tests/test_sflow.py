# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de l'entrée sFlow v5 (issue #97) : décodeur et écoute réelle.

Les datagrammes sont construits octet par octet (struct, gros-boutien)
exactement comme un agent sFlow les émettrait — RFC 3176 à la lettre.
"""

from __future__ import annotations

import asyncio
import struct
from typing import Any

from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs.base import Output
from pystashtemplate.service import EntreeSflow, Service
from pystashtemplate.sflow import analyser_sflow


class SortieTemoins(Output):
    """Sortie de couture : chaque document envoyé est mémorisé."""

    plugin = "temoins"

    async def open(self) -> None:
        return None

    async def send(self, batch: Any) -> None:
        self.options.setdefault("recus", []).extend(batch)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        self.options.setdefault("fermee", True)

    async def health(self) -> dict[str, Any]:
        return {"status": "green"}


async def _attendre(condition: Any, delai: float = 2.0) -> None:
    """Attend que la condition (callable) devienne vraie."""
    boucle = asyncio.get_event_loop()
    fin = boucle.time() + delai
    while boucle.time() < fin:
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"condition non atteinte en {delai} secondes")


def _u32(nombre: int) -> bytes:
    return struct.pack(">I", nombre)


def _u64(nombre: int) -> bytes:
    return struct.pack(">Q", nombre)


def _entete_paquet_tcp() -> bytes:
    """En-tête Ethernet + IPv4 + TCP, comme capturé par un agent."""
    ethernet = b"\x01\x23\x45\x67\x89\xab" + b"\xba\x98\x76\x54\x32\x10" + struct.pack(">H", 0x0800)
    ip = (
        b"\x45\x00"  # version 4, IHL 5, tos
        + struct.pack(">H", 40)  # longueur totale
        + _u32(0)  # identification, drapeaux
        + b"\x40\x06"  # ttl 64, protocole TCP
        + b"\x00\x00"  # somme de contrôle
        + bytes([10, 0, 0, 1])
        + bytes([10, 0, 0, 2])
    )
    tcp = struct.pack(">HH", 54321, 443) + b"\x00" * 16
    return ethernet + ip + tcp


def _datagramme_flux() -> bytes:
    enregistrement = _u32(1) + _u32(24) + _u32(0) + _u32(len(_entete_paquet_tcp()))
    enregistrement += _entete_paquet_tcp()
    corps = (
        _u32(7)  # séquence de l'échantillon
        + _u32(3)  # source_id
        + _u32(100)  # taux d'échantillonnage
        + _u32(1000)  # pool
        + _u32(0)  # pertes
        + _u32(1)  # interface d'entrée
        + _u32(2)  # interface de sortie
        + _u32(1)  # un enregistrement
        + _u32(1)  # format : en-tête de paquet brut
        + _u32(len(enregistrement))
        + enregistrement
    )
    return (
        _u32(5)  # version
        + _u32(1)
        + bytes([192, 168, 1, 1])  # agent IPv4
        + _u32(0)  # sous-agent
        + _u32(42)  # séquence du datagramme
        + _u32(123456)  # uptime
        + _u32(1)  # un échantillon
        + _u32(1)  # type : flux
        + _u32(len(corps))
        + corps
    )


def _compteurs_interface() -> bytes:
    return (
        _u32(5)  # ifindex
        + _u32(6)  # iftype
        + _u64(1_000_000_000)  # ifspeed
        + _u32(0)  # ifdirection
        + _u32(1)  # ifstatus
        + _u64(111)  # in_octets
        + _u32(222)  # in_packets
        + _u32(2)  # in_errors
        + _u32(3)  # in_discards
        + _u64(444)  # out_octets
        + _u32(555)  # out_packets
        + _u32(6)  # out_errors
        + _u32(7)  # out_discards
        + _u32(0)  # ifpromiscuous_mode
    )


def _datagramme_compteurs() -> bytes:
    enregistrement = _u32(1) + _u32(len(_compteurs_interface())) + _compteurs_interface()
    corps = _u32(8) + _u32(4) + _u32(1) + enregistrement
    return (
        _u32(5)
        + _u32(2)
        + bytes.fromhex("20010db8000000000000000000000001")  # agent IPv6
        + _u32(1)
        + _u32(43)
        + _u32(999)
        + _u32(1)
        + _u32(2)  # type : compteurs
        + _u32(len(corps))
        + corps
    )


class TestAnalyserSflow:
    def test_echantillon_flux(self) -> None:
        documents = analyser_sflow(_datagramme_flux())
        assert len(documents) == 1
        doc = documents[0]
        assert doc["type"] == "flux"
        assert doc["agent"] == "192.168.1.1"
        assert doc["sub_agent_id"] == 0
        assert doc["sequence"] == 42
        assert doc["uptime_ms"] == 123456
        assert doc["sequence_echantillon"] == 7
        assert doc["sampling_rate"] == 100
        assert doc["sample_pool"] == 1000
        assert doc["input_interface"] == 1
        assert doc["output_interface"] == 2
        assert doc["dst_mac"] == "01:23:45:67:89:ab"
        assert doc["src_mac"] == "ba:98:76:54:32:10"
        assert doc["ethertype"] == "0x0800"
        assert doc["packet_size"] == 24
        assert doc["src_ip"] == "10.0.0.1"
        assert doc["dst_ip"] == "10.0.0.2"
        assert doc["ip_protocol"] == 6
        assert doc["ip_ttl"] == 64
        assert doc["src_port"] == 54321
        assert doc["dst_port"] == 443

    def test_echantillon_compteurs(self) -> None:
        documents = analyser_sflow(_datagramme_compteurs())
        assert len(documents) == 1
        doc = documents[0]
        assert doc["type"] == "compteur"
        assert doc["agent"] == "2001:0db8:0000:0000:0000:0000:0000:0001"
        assert doc["ifindex"] == 5
        assert doc["iftype"] == 6
        assert doc["ifspeed"] == 1_000_000_000
        assert doc["in_octets"] == 111
        assert doc["in_packets"] == 222
        assert doc["in_errors"] == 2
        assert doc["out_octets"] == 444
        assert doc["out_packets"] == 555

    def test_version_non_cinq_ecartee(self) -> None:
        assert analyser_sflow(_u32(4) + b"\x00" * 30) == []

    def test_tronque_ecarte(self) -> None:
        assert analyser_sflow(_datagramme_flux()[:-3]) == []
        assert analyser_sflow(b"") == []

    def test_type_echantillon_inconnu_ecarte(self) -> None:
        trame = _datagramme_flux()
        trame = trame.replace(_u32(1) + _u32(len(trame) - 36), _u32(9) + _u32(4) + b"\x00" * 4, 1)
        # datagramme recomposé : version, agent, en-têtes, 1 échantillon de type inconnu
        documents = analyser_sflow(trame)
        assert documents == []

    def test_format_enregistrement_inconnu_ecarte(self) -> None:
        enregistrement = _u32(2) + _u32(4) + b"\x00" * 4  # format 2, inconnu
        corps = (
            _u32(7)
            + _u32(3)
            + _u32(100)
            + _u32(1000)
            + _u32(0)
            + _u32(1)
            + _u32(2)
            + _u32(1)
            + _u32(2)
            + _u32(len(enregistrement))
            + enregistrement
        )
        trame = (
            _u32(5)
            + _u32(1)
            + bytes([1, 2, 3, 4])
            + _u32(0)
            + _u32(1)
            + _u32(1)
            + _u32(1)
            + _u32(1)
            + _u32(len(corps))
            + corps
        )
        documents = analyser_sflow(trame)
        assert len(documents) == 1
        assert "src_ip" not in documents[0]

    def test_agent_mac(self) -> None:
        trame = _u32(5) + _u32(3) + b"\xaa\xbb\xcc\xdd\xee\xff" + _u32(0) + _u32(0) + _u32(0) + _u32(0)
        assert analyser_sflow(trame) == []

    def test_adresse_inconnue(self) -> None:
        trame = _u32(5) + _u32(9) + _u32(0) + _u32(0) + _u32(0) + _u32(0) + _u32(0)
        assert analyser_sflow(trame) == []


class TestEntreeSflow:
    def test_datagramme_reel_udp(self) -> None:
        asyncio.run(self._scenario())

    async def _scenario(self) -> None:
        interpreter = PipelineInterpreter("output { temoins {} }")
        sortie = SortieTemoins({})
        service = Service(interpreter, sorties=[sortie])
        entree = EntreeSflow("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        boucle = asyncio.get_running_loop()
        transport, _ = await boucle.create_datagram_endpoint(asyncio.DatagramProtocol, remote_addr=("127.0.0.1", port))
        transport.sendto(_datagramme_flux())
        transport.sendto(_datagramme_compteurs())
        transport.close()
        await _attendre(lambda: len(sortie.options.get("recus", [])) == 2)
        await service.arreter()
        types = sorted(d["type"] for d in sortie.options["recus"])
        assert types == ["compteur", "flux"]
