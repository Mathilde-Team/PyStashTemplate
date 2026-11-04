# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests du service autonome asynchrone (issue #39).

Chaque entrée (syslog TCP/UDP, HTTP) écoute réellement sur `localhost`
avec un port choisi par l'OS — les tests pilotent le service comme un
expéditeur réseau le ferait, file bornée et arrêt propre compris.
"""

from __future__ import annotations

import asyncio
import functools
import json
from typing import Any, Callable

from pystashtemplate.interpreter import OutputAction, PipelineInterpreter
from pystashtemplate.outputs.base import Output
from pystashtemplate.service import EntreeHttp, EntreeSyslogTcp, EntreeSyslogUdp, Service, analyser_syslog


def asynchrone(test: Callable[..., Any]) -> Callable[..., None]:
    """Enveloppe un test asynchrone : `asyncio.run` au appel, convention du dépôt."""

    @functools.wraps(test)
    def enveloppe(instance: Any) -> None:
        asyncio.run(test(instance))

    return enveloppe


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


def _sortie() -> SortieTemoins:
    return SortieTemoins({})


def _service(pipeline: str = "output { temoins {} }", file_max: int = 100) -> Service:
    interpreter = PipelineInterpreter(pipeline)
    sortie = _sortie()
    service = Service(interpreter, sorties=[sortie], file_max=file_max)
    return service


async def _attendre(condition: Any, delai: float = 2.0) -> None:
    """Attend que la condition (callable) devienne vraie."""
    boucle = asyncio.get_event_loop()
    fin = boucle.time() + delai
    while boucle.time() < fin:
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"condition non atteinte en {delai} secondes")


class TestAnalyserSyslog:
    def test_rfc5424(self) -> None:
        trame = b"<34>1 2026-11-04T12:00:00Z monhote monapp 1234 ID47 - Bonjour le monde"
        doc = analyser_syslog(trame)
        assert doc is not None
        assert doc["message"] == "Bonjour le monde"
        assert doc["host"] == "monhote"
        assert doc["appname"] == "monapp"
        assert doc["procid"] == "1234"
        assert doc["msgid"] == "ID47"
        assert doc["facility"] == "auth"
        assert doc["severity"] == "crit"

    def test_rfc5424_saut_de_ligne(self) -> None:
        doc = analyser_syslog(b"<134>1 2026-11-04T12:00:00Z h a 1 2 - ping\n")
        assert doc is not None
        assert doc["message"] == "ping"
        assert doc["facility"] == "local0"

    def test_rfc3164(self) -> None:
        doc = analyser_syslog(b"<13>Nov  4 12:00:00 monhote monprog[42]: \xc3\xa7a tourne")
        assert doc is not None
        assert doc["message"] == "ça tourne"
        assert doc["host"] == "monhote"
        assert doc["appname"] == "monprog[42]"
        assert doc["facility"] == "user"
        assert doc["severity"] == "notice"

    def test_pas_du_syslog(self) -> None:
        assert analyser_syslog(b"n'importe quoi") is None
        assert analyser_syslog(b"") is None


class TestSyslogTcp:
    @asynchrone
    async def test_octet_counting(self) -> None:
        service = _service()
        entree = EntreeSyslogTcp("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        lecteur, ecrivain = await asyncio.open_connection("127.0.0.1", port)
        trame = b"<134>1 2026-11-04T12:00:00Z h a 1 2 - un"
        ecrivain.write(str(len(trame)).encode() + b" " + trame)
        trame2 = b"<134>1 2026-11-04T12:00:00Z h a 1 2 - deux"
        ecrivain.write(str(len(trame2)).encode() + b" " + trame2)
        await ecrivain.drain()
        sortie = service._sorties[0]
        await _attendre(lambda: len(sortie.options.get("recus", [])) == 2)
        ecrivain.close()
        await ecrivain.wait_closed()
        await service.arreter()
        assert [d["message"] for d in sortie.options.get("recus", [])] == ["un", "deux"]

    @asynchrone
    async def test_lf(self) -> None:
        service = _service()
        entree = EntreeSyslogTcp("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        lecteur, ecrivain = await asyncio.open_connection("127.0.0.1", port)
        ecrivain.write(b"<13>Nov  4 12:00:00 h p[1]: ligne A\n<13>Nov  4 12:00:00 h p[1]: ligne B\n")
        await ecrivain.drain()
        sortie = service._sorties[0]
        await _attendre(lambda: len(sortie.options.get("recus", [])) == 2)
        ecrivain.close()
        await ecrivain.wait_closed()
        await service.arreter()
        assert [d["message"] for d in sortie.options.get("recus", [])] == ["ligne A", "ligne B"]

    @asynchrone
    async def test_trame_illisible_ecartee(self) -> None:
        service = _service()
        entree = EntreeSyslogTcp("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        lecteur, ecrivain = await asyncio.open_connection("127.0.0.1", port)
        ecrivain.write(b"ce n'est pas du syslog\n")
        await ecrivain.drain()
        await asyncio.sleep(0.1)
        sortie = service._sorties[0]
        assert sortie.options.get("recus", []) == []
        ecrivain.close()
        await ecrivain.wait_closed()
        await service.arreter()


class TestSyslogUdp:
    @asynchrone
    async def test_datagramme(self) -> None:
        service = _service()
        entree = EntreeSyslogUdp("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        boucle = asyncio.get_running_loop()
        transport, _ = await boucle.create_datagram_endpoint(asyncio.DatagramProtocol, remote_addr=("127.0.0.1", port))
        transport.sendto(b"<134>1 2026-11-04T12:00:00Z h a 1 2 - via udp")
        transport.close()
        sortie = service._sorties[0]
        await _attendre(lambda: len(sortie.options.get("recus", [])) == 1)
        await service.arreter()
        assert sortie.options.get("recus", [])[0]["message"] == "via udp"


class TestHttp:
    @asynchrone
    async def test_post_json(self) -> None:
        service = _service()
        entree = EntreeHttp("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        lecteur, ecrivain = await asyncio.open_connection("127.0.0.1", port)
        corps = json.dumps({"message": "salut", "niveau": 3}).encode()
        requete = (
            b"POST /logs HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\n"
            + f"Content-Length: {len(corps)}".encode()
            + b"\r\n\r\n"
            + corps
        )
        ecrivain.write(requete)
        await ecrivain.drain()
        ligne = await asyncio.wait_for(lecteur.readline(), timeout=2)
        assert ligne.startswith(b"HTTP/1.1 200")
        sortie = service._sorties[0]
        await _attendre(lambda: len(sortie.options.get("recus", [])) == 1)
        ecrivain.close()
        await ecrivain.wait_closed()
        await service.arreter()
        assert sortie.options.get("recus", [])[0]["niveau"] == 3

    @asynchrone
    async def test_post_ndjson_et_methode_refusee(self) -> None:
        service = _service()
        entree = EntreeHttp("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        lecteur, ecrivain = await asyncio.open_connection("127.0.0.1", port)
        corps = b'{"a": 1}\n{"a": 2}\n'
        ecrivain.write(
            b"POST /logs HTTP/1.1\r\nHost: t\r\nContent-Length: " + str(len(corps)).encode() + b"\r\n\r\n" + corps
        )
        await ecrivain.drain()
        for _ in range(3):  # réponse 200 complète : status + en-tête + ligne vide
            await asyncio.wait_for(lecteur.readline(), timeout=2)
        # GET : refusé (405), aucune trace côté pipeline.
        ecrivain.write(b"GET /logs HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\n\r\n")
        await ecrivain.drain()
        ligne = await asyncio.wait_for(lecteur.readline(), timeout=2)
        assert ligne.startswith(b"HTTP/1.1 405")
        sortie = service._sorties[0]
        await _attendre(lambda: len(sortie.options.get("recus", [])) == 2)
        ecrivain.close()
        await ecrivain.wait_closed()
        await service.arreter()
        assert [d["a"] for d in sortie.options.get("recus", [])] == [1, 2]


class TestBords:
    @asynchrone
    async def test_trame_octet_counting_tronquee(self) -> None:
        service = _service()
        entree = EntreeSyslogTcp("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        lecteur, ecrivain = await asyncio.open_connection("127.0.0.1", port)
        ecrivain.write(b"100 <134>1 court")  # longueur annoncée jamais atteinte
        await ecrivain.drain()
        ecrivain.close()
        await ecrivain.wait_closed()
        await service.arreter()
        assert service._sorties[0].options.get("recus", []) == []

    @asynchrone
    async def test_corps_http_tronque(self) -> None:
        service = _service()
        entree = EntreeHttp("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        lecteur, ecrivain = await asyncio.open_connection("127.0.0.1", port)
        ecrivain.write(b"POST /logs HTTP/1.1\r\nHost: t\r\nContent-Length: 100\r\n\r\ncourt")
        await ecrivain.drain()
        ecrivain.close()
        await ecrivain.wait_closed()
        await service.arreter()
        assert service._sorties[0].options.get("recus", []) == []

    @asynchrone
    async def test_arreter_sans_demarrer(self) -> None:
        for entree in (
            EntreeSyslogTcp("127.0.0.1", 0),
            EntreeSyslogUdp("127.0.0.1", 0),
            EntreeHttp("127.0.0.1", 0),
        ):
            await entree.arreter()  # idempotent : rien ouvert, rien à fermer

    @asynchrone
    async def test_sorties_compilees_du_pipeline(self) -> None:
        interpreter = PipelineInterpreter("output { null {} }")
        service = Service(interpreter)  # sorties=None : compiler_sorties
        entree = EntreeSyslogUdp("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        boucle = asyncio.get_running_loop()
        transport, _ = await boucle.create_datagram_endpoint(asyncio.DatagramProtocol, remote_addr=("127.0.0.1", port))
        transport.sendto(b"<134>1 2026-11-04T12:00:00Z h a 1 2 - via null")
        transport.close()
        await service.arreter()  # la sortie null avale le document sans erreur

    @asynchrone
    async def test_trame_json_illisible_ecartee(self) -> None:
        service = _service()
        await service.demarrer([EntreeSyslogUdp("127.0.0.1", 0)])
        await service.deposer(b"{pas du tout du json")
        assert service._file.qsize() == 0
        await service.arreter()

    @asynchrone
    async def test_sortie_absente_du_pipeline(self) -> None:
        class InterpreterFantome:
            async def apply_events(self, doc: Any) -> list[tuple[dict, list]]:
                return [(doc, [OutputAction(plugin="inconnu", options={})])]

        service = Service(InterpreterFantome(), sorties=[_sortie()])
        await service.demarrer([EntreeSyslogUdp("127.0.0.1", 0)])
        await service.deposer(b"<134>1 2026-11-04T12:00:00Z h a 1 2 - fantome")
        await _attendre(lambda: service._file.empty())
        await service.arreter()
        assert service._sorties[0].options.get("recus", []) == []


class TestContrePression:
    @asynchrone
    async def test_file_bornee_bloque_le_depot(self) -> None:
        gate = asyncio.Event()
        traite = []

        class InterpreterLent:
            async def apply_events(self, doc: Any) -> list[tuple[dict, list]]:
                if doc.get("clef") == 1:
                    await gate.wait()
                traite.append(doc.get("clef"))
                return [(doc, [])]

        service = Service(InterpreterLent(), sorties=[_sortie()], file_max=1, travailleurs=1)
        entree = EntreeSyslogTcp("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        lecteur, ecrivain = await asyncio.open_connection("127.0.0.1", port)

        def trame(clef: int) -> bytes:
            corps = json.dumps({"clef": clef}).encode()
            return str(len(corps)).encode() + b" " + corps

        ecrivain.write(trame(1))
        await ecrivain.drain()
        await asyncio.sleep(0.05)  # le travailleur prend le doc 1 et bloque
        # file vide (doc 1 en traitement) : le doc 2 passe, puis le dépôt
        # du doc 3 doit BLOQUER (file pleine) jusqu'au déblocage.
        ecrivain.write(trame(2))
        await ecrivain.drain()
        await asyncio.sleep(0.05)
        depot3 = asyncio.create_task(service.deposer(json.dumps({"clef": 3}).encode()))
        await asyncio.sleep(0.05)
        assert not depot3.done()  # contre-pression : file pleine
        gate.set()
        await asyncio.wait_for(depot3, timeout=2)
        assert traite == [1, 2, 3]
        ecrivain.close()
        await ecrivain.wait_closed()
        await service.arreter()

    @asynchrone
    async def test_arret_propre_vide_la_file(self) -> None:
        gate = asyncio.Event()

        class InterpreterBloque:
            async def apply_events(self, doc: Any) -> list[tuple[dict, list]]:
                await gate.wait()
                return [(doc, [OutputAction(plugin="temoins", options={})])]

        service = Service(InterpreterBloque(), sorties=[_sortie()], file_max=10, travailleurs=1)
        entree = EntreeSyslogTcp("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        lecteur, ecrivain = await asyncio.open_connection("127.0.0.1", port)
        for i in range(5):
            ecrivain.write(f"<134>1 2026-11-04T12:00:00Z h a 1 2 - message {i}".encode() + b"\n")
        await ecrivain.drain()
        await _attendre(lambda: service._file.qsize() == 4)  # le 5e est au travailleur
        gate.set()  # l'arrêt propre doit les vider jusqu'au dernier
        await service.arreter()
        sortie = service._sorties[0]
        assert len(sortie.options.get("recus", [])) == 5
        assert sortie.options["fermee"] is True


class TestPipelineReel:
    @asynchrone
    async def test_filter_et_split(self) -> None:
        interpreter = PipelineInterpreter(
            """
            filter {
              mutate { add_tag => ["vu"] }
              clone { clones => ["copie"] }
            }
            output { temoins {} }
            """
        )
        sortie = _sortie()
        service = Service(interpreter, sorties=[sortie])
        entree = EntreeSyslogUdp("127.0.0.1", 0)
        await service.demarrer([entree])
        _, port = entree.liaison()
        boucle = asyncio.get_running_loop()
        transport, _ = await boucle.create_datagram_endpoint(asyncio.DatagramProtocol, remote_addr=("127.0.0.1", port))
        transport.sendto(b"<134>1 2026-11-04T12:00:00Z h a 1 2 - original")
        transport.close()
        await _attendre(lambda: len(sortie.options.get("recus", [])) == 2)
        await service.arreter()
        assert all("vu" in d["tags"] for d in sortie.options.get("recus", []))
