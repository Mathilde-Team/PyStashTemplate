# SPDX-License-Identifier: GPL-3.0-or-later
"""Service autonome asynchrone (issue #39) : PyStashTemplate à l'écoute.

Le service écoute un port (ou plusieurs), applique le pipeline
`filter{}`/`output{}` de chaque document reçu et pousse vers les
sorties agnostiques — la boucle asyncio tient debout toute seule,
comme le connecteur omprog de RsysLogstach (#111), sans rsyslog au
milieu :

- **entrées** : syslog TCP (cadrage RFC 6587 octet-counting ou LF),
  syslog UDP, HTTP (`POST` JSON ou NDJSON) — l'analyse RFC 5424/3164
  est partagée par toutes les entrées syslog ;
- **contre-pression** : une file bornée (`file_max`) entre la
  réception et les sorties — pleine, le récepteur TCP cesse de lire
  (la fenêtre TCP se referme, l'expéditeur ralentit) ; en UDP les
  datagrammes sont perdus (documenté, c'est le mieux possible) ;
- **arrêt propre** : `arreter()` ferme les écoutes, vide la file
  (chaque document reçu part vers les sorties), pousse les tampons et
  ferme les sorties.

Chaque événement produit par le pipeline (`apply_events` — `clone`/
`split` compris) part vers chaque sortie désignée par ses actions
d'`output{}`. sFlow attend sa sous-issue dédiée.
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, ClassVar

from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.sflow import analyser_sflow
from pystashtemplate.tracing import atraced, traced

__all__ = [
    "Service",
    "EntreeSyslogTcp",
    "EntreeSyslogUdp",
    "EntreeSflow",
    "EntreeHttp",
    "analyser_syslog",
    "PRIORITE",
]

# Mots-clés RFC 5424 (facilité × 8 + sévérité), mêmes tables que la
# sortie `syslog` (#30) — les codes non nommés (24-63, réserves)
# retombent sur `facility<code>`.
FACILITES: ClassVar[dict[str, int]] = {
    "kernel": 0,
    "user": 1,
    "mail": 2,
    "daemon": 3,
    "auth": 4,
    "syslog": 5,
    "lpr": 6,
    "news": 7,
    "uucp": 8,
    "cron": 9,
    "authpriv": 10,
    "ftp": 11,
    "ntp": 12,
    "audit": 13,
    "alerte": 14,
    "horloge": 15,
    "local0": 16,
    "local1": 17,
    "local2": 18,
    "local3": 19,
    "local4": 20,
    "local5": 21,
    "local6": 22,
    "local7": 23,
}
SEVERITES: ClassVar[dict[str, int]] = {
    "emerg": 0,
    "alert": 1,
    "crit": 2,
    "err": 3,
    "warning": 4,
    "notice": 5,
    "info": 6,
    "debug": 7,
}
PRIORITE: ClassVar[dict[str, str]] = {
    **{f"facility{code}": nom for nom, code in FACILITES.items()},
    **{f"severity{code}": nom for nom, code in SEVERITES.items()},
}


def _mot(code: int, famille: str) -> str:
    """Mot-clé d'une priorité, `facility<code>` si la réserve sans nom."""
    return PRIORITE.get(f"{famille}{code}", f"{famille}{code}")


_RFC5424 = re.compile(
    rb"^<(?P<pri>\d{1,3})>(?P<version>\d) (?P<ts>\S+) (?P<host>\S+) (?P<app>\S+) "
    rb"(?P<procid>\S+) (?P<msgid>\S+) (?P<sd>-|\[.*?\]) (?P<msg>.*)$",
    re.DOTALL,
)
_RFC3164 = re.compile(
    rb"^<(?P<pri>\d{1,3})>(?P<ts>[A-Z][a-z]{2} {1,2}\d{1,2} \d{2}:\d{2}:\d{2}) "
    rb"(?P<host>\S+) (?P<tag>[^:]{1,32}):? ?(?P<msg>.*)$",
    re.DOTALL,
)


@traced
def analyser_syslog(trame: bytes) -> dict[str, Any] | None:
    """Analyse un message syslog RFC 5424 ou 3164 en document événement.

    Args:
        trame: Le message brut (sauts de ligne finaux tolérés).

    Returns:
        Document façon Logstash : `message`, `host`, `facility`,
        `severity`, et `timestamp`/`appname`/`procid`/`msgid` en
        RFC 5424 — ou `None` si la trame n'est pas du syslog (un
        récepteur tolère le bruit, il le journalise et passe).
    """
    trame = trame.strip(b"\r\n")
    m5424 = _RFC5424.match(trame)
    if m5424:
        pri = int(m5424["pri"])
        return {
            "message": m5424["msg"].decode("utf-8", errors="replace"),
            "host": m5424["host"].decode("utf-8", errors="replace"),
            "timestamp": m5424["ts"].decode("utf-8", errors="replace"),
            "appname": m5424["app"].decode("utf-8", errors="replace"),
            "procid": m5424["procid"].decode("utf-8", errors="replace"),
            "msgid": m5424["msgid"].decode("utf-8", errors="replace"),
            "facility": _mot(pri // 8, "facility"),
            "severity": _mot(pri % 8, "severity"),
        }
    m3164 = _RFC3164.match(trame)
    if m3164:
        pri = int(m3164["pri"])
        return {
            "message": m3164["msg"].decode("utf-8", errors="replace"),
            "host": m3164["host"].decode("utf-8", errors="replace"),
            "timestamp": m3164["ts"].decode("utf-8", errors="replace"),
            "appname": m3164["tag"].decode("utf-8", errors="replace"),
            "facility": _mot(pri // 8, "facility"),
            "severity": _mot(pri % 8, "severity"),
        }
    return None


@atraced
async def _lire_trame(lecteur: asyncio.StreamReader) -> bytes | None:
    """Lit UNE trame syslog TCP (RFC 6587) : octet-counting ou LF.

    Les cadrages ne se mélangent pas au milieu d'une trame : on lit
    octet par octet jusqu'au saut de ligne (LF) ou jusqu'à un espace
    précédé de chiffres (longueur annoncée), puis exactement cette
    longueur.

    Returns:
        La trame, ou `None` si la connexion se ferme avant une trame
        complète.
    """
    tampon = b""
    while True:
        try:
            octet = await lecteur.readexactly(1)
        except asyncio.IncompleteReadError:
            logger.debug("_lire_trame | connexion fermée avant la trame")
            return None
        if octet == b"\n":
            return tampon.rstrip(b"\r")
        if octet == b" " and tampon.isdigit():
            try:
                corps = await lecteur.readexactly(int(tampon))
            except asyncio.IncompleteReadError:
                logger.debug("_lire_trame | connexion fermée avant la fin de la trame")
                return None
            return corps
        tampon += octet


@atraced
async def _lire_requete(lecteur: asyncio.StreamReader) -> tuple[list[bytes] | None, bytes]:
    """Lit UNE requête HTTP POST ; rend les documents du corps.

    Returns:
        `(trames, réponse)` — trames vides si le corps est vide, `None`
        si la connexion se ferme avant une requête complète.
    """
    requete = b""
    while b"\r\n\r\n" not in requete:
        morceau = await lecteur.read(4096)
        if not morceau:
            return None, b""
        requete += morceau
    en_tetes, _, corps = requete.partition(b"\r\n\r\n")
    ligne = en_tetes.split(b"\r\n")[0]
    if not ligne.startswith(b"POST"):
        return [], b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n"
    longueur = 0
    for en_tete in en_tetes.split(b"\r\n")[1:]:
        nom, _, valeur = en_tete.partition(b":")
        if nom.strip().lower() == b"content-length":
            longueur = int(valeur)
    while len(corps) < longueur:
        morceau = await lecteur.read(longueur - len(corps))
        if not morceau:
            return None, b""
        corps += morceau
    trames: list[bytes] = []
    texte = corps.decode("utf-8", errors="replace").strip()
    if texte:
        try:
            charge = json.loads(texte)
            trames = [
                json.dumps(element).encode("utf-8") for element in (charge if isinstance(charge, list) else [charge])
            ]
        except json.JSONDecodeError:
            logger.debug("_lire_requete | corps pas du JSON, tenté comme NDJSON")
            trames = [ligne_.encode("utf-8") for ligne_ in texte.splitlines() if ligne_.strip()]
    return trames, b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"


class EntreeSyslogTcp:
    """Entrée syslog TCP : cadrage RFC 6587 (octet-counting ou LF)."""

    type = "syslog-tcp"

    def __init__(self, hote: str, port: int) -> None:
        logger.debug("EntreeSyslogTcp.__init__ | entrée")
        self._hote = hote
        self._port = port
        self._serveur: asyncio.AbstractServer | None = None
        self._connexions: set[asyncio.StreamWriter] = set()
        logger.debug("EntreeSyslogTcp.__init__ | sortie")

    @traced
    def liaison(self) -> tuple[str, int]:
        """Adresse effectivement liée (port 0 → choisi par l'OS)."""
        assert self._serveur is not None
        prise = self._serveur.sockets[0].getsockname()
        return prise[0], prise[1]

    @atraced
    async def demarrer(self, service: "Service") -> None:
        """Ouvre l'écoute : chaque connexion nourrit la file du service."""

        async def servir(lecteur: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            logger.debug("EntreeSyslogTcp.servir | connexion acceptée")
            self._connexions.add(writer)
            try:
                while True:
                    trame = await _lire_trame(lecteur)
                    if trame is None:
                        break
                    await service.deposer(trame)
            finally:
                self._connexions.discard(writer)
                writer.close()
                logger.debug("EntreeSyslogTcp.servir | connexion fermée")

        self._serveur = await asyncio.start_server(servir, self._hote, self._port)
        logger.debug("EntreeSyslogTcp.demarrer | écoute {}:{}", self._hote, self._port)

    @atraced
    async def arreter(self) -> None:
        """Ferme l'écoute et les connexions ouvertes (elles se vident d'abord)."""
        if self._serveur is not None:
            self._serveur.close()
            for writer in list(self._connexions):
                writer.close()
                await writer.wait_closed()
            await self._serveur.wait_closed()
            self._serveur = None


class EntreeSyslogUdp:
    """Entrée syslog UDP : un datagramme, un message."""

    type = "syslog-udp"

    def __init__(self, hote: str, port: int) -> None:
        logger.debug("EntreeSyslogUdp.__init__ | entrée")
        self._hote = hote
        self._port = port
        self._transport: asyncio.DatagramTransport | None = None
        logger.debug("EntreeSyslogUdp.__init__ | sortie")

    @traced
    def liaison(self) -> tuple[str, int]:
        """Adresse effectivement liée."""
        assert self._transport is not None
        return self._transport.get_extra_info("sockname")[:2]

    @atraced
    async def demarrer(self, service: "Service") -> None:
        """Ouvre l'écoute : chaque datagramme nourrit la file du service."""

        class Recepteur(asyncio.DatagramProtocol):
            def datagram_received(self, data: bytes, addr: Any) -> None:
                logger.debug("EntreeSyslogUdp.Recepteur.datagram_received | entrée")
                asyncio.get_running_loop().create_task(service.deposer(data))
                logger.debug("EntreeSyslogUdp.Recepteur.datagram_received | sortie")

            def error_received(self, exc: Exception) -> None:
                logger.debug("EntreeSyslogUdp.Recepteur.error_received | entrée")
                logger.warning(_("entrée syslog udp : datagramme perdu erreur={error}"), error=exc)

        transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
            Recepteur, local_addr=(self._hote, self._port)
        )
        self._transport = transport
        logger.debug("EntreeSyslogUdp.demarrer | écoute {}:{}", self._hote, self._port)

    @atraced
    async def arreter(self) -> None:
        """Ferme l'écoute."""
        if self._transport is not None:
            self._transport.close()
            self._transport = None


class EntreeSflow:
    """Entrée sFlow v5 (RFC 3176) : un datagramme, des échantillons.

    Chaque échantillon (flux ou compteurs) devient un document pour le
    pipeline — le décodage vit dans `pystashtemplate.sflow` (#97).
    """

    type = "sflow"

    def __init__(self, hote: str, port: int) -> None:
        logger.debug("EntreeSflow.__init__ | entrée")
        self._hote = hote
        self._port = port
        self._transport: asyncio.DatagramTransport | None = None
        logger.debug("EntreeSflow.__init__ | sortie")

    @traced
    def liaison(self) -> tuple[str, int]:
        """Adresse effectivement liée."""
        assert self._transport is not None
        return self._transport.get_extra_info("sockname")[:2]

    @atraced
    async def demarrer(self, service: "Service") -> None:
        """Ouvre l'écoute : chaque datagramme nourrit la file du service."""

        entree = self

        class Recepteur(asyncio.DatagramProtocol):
            def datagram_received(self, data: bytes, addr: Any) -> None:
                logger.debug("EntreeSflow.Recepteur.datagram_received | entrée")
                asyncio.get_running_loop().create_task(entree._deposer(service, data))
                logger.debug("EntreeSflow.Recepteur.datagram_received | sortie")

            def error_received(self, exc: Exception) -> None:
                logger.debug("EntreeSflow.Recepteur.error_received | entrée")
                logger.warning(_("entrée sflow : datagramme perdu erreur={error}"), error=exc)
                logger.debug("EntreeSflow.Recepteur.error_received | sortie")

        transport, _ = await asyncio.get_running_loop().create_datagram_endpoint(
            Recepteur, local_addr=(self._hote, self._port)
        )
        self._transport = transport
        logger.debug("EntreeSflow.demarrer | écoute {}:{}", self._hote, self._port)

    @atraced
    async def _deposer(self, service: "Service", donnees: bytes) -> None:
        """Décode un datagramme et met chaque échantillon en file."""
        for document in analyser_sflow(donnees):
            await service.deposer(json.dumps(document).encode("utf-8"))

    @atraced
    async def arreter(self) -> None:
        """Ferme l'écoute."""
        if self._transport is not None:
            self._transport.close()
            self._transport = None


class EntreeHttp:
    """Entrée HTTP : `POST` JSON (objet, tableau) ou NDJSON."""

    type = "http"

    def __init__(self, hote: str, port: int) -> None:
        logger.debug("EntreeHttp.__init__ | entrée")
        self._hote = hote
        self._port = port
        self._serveur: asyncio.AbstractServer | None = None
        self._connexions: set[asyncio.StreamWriter] = set()
        logger.debug("EntreeHttp.__init__ | sortie")

    @traced
    def liaison(self) -> tuple[str, int]:
        """Adresse effectivement liée (port 0 → choisi par l'OS)."""
        assert self._serveur is not None
        prise = self._serveur.sockets[0].getsockname()
        return prise[0], prise[1]

    @atraced
    async def demarrer(self, service: "Service") -> None:
        """Ouvre l'écoute : chaque `POST` nourrit la file du service."""

        async def servir(lecteur: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            logger.debug("EntreeHttp.servir | connexion acceptée")
            self._connexions.add(writer)
            try:
                while True:
                    trames, reponse = await _lire_requete(lecteur)
                    if trames is None:
                        break
                    for trame in trames:
                        await service.deposer(trame)
                    writer.write(reponse)
                    await writer.drain()
            finally:
                self._connexions.discard(writer)
                writer.close()
                logger.debug("EntreeHttp.servir | connexion fermée")

        self._serveur = await asyncio.start_server(servir, self._hote, self._port)
        logger.debug("EntreeHttp.demarrer | écoute {}:{}", self._hote, self._port)

    @atraced
    async def arreter(self) -> None:
        """Ferme l'écoute et les connexions ouvertes."""
        if self._serveur is not None:
            self._serveur.close()
            for writer in list(self._connexions):
                writer.close()
                await writer.wait_closed()
            await self._serveur.wait_closed()
            self._serveur = None


class Service:
    """Boucle asyncio : entrées → file bornée → pipeline → sorties.

    Args:
        interpreter: Pipeline compilé (`filter{}`/`output{}`).
        sorties: Sorties déjà instanciées (couture de test) ;
            `None` : `compiler_sorties(interpreter)`.
        file_max: Taille de la file (contre-pression au-delà).
        travailleurs: Consommateurs du pipeline en parallèle.
    """

    def __init__(
        self,
        interpreter: Any,
        *,
        sorties: list[Any] | None = None,
        file_max: int = 1000,
        travailleurs: int = 2,
    ) -> None:
        logger.debug("Service.__init__ | entrée")
        self._interpreter = interpreter
        if sorties is None:
            from pystashtemplate.outputs import compiler_sorties

            sorties = compiler_sorties(interpreter)
        self._sorties = sorties
        self._sorties_par_plugin = {sortie.plugin: sortie for sortie in self._sorties}
        self._file: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=file_max)
        self._entrees: list[Any] = []
        self._arret = asyncio.Event()
        self._travailleurs: list[asyncio.Task[None]] = []
        self._nb_travailleurs = travailleurs
        logger.debug("Service.__init__ | sortie")

    @atraced
    async def demarrer(self, entrees: list[Any]) -> None:
        """Ouvre les entrées et lance les travailleurs du pipeline."""
        for entree in entrees:
            await entree.demarrer(self)
            self._entrees.append(entree)
        self._travailleurs = [asyncio.create_task(self._travailler()) for _ in range(self._nb_travailleurs)]
        logger.debug("Service.demarrer | {} entrée(s), {} travailleur(s)", len(entrees), self._nb_travailleurs)

    @atraced
    async def arreter(self) -> None:
        """Arrêt propre : ferme les entrées, vide la file, pousse les sorties."""
        for entree in self._entrees:
            await entree.arreter()
        self._entrees = []
        self._arret.set()
        for travailleur in self._travailleurs:
            await travailleur
        self._travailleurs = []
        for sortie in self._sorties:
            await sortie.flush()
            await sortie.close()
        logger.debug("Service.arreter | sorties poussées et fermées")

    @atraced
    async def deposer(self, trame: bytes) -> None:
        """Analyse une trame reçue et la met en file (bloque si pleine).

        Une trame illisible est journalisée puis écartée : un service
        ne doit pas tomber pour du bruit sur le réseau.
        """
        if trame.startswith(b"{"):
            try:
                document = json.loads(trame.decode("utf-8"))
            except json.JSONDecodeError:
                logger.warning(_("service : trame json illisible écartée"))
                return
        else:
            document = analyser_syslog(trame)
            if document is None:
                logger.warning(_("service : trame syslog illisible écartée"))
                return
        await self._file.put(document)  # contre-pression : file pleine → attend

    @atraced
    async def _travailler(self) -> None:
        """Consomme la file : pipeline, puis chaque sortie visée."""
        while True:
            vide = self._file.empty()
            if vide and self._arret.is_set():
                break
            try:
                document = await asyncio.wait_for(self._file.get(), timeout=0.1)
            except asyncio.TimeoutError:
                logger.debug("_travailler | file vide, nouvelle passe")
                continue
            try:
                for evenement, actions in await self._interpreter.apply_events(document):
                    for action in actions:
                        sortie = self._sorties_par_plugin.get(action.plugin)
                        if sortie is None:
                            logger.warning(
                                _("service : sortie « {} » absente du pipeline"),
                                action.plugin,
                            )
                            continue
                        await sortie.send([evenement])
            finally:
                self._file.task_done()
