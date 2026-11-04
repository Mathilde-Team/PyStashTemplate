# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie syslog : émission RFC 5424 / RFC 3164 vers UDP, TCP ou TCP+TLS.

Issue #30. Comme le plugin `syslog` de Logstash :

- `host` (requis) et `port` (514 par défaut) désignent le récepteur
  (rsyslog, syslog-ng, entrée syslog de VictoriaLogs…) ;
- `protocol` choisit le transport : `udp` (défaut), `tcp` ou `ssl-tcp`
  (TLS via `cacert`/`ssl` des options communes) ;
- `rfc` choisit le format : `rfc3164` (BSD, défaut) ou `rfc5424` ;
- `facility`/`severity` (mots-clés RFC 5424), `appname`, `procid`,
  `msgid`, `sourcehost` (gabarits sprintf) remplissent l'en-tête ;
- `codec` encode le MESSAGE (document JSON par défaut).

Le cadrage TCP suit le RFC 6587 : `framing` = `octet-counting`
(défaut, `LEN SP MESSAGE`) ou `lf` (message terminé par saut de
ligne). Syslog n'accuse rien — ni UDP (feu-et-oubli) ni TCP (écriture
du flux) : l'issue est globale au lot, tout échec de transport est
transitoire (retentes, puis `RETRYABLE` ou DLQ avec
`dlq_transitoires` #7), et le document voyage entier dans le MESSAGE
(l'en-tête syslog porte le routage, pas les champs).

Le banc de conformité (Vector, source `syslog`) vérifie TCP
octet-counting, TCP LF, UDP, et les deux RFC.
"""

from __future__ import annotations

import asyncio
import ssl
import time
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, ClassVar

from loguru import logger

from pystashtemplate.dlq import REASON_TRANSIENT_EXHAUSTED
from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.base import Document, Output
from pystashtemplate.outputs.codecs import encoder_json_lines, encoder_rubydebug
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.templates import compile_template, render
from pystashtemplate.tracing import atraced, traced

__all__ = ["SyslogOutput", "priorite", "message_syslog"]

# Mots-clés RFC 5424 §6.2.1 (facilité) — alias 3164 acceptés.
FACILITES: dict[str, int] = {
    "kernel": 0,
    "user": 1,
    "user-level": 1,
    "mail": 2,
    "daemon": 3,
    "auth": 4,
    "security": 4,
    "authorization": 5,
    "syslog": 5,
    "lpr": 6,
    "line-printer": 6,
    "news": 7,
    "network-news": 7,
    "uucp": 8,
    "cron": 9,
    "clock": 9,
    "authpriv": 10,
    "ftp": 11,
    "ntp": 12,
    "audit": 13,
    "alert": 14,
    "local0": 16,
    "local1": 17,
    "local2": 18,
    "local3": 19,
    "local4": 20,
    "local5": 21,
    "local6": 22,
    "local7": 23,
}

# Mots-clés RFC 5424 §6.2.7 (sévérité) — alias courants acceptés.
SEVERITES: dict[str, int] = {
    "emerg": 0,
    "emergency": 0,
    "panic": 0,
    "alert": 1,
    "crit": 2,
    "critical": 2,
    "err": 3,
    "error": 3,
    "warning": 4,
    "warn": 4,
    "notice": 5,
    "info": 6,
    "informational": 6,
    "debug": 7,
}


def priorite(facility: str, severity: str) -> int:
    """PRI = facilité × 8 + sévérité (RFC 5424 §6.2.1/6.2.7).

    Raises:
        ValueError: mot-clé de facilité ou de sévérité inconnu.
    """
    logger.debug("priorite | entrée")
    if facility not in FACILITES:
        logger.debug("priorite | facilité inconnue : {}", facility)
        raise ValueError(_("sortie syslog : facilité « {} » inconnue (mots-clés RFC 5424)").format(facility))
    if severity not in SEVERITES:
        logger.debug("priorite | sévérité inconnue : {}", severity)
        raise ValueError(_("sortie syslog : sévérité « {} » inconnue (mots-clés RFC 5424)").format(severity))
    logger.debug("priorite | sortie")
    return FACILITES[facility] * 8 + SEVERITES[severity]


@traced
def message_syslog(
    doc: dict[str, Any],
    *,
    rfc: str,
    facility: str,
    severity: str,
    appname: str,
    procid: str,
    msgid: str,
    sourcehost: str,
    message: str,
    now: datetime | None = None,
) -> bytes:
    """Compose le message syslog d'un document (sans cadrage TCP).

    Args:
        doc: Document (horodatage `@timestamp` lu s'il existe).
        rfc: `rfc3164` (BSD) ou `rfc5424`.
        facility/severity: Mots-clés RFC 5424 (déjà rendus).
        appname/procid/msgid/sourcehost: Champs d'en-tête (déjà rendus).
        message: MESSAGE (document encodé par le `codec`).
        now: Horloge de test.

    Returns:
        Le message, terminé par un saut de ligne (cadrage `lf` du RFC
        6587 ; le cadrage octet-counting l'ajoute à `ecrire`).
    """
    horodatage = now or datetime.now(timezone.utc)
    brut = doc.get("@timestamp")
    if isinstance(brut, (int, float)):
        horodatage = datetime.fromtimestamp(brut, tz=timezone.utc)
    elif isinstance(brut, datetime):
        horodatage = brut
    pri = priorite(facility, severity)
    hote = (sourcehost or "-").replace(" ", "")
    if rfc == "rfc5424":
        en_tete = f"<{pri}>1 {horodatage.isoformat()} {hote or '-'} {appname or '-'} {procid or '-'} {msgid or '-'} -"
    else:
        horodatage_bsd = horodatage.astimezone().strftime("%b %e %H:%M:%S")
        en_tete = f"<{pri}>{horodatage_bsd} {hote or '-'} {(appname or '-')[:32]}:"
    return f"{en_tete} {message}\n".encode("utf-8")


@enregistrer
class SyslogOutput(Output):
    """Émetteur syslog asynchrone, branché sur le socle des sorties (#4, #8)."""

    plugin = "syslog"
    capacites = Capacites(actions=frozenset({"index"}), accepte_id=False, accuse_par_evenement=False)

    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = {
        "host": OptionSpec("host", (str,), description="Récepteur syslog (hôte)", requis=True),
        "port": OptionSpec("port", (int,), 514, description="Port du récepteur (514 par défaut)"),
        "protocol": OptionSpec(
            "protocol",
            (str,),
            "udp",
            valeurs=("udp", "tcp", "ssl-tcp"),
            description="Transport : udp (défaut), tcp ou ssl-tcp (TLS)",
        ),
        "rfc": OptionSpec(
            "rfc",
            (str,),
            "rfc3164",
            valeurs=("rfc3164", "rfc5424"),
            description="Format du message (rfc3164 par défaut)",
        ),
        "framing": OptionSpec(
            "framing",
            (str,),
            "octet-counting",
            valeurs=("octet-counting", "lf"),
            description="Cadrage TCP RFC 6587 (octet-counting par défaut)",
        ),
        "facility": OptionSpec(
            "facility",
            (str,),
            "user-level",
            description="Facilité RFC 5424 (user-level par défaut)",
        ),
        "severity": OptionSpec(
            "severity",
            (str,),
            "notice",
            description="Sévérité RFC 5424 (notice par défaut)",
        ),
        "appname": OptionSpec(
            "appname",
            (str,),
            "LOGSTASH",
            description="APP-NAME (LOGSTASH par défaut, comme le plugin)",
        ),
        "procid": OptionSpec("procid", (str,), "-", description="PROCID (- par défaut)"),
        "msgid": OptionSpec("msgid", (str,), "-", description="MSGID (- par défaut)"),
        "sourcehost": OptionSpec(
            "sourcehost",
            (str,),
            "%{host}",
            description="HOSTNAME du message (gabarit %{host} par défaut)",
        ),
        "codec": OptionSpec(
            "codec",
            (str,),
            "json",
            valeurs=("json", "json_lines", "rubydebug"),
            description="Encodage du MESSAGE (json par défaut)",
        ),
        "dlq_transitoires": OptionSpec(
            "dlq_transitoires",
            (bool,),
            False,
            description="Les échecs transitoires épuisés partent aussi en DLQ (#7)",
        ),
    }

    def __init__(
        self,
        options: dict[str, Any],
        *,
        transport: Any = None,
        dlq: Any = None,
    ) -> None:
        """Prépare le formatage et la politique de reprise.

        Args:
            options: Options du bloc `output{}` : destination (`host`,
                `port`, `protocol`), format (`rfc`, `framing`,
                `facility`, `severity`, `appname`, `procid`, `msgid`,
                `sourcehost`, `codec`), reprise (`retry_max`,
                `retry_backoff`, `batch_size`) et DLQ (`dlq_path`,
                `dlq_transitoires`).
            transport: Transport de remplacement (couture de test) ;
                doit exposer `async ecrire(trames: list[bytes])` et
                `async fermer()`. `None` : UDP/TCP réel selon
                `protocol`.
            dlq: DLQ toute construite (tests), prime sur `dlq_path`.

        Raises:
            ValueError: `host` absent, facilité/sévérité inconnue,
                option inconnue ou valeur invalide.
        """
        logger.debug("SyslogOutput.__init__ | entrée")
        super().__init__(options)
        self._hote = str(options["host"])
        self._port = int(options.get("port", 514))
        self._protocole = str(options.get("protocol", "udp"))
        self._rfc = str(options.get("rfc", "rfc3164"))
        self._framing = str(options.get("framing", "octet-counting"))
        self._facility = str(options.get("facility", "user-level"))
        self._severity = str(options.get("severity", "notice"))
        priorite(self._facility, self._severity)  # valide à la compilation
        self._appname = compile_template(str(options.get("appname", "LOGSTASH")))
        self._procid = compile_template(str(options.get("procid", "-")))
        self._msgid = compile_template(str(options.get("msgid", "-")))
        self._sourcehost = compile_template(str(options.get("sourcehost", "%{host}")))
        self._codec = str(options.get("codec", "json"))
        self._batch_size = max(1, int(options.get("batch_size", 1)))
        self._retry_max = max(0, int(options.get("retry_max", 0)))
        self._retry_backoff = float(options.get("retry_backoff", 1.0))
        self._dlq_transitoires = bool(options.get("dlq_transitoires", False))
        self._dlq = dlq
        if self._dlq is None and options.get("dlq_path"):
            from pystashtemplate.dlq import LocalDeadLetterQueue

            self._dlq = LocalDeadLetterQueue(str(options["dlq_path"]))
        self._transport = transport
        self._transport_injecte = transport is not None
        self._pending: list[dict[str, Any]] = []
        logger.debug("SyslogOutput.__init__ | sortie")

    @traced
    def _contexte_tls(self) -> ssl.SSLContext | None:
        """Contexte TLS pour `ssl-tcp` (options communes `ssl`/`cacert`)."""
        if self._protocole != "ssl-tcp":
            return None
        contexte = ssl.create_default_context()
        if self.options.get("cacert"):
            contexte.load_verify_locations(str(self.options["cacert"]))
        return contexte

    @traced
    def _transport_reel(self) -> Any:
        """Transport réel selon `protocol`, construit au premier envoi.

        TCP/ssl-tcp garde une connexion ouverte (reconnectée après un
        échec d'écriture) ; UDP ouvre un socket datagramme une fois.
        """
        return _TransportReseau(
            hote=self._hote,
            port=self._port,
            protocole=self._protocole,
            framing=self._framing,
            contexte_tls=self._contexte_tls(),
        )

    def _transport_actif(self) -> Any:
        """Transport partagé : couture de test ou transport réel."""
        logger.debug("_transport_actif | entrée")
        if self._transport is None:
            self._transport = self._transport_reel()
        logger.debug("_transport_actif | sortie")
        return self._transport

    @atraced
    async def open(self) -> None:
        """Ouvre le transport (idempotent, sans effet pour un test)."""
        logger.debug("SyslogOutput.open | entrée")
        self._transport_actif()
        logger.debug("SyslogOutput.open | sortie")

    @atraced
    async def close(self) -> None:
        """Ferme le transport (idempotent)."""
        logger.debug("SyslogOutput.close | entrée")
        if self._transport is not None:
            await self._transport.fermer()
            self._transport = None
        logger.debug("SyslogOutput.close | sortie")

    @atraced
    async def health(self) -> dict[str, Any]:
        """Prouve la joignabilité : TCP se connecte, UDP ouvre son socket.

        UDP n'accuse rien (feu-et-oubli) : la socket ouverte prouve
        seulement la route locale — c'est le mieux vérifiable.
        """
        if self._protocole == "udp":
            transport = await self._transport_actif().ouvrir_udp()
            transport.close()
            return {"status": "joignable", "protocole": "udp"}
        lecteur, writer = await asyncio.open_connection(self._hote, self._port, ssl=self._contexte_tls())
        writer.close()
        return {"status": "joignable", "protocole": self._protocole}

    @traced
    def _encoder(self, doc: dict[str, Any]) -> str:
        """MESSAGE : document encodé selon le `codec`."""
        return encoder_json_lines(doc) if self._codec in ("json", "json_lines") else encoder_rubydebug(doc)

    @atraced
    async def envoyer(self, doc: dict[str, Any]) -> SendOutcome:
        """Tamponne un document et envoie le lot quand il est plein.

        Args:
            doc: Document prêt à envoyer.

        Returns:
            `SendOutcome` de CE document : réel si le lot est parti avec
            lui, `EN_ATTENTE` s'il est seulement tamponné.
        """
        logger.debug("SyslogOutput.envoyer | entrée")
        self._pending.append(doc)
        self.observateur.file(self.nom, len(self._pending))
        if len(self._pending) < self._batch_size:
            logger.debug("SyslogOutput.envoyer | document tamponné ({})", len(self._pending))
            logger.debug("SyslogOutput.envoyer | sortie")
            return SendOutcome.EN_ATTENTE
        outcomes = await self._flush()
        logger.debug("SyslogOutput.envoyer | sortie")
        return outcomes[-1]

    @atraced
    async def send(self, batch: Sequence[Document]) -> None:
        """Envoie un lot de documents (contrat `Output` #4)."""
        logger.debug("SyslogOutput.send | entrée ({} document(s))", len(batch))
        for doc in batch:
            outcome = await self.envoyer(dict(doc))
            logger.debug("SyslogOutput.send | {} → {}", outcome.value, doc)
        logger.debug("SyslogOutput.send | sortie")

    @atraced
    async def flush(self) -> None:
        """Pousse les documents en attente (contrat `Output` : arrêt propre)."""
        logger.debug("SyslogOutput.flush | entrée")
        await self._flush()
        logger.debug("SyslogOutput.flush | sortie")

    @atraced
    async def _flush(self) -> list[SendOutcome]:
        """Envoie le tampon et renvoie l'issue de chaque document.

        Un lot vide ne produit rien. L'issue est globale : syslog
        n'accuse rien, tout échec de transport est transitoire
        (retentes exponentielles, puis `RETRYABLE` ou DLQ).
        """
        if not self._pending:
            return []
        pendings = self._pending
        self._pending = []
        self.observateur.file(self.nom, 0)
        trames = [self._trame(doc) for doc in pendings]
        logger.debug("_flush | {} document(s)", len(pendings))
        essai = 0
        erreur = _("sortie syslog : aucun envoi")
        while True:
            try:
                debut = time.perf_counter()
                await self._transport_actif().ecrire(trames)
                self.observateur.latence(self.nom, time.perf_counter() - debut)
                self.observateur.evenements(self.nom, "indexed", len(pendings))
                return [SendOutcome.INDEXED] * len(pendings)
            except Exception as exc:  # noqa: BLE001 — transport, transitoire
                erreur = f"{type(exc).__name__} : {exc}"
                logger.warning(
                    _("envoi syslog échoué erreur={error} documents={nombre}"),
                    error=erreur,
                    nombre=len(pendings),
                )
                if not self._transport_injecte:
                    self._transport = None  # reconnexion à la prochaine retente
                if essai >= self._retry_max:
                    break
                essai += 1
                self.observateur.reessai(self.nom)
                await asyncio.sleep(self._retry_backoff * 2 ** (essai - 1))
        logger.warning(
            _("envoi syslog : réessais épuisés documents={nombre} erreur={error}"),
            nombre=len(pendings),
            error=erreur,
        )
        return await self._transitoires_epuises(pendings, erreur)

    @traced
    def _trame(self, doc: dict[str, Any]) -> bytes:
        """Message syslog du document, cadré selon `framing` (TCP)."""
        message = message_syslog(
            doc,
            rfc=self._rfc,
            facility=self._facility,
            severity=self._severity,
            appname=render(self._appname, doc),
            procid=render(self._procid, doc),
            msgid=render(self._msgid, doc),
            sourcehost=render(self._sourcehost, doc),
            message=self._encoder(doc),
        )
        if self._protocole == "udp" or self._framing == "lf":
            return message
        return f"{len(message)} ".encode("ascii") + message

    @atraced
    async def _transitoires_epuises(self, pendings: list[dict[str, Any]], erreur: str) -> list[SendOutcome]:
        """Lot en échec transitoire, réessais épuisés : RETRYABLE, ou DLQ si `dlq_transitoires`."""
        if self._dlq is None or not self._dlq_transitoires:
            return [SendOutcome.RETRYABLE] * len(pendings)
        logger.warning(
            _("lot syslog en échec transitoire mis en DLQ documents={nombre}"),
            nombre=len(pendings),
        )
        for doc in pendings:
            await self._dlq.write(
                doc,
                index=f"{self._hote}:{self._port}",
                error=erreur,
                output=self.nom,
                reason=REASON_TRANSIENT_EXHAUSTED,
            )
        return [SendOutcome.DEAD_LETTERED] * len(pendings)


class _TransportReseau:
    """Transport UDP/TCP réel, avec reconnexion TCP après un échec."""

    def __init__(self, hote: str, port: int, protocole: str, framing: str, contexte_tls: ssl.SSLContext | None) -> None:
        logger.debug("_TransportReseau.__init__ | entrée")
        self._hote = hote
        self._port = port
        self._protocole = protocole
        self._framing = framing
        self._contexte_tls = contexte_tls
        self._writer: Any = None
        self._udp: Any = None
        logger.debug("_TransportReseau.__init__ | sortie")

    @traced
    async def ouvrir_udp(self) -> Any:
        """Ouvre un socket UDP vers la destination."""
        transport, _ = await asyncio.get_event_loop().create_datagram_endpoint(
            asyncio.DatagramProtocol,
            remote_addr=(self._hote, self._port),
        )
        return transport

    @traced
    async def ecrire(self, trames: list[bytes]) -> None:
        """Écrit les trames : datagrammes UDP, ou flux TCP reconnecté."""
        if self._protocole == "udp":
            transport = await self.ouvrir_udp()
            try:
                for trame in trames:
                    transport.sendto(trame)
            finally:
                transport.close()
            return
        if self._writer is None:
            _, self._writer = await asyncio.open_connection(
                self._hote,
                self._port,
                ssl=self._contexte_tls,
            )
        for trame in trames:
            self._writer.write(trame)
        await self._writer.drain()

    @traced
    async def fermer(self) -> None:
        """Ferme la connexion TCP ouverte."""
        if self._writer is not None:
            self._writer.close()
            self._writer = None
