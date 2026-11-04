# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie syslog (issue #30), faux transport.

Options du plugin `syslog` de Logstash (défauts compris), messages
RFC 5424/3164 (PRI, en-tête, gabarits), cadrage TCP RFC 6587, échec
transitoire avec retentes, DLQ des transitoires épuisés.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.syslog import SyslogOutput, message_syslog, priorite


class FauxTransport:
    """Échoue `echecs` fois puis réussit ; garde les trames reçues."""

    def __init__(self, echecs: int = 0) -> None:
        self.echecs_restants = echecs
        self.trames: list[bytes] = []
        self.fermetures = 0

    async def ecrire(self, trames: list[bytes]) -> None:
        if self.echecs_restants > 0:
            self.echecs_restants -= 1
            raise ConnectionRefusedError("récepteur fermé")
        self.trames.extend(trames)

    async def fermer(self) -> None:
        self.fermetures += 1

    async def ouvrir_udp(self) -> Any:
        class _Datagramme:
            def close(self) -> None:
                pass

        return _Datagramme()


def sortie(transport: FauxTransport, **options: Any) -> SyslogOutput:
    """Sortie branchée sur le faux transport (options minimales fournies)."""
    return SyslogOutput({"host": "rsyslog", **options}, transport=transport)


def envoyer(s: SyslogOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Envoie les documents puis ferme la sortie dans la même boucle."""

    async def scenario() -> list[SendOutcome]:
        try:
            return [await s.envoyer(doc) for doc in documents]
        finally:
            await s.close()

    return asyncio.run(scenario())


def test_inscrite_au_registre() -> None:
    assert REGISTRY["syslog"] is SyslogOutput


def test_capacites_insertions_seules() -> None:
    capacites = SyslogOutput.capacites
    assert capacites.actions == {"index"}
    assert not capacites.accepte_id
    assert not capacites.accuse_par_evenement


def test_host_requis() -> None:
    with pytest.raises(ValueError, match="host"):
        SyslogOutput({}, transport=FauxTransport())


def test_option_inconnue_refusee() -> None:
    with pytest.raises(ValueError, match="inconnue"):
        sortie(FauxTransport(), serveur="rsyslog")


@pytest.mark.parametrize(
    ("facility", "severity", "attendu"),
    [
        ("user-level", "notice", 13),
        ("kernel", "emerg", 0),
        ("local7", "debug", 191),
        ("mail", "err", 19),
        ("auth", "warning", 36),
    ],
)
def test_priorite(facility: str, severity: str, attendu: int) -> None:
    assert priorite(facility, severity) == attendu


def test_priorite_inconnue_refusee() -> None:
    with pytest.raises(ValueError, match="facilité"):
        priorite("inexistant", "notice")
    with pytest.raises(ValueError, match="sévérité"):
        priorite("user-level", "fatal")


def test_message_rfc5424() -> None:
    doc = {"message": "panique", "host": "web-1"}
    message = message_syslog(
        doc,
        rfc="rfc5424",
        facility="user-level",
        severity="err",
        appname="app",
        procid="123",
        msgid="m1",
        sourcehost="web-1",
        message='{"message": "panique"}',
        now=datetime(2026, 11, 4, 19, 0, 0, tzinfo=timezone.utc),
    )
    assert message == (b'<11>1 2026-11-04T19:00:00+00:00 web-1 app 123 m1 - {"message": "panique"}\n')


def test_message_rfc3164() -> None:
    message = message_syslog(
        {"message": "m"},
        rfc="rfc3164",
        facility="daemon",
        severity="info",
        appname="app",
        procid="-",
        msgid="-",
        sourcehost="web-1",
        message="m",
        now=datetime(2026, 11, 4, 19, 0, 0, tzinfo=timezone.utc),
    )
    # <30> = daemon (3) x 8 + info (6)
    assert message.startswith(b"<30>Nov  4 19:00:00 web-1 app: m\n")


def test_message_rfc5424_nilhost() -> None:
    message = message_syslog(
        {},
        rfc="rfc5424",
        facility="user",
        severity="notice",
        appname="-",
        procid="-",
        msgid="-",
        sourcehost="",
        message="m",
        now=datetime(2026, 11, 4, 19, 0, 0, tzinfo=timezone.utc),
    )
    assert message.startswith(b"<13>1 2026-11-04T19:00:00+00:00 - - - - - m\n")


def test_cadrage_octet_counting_par_defaut() -> None:
    transport = FauxTransport()
    s = sortie(transport, protocol="tcp")
    envoyer(s, [{"message": "m"}])
    trame = transport.trames[0]
    taille, _, reste = trame.partition(b" ")
    assert int(taille) == len(reste)
    assert trame.endswith(b"\n")


def test_cadrage_lf() -> None:
    transport = FauxTransport()
    s = sortie(transport, protocol="tcp", framing="lf")
    envoyer(s, [{"message": "m"}])
    trame = transport.trames[0]
    assert not trame.startswith(b"19 ")
    assert trame.endswith(b"\n")


def test_udp_sans_cadrage() -> None:
    transport = FauxTransport()
    s = sortie(transport, protocol="udp")
    envoyer(s, [{"message": "m"}])
    trame = transport.trames[0]
    assert not trame.split(b" ")[0].isdigit() or trame.startswith(b"<")
    assert trame.startswith(b"<")


def test_valeur_json_par_defaut() -> None:
    transport = FauxTransport()
    doc = {"message": "m", "n": 1}
    s = sortie(transport, protocol="tcp", framing="lf", rfc="rfc5424")
    envoyer(s, [doc])
    trame = transport.trames[0]
    partie = trame.split(b" ", 7)[-1]
    assert json.loads(partie.decode("utf-8")) == doc


def test_codec_rubydebug() -> None:
    transport = FauxTransport()
    s = sortie(transport, protocol="tcp", framing="lf", codec="rubydebug")
    envoyer(s, [{"message": "m"}])
    assert b'"message" => "m"' in transport.trames[0]


def test_gabarits_rendus_par_document() -> None:
    transport = FauxTransport()
    doc = {"host": "web-1", "app": "facturation"}
    s = sortie(
        transport,
        protocol="tcp",
        framing="lf",
        rfc="rfc5424",
        appname="%{app}",
        sourcehost="%{host}",
    )
    envoyer(s, [doc])
    assert b" web-1 facturation - - -" in transport.trames[0]


def test_batch_size_tamponne_puis_envoie() -> None:
    transport = FauxTransport()
    s = sortie(transport, batch_size=2)

    async def scenario() -> list[SendOutcome]:
        premiers = [await s.envoyer({"n": i}) for i in range(2)]
        await s.flush()
        return premiers

    assert asyncio.run(scenario()) == [SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
    assert len(transport.trames) == 2


def test_echec_transitoire_retabli() -> None:
    transport = FauxTransport(echecs=1)
    s = sortie(transport, retry_max=1, retry_backoff=0)

    async def scenario() -> SendOutcome:
        return await s.envoyer({"message": "m"})

    assert asyncio.run(scenario()) == SendOutcome.INDEXED


def test_echec_transitoire_epuise_rendu_a_l_appelant() -> None:
    transport = FauxTransport(echecs=5)
    s = sortie(transport, retry_max=0)

    async def scenario() -> SendOutcome:
        return await s.envoyer({"message": "m"})

    assert asyncio.run(scenario()) == SendOutcome.RETRYABLE


def test_echec_transitoire_epuise_en_dlq(tmp_path: Path) -> None:
    transport = FauxTransport(echecs=5)
    s = SyslogOutput(
        {"host": "rsyslog", "dlq_transitoires": True},
        transport=transport,
        dlq=LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl")),
    )

    async def scenario() -> SendOutcome:
        return await s.envoyer({"message": "m"})

    assert asyncio.run(scenario()) == SendOutcome.DEAD_LETTERED
    entree = json.loads((tmp_path / "dlq.jsonl").read_text(encoding="utf-8"))
    assert entree["@dlq_index"] == "rsyslog:514"
    assert entree["@dlq_output"] == "syslog"


def test_send_et_flush_contrat_output() -> None:
    transport = FauxTransport()

    async def scenario() -> None:
        s = sortie(transport, protocol="tcp", framing="lf")
        await s.open()
        await s.send([{"message": "a"}, {"message": "b"}])
        await s.flush()
        await s.close()
        await s.close()  # idempotent

    asyncio.run(scenario())
    assert len(transport.trames) == 2


def test_health_udp() -> None:
    transport = FauxTransport()

    async def scenario() -> dict[str, Any]:
        s = sortie(transport, protocol="udp")
        return await s.health()

    assert asyncio.run(scenario()) == {"status": "joignable", "protocole": "udp"}


def test_health_tcp_se_connecte() -> None:
    async def scenario() -> dict[str, Any]:
        serveur = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        port = serveur.sockets[0].getsockname()[1]
        s = SyslogOutput({"host": "127.0.0.1", "port": port, "protocol": "tcp"})
        try:
            return await s.health()
        finally:
            await serveur.start_serving()
            serveur.close()
            await serveur.wait_closed()

    assert asyncio.run(scenario()) == {"status": "joignable", "protocole": "tcp"}


def test_lot_vide_ne_produit_rien() -> None:
    transport = FauxTransport()

    async def scenario() -> None:
        s = sortie(transport)
        await s.flush()

    asyncio.run(scenario())
    assert transport.trames == []


def test_message_rfc5424_horodatage_du_document() -> None:
    epoch = datetime(2026, 11, 4, 19, 0, 0, tzinfo=timezone.utc).timestamp()
    message = message_syslog(
        {"@timestamp": epoch},
        rfc="rfc5424",
        facility="user",
        severity="notice",
        appname="-",
        procid="-",
        msgid="-",
        sourcehost="-",
        message="m",
    )
    assert b"2026-11-04T" in message  # 1762302000 = 04/11/2026
    message_dt = message_syslog(
        {"@timestamp": datetime(2026, 11, 4, 19, 0, 0, tzinfo=timezone.utc)},
        rfc="rfc5424",
        facility="user",
        severity="notice",
        appname="-",
        procid="-",
        msgid="-",
        sourcehost="-",
        message="m",
    )
    assert message_dt.startswith(b"<13>1 2026-11-04T19:00:00+00:00")


def test_dlq_path_construit_la_dlq(tmp_path: Path) -> None:
    s = SyslogOutput({"host": "rsyslog", "dlq_path": str(tmp_path / "dlq.jsonl")})
    assert s._dlq is not None


def test_contexte_tls_avec_cacert(tmp_path: Path) -> None:
    import datetime
    import ssl

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    cle = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    sujet = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, "banc")])
    maintenant = datetime.datetime.now(datetime.timezone.utc)
    certificat = (
        x509.CertificateBuilder()
        .subject_name(sujet)
        .issuer_name(sujet)
        .public_key(cle.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(maintenant)
        .not_valid_after(maintenant)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(cle, hashes.SHA256())
    )
    pem = tmp_path / "ca.pem"
    pem.write_bytes(certificat.public_bytes(serialization.Encoding.PEM))
    s = sortie(FauxTransport(), protocol="ssl-tcp", cacert=str(pem))
    contexte = s._contexte_tls()
    assert isinstance(contexte, ssl.SSLContext)


def test_transport_reel_tcp_ecrit_les_trames() -> None:
    from pystashtemplate.outputs.syslog import _TransportReseau

    async def scenario() -> tuple[list[bytes], int]:
        recues: list[bytes] = []

        async def servir(lecteur: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            recues.append(await lecteur.read(256))
            writer.close()

        serveur = await asyncio.start_server(servir, "127.0.0.1", 0)
        port = serveur.sockets[0].getsockname()[1]
        transport = _TransportReseau("127.0.0.1", port, "tcp", "lf", None)
        try:
            await transport.ecrire([b"<13>1 message\n"])
            await transport.fermer()
        finally:
            serveur.close()
            await serveur.wait_closed()
        return recues, port

    recues, _ = asyncio.run(scenario())
    assert recues and recues[0].startswith(b"<13>1")


def test_transport_reel_udp_envoie_les_datagrammes() -> None:
    from pystashtemplate.outputs.syslog import _TransportReseau

    async def scenario() -> list[bytes]:
        recues: list[bytes] = []
        recus = asyncio.Event()

        class Recepteur(asyncio.DatagramProtocol):
            def datagram_received(self, data: bytes, addr: Any) -> None:
                recues.append(data)
                if len(recues) >= 2:
                    recus.set()

        boucle = asyncio.get_running_loop()
        transport_ecoute, _ = await boucle.create_datagram_endpoint(Recepteur, local_addr=("127.0.0.1", 0))
        port = transport_ecoute.get_extra_info("sockname")[1]
        transport = _TransportReseau("127.0.0.1", port, "udp", "lf", None)
        try:
            await transport.ecrire([b"<13>message\n", b"<13>autre\n"])
            await asyncio.wait_for(recus.wait(), timeout=5)
        finally:
            transport_ecoute.close()
        return recues

    recues = asyncio.run(scenario())
    assert b"<13>message\n" in recues


def test_echec_sans_transport_injecte_rendu_a_l_appelant() -> None:
    # port TCP fermé sur localhost : le transport réel échoue, la
    # reconnexion ne fait pas illusion, le lot revient à l'appelant
    s = SyslogOutput({"host": "127.0.0.1", "protocol": "tcp", "retry_max": 0})

    async def scenario() -> SendOutcome:
        return await s.envoyer({"message": "m"})

    assert asyncio.run(scenario()) == SendOutcome.RETRYABLE


def test_contexte_tls_sans_cacert() -> None:
    s = sortie(FauxTransport(), protocol="ssl-tcp")
    assert s._contexte_tls() is not None


def test_transport_tcp_reutilise_la_connexion() -> None:
    from pystashtemplate.outputs.syslog import _TransportReseau

    async def scenario() -> list[bytes]:
        recues: list[bytes] = []

        async def servir(lecteur: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            while True:
                donnees = await lecteur.read(256)
                if not donnees:
                    break
                recues.append(donnees)
            writer.close()

        serveur = await asyncio.start_server(servir, "127.0.0.1", 0)
        port = serveur.sockets[0].getsockname()[1]
        transport = _TransportReseau("127.0.0.1", port, "tcp", "lf", None)
        try:
            await transport.ecrire([b"premier\n"])
            await transport.ecrire([b"second\n"])  # même connexion
            await transport.fermer()
        finally:
            serveur.close()
            await serveur.wait_closed()
        return recues

    recues = asyncio.run(scenario())
    flux = b"".join(recues)
    assert b"premier\n" in flux and b"second\n" in flux


def test_transport_fermer_sans_connexion() -> None:
    from pystashtemplate.outputs.syslog import _TransportReseau

    async def scenario() -> None:
        transport = _TransportReseau("127.0.0.1", 1, "tcp", "lf", None)
        await transport.fermer()

    asyncio.run(scenario())
