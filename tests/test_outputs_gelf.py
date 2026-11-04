# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie Graylog GELF (issue #26).

Message GELF 1.1 (fonction pure), transport HTTP sur un faux serveur
`httpx.MockTransport`, transports TCP (clair et TLS) et UDP (morceaux,
gzip) sur de vrais serveurs asyncio locaux.
"""

from __future__ import annotations

import asyncio
import datetime
import gzip
import ipaddress
import json
import socket
import ssl
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.gelf import GelfOutput, decouper_udp, message_gelf
from pystashtemplate.templates import compile_template

DOC = {
    "@timestamp": "2026-11-04T10:00:00Z",
    "@version": "1",
    "message": "bonjour",
    "host": "srv1",
    "level": "warning",
    "app": "banc",
}


def _gelf(doc: dict[str, Any], **options: Any) -> dict[str, Any]:
    """Message GELF d'un document, options par défaut de la sortie."""
    return GelfOutput({"hosts": "http://graylog:12201", **options})._message(doc)


# --- Message GELF -----------------------------------------------------------


class TestMessage:
    def test_inscrite_au_registre(self) -> None:
        assert REGISTRY["gelf"] is GelfOutput

    def test_champs_standard(self) -> None:
        message = _gelf(DOC)
        assert message == {
            "version": "1.1",
            "host": "srv1",
            "short_message": "bonjour",
            "full_message": "bonjour",
            "timestamp": 1793786400.0,
            "level": 4,
            "_app": "banc",
        }

    def test_short_message_prioritaire_et_non_repete(self) -> None:
        message = _gelf({**DOC, "short_message": "court"})
        assert message["short_message"] == "court"
        assert "_short_message" not in message

    def test_short_message_champ_configurable(self) -> None:
        message = _gelf({**DOC, "resume": "r"}, short_message="resume")
        assert message["short_message"] == "r"
        assert "_resume" not in message

    def test_sans_message_tiret(self) -> None:
        message = _gelf({"host": "h"})
        assert message["short_message"] == "-"
        assert "full_message" not in message
        assert "timestamp" not in message
        assert message["level"] == 6  # repli « info »

    def test_full_message_desactive(self) -> None:
        assert "full_message" not in _gelf(DOC, full_message="")

    def test_host_objet_ecs(self) -> None:
        assert _gelf({**DOC, "host": {"name": "ecs1", "ip": "10.0.0.1"}})["host"] == "ecs1"

    def test_host_objet_sans_nom_repli_machine(self) -> None:
        assert _gelf({**DOC, "host": {"ip": "10.0.0.1"}})["host"] == socket.gethostname()

    def test_host_absent_repli_machine(self) -> None:
        assert _gelf({"message": "m"})["host"] == socket.gethostname()

    def test_sender_gabarit(self) -> None:
        assert _gelf(DOC, sender="%{app}-%{host}")["host"] == "banc-srv1"

    @pytest.mark.parametrize(
        ("valeur", "attendu"),
        [("err", 3), ("DEBUG", 7), ("3", 3), ("12", 7), ("-2", 0), ("inconnu", 6), ("", 6)],
    )
    def test_niveaux(self, valeur: str, attendu: int) -> None:
        assert _gelf({**DOC, "level": valeur})["level"] == attendu

    def test_niveau_gabarit_unique_sans_repli(self) -> None:
        assert "level" not in _gelf({"message": "m"}, level="%{severite}")
        assert _gelf({"message": "m", "severite": "crit"}, level="%{severite}")["level"] == 2

    def test_champs_additionnels(self) -> None:
        doc = {
            "message": "m",
            "id": 7,
            "_prive": "p",
            "a b/c": "x",
            "é": "accent",
            "_": "vide",
            "ok": True,
            "ko": False,
            "liste": [1, "deux", True],
            "objet": {"x": {"y": 1}, "z": None},
            "rien": None,
            "flottant": 1.5,
            "@metadata": {"cache": 1},
            "tags": ["t1", "t2"],
        }
        message = _gelf(doc)
        attendus = {
            "__id": 7,
            "_prive": "p",
            "_a_b_c": "x",
            "_champ": "vide",
            "_ok": "true",
            "_ko": "false",
            "_liste": "1, deux, true",
            "_objet_x_y": 1,
            "_flottant": 1.5,
            "_tags": "t1, t2",
        }
        for cle, valeur in attendus.items():
            assert message[cle] == valeur, cle
        assert "_rien" not in message
        assert "_objet_z" not in message
        assert not any(cle.startswith("_metadata") for cle in message)
        # « é » devient « _ » puis le « _ » initial est retiré : nom de repli.
        assert message["_champ"] in ("vide", "accent")

    def test_ship_metadata_et_tags_desactives(self) -> None:
        message = _gelf({**DOC, "tags": ["t"]}, ship_metadata=False, ship_tags=False)
        assert not any(cle.startswith("_") for cle in message)

    def test_tags_vides_non_envoyes(self) -> None:
        assert "_tags" not in _gelf({**DOC, "tags": []})

    def test_custom_fields(self) -> None:
        message = _gelf(DOC, custom_fields={"env": "prod", "_equipe": {"nom": "ops"}})
        assert message["_env"] == "prod"
        assert message["_equipe_nom"] == "ops"

    @pytest.mark.parametrize(
        ("valeur", "attendu"),
        [
            (datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC), 1767225600.0),
            (1767225600, 1767225600.0),
            ("2026-01-01T00:00:00.250+00:00", 1767225600.25),
        ],
    )
    def test_horodatages(self, valeur: Any, attendu: float) -> None:
        assert _gelf({"message": "m", "@timestamp": valeur})["timestamp"] == attendu

    @pytest.mark.parametrize("valeur", ["pas une date", "", True, None])
    def test_horodatage_illisible_omis(self, valeur: Any) -> None:
        assert "timestamp" not in _gelf({"message": "m", "@timestamp": valeur})

    def test_fonction_pure_options_par_defaut(self) -> None:
        message = message_gelf({"message": "m", "x": 1}, sender=compile_template("h"))
        assert message == {"version": "1.1", "host": "h", "short_message": "m", "_x": 1}


# --- Découpage UDP -----------------------------------------------------------


class TestDecoupage:
    def test_petit_message_tel_quel(self) -> None:
        assert decouper_udp(b"abc", 100) == [b"abc"]

    def test_morceaux(self) -> None:
        morceaux = decouper_udp(b"x" * 25, 22, identifiant=b"12345678")
        assert len(morceaux) == 3
        assert [m[:2] for m in morceaux] == [b"\x1e\x0f"] * 3
        assert [m[2:10] for m in morceaux] == [b"12345678"] * 3
        assert [(m[10], m[11]) for m in morceaux] == [(0, 3), (1, 3), (2, 3)]
        assert b"".join(m[12:] for m in morceaux) == b"x" * 25

    def test_identifiant_aleatoire(self) -> None:
        morceaux = decouper_udp(b"x" * 30, 20)
        assert len({m[2:10] for m in morceaux}) == 1

    def test_trop_de_morceaux(self) -> None:
        with pytest.raises(ValueError, match="trop gros"):
            decouper_udp(b"x" * 129, 13)


# --- Options -------------------------------------------------------------------


class TestOptions:
    @pytest.mark.parametrize("option", ["index", "pipeline", "routing", "doc_as_upsert"])
    def test_options_bulk_refusees(self, option: str) -> None:
        with pytest.raises(ValueError):
            GelfOutput({"hosts": "http://g:12201", option: "x"})

    def test_action_autre_que_index_refusee(self) -> None:
        with pytest.raises(ValueError):
            GelfOutput({"hosts": "http://g:12201", "action": "delete"})

    def test_transport_inconnu_refuse(self) -> None:
        with pytest.raises(ValueError):
            GelfOutput({"hosts": "http://g:12201", "transport": "amqp"})

    def test_hosts_sans_hote_refuse(self) -> None:
        with pytest.raises(ValueError, match="hôte"):
            GelfOutput({"hosts": "http://:12201"})

    def test_hosts_sans_schema_ni_port(self) -> None:
        sortie = GelfOutput({"hosts": ["graylog"], "transport": "udp"})
        assert (sortie._hote, sortie._port, sortie._tls) == ("graylog", 12201, False)

    def test_tls(self) -> None:
        sortie = GelfOutput({"hosts": "tls://graylog:12202", "transport": "tcp"})
        assert (sortie._hote, sortie._port, sortie._tls) == ("graylog", 12202, True)


# --- Transport HTTP ---------------------------------------------------------------


class FauxGraylog:
    """Répond `statut` (202 sans corps, comme Graylog) et garde les requêtes."""

    def __init__(self, statut: int = 202) -> None:
        self.statut = statut
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.statut)


def _http(serveur: FauxGraylog, *, dlq: LocalDeadLetterQueue | None = None, **options: Any) -> GelfOutput:
    return GelfOutput(
        {"hosts": "http://graylog:12201", **options}, transport=httpx.MockTransport(serveur.handler), dlq=dlq
    )


def _envoyer(sortie: GelfOutput, documents: list[dict[str, Any]], *, vider: bool = False) -> list[SendOutcome]:
    async def scenario() -> list[SendOutcome]:
        try:
            await sortie.open()
            resultats = [await sortie.envoyer(dict(doc)) for doc in documents]
            if vider:
                await sortie.flush()
            return resultats
        finally:
            await sortie.close()

    return asyncio.run(scenario())


class TestHttp:
    def test_un_message_par_requete(self) -> None:
        serveur = FauxGraylog()
        assert _envoyer(_http(serveur), [DOC]) == [SendOutcome.INDEXED]
        requete = serveur.requests[0]
        assert requete.method == "POST"
        assert requete.url.path == "/gelf"
        assert requete.headers["Content-Type"] == "application/json"
        assert json.loads(requete.content)["short_message"] == "bonjour"

    def test_lot_ndjson(self) -> None:
        serveur = FauxGraylog()
        documents = [{**DOC, "message": f"m{i}"} for i in range(3)]
        _envoyer(_http(serveur, batch_size=3), documents)
        corps = serveur.requests[0].content.decode("utf-8")
        # Chaque message terminé par un saut de ligne, le dernier compris.
        assert corps.endswith("}\n")
        assert [json.loads(ligne)["short_message"] for ligne in corps.splitlines()] == ["m0", "m1", "m2"]

    def test_message_seul_termine_par_saut_de_ligne(self) -> None:
        serveur = FauxGraylog()
        _envoyer(_http(serveur), [DOC, DOC])
        assert all(r.content.endswith(b"}\n") and r.content.count(b"\n") == 1 for r in serveur.requests)

    def test_400_en_dlq(self, tmp_path: Path) -> None:
        chemin = tmp_path / "dlq.jsonl"
        sortie = _http(FauxGraylog(400), dlq=LocalDeadLetterQueue(str(chemin)))
        assert _envoyer(sortie, [DOC]) == [SendOutcome.DEAD_LETTERED]
        assert json.loads(chemin.read_text(encoding="utf-8"))["document"]["message"] == "bonjour"

    def test_sante_ok(self) -> None:
        serveur = FauxGraylog(405)
        sortie = _http(serveur)

        async def scenario() -> dict[str, Any]:
            try:
                return await sortie.health()
            finally:
                await sortie.close()

        assert asyncio.run(scenario()) == {"transport": "http", "status": "ok"}
        assert serveur.requests[0].method == "HEAD"

    def test_sante_5xx_leve(self) -> None:
        sortie = _http(FauxGraylog(503))

        async def scenario() -> dict[str, Any]:
            try:
                return await sortie.health()
            finally:
                await sortie.close()

        with pytest.raises(httpx.HTTPStatusError):
            asyncio.run(scenario())


# --- Transport TCP ----------------------------------------------------------------


class ServeurTcp:
    """Serveur GELF TCP local : découpe le flux sur l'octet nul."""

    def __init__(self, contexte: ssl.SSLContext | None = None, couper_apres: int | None = None) -> None:
        self.contexte = contexte
        self.couper_apres = couper_apres
        self.messages: list[dict[str, Any]] = []
        self.connexions = 0
        self._serveur: asyncio.Server | None = None

    async def _client(self, lecteur: asyncio.StreamReader, ecrivain: asyncio.StreamWriter) -> None:
        self.connexions += 1
        premiere = self.connexions == 1
        try:
            while True:
                trame = await lecteur.readuntil(b"\0")
                self.messages.append(json.loads(trame[:-1]))
                if premiere and self.couper_apres is not None and len(self.messages) >= self.couper_apres:
                    break
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        ecrivain.close()

    async def demarrer(self) -> int:
        self._serveur = await asyncio.start_server(self._client, "127.0.0.1", 0, ssl=self.contexte)
        return self._serveur.sockets[0].getsockname()[1]

    async def arreter(self) -> None:
        assert self._serveur is not None
        self._serveur.close()
        await self._serveur.wait_closed()


def _port_libre() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestTcp:
    def test_messages_termines_par_octet_nul(self) -> None:
        async def scenario() -> tuple[list[SendOutcome], ServeurTcp]:
            serveur = ServeurTcp()
            port = await serveur.demarrer()
            sortie = GelfOutput({"hosts": f"tcp://127.0.0.1:{port}", "transport": "tcp", "batch_size": 2})
            try:
                resultats = [await sortie.envoyer({**DOC, "message": f"m{i}"}) for i in range(2)]
                assert await sortie.health() == {"transport": "tcp", "status": "ok"}
                for _ in range(50):
                    if len(serveur.messages) == 2:
                        break
                    await asyncio.sleep(0.01)
                return resultats, serveur
            finally:
                await sortie.close()
                await sortie.close()
                await serveur.arreter()

        resultats, serveur = asyncio.run(scenario())
        assert resultats == [SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
        assert [m["short_message"] for m in serveur.messages] == ["m0", "m1"]
        assert serveur.connexions == 1

    def test_connexion_refusee_retentes_puis_rendu(self) -> None:
        sortie = GelfOutput(
            {"hosts": f"127.0.0.1:{_port_libre()}", "transport": "tcp", "retry_max": 1, "retry_backoff": 0}
        )
        assert _envoyer(sortie, [DOC]) == [SendOutcome.RETRYABLE]

    def test_connexion_refusee_dlq_transitoires(self, tmp_path: Path) -> None:
        chemin = tmp_path / "dlq.jsonl"
        sortie = GelfOutput(
            {"hosts": f"127.0.0.1:{_port_libre()}", "transport": "tcp", "dlq_transitoires": True},
            dlq=LocalDeadLetterQueue(str(chemin)),
        )
        assert _envoyer(sortie, [DOC]) == [SendOutcome.DEAD_LETTERED]
        assert chemin.exists()

    def test_reconnexion_apres_coupure(self) -> None:
        async def scenario() -> tuple[list[SendOutcome], ServeurTcp]:
            serveur = ServeurTcp(couper_apres=1)
            port = await serveur.demarrer()
            sortie = GelfOutput(
                {"hosts": f"127.0.0.1:{port}", "transport": "tcp", "retry_max": 3, "retry_backoff": 0.05}
            )
            try:
                resultats = [await sortie.envoyer({**DOC, "message": "m0"})]
                for _ in range(50):
                    if serveur.connexions and len(serveur.messages) == 1:
                        break
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.05)  # le serveur a fermé : l'écriture suivante échoue
                for i in (1, 2, 3):
                    resultats.append(await sortie.envoyer({**DOC, "message": f"m{i}"}))
                for _ in range(100):
                    if serveur.connexions >= 2 and serveur.messages[-1]["short_message"] == "m3":
                        break
                    await asyncio.sleep(0.01)
                return resultats, serveur
            finally:
                await sortie.close()
                await serveur.arreter()

        resultats, serveur = asyncio.run(scenario())
        assert resultats == [SendOutcome.INDEXED] * 4
        assert serveur.connexions >= 2
        assert serveur.messages[-1]["short_message"] == "m3"

    def test_fermeture_en_erreur_journalisee(self) -> None:
        class Ecrivain:
            def close(self) -> None:
                pass

            async def wait_closed(self) -> None:
                raise ConnectionResetError("déjà coupée")

        sortie = GelfOutput({"hosts": "127.0.0.1:1", "transport": "tcp"})
        sortie._ecrivain = Ecrivain()  # type: ignore[assignment]
        asyncio.run(sortie._fermer_tcp())
        assert sortie._ecrivain is None


# --- TLS ------------------------------------------------------------------------


def _pem(cle: rsa.RSAPrivateKey) -> bytes:
    return cle.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


def _auto_signe(tmp_path: Path, nom: str) -> tuple[Path, Path]:
    """Certificat auto-signé valable pour 127.0.0.1 (sert aussi d'autorité)."""
    cle = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    sujet = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, nom)])
    maintenant = datetime.datetime.now(datetime.UTC)
    certificat = (
        x509.CertificateBuilder()
        .subject_name(sujet)
        .issuer_name(sujet)
        .public_key(cle.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(maintenant - datetime.timedelta(days=1))
        .not_valid_after(maintenant + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        .sign(cle, hashes.SHA256())
    )
    crt, key = tmp_path / f"{nom}.crt", tmp_path / f"{nom}.key"
    crt.write_bytes(certificat.public_bytes(serialization.Encoding.PEM))
    key.write_bytes(_pem(cle))
    return crt, key


class TestTls:
    def _scenario(self, tmp_path: Path, mtls: bool, **options: Any) -> list[dict[str, Any]]:
        crt, key = _auto_signe(tmp_path, "serveur")
        contexte = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        contexte.load_cert_chain(str(crt), str(key))
        if mtls:
            client_crt, client_key = _auto_signe(tmp_path, "client")
            contexte.verify_mode = ssl.CERT_REQUIRED
            contexte.load_verify_locations(cafile=str(client_crt))
            options |= {"ssl_certificate": str(client_crt), "ssl_key": str(client_key)}
        options.setdefault("cacert", str(crt))

        async def scenario() -> list[dict[str, Any]]:
            serveur = ServeurTcp(contexte)
            port = await serveur.demarrer()
            sortie = GelfOutput({"hosts": f"tls://127.0.0.1:{port}", "transport": "tcp", **options})
            try:
                assert await sortie.envoyer(dict(DOC)) == SendOutcome.INDEXED
                for _ in range(100):
                    if serveur.messages:
                        break
                    await asyncio.sleep(0.01)
                return serveur.messages
            finally:
                await sortie.close()
                await serveur.arreter()

        return asyncio.run(scenario())

    def test_tls_verifie(self, tmp_path: Path) -> None:
        assert self._scenario(tmp_path, False)[0]["short_message"] == "bonjour"

    def test_mtls(self, tmp_path: Path) -> None:
        assert self._scenario(tmp_path, True)[0]["short_message"] == "bonjour"

    def test_sans_verification(self, tmp_path: Path) -> None:
        assert self._scenario(tmp_path, False, ssl=False, cacert="")[0]["host"] == "srv1"

    def test_contexte_par_defaut(self) -> None:
        sortie = GelfOutput({"hosts": "tls://graylog:12201", "transport": "tcp"})
        assert sortie._contexte_tls().verify_mode == ssl.CERT_REQUIRED


# --- Transport UDP -----------------------------------------------------------------


class ServeurUdp(asyncio.DatagramProtocol):
    """Serveur GELF UDP local : garde les datagrammes reçus."""

    def __init__(self) -> None:
        self.datagrammes: list[bytes] = []

    def datagram_received(self, data: bytes, addr: Any) -> None:
        self.datagrammes.append(data)


def _udp(documents: list[dict[str, Any]], **options: Any) -> tuple[list[SendOutcome], list[bytes]]:
    async def scenario() -> tuple[list[SendOutcome], list[bytes]]:
        boucle = asyncio.get_running_loop()
        transport, serveur = await boucle.create_datagram_endpoint(ServeurUdp, local_addr=("127.0.0.1", 0))
        port = transport.get_extra_info("sockname")[1]
        sortie = GelfOutput({"hosts": f"udp://127.0.0.1:{port}", "transport": "udp", **options})
        try:
            await sortie.open()
            resultats = [await sortie.envoyer(dict(doc)) for doc in documents]
            assert await sortie.health() == {"transport": "udp", "status": "unchecked"}
            for _ in range(100):
                if serveur.datagrammes:
                    break
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.02)
            return resultats, serveur.datagrammes
        finally:
            await sortie.close()
            await sortie.close()
            transport.close()

    return asyncio.run(scenario())


class TestUdp:
    def test_datagramme_simple(self) -> None:
        resultats, datagrammes = _udp([DOC])
        assert resultats == [SendOutcome.INDEXED]
        assert json.loads(datagrammes[0])["short_message"] == "bonjour"

    def test_gzip(self) -> None:
        _resultats, datagrammes = _udp([DOC], compression=True)
        assert datagrammes[0][:2] == b"\x1f\x8b"
        assert json.loads(gzip.decompress(datagrammes[0]))["host"] == "srv1"

    def test_morceaux_reassembles(self) -> None:
        _resultats, datagrammes = _udp([{**DOC, "message": "x" * 300}], chunksize=100)
        assert len(datagrammes) > 1
        assert all(d[:2] == b"\x1e\x0f" for d in datagrammes)
        ordonnes = sorted(datagrammes, key=lambda d: d[10])
        assert json.loads(b"".join(d[12:] for d in ordonnes))["short_message"] == "x" * 300

    def test_trop_gros_permanent(self, tmp_path: Path) -> None:
        chemin = tmp_path / "dlq.jsonl"
        sortie = GelfOutput(
            {"hosts": "127.0.0.1:9", "transport": "udp", "chunksize": 13}, dlq=LocalDeadLetterQueue(str(chemin))
        )
        assert _envoyer(sortie, [DOC]) == [SendOutcome.DEAD_LETTERED]
        assert "trop gros" in json.loads(chemin.read_text(encoding="utf-8"))["@dlq_error"]

    def test_erreur_socket_retentee(self) -> None:
        async def scenario() -> tuple[SendOutcome, list[bytes]]:
            boucle = asyncio.get_running_loop()
            transport, serveur = await boucle.create_datagram_endpoint(ServeurUdp, local_addr=("127.0.0.1", 0))
            port = transport.get_extra_info("sockname")[1]
            sortie = GelfOutput({"hosts": f"127.0.0.1:{port}", "transport": "udp", "retry_max": 1, "retry_backoff": 0})
            try:
                await sortie._socket_udp()
                assert sortie._protocole_udp is not None
                sortie._protocole_udp.error_received(ConnectionRefusedError("port injoignable"))
                issue = await sortie.envoyer(dict(DOC))
                for _ in range(100):
                    if len(serveur.datagrammes) >= 2:
                        break
                    await asyncio.sleep(0.01)
                return issue, serveur.datagrammes
            finally:
                await sortie.close()
                transport.close()

        # 1er essai : erreur signalée → retenté ; 2e essai sans erreur → reçu.
        issue, datagrammes = asyncio.run(scenario())
        assert issue == SendOutcome.INDEXED
        assert len(datagrammes) == 2
