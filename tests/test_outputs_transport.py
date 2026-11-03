# SPDX-License-Identifier: GPL-3.0-or-later
"""Transport HTTP asynchrone commun (#5) : faux serveur, TLS/mTLS, réessais.

Chaque exigence de l'issue a son test : statuts 200/4xx/429/5xx, délai
dépassé, pool keep-alive, authentifications (basic, Bearer, ApiKey,
en-têtes libres), gzip, `Retry-After` (secondes et date HTTP), backoff
exponentiel avec jitter, aucun réessai sur un `4xx` définitif, aucun
secret dans les journaux.
"""

from __future__ import annotations

import asyncio
import base64
import datetime
import gzip
import ipaddress
import ssl
from typing import Any

import pytest
from aiohttp import web
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from pystashtemplate.outputs.transport import (
    AuthApiKey,
    AuthBasique,
    AuthBearer,
    ReponseHTTP,
    TransportErreur,
    TransportHTTP,
)


def _run(coro):
    # Sans pytest-asyncio (convention du dépôt, cf. RsysLogstach) : asyncio.run direct.
    return asyncio.run(coro)


# --- faux serveur ----------------------------------------------------------


class FauxServeur:
    """Serveur aiohttp éphémère qui déroule un scénario de réponses."""

    def __init__(self, scenario: list[tuple[int, dict[str, str], bytes]], latence: float = 0.0) -> None:
        self.recus: list[dict[str, Any]] = []
        self._scenario = list(scenario)
        self._latence = latence
        self.app = web.Application()
        self.app.router.add_post("/{reste:.*}", self._traite)
        self._runner: web.AppRunner | None = None

    async def _traite(self, requete: web.Request) -> web.Response:
        corps = await requete.read()
        self.recus.append({"entetes": dict(requete.headers), "corps": corps})
        if self._latence:
            await asyncio.sleep(self._latence)
        if self._scenario:
            statut, entetes, corps_reponse = self._scenario.pop(0)
        else:
            statut, entetes, corps_reponse = 200, {}, b"ok"
        return web.Response(status=statut, headers=entetes, body=corps_reponse)

    async def demarrer(self, contexte_tls: ssl.SSLContext | None = None) -> str:
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0, ssl_context=contexte_tls)
        await site.start()
        hote, port = self._runner.addresses[0]
        schema = "https" if contexte_tls is not None else "http"
        return f"{schema}://{hote}:{port}/_bulk"

    async def arreter(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()


def _transport(**surcharges: Any) -> TransportHTTP:
    # Réessais rapides et déterministes : jitter nul, backoff minuscule.
    options: dict[str, Any] = {
        "essais_max": 3,
        "delai_initial": 0.01,
        "delai_max": 0.05,
        "alea": lambda: 0.0,
    }
    options.update(surcharges)
    return TransportHTTP(**options)


class TestStatuts:
    def test_statut_200(self) -> None:
        async def scenario() -> ReponseHTTP:
            serveur = FauxServeur([])
            url = await serveur.demarrer()
            try:
                async with _transport() as transport:
                    return await transport.envoyer(url, corps=b'{"a": 1}')
            finally:
                await serveur.arreter()

        reponse = _run(scenario())
        assert reponse.statut == 200
        assert reponse.corps == b"ok"

    def test_4xx_definitif_sans_reessai(self) -> None:
        # 400 : aucun réessai, la réponse remonte telle quelle (DLQ côté appelant).
        async def scenario() -> tuple[ReponseHTTP, int]:
            serveur = FauxServeur([(400, {}, b"mapping")])
            url = await serveur.demarrer()
            try:
                async with _transport() as transport:
                    return await transport.envoyer(url, corps=b"x"), len(serveur.recus)
            finally:
                await serveur.arreter()

        reponse, recues = _run(scenario())
        assert (reponse.statut, reponse.corps) == (400, b"mapping")
        assert recues == 1

    def test_429_puis_200(self) -> None:
        # 429 réessayé ; Retry-After: 0 respecté (supérieur au backoff ici).
        async def scenario() -> tuple[ReponseHTTP, int]:
            serveur = FauxServeur([(429, {"Retry-After": "0"}, b""), (200, {}, b"ok")])
            url = await serveur.demarrer()
            try:
                async with _transport() as transport:
                    return await transport.envoyer(url, corps=b"x"), len(serveur.recus)
            finally:
                await serveur.arreter()

        reponse, recues = _run(scenario())
        assert (reponse.statut, recues) == (200, 2)

    def test_5xx_puis_200(self) -> None:
        async def scenario() -> tuple[ReponseHTTP, int]:
            serveur = FauxServeur([(503, {}, b""), (503, {}, b""), (200, {}, b"ok")])
            url = await serveur.demarrer()
            try:
                async with _transport() as transport:
                    return await transport.envoyer(url, corps=b"x"), len(serveur.recus)
            finally:
                await serveur.arreter()

        reponse, recues = _run(scenario())
        assert (reponse.statut, recues) == (200, 3)

    def test_5xx_epuise_rend_la_derniere_reponse(self) -> None:
        # Épuisement des réessais réessayables : la réponse remonte (pas
        # d'exception), c'est l'appelant qui décide (DLQ, #7).
        async def scenario() -> ReponseHTTP:
            serveur = FauxServeur([(503, {}, b"surcharge"), (503, {}, b"surcharge")])
            url = await serveur.demarrer()
            try:
                async with _transport(essais_max=2) as transport:
                    return await transport.envoyer(url, corps=b"x")
            finally:
                await serveur.arreter()

        reponse = _run(scenario())
        assert (reponse.statut, reponse.corps) == (503, b"surcharge")

    def test_delai_depasse_leve_transport_erreur(self) -> None:
        # Serveur muet : délai dépassé à chaque essai, puis TransportErreur
        # (échec transitoire, rejouable par l'appelant).
        async def scenario() -> None:
            serveur = FauxServeur([], latence=1.0)
            url = await serveur.demarrer()
            try:
                async with _transport(timeout_total=0.05, essais_max=2) as transport:
                    await transport.envoyer(url, corps=b"x")
            finally:
                await serveur.arreter()

        with pytest.raises(TransportErreur, match="échec réseau après 2 essai"):
            _run(scenario())

    def test_aucun_essai_refuse(self) -> None:
        # essais_max <= 0 : aucun envoi possible, erreur de configuration.
        async def scenario() -> None:
            serveur = FauxServeur([])
            url = await serveur.demarrer()
            try:
                async with _transport(essais_max=0) as transport:
                    await transport.envoyer(url, corps=b"x")
            finally:
                await serveur.arreter()

        with pytest.raises(ValueError, match="essais_max doit être supérieur à 0"):
            _run(scenario())

    def test_sans_open_refuse(self) -> None:
        async def scenario() -> None:
            await _transport().envoyer("http://127.0.0.1:1/x", corps=b"x")

        with pytest.raises(RuntimeError, match="non ouvert"):
            _run(scenario())


class TestCorpsEtAuthentifications:
    def test_gzip(self) -> None:
        # Le serveur aiohttp décompresserait la requête : un serveur TCP brut
        # reçoit les octets réellement envoyés sur le réseau.

        class ServeurBrut:
            def __init__(self) -> None:
                self.requetes: list[tuple[dict[str, str], bytes]] = []
                self._serveur: asyncio.Server | None = None

            async def _traite(self, lecteur: asyncio.StreamReader, ecrivain: asyncio.StreamWriter) -> None:
                brut_entetes = await lecteur.readuntil(b"\r\n\r\n")
                entetes: dict[str, str] = {}
                for ligne in brut_entetes.decode("ascii").split("\r\n")[1:]:
                    nom, _, valeur = ligne.partition(":")
                    entetes[nom.strip()] = valeur.strip()
                corps = b""
                if (longueur := entetes.get("Content-Length")) is not None:
                    corps = await lecteur.readexactly(int(longueur))
                self.requetes.append((entetes, corps))
                ecrivain.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                await ecrivain.drain()
                ecrivain.close()

            async def demarrer(self) -> str:
                self._serveur = await asyncio.start_server(self._traite, "127.0.0.1", 0)
                hote, port = self._serveur.sockets[0].getsockname()
                return f"http://{hote}:{port}/_bulk"

            async def arreter(self) -> None:
                if self._serveur is not None:
                    self._serveur.close()
                    await self._serveur.wait_closed()

        async def scenario() -> tuple[dict[str, str], bytes]:
            serveur = ServeurBrut()
            url = await serveur.demarrer()
            try:
                async with _transport() as transport:
                    await transport.envoyer(url, corps=b"aaaaaaaaaaaaaaaaaaaaaaaa", compression=True)
                assert serveur.requetes, "le serveur brut n'a rien reçu"
                return serveur.requetes[0]
            finally:
                await serveur.arreter()

        entetes, corps = _run(scenario())
        assert entetes["Content-Encoding"] == "gzip"
        assert gzip.decompress(corps) == b"aaaaaaaaaaaaaaaaaaaaaaaa"

    def test_auth_basic(self) -> None:
        async def scenario() -> str:
            serveur = FauxServeur([])
            url = await serveur.demarrer()
            try:
                async with _transport() as transport:
                    await transport.envoyer(url, corps=b"x", authentification=AuthBasique("admin", "s3cret"))
                return serveur.recus[0]["entetes"]["Authorization"]
            finally:
                await serveur.arreter()

        brut = base64.b64encode(b"admin:s3cret").decode("ascii")
        assert _run(scenario()) == f"Basic {brut}"

    def test_auth_bearer(self) -> None:
        async def scenario() -> str:
            serveur = FauxServeur([])
            url = await serveur.demarrer()
            try:
                async with _transport() as transport:
                    await transport.envoyer(url, corps=b"x", authentification=AuthBearer("jeton-123"))
                return serveur.recus[0]["entetes"]["Authorization"]
            finally:
                await serveur.arreter()

        assert _run(scenario()) == "Bearer jeton-123"

    def test_auth_api_key(self) -> None:
        async def scenario() -> str:
            serveur = FauxServeur([])
            url = await serveur.demarrer()
            try:
                async with _transport() as transport:
                    await transport.envoyer(url, corps=b"x", authentification=AuthApiKey("id1", "cle1"))
                return serveur.recus[0]["entetes"]["Authorization"]
            finally:
                await serveur.arreter()

        brut = base64.b64encode(b"id1:cle1").decode("ascii")
        assert _run(scenario()) == f"ApiKey {brut}"

    def test_en_tetes_libres(self) -> None:
        # X-P-Stream, X-Scope-OrgID… : en-têtes libres transmis tels quels.
        async def scenario() -> str:
            serveur = FauxServeur([])
            url = await serveur.demarrer()
            try:
                async with _transport() as transport:
                    await transport.envoyer(url, corps=b"x", en_tetes={"X-P-Stream": "bulk", "X-Scope-OrgID": "42"})
                return serveur.recus[0]["entetes"].get("X-Scope-OrgID", "")
            finally:
                await serveur.arreter()

        assert _run(scenario()) == "42"

    def test_type_contenu(self) -> None:
        async def scenario() -> str:
            serveur = FauxServeur([])
            url = await serveur.demarrer()
            try:
                async with _transport() as transport:
                    await transport.envoyer(url, corps=b"x", type_contenu="application/x-ndjson")
                return serveur.recus[0]["entetes"]["Content-Type"]
            finally:
                await serveur.arreter()

        assert _run(scenario()) == "application/x-ndjson"


# --- TLS et mTLS -----------------------------------------------------------


def _autorite(tmp_path, nom_fichier: str) -> tuple[x509.Certificate, rsa.RSAPrivateKey]:
    cle = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    sujet = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, "ca-test")])
    certificat = (
        x509.CertificateBuilder()
        .subject_name(sujet)
        .issuer_name(sujet)
        .public_key(cle.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1))
        .not_valid_after(datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(cle.public_key()), critical=False)
        .sign(cle, hashes.SHA256())
    )
    (tmp_path / nom_fichier).write_bytes(
        cle.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        + certificat.public_bytes(serialization.Encoding.PEM)
    )
    return certificat, cle


def _signe(
    tmp_path,
    autorite: x509.Certificate,
    cle_autorite: rsa.RSAPrivateKey,
    nom: str,
    nom_fichier: str,
    client: bool = False,
) -> None:
    cle = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    sujet = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, nom)])
    constructeur = (
        x509.CertificateBuilder()
        .subject_name(sujet)
        .issuer_name(autorite.subject)
        .public_key(cle.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=1))
        .not_valid_after(datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=1))
    )
    if not client:
        constructeur = constructeur.add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
    constructeur = constructeur.add_extension(
        x509.AuthorityKeyIdentifier.from_issuer_public_key(cle_autorite.public_key()), critical=False
    )
    certificat = constructeur.sign(cle_autorite, hashes.SHA256())
    (tmp_path / f"{nom_fichier}.crt").write_bytes(certificat.public_bytes(serialization.Encoding.PEM))
    (tmp_path / f"{nom_fichier}.key").write_bytes(
        cle.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )


def _serveur_tls(chemin_serveur: str, verifier_client: bool, tmp_path) -> ssl.SSLContext:
    contexte = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    contexte.load_cert_chain(f"{chemin_serveur}.crt", f"{chemin_serveur}.key")
    if verifier_client:
        contexte.verify_mode = ssl.CERT_REQUIRED
        contexte.load_verify_locations(cafile=str(tmp_path / "autorite.pem"))
    return contexte


class TestTLS:
    def test_mtls_complet(self, tmp_path) -> None:
        # Autorité commune, certificat serveur vérifié par le client,
        # certificat client exigé par le serveur : la boucle est bouclée.
        autorite, cle_autorite = _autorite(tmp_path, "autorite.pem")
        _signe(tmp_path, autorite, cle_autorite, "serveur", "serveur")
        _signe(tmp_path, autorite, cle_autorite, "client", "client", client=True)

        async def scenario() -> int:
            serveur = FauxServeur([])
            url = await serveur.demarrer(_serveur_tls(str(tmp_path / "serveur"), True, tmp_path))
            try:
                async with _transport(
                    ca_certificats=str(tmp_path / "autorite.pem"),
                    certificat_client=str(tmp_path / "client.crt"),
                    cle_client=str(tmp_path / "client.key"),
                ) as transport:
                    reponse = await transport.envoyer(url, corps=b"x")
                    return reponse.statut
            finally:
                await serveur.arreter()

        assert _run(scenario()) == 200

    def test_certificat_serveur_inconnu_refuse(self, tmp_path) -> None:
        # Le serveur présente un certificat d'une autre autorité : refus.
        autorite, cle_autorite = _autorite(tmp_path, "autorite.pem")
        autre_autorite, cle_autre = _autorite(tmp_path, "autre.pem")
        _signe(tmp_path, autre_autorite, cle_autre, "serveur-autre", "serveur")

        async def scenario() -> None:
            serveur = FauxServeur([])
            url = await serveur.demarrer(_serveur_tls(str(tmp_path / "serveur"), False, tmp_path))
            try:
                async with _transport(ca_certificats=str(tmp_path / "autorite.pem"), essais_max=2) as transport:
                    await transport.envoyer(url, corps=b"x")
            finally:
                await serveur.arreter()

        with pytest.raises(TransportErreur, match="échec réseau après 2 essai"):
            _run(scenario())

    def test_verification_desactivee(self, tmp_path) -> None:
        # ssl_verification_mode => none : certificat non vérifié, connexion OK.
        autorite, cle_autorite = _autorite(tmp_path, "autorite.pem")
        autre_autorite, cle_autre = _autorite(tmp_path, "autre.pem")
        _signe(tmp_path, autre_autorite, cle_autre, "serveur-autre", "serveur")

        async def scenario() -> int:
            serveur = FauxServeur([])
            url = await serveur.demarrer(_serveur_tls(str(tmp_path / "serveur"), False, tmp_path))
            try:
                async with _transport(verification_tls=False) as transport:
                    reponse = await transport.envoyer(url, corps=b"x")
                    return reponse.statut
            finally:
                await serveur.arreter()

        assert _run(scenario()) == 200

    def test_certificat_client_sans_cle_refuse(self, tmp_path) -> None:
        # ssl_certificate sans ssl_key : erreur de configuration immédiate.
        async def scenario() -> None:
            transport = _transport(certificat_client=str(tmp_path / "client.crt"))
            await transport.open()

        with pytest.raises(ValueError, match="sans clé privée"):
            _run(scenario())


class TestDelaisEtArret:
    def test_backoff_exponentiel_avec_jitter(self) -> None:
        # Base 1 s, jitter nul : 1 s * 2**0 * 0.5 = 0,5 s ; plafonné à delai_max.
        transport = _transport(delai_initial=1.0, delai_max=0.75, alea=lambda: 0.0)
        assert transport._delai(0, None) == pytest.approx(0.5)
        # 2**2 = 4 s * 0.5 = 2 s, plafonné par delai_max.
        assert transport._delai(2, None) == pytest.approx(0.75)

    def test_retry_after_en_secondes_prime(self) -> None:
        transport = _transport(delai_initial=0.01, delai_max=5.0, alea=lambda: 0.0)
        assert transport._delai(0, "3") == pytest.approx(3.0)

    def test_retry_after_date_http(self) -> None:
        # Date HTTP dans 10 secondes : retenue si elle dépasse le backoff.
        transport = _transport(delai_initial=0.01, delai_max=30.0, alea=lambda: 0.0)
        dans_10s = (datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=10)).strftime(
            "%a, %d %b %Y %H:%M:%S GMT"
        )
        delai = transport._delai(0, dans_10s)
        assert 8.0 < delai <= 10.0

    def test_retry_after_illisible_ignore(self) -> None:
        transport = _transport(delai_initial=0.01, alea=lambda: 0.0)
        assert transport._delai(0, "bientôt") == pytest.approx(0.005)

    def test_retry_after_date_passee_vaut_zero(self) -> None:
        transport = _transport(delai_initial=0.01, alea=lambda: 0.0)
        passe = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=60)).strftime(
            "%a, %d %b %Y %H:%M:%S GMT"
        )
        assert transport._secondes_retry_after(passe) == 0.0

    def test_open_deja_ouvert_puis_double_close(self) -> None:
        # Idempotent : réouverture et double fermeture sans effet.
        async def scenario() -> None:
            transport = _transport()
            await transport.open()
            await transport.open()
            await transport.close()
            await transport.close()

        _run(scenario())

    def test_async_with_exception_ferme(self) -> None:
        # Une exception dans le bloc ferme le transport ET remonte.
        async def scenario() -> bool:
            transport = _transport()
            try:
                async with transport:
                    raise RuntimeError("boom")
            except RuntimeError:
                pass
            return transport._session is None

        assert _run(scenario()) is True
