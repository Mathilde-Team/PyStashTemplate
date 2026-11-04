# SPDX-License-Identifier: GPL-3.0-or-later
"""Authentification par certificat client (mTLS) vers OpenSearch (RsysLogstach#100, rapatrié par #110).

Bout en bout réel : sortie `OpenSearchOutput` de PyStashTemplate (client
httpx authentique), serveur HTTPS local qui EXIGE un
certificat client signé par une autorité de test. Certificats générés
par `openssl` dans un dossier temporaire (test sauté si `openssl` est
absent).
"""

from __future__ import annotations

import asyncio
import shutil
import ssl
import subprocess
from pathlib import Path

import pytest

from pystashtemplate.indexer import OpenSearchIndexer, SendOutcome

pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl absent")


def _openssl(*args: str, cwd: Path) -> None:
    subprocess.run(["openssl", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture(scope="module")
def pki(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Autorité de test, certificat serveur (127.0.0.1) et certificat client."""
    d = tmp_path_factory.mktemp("pki")
    _openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=ca-test",
             "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign",
             "-keyout", "ca.key", "-out", "ca.pem", cwd=d)  # fmt: skip
    (d / "san.ext").write_text("subjectAltName=IP:127.0.0.1\n", encoding="utf-8")
    for nom, ext in (("serveur", ["-extfile", "san.ext"]), ("client", [])):
        _openssl("req", "-newkey", "rsa:2048", "-nodes", "-subj", f"/CN={nom}",
                 "-keyout", f"{nom}.key", "-out", f"{nom}.csr", cwd=d)  # fmt: skip
        _openssl("x509", "-req", "-in", f"{nom}.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial",
                 "-days", "1", "-out", f"{nom}.pem", *ext, cwd=d)  # fmt: skip
    return d


async def _serveur_mtls(pki: Path) -> tuple[asyncio.Server, int, list[str]]:
    """Serveur HTTPS minimal, certificat client obligatoire ; note le CN présenté."""
    contexte = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=str(pki / "ca.pem"))
    contexte.load_cert_chain(pki / "serveur.pem", pki / "serveur.key")
    contexte.verify_mode = ssl.CERT_REQUIRED
    clients: list[str] = []

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        cert = writer.get_extra_info("peercert") or {}
        clients.extend(v for rdn in cert.get("subject", ()) for k, v in rdn if k == "commonName")
        await reader.read(65536)
        # Réponse _bulk (l'envoi passe par l'API _bulk de la sortie, #160).
        body = b'{"items":[{"index":{"status":201}}]}'
        writer.write(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: "
            + str(len(body)).encode()
            + b"\r\nconnection: close\r\n\r\n"
            + body
        )
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=contexte)
    return server, server.sockets[0].getsockname()[1], clients


async def _envoyer(pki: Path, *, avec_certificat: bool) -> tuple[SendOutcome, list[str]]:
    server, port, clients = await _serveur_mtls(pki)
    try:
        indexer = OpenSearchIndexer(
            hosts=[{"host": "127.0.0.1", "port": port}],
            auth=None,
            index_pattern="switch-logs-%Y.%m.%d",
            use_ssl=True,
            verify_certs=True,
            ca_certs=str(pki / "ca.pem"),
            client_cert=str(pki / "client.pem") if avec_certificat else None,
            client_key=str(pki / "client.key") if avec_certificat else None,
        )
        try:
            return await indexer.send({"a": 1}), clients
        finally:
            await indexer.close()
    finally:
        server.close()
        await server.wait_closed()


def test_certificat_client_accepte(pki: Path) -> None:
    """Avec --tls-certificate / --tls-key : document indexé, sans utilisateur ni mot de passe."""
    outcome, clients = asyncio.run(_envoyer(pki, avec_certificat=True))
    assert outcome is SendOutcome.INDEXED
    assert "client" in clients


def test_sans_certificat_client_refuse(pki: Path) -> None:
    """Sans certificat client, le serveur coupe la poignée de main : rien n'est indexé."""
    outcome, clients = asyncio.run(_envoyer(pki, avec_certificat=False))
    assert outcome is not SendOutcome.INDEXED
    assert clients == []
