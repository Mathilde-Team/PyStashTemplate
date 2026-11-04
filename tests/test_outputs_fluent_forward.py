# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie fluent_forward (issue #31), faux serveur forward.

Un vrai serveur TCP asyncio local joue Fluentd / Fluent Bit : il
décode les messages MessagePack (Forward, PackedForward, compressé),
répond aux `chunk` par `{"ack": chunk}` et mène la poignée de main
HELO/PING/PONG avec clé partagée et utilisateur. Des variantes
dérèglent le serveur (accusé faux ou absent, HELO/PONG invalides,
authentification refusée) pour couvrir chaque chemin d'échec.
"""

from __future__ import annotations

import asyncio
import datetime
import gzip
import hashlib
import ssl
import struct
from pathlib import Path
from typing import Any

import msgpack
import pytest
from test_outputs_transport import _autorite, _serveur_tls, _signe

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.fluent_forward import EXT_EVENT_TIME, ErreurForward, FluentForwardOutput

CLE = "banc-secret"


def _sha(*morceaux: bytes | str) -> str:
    sha = hashlib.sha512()
    for morceau in morceaux:
        sha.update(morceau.encode() if isinstance(morceau, str) else morceau)
    return sha.hexdigest()


def _temps(valeur: Any) -> float:
    """EventTime (extension 0) ou entier → secondes flottantes."""
    # Pas d'isinstance : sous la couverture, la classe ExtType rendue par
    # l'extension C n'est pas `msgpack.ExtType`.
    if hasattr(valeur, "code") and hasattr(valeur, "data"):
        assert valeur.code == EXT_EVENT_TIME
        secondes, nanos = struct.unpack(">II", valeur.data)
        return secondes + nanos / 1e9
    return float(valeur)


class FauxServeurForward:
    """Serveur forward minimal ; `comportement` choisit ses dérèglements.

    Comportements : `normal`, `ack_faux`, `sans_ack` (ferme sans
    accuser), `ferme_n` (ferme sans accuser les `n` premiers messages),
    `helo_faux`, `pong_court`, `pong_refus`, `pong_empreinte`,
    `silence` (n'accuse jamais, garde la connexion).
    """

    def __init__(
        self,
        comportement: str = "normal",
        *,
        cle: str | None = None,
        utilisateurs: dict[str, str] | None = None,
        echecs: int = 0,
    ) -> None:
        self.comportement = comportement
        self.cle = cle
        self.utilisateurs = utilisateurs
        self.echecs_restants = echecs
        self.messages: list[list[Any]] = []
        self.evenements: list[tuple[str, float, dict[str, Any]]] = []
        self.connexions = 0
        self.pings: list[list[Any]] = []
        self._serveur: asyncio.Server | None = None

    async def demarrer(self, contexte_tls: ssl.SSLContext | None = None) -> int:
        self._serveur = await asyncio.start_server(self._client, "127.0.0.1", 0, ssl=contexte_tls)
        return self._serveur.sockets[0].getsockname()[1]

    async def arreter(self) -> None:
        assert self._serveur is not None
        self._serveur.close()
        await self._serveur.wait_closed()

    async def _lire(self, reader: asyncio.StreamReader, unpacker: msgpack.Unpacker) -> Any:
        while True:
            for objet in unpacker:
                return objet
            donnees = await reader.read(65536)
            if not donnees:
                return None
            unpacker.feed(donnees)

    async def _poignee(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, unpacker: Any) -> bool:
        nonce = b"nonce-du-serveur"
        sel_auth = b"sel-auth" if self.utilisateurs is not None else b""
        if self.comportement == "helo_faux":
            writer.write(msgpack.packb(["BONJOUR", {}]))
            await writer.drain()
            return False
        writer.write(msgpack.packb(["HELO", {"nonce": nonce, "auth": sel_auth, "keepalive": True}], use_bin_type=True))
        await writer.drain()
        ping = await self._lire(reader, unpacker)
        self.pings.append(ping)
        _, hote, sel, empreinte, utilisateur, mot_de_passe = ping
        ok = empreinte == _sha(sel, hote, nonce, str(self.cle))
        raison = b"" if ok else b"shared_key mismatch"
        if ok and self.utilisateurs is not None:
            attendu = self.utilisateurs.get(utilisateur, "")
            ok = mot_de_passe == _sha(sel_auth, utilisateur, attendu)
            raison = b"" if ok else b"username/password mismatch"
        if self.comportement == "pong_refus":
            ok, raison = False, b"refus du banc"
        if self.comportement == "pong_court":
            writer.write(msgpack.packb(["PONG", True]))
        else:
            empreinte_serveur = _sha(sel, "serveur-banc", nonce, str(self.cle))
            if self.comportement == "pong_empreinte":
                empreinte_serveur = "0" * 128
            writer.write(msgpack.packb(["PONG", ok, raison, "serveur-banc", empreinte_serveur]))
        await writer.drain()
        return ok and self.comportement not in ("pong_court", "pong_refus", "pong_empreinte")

    async def _client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connexions += 1
        unpacker = msgpack.Unpacker(raw=False, strict_map_key=False)
        try:
            if self.cle is not None and not await self._poignee(reader, writer, unpacker):
                await reader.read()  # le client ferme
                return
            while True:
                message = await self._lire(reader, unpacker)
                if message is None:
                    return
                self._enregistrer(message)
                chunk = message[2].get("chunk")
                if self.echecs_restants > 0:
                    self.echecs_restants -= 1
                    return
                if chunk is None or self.comportement == "silence":
                    continue
                if self.comportement == "sans_ack":
                    return
                ack = "faux" if self.comportement == "ack_faux" else chunk
                writer.write(msgpack.packb({"ack": ack}))
                await writer.drain()
        finally:
            writer.close()

    def _enregistrer(self, message: list[Any]) -> None:
        self.messages.append(message)
        tag, entrees, option = message
        if isinstance(entrees, bytes):
            flux = gzip.decompress(entrees) if option.get("compressed") == "gzip" else entrees
            unpacker = msgpack.Unpacker(raw=False)
            unpacker.feed(flux)
            entrees = list(unpacker)
        for temps, doc in entrees:
            self.evenements.append((tag, _temps(temps), doc))


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


async def _avec_serveur(serveur: FauxServeurForward, options: dict[str, Any], scenario: Any, **kwargs: Any) -> Any:
    port = await serveur.demarrer(kwargs.pop("contexte_tls", None))
    sortie = FluentForwardOutput({"host": "127.0.0.1", "port": port, **options}, **kwargs)
    try:
        return await scenario(sortie)
    finally:
        await sortie.close()
        await serveur.arreter()


async def _envoyer_tout(sortie: FluentForwardOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    resultats = [await sortie.envoyer(doc) for doc in documents]
    await sortie.flush()
    return resultats


# -- registre, options --------------------------------------------------


def test_inscrite_au_registre() -> None:
    assert REGISTRY["fluent_forward"] is FluentForwardOutput


def test_capacites_insertions_seules_sans_id_ni_accuse_par_element() -> None:
    capacites = FluentForwardOutput.capacites
    assert capacites.actions == {"index"}
    assert not capacites.accepte_id
    assert not capacites.accuse_par_evenement


def test_tag_requis_et_options_validees() -> None:
    with pytest.raises(ValueError, match="tag"):
        FluentForwardOutput({"host": "fluentbit"})
    with pytest.raises(ValueError, match="mode"):
        FluentForwardOutput({"tag": "t", "mode": "message"})
    with pytest.raises(ValueError, match="inconnue|inconnu|unknown"):
        FluentForwardOutput({"tag": "t", "option_inexistante": 1})


def test_valeurs_par_defaut() -> None:
    sortie = FluentForwardOutput({"tag": "t"})
    assert (sortie._host, sortie._port) == ("localhost", 24224)
    assert not sortie._packed and not sortie._gzip and not sortie._require_ack
    assert sortie._ack_timeout == 190 and sortie._ssl is None
    assert sortie._self_hostname  # nom d'hôte local


# -- encodage du temps ---------------------------------------------------


@pytest.mark.parametrize(
    ("valeur", "attendu"),
    [
        ("2026-11-04T10:00:00.123456Z", (1793786400, 123456000)),
        (datetime.datetime(2026, 11, 4, 10, 0, 0, 500000, tzinfo=datetime.UTC), (1793786400, 500000000)),
        (1700000000, (1700000000, 0)),
        (1700000000.25, (1700000000, 250000000)),
    ],
)
def test_temps_event_time_exact(valeur: Any, attendu: tuple[int, int]) -> None:
    temps = msgpack.unpackb(FluentForwardOutput({"tag": "t"})._temps({"@timestamp": valeur}))
    assert temps.code == EXT_EVENT_TIME
    assert struct.unpack(">II", temps.data) == attendu


@pytest.mark.parametrize("valeur", ["pas une date", True, None])
def test_temps_illisible_ou_absent_vaut_maintenant(valeur: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    import pystashtemplate.outputs.fluent_forward as module

    monkeypatch.setattr(module.time, "time", lambda: 1600000000.5)
    temps = msgpack.unpackb(FluentForwardOutput({"tag": "t"})._temps({"@timestamp": valeur}))
    assert struct.unpack(">II", temps.data) == (1600000000, 500000000)


def test_temps_entier_et_time_key() -> None:
    sortie = FluentForwardOutput({"tag": "t", "time_as_integer": True, "time_key": "quand"})
    assert msgpack.unpackb(sortie._temps({"quand": "2026-11-04T10:00:00.9Z"})) == 1793786400


# -- envoi ----------------------------------------------------------------


def test_forward_un_message_par_etiquette_sans_accuse() -> None:
    serveur = FauxServeurForward()
    documents = [
        {"app": "a", "message": "m1", "@timestamp": "2026-11-04T10:00:00Z"},
        {"app": "b", "message": "m2", "@timestamp": "2026-11-04T10:00:01Z"},
        {"app": "a", "message": "m3", "@timestamp": "2026-11-04T10:00:02Z"},
    ]

    async def scenario(sortie: FluentForwardOutput) -> list[SendOutcome]:
        resultats = await _envoyer_tout(sortie, documents)
        await asyncio.sleep(0.05)  # le serveur lit après l'écriture
        return resultats

    resultats = _run(_avec_serveur(serveur, {"tag": "rsyslog.%{app}", "batch_size": 3}, scenario))
    assert resultats == [SendOutcome.EN_ATTENTE, SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
    assert [m[0] for m in serveur.messages] == ["rsyslog.a", "rsyslog.b"]
    assert all(isinstance(m[1], list) for m in serveur.messages)
    assert serveur.messages[0][2] == {"size": 2}
    assert [(t, d["message"]) for t, _s, d in serveur.evenements] == [
        ("rsyslog.a", "m1"),
        ("rsyslog.a", "m3"),
        ("rsyslog.b", "m2"),
    ]
    assert serveur.evenements[0][1] == 1793786400.0


def test_packed_forward_avec_accuse() -> None:
    serveur = FauxServeurForward()
    options = {"tag": "pk", "mode": "packed_forward", "require_ack": True, "batch_size": 2}
    resultats = _run(
        _avec_serveur(serveur, options, lambda s: _envoyer_tout(s, [{"message": "p1"}, {"message": "p2"}]))
    )
    assert resultats == [SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
    tag, flux, option = serveur.messages[0]
    assert tag == "pk" and isinstance(flux, bytes)
    assert option["size"] == 2 and "chunk" in option and "compressed" not in option
    assert [d["message"] for _t, _s, d in serveur.evenements] == ["p1", "p2"]


def test_compression_gzip_et_types_non_natifs() -> None:
    serveur = FauxServeurForward()
    quand = datetime.datetime(2026, 11, 4, tzinfo=datetime.UTC)
    options = {"tag": "gz", "compression": True, "require_ack": True}
    resultats = _run(_avec_serveur(serveur, options, lambda s: _envoyer_tout(s, [{"message": "g", "vu": quand}])))
    assert resultats == [SendOutcome.INDEXED]
    assert serveur.messages[0][2]["compressed"] == "gzip"
    assert serveur.evenements[0][2]["vu"] == str(quand)  # repli `default=str`


def test_send_lot_du_contrat_output_et_flush_vide() -> None:
    serveur = FauxServeurForward()

    async def scenario(sortie: FluentForwardOutput) -> None:
        await sortie.send([{"message": "s1"}, {"message": "s2"}])
        await sortie.flush()  # tampon vide : rien
        assert await sortie._flush() == []
        await asyncio.sleep(0.05)

    _run(_avec_serveur(serveur, {"tag": "s", "require_ack": True}, scenario))
    assert [d["message"] for _t, _s, d in serveur.evenements] == ["s1", "s2"]
    assert serveur.connexions == 1  # connexion réutilisée


def test_health_et_open_idempotent() -> None:
    serveur = FauxServeurForward()

    async def scenario(sortie: FluentForwardOutput) -> dict[str, Any]:
        await sortie.open()
        await sortie.open()
        return await sortie.health()

    sante = _run(_avec_serveur(serveur, {"tag": "t"}, scenario))
    assert sante["authentifie"] is False and sante["host"] == "127.0.0.1"
    assert serveur.connexions == 1


# -- échecs, retentes, DLQ --------------------------------------------------


def test_retente_apres_connexion_fermee_sans_accuse(monkeypatch: pytest.MonkeyPatch) -> None:
    import pystashtemplate.outputs.fluent_forward as module

    attentes: list[float] = []

    async def dormir(secondes: float) -> None:
        attentes.append(secondes)

    monkeypatch.setattr(module.asyncio, "sleep", dormir)
    serveur = FauxServeurForward(echecs=2)
    options = {"tag": "r", "require_ack": True, "retry_max": 3, "retry_backoff": 0.5}
    resultats = _run(_avec_serveur(serveur, options, lambda s: _envoyer_tout(s, [{"message": "r1"}])))
    assert resultats == [SendOutcome.INDEXED]
    assert attentes == [0.5, 1.0]
    assert serveur.connexions == 3  # connexion neuve à chaque essai


def test_reessais_epuises_rendus_a_l_appelant() -> None:
    serveur = FauxServeurForward("sans_ack")
    resultats = _run(
        _avec_serveur(serveur, {"tag": "x", "require_ack": True}, lambda s: _envoyer_tout(s, [{"message": "x"}]))
    )
    assert resultats == [SendOutcome.RETRYABLE]


def test_seuls_les_messages_non_acquittes_sont_retentes(tmp_path: Path) -> None:
    # Premier message (étiquette a) acquitté, second (b) jamais : seul b part en DLQ.
    serveur = FauxServeurForward()
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    options = {"tag": "%{app}", "require_ack": True, "batch_size": 2, "dlq_transitoires": True}

    async def scenario(sortie: FluentForwardOutput) -> list[SendOutcome]:
        envoyer_groupe = sortie._envoyer_groupe

        async def groupe(tag: str, pendings: list[dict[str, Any]]) -> None:
            if tag == "b":
                raise ConnectionResetError("coupure")
            await envoyer_groupe(tag, pendings)

        sortie._envoyer_groupe = groupe  # type: ignore[method-assign]
        return await _envoyer_tout(sortie, [{"app": "a"}, {"app": "b"}])

    resultats = _run(_avec_serveur(serveur, options, scenario, dlq=dlq))
    assert resultats == [SendOutcome.EN_ATTENTE, SendOutcome.DEAD_LETTERED]
    entrees = (tmp_path / "dlq.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(entrees) == 1 and '"b"' in entrees[0]


def test_dlq_path_et_issue_par_document(tmp_path: Path) -> None:
    serveur = FauxServeurForward("ack_faux")
    options = {
        "tag": "%{app}",
        "require_ack": True,
        "batch_size": 2,
        "dlq_path": str(tmp_path / "dlq.jsonl"),
        "dlq_transitoires": True,
    }

    resultats = _run(_avec_serveur(serveur, options, lambda s: _envoyer_tout(s, [{"app": "a"}, {"app": "b"}])))
    assert resultats == [SendOutcome.EN_ATTENTE, SendOutcome.DEAD_LETTERED]
    assert len((tmp_path / "dlq.jsonl").read_text(encoding="utf-8").splitlines()) == 2


def test_accuse_attendu_borne_par_ack_response_timeout() -> None:
    serveur = FauxServeurForward("silence")
    options = {"tag": "t", "require_ack": True, "ack_response_timeout": 0.1}
    resultats = _run(_avec_serveur(serveur, options, lambda s: _envoyer_tout(s, [{"message": "t"}])))
    assert resultats == [SendOutcome.RETRYABLE]


def test_serveur_injoignable() -> None:
    async def scenario() -> list[SendOutcome]:
        serveur = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        port = serveur.sockets[0].getsockname()[1]
        serveur.close()
        await serveur.wait_closed()
        sortie = FluentForwardOutput({"host": "127.0.0.1", "port": port, "tag": "t"})
        return [await sortie.envoyer({"message": "x"})]

    assert _run(scenario()) == [SendOutcome.RETRYABLE]


def test_fermeture_d_une_connexion_deja_rompue() -> None:
    class Ecrivain:
        def close(self) -> None:
            pass

        async def wait_closed(self) -> None:
            raise ConnectionResetError("déjà rompue")

    async def scenario() -> None:
        sortie = FluentForwardOutput({"tag": "t"})
        sortie._writer = Ecrivain()
        await sortie.close()
        assert sortie._writer is None
        await sortie.close()  # idempotent

    _run(scenario())


# -- poignée de main ---------------------------------------------------------


def test_cle_partagee_et_utilisateur() -> None:
    serveur = FauxServeurForward(cle=CLE, utilisateurs={"banc": "BancConformite1"})
    options = {
        "tag": "auth",
        "shared_key": CLE,
        "self_hostname": "client-banc",
        "user": "banc",
        "password": "BancConformite1",
        "require_ack": True,
    }

    async def scenario(sortie: FluentForwardOutput) -> Any:
        resultats = await _envoyer_tout(sortie, [{"message": "k"}])
        return resultats, await sortie.health()

    resultats, sante = _run(_avec_serveur(serveur, options, scenario))
    assert resultats == [SendOutcome.INDEXED] and sante["authentifie"] is True
    ping = serveur.pings[0]
    assert ping[0] == "PING" and ping[1] == "client-banc" and ping[4] == "banc"
    assert len(ping[2]) == 32  # sel hexadécimal, type str


def test_cle_partagee_sans_authentification_utilisateur() -> None:
    serveur = FauxServeurForward(cle=CLE)
    resultats = _run(
        _avec_serveur(
            serveur,
            {"tag": "k", "shared_key": CLE, "require_ack": True},
            lambda s: _envoyer_tout(s, [{"message": "k"}]),
        )
    )
    assert resultats == [SendOutcome.INDEXED]
    assert serveur.pings[0][4:] == ["", ""]


@pytest.mark.parametrize(
    ("comportement", "options", "motif"),
    [
        ("helo_faux", {}, "HELO attendu"),
        ("pong_court", {}, "PONG attendu"),
        ("pong_refus", {}, "refus du banc"),
        ("pong_empreinte", {}, "empreinte du serveur"),
        ("normal", {"password": "faux"}, "username/password mismatch"),
    ],
)
def test_poignee_de_main_en_echec(comportement: str, options: dict[str, Any], motif: str) -> None:
    serveur = FauxServeurForward(comportement, cle=CLE, utilisateurs={"banc": "BancConformite1"})
    toutes = {"tag": "t", "shared_key": CLE, "user": "banc", "password": "BancConformite1", **options}

    async def scenario(sortie: FluentForwardOutput) -> None:
        with pytest.raises(ErreurForward, match=motif):
            await sortie.open()
        assert sortie._writer is None  # connexion refermée

    _run(_avec_serveur(serveur, toutes, scenario))


def test_cle_partagee_differente() -> None:
    serveur = FauxServeurForward("normal", cle="autre-cle")

    async def scenario(sortie: FluentForwardOutput) -> None:
        # Le faux serveur répond PONG refusé (comme Fluentd) ; Fluent Bit,
        # lui, ferme la connexion (« connexion fermée par le serveur »).
        with pytest.raises(ErreurForward, match="shared_key mismatch"):
            await sortie.open()

    _run(_avec_serveur(serveur, {"tag": "t", "shared_key": CLE}, scenario))


def test_lecture_connexion_fermee_par_le_serveur() -> None:
    async def scenario() -> None:
        sortie = FluentForwardOutput({"tag": "t"})
        lecteur = asyncio.StreamReader()
        lecteur.feed_eof()
        sortie._reader = lecteur
        with pytest.raises(ErreurForward, match="connexion fermée"):
            await sortie._lire(1)

    _run(scenario())


def test_texte_decode_les_octets_seulement() -> None:
    assert FluentForwardOutput._texte(b"\xa6abc") == "\ufffdabc"
    assert FluentForwardOutput._texte(True) is True


# -- TLS ---------------------------------------------------------------------


def test_tls_mutuel(tmp_path: Path) -> None:
    autorite, cle_autorite = _autorite(tmp_path, "autorite.pem")
    _signe(tmp_path, autorite, cle_autorite, "serveur", "serveur")
    _signe(tmp_path, autorite, cle_autorite, "client", "client", client=True)
    serveur = FauxServeurForward()
    options = {
        "tag": "tls",
        "tls": True,
        "cacert": str(tmp_path / "autorite.pem"),
        "ssl_certificate": str(tmp_path / "client.crt"),
        "ssl_key": str(tmp_path / "client.key"),
        "require_ack": True,
    }
    contexte = _serveur_tls(str(tmp_path / "serveur"), True, tmp_path)
    resultats = _run(
        _avec_serveur(serveur, options, lambda s: _envoyer_tout(s, [{"message": "tls"}]), contexte_tls=contexte)
    )
    assert resultats == [SendOutcome.INDEXED]


def test_tls_sans_verification() -> None:
    sortie = FluentForwardOutput({"tag": "t", "tls": True, "ssl": False})
    assert sortie._ssl is not None
    assert sortie._ssl.verify_mode == ssl.CERT_NONE and not sortie._ssl.check_hostname
