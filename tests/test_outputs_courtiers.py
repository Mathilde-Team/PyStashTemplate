# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests des sorties « courtiers de messages » (issue #33) : redis, rabbitmq, nats.

Faux clients : socle commun (tampon, retentes des seuls messages en
échec, refus définitifs en DLQ, transitoires épuisés), puis la partie
propre à chaque courtier (options, connexion, publication, santé). Les
vrais serveurs sont couverts par les bancs `tests/test_integration_*.py`.
"""

from __future__ import annotations

import asyncio
import json
import ssl
from pathlib import Path
from typing import Any

import pytest
from aio_pika.exceptions import AMQPConnectionError, DeliveryError
from nats.errors import MaxPayloadError, NoRespondersError
from nats.js.errors import APIError, NoStreamResponseError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs import REGISTRY, courtier_nats, courtier_rabbitmq, courtier_redis
from pystashtemplate.outputs.courtier import EchecDefinitif, EchecTransitoire, Message, SortieCourtier
from pystashtemplate.outputs.courtier_nats import ClientNats, NatsOutput
from pystashtemplate.outputs.courtier_rabbitmq import ClientAmqp, RabbitMqOutput
from pystashtemplate.outputs.courtier_redis import RedisOutput
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.metriques import CompteursSortie


def run(coro: Any) -> Any:
    return asyncio.run(coro)


async def envoyer_tout(sortie: SortieCourtier, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    try:
        return [await sortie.envoyer(doc) for doc in documents]
    finally:
        await sortie.close()


def lignes_dlq(chemin: Path) -> list[dict[str, Any]]:
    return [json.loads(ligne) for ligne in chemin.read_text(encoding="utf-8").splitlines()]


# --------------------------------------------------------------------------
# Socle commun, via une sortie minimale scriptée
# --------------------------------------------------------------------------


class SortieScriptee(SortieCourtier):
    """Rejoue une suite d'issues de lot (ou d'exceptions) ; compte connexions et fermetures."""

    plugin = "scriptee"
    OPTION_DESTINATION = "cible"
    DESTINATION_DEFAUT = "defaut"

    def __init__(self, options: dict[str, Any], script: list[Any], **kw: Any) -> None:
        super().__init__(options, **kw)
        self.script = list(script)
        self.lots: list[list[str]] = []
        self.connexions = 0
        self.fermetures = 0
        self.fermeture_leve = False

    async def _connecter(self) -> Any:
        self.connexions += 1
        return object()

    async def _deconnecter(self, client: Any) -> None:
        self.fermetures += 1
        if self.fermeture_leve and self.fermetures == 1:
            raise RuntimeError("déjà fermé")

    async def _publier_lot(self, client: Any, messages: list[Message]) -> list[Any]:
        self.lots.append([m.doc["m"] for m in messages])
        etape = self.script.pop(0) if self.script else None
        if isinstance(etape, Exception):
            raise etape
        if etape is None:
            return [None] * len(messages)
        return etape(messages)

    async def _sante(self, client: Any) -> dict[str, Any]:
        return {"ok": True}


SortieScriptee.OPTIONS_PROPRES = {
    **courtier_redis.OPTIONS_COURTIER,
    "cible": courtier_redis.OptionSpec("cible", (str,)),
}


def test_destination_par_defaut_et_gabarit() -> None:
    s = SortieScriptee({}, [])
    assert s._preparer({"m": 1}).destination == "defaut"
    s = SortieScriptee({"cible": "logs-%{app}"}, [])
    message = s._preparer({"m": 1, "app": "web"})
    assert message.destination == "logs-web"
    assert json.loads(message.charge) == {"m": 1, "app": "web"}


def test_codec_rubydebug() -> None:
    s = SortieScriptee({"codec": "rubydebug"}, [])
    assert b"=>" in s._charge({"m": 1})


def test_lot_tamponne_puis_indexe_et_metriques() -> None:
    s = SortieScriptee({"batch_size": 3}, [None])
    compteurs = CompteursSortie()
    s.observateur = compteurs
    issues = run(envoyer_tout(s, [{"m": i} for i in range(3)]))
    assert issues == [SendOutcome.EN_ATTENTE, SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
    assert s.lots == [[0, 1, 2]]
    assert s.connexions == 1 and s.fermetures == 1


def test_flush_vide_et_send() -> None:
    s = SortieScriptee({"batch_size": 10}, [None])

    async def scenario() -> None:
        await s.flush()  # rien en attente : aucun lot
        await s.send([{"m": 1}, {"m": 2}])
        assert s.lots == []
        await s.flush()
        await s.close()

    run(scenario())
    assert s.lots == [[1, 2]]


def test_open_idempotent_et_close_sans_ouverture() -> None:
    s = SortieScriptee({}, [])

    async def scenario() -> dict[str, Any]:
        await s.close()  # jamais ouvert : rien
        await s._reconnecter()  # aucun client : rien à fermer
        await s.open()
        await s.open()
        return await s.health()

    assert run(scenario()) == {"ok": True}
    assert s.connexions == 1 and s.fermetures == 0


def test_retente_des_seuls_messages_en_echec(monkeypatch: pytest.MonkeyPatch) -> None:
    attentes: list[float] = []

    async def dormir(delai: float) -> None:
        attentes.append(delai)

    monkeypatch.setattr(asyncio, "sleep", dormir)

    def un_sur_deux(messages: list[Message]) -> list[Any]:
        return [EchecTransitoire("nack") if m.doc["m"] % 2 else None for m in messages]

    s = SortieScriptee({"batch_size": 4, "retry_max": 2, "retry_backoff": 0.5}, [un_sur_deux, None])
    issues = run(envoyer_tout(s, [{"m": i} for i in range(4)]))
    assert issues[-1] is SendOutcome.INDEXED
    assert s.lots == [[0, 1, 2, 3], [1, 3]]
    assert attentes == [0.5]


def test_transitoires_epuises_rendus_a_l_appelant(monkeypatch: pytest.MonkeyPatch) -> None:
    async def dormir(delai: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", dormir)
    tout_echoue = [lambda ms: [EchecTransitoire("panne")] * len(ms)] * 3
    s = SortieScriptee({"retry_max": 2}, tout_echoue)
    assert run(envoyer_tout(s, [{"m": 1}])) == [SendOutcome.RETRYABLE]
    assert len(s.lots) == 3


def test_transitoires_epuises_en_dlq_avec_dlq_transitoires(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    s = SortieScriptee({"dlq_transitoires": True, "cible": "c"}, [RuntimeError("coupure")], dlq=dlq)
    assert run(envoyer_tout(s, [{"m": 1}])) == [SendOutcome.DEAD_LETTERED]
    (entree,) = lignes_dlq(tmp_path / "dlq.jsonl")
    assert "RuntimeError" in json.dumps(entree)
    # l'exception du lot a fait abandonner le client
    assert s.fermetures == 1


def test_transitoires_epuises_sans_dlq_transitoires_restent_retryable(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    s = SortieScriptee({}, [RuntimeError("coupure")], dlq=dlq)
    assert run(envoyer_tout(s, [{"m": 1}])) == [SendOutcome.RETRYABLE]
    assert not (tmp_path / "dlq.jsonl").exists()


def test_refus_definitif_en_dlq_seul_le_document_refuse(tmp_path: Path) -> None:
    def refuse_le_premier(messages: list[Message]) -> list[Any]:
        return [EchecDefinitif("WRONGTYPE"), None]

    s = SortieScriptee({"batch_size": 2, "dlq_path": str(tmp_path / "dlq.jsonl")}, [refuse_le_premier])
    compteurs = CompteursSortie()
    s.observateur = compteurs
    assert run(envoyer_tout(s, [{"m": 1}, {"m": 2}])) == [SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
    (entree,) = lignes_dlq(tmp_path / "dlq.jsonl")
    assert "WRONGTYPE" in json.dumps(entree)
    assert s.lots == [[1, 2]]  # pas de renvoi


def test_refus_definitif_sans_dlq_rendu_a_l_appelant() -> None:
    s = SortieScriptee({}, [lambda ms: [EchecDefinitif("trop gros")]])
    assert run(envoyer_tout(s, [{"m": 1}])) == [SendOutcome.RETRYABLE]


def test_reconnexion_apres_exception_et_fermeture_en_echec(monkeypatch: pytest.MonkeyPatch) -> None:
    async def dormir(delai: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", dormir)
    s = SortieScriptee({"retry_max": 1}, [RuntimeError("coupure"), None])
    s.fermeture_leve = True
    assert run(envoyer_tout(s, [{"m": 1}])) == [SendOutcome.INDEXED]
    assert s.connexions == 2


def test_tls_desactive_par_defaut_et_variantes(tmp_path: Path) -> None:
    assert SortieScriptee({}, [])._tls() is None
    contexte = SortieScriptee({"ssl_enabled": True}, [])._tls()
    assert isinstance(contexte, ssl.SSLContext) and contexte.verify_mode == ssl.CERT_REQUIRED
    sans_verif = SortieScriptee({"ssl_enabled": True, "ssl": False}, [])._tls()
    assert sans_verif.verify_mode == ssl.CERT_NONE and not sans_verif.check_hostname
    with pytest.raises((FileNotFoundError, ssl.SSLError, OSError)):
        SortieScriptee({"ssl_enabled": True, "cacert": str(tmp_path / "absent.pem")}, [])._tls()
    with pytest.raises((FileNotFoundError, ssl.SSLError, OSError)):
        SortieScriptee({"ssl_enabled": True, "ssl_certificate": str(tmp_path / "c.pem")}, [])._tls()


def test_inscrits_au_registre_avec_capacites() -> None:
    for nom, classe in (("redis", RedisOutput), ("rabbitmq", RabbitMqOutput), ("nats", NatsOutput)):
        assert REGISTRY[nom] is classe
        assert classe.capacites.actions == frozenset({"index"})
        assert not classe.capacites.accepte_id


# --------------------------------------------------------------------------
# redis
# --------------------------------------------------------------------------


class FauxPipeline:
    def __init__(self, reponses: list[Any]) -> None:
        self.reponses = reponses
        self.commandes: list[tuple[Any, ...]] = []

    def rpush(self, cle: str, valeur: bytes) -> None:
        self.commandes.append(("rpush", cle, valeur))

    def publish(self, canal: str, valeur: bytes) -> None:
        self.commandes.append(("publish", canal, valeur))

    def xadd(self, cle: str, champs: dict[str, bytes], maxlen: Any = None, approximate: bool = True) -> None:
        self.commandes.append(("xadd", cle, champs, maxlen))

    async def execute(self, raise_on_error: bool = True) -> list[Any]:
        assert raise_on_error is False
        return self.reponses


class FauxRedis:
    def __init__(self, reponses: list[Any] | None = None, ping_leve: bool = False, **kw: Any) -> None:
        self.kw = kw
        self.ping_leve = ping_leve
        self.reponses = reponses or []
        self.pipe: FauxPipeline | None = None
        self.ferme = False

    async def ping(self) -> bool:
        if self.ping_leve:
            raise RedisConnectionError("refusé")
        return True

    async def info(self, section: str) -> dict[str, Any]:
        return {"redis_version": "8.2.0"}

    def pipeline(self, transaction: bool = True) -> FauxPipeline:
        assert transaction is False
        self.pipe = FauxPipeline(self.reponses)
        return self.pipe

    async def aclose(self) -> None:
        self.ferme = True


def test_redis_key_requise_et_data_type_valide() -> None:
    with pytest.raises(ValueError):
        RedisOutput({})
    with pytest.raises(ValueError):
        RedisOutput({"key": "k", "data_type": "hash"})


def test_redis_noeuds() -> None:
    assert RedisOutput({"key": "k"})._noeuds == [("127.0.0.1", 6379)]
    assert RedisOutput({"key": "k", "host": "r1", "port": 7000})._noeuds == [("r1", 7000)]
    assert RedisOutput({"key": "k", "hosts": ["r1:7001", "[::1]", "[::1]:7002"]})._noeuds == [
        ("r1", 7001),
        ("::1", 6379),
        ("::1", 7002),
    ]
    with pytest.raises(ValueError, match="invalide"):
        RedisOutput({"key": "k", "host": "r1:abc"})
    with pytest.raises(ValueError, match="vide"):
        RedisOutput({"key": "k", "host": []})


def test_redis_args_client_auth_et_tls() -> None:
    args = RedisOutput({"key": "k", "db": 2, "timeout": 3})._args_client("r", 6379)
    assert args == {"host": "r", "port": 6379, "db": 2, "socket_timeout": 3.0, "socket_connect_timeout": 3.0}
    args = RedisOutput(
        {
            "key": "k",
            "user": "u",
            "password": "p",
            "ssl_enabled": True,
            "ssl": False,
            "cacert": "/ca.pem",
            "ssl_certificate": "/c.pem",
            "ssl_key": "/k.pem",
        }
    )._args_client("r", 6380)
    assert args["username"] == "u" and args["password"] == "p"
    assert args["ssl"] is True and args["ssl_cert_reqs"] == "none" and args["ssl_check_hostname"] is False
    assert args["ssl_ca_certs"] == "/ca.pem" and args["ssl_certfile"] == "/c.pem" and args["ssl_keyfile"] == "/k.pem"
    assert RedisOutput({"key": "k", "ssl_enabled": True})._args_client("r", 1)["ssl_cert_reqs"] == "required"


def test_redis_connexion_premier_noeud_joignable(monkeypatch: pytest.MonkeyPatch) -> None:
    crees: list[FauxRedis] = []

    def fabrique(**kw: Any) -> FauxRedis:
        client = FauxRedis(ping_leve=kw["host"] == "mort", **kw)
        crees.append(client)
        return client

    monkeypatch.setattr(courtier_redis, "Redis", fabrique)
    s = RedisOutput({"key": "k", "host": ["mort", "vif"]})
    client = run(s._connecter())
    assert client.kw["host"] == "vif"
    assert crees[0].ferme and not crees[1].ferme

    tous_morts = RedisOutput({"key": "k", "host": ["mort"]})
    with pytest.raises(ConnectionError, match="aucun nœud"):
        run(tous_morts._connecter())


def test_redis_liste_canal_flux_et_analyse_des_reponses(tmp_path: Path) -> None:
    client = FauxRedis(reponses=[1, ResponseError("WRONGTYPE Operation"), RedisConnectionError("perdu")])
    s = RedisOutput({"key": "%{k}", "batch_size": 3}, client=client)
    issues = run(s._publier_lot(client, [s._preparer({"k": f"c{i}"}) for i in range(3)]))
    assert issues[0] is None
    assert isinstance(issues[1], EchecDefinitif) and "WRONGTYPE" in issues[1].erreur
    assert isinstance(issues[2], EchecTransitoire)
    assert [c[:2] for c in client.pipe.commandes] == [("rpush", "c0"), ("rpush", "c1"), ("rpush", "c2")]

    canal = RedisOutput({"key": "ch", "data_type": "channel"}, client=client)
    client.reponses = [0]
    run(canal._publier_lot(client, [canal._preparer({})]))
    assert client.pipe.commandes[0][0] == "publish"

    flux = RedisOutput({"key": "fl", "data_type": "stream", "stream_maxlen": 10}, client=client)
    client.reponses = [b"1-0"]
    run(flux._publier_lot(client, [flux._preparer({"m": 1})]))
    assert client.pipe.commandes[0][0] == "xadd" and client.pipe.commandes[0][3] == 10
    flux_libre = RedisOutput({"key": "fl", "data_type": "stream"}, client=client)
    run(flux_libre._publier_lot(client, [flux_libre._preparer({"m": 1})]))
    assert client.pipe.commandes[0][3] is None


def test_redis_bout_en_bout_dlq_et_sante(tmp_path: Path) -> None:
    client = FauxRedis(reponses=[ResponseError("WRONGTYPE"), 2])
    s = RedisOutput({"key": "k", "batch_size": 2, "dlq_path": str(tmp_path / "d.jsonl")}, client=client)

    async def scenario() -> tuple[list[SendOutcome], dict[str, Any]]:
        issues = [await s.envoyer({"m": 1}), await s.envoyer({"m": 2})]
        sante = await s.health()
        await s.close()
        return issues, sante

    issues, sante = run(scenario())
    assert issues == [SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
    assert sante == {"ping": True, "version": "8.2.0"}
    assert client.ferme
    assert len(lignes_dlq(tmp_path / "d.jsonl")) == 1


# --------------------------------------------------------------------------
# rabbitmq
# --------------------------------------------------------------------------


class FauxEchange:
    def __init__(self, erreurs: dict[int, Exception] | None = None) -> None:
        self.erreurs = erreurs or {}
        self.publies: list[tuple[Any, str, bool]] = []

    async def publish(self, message: Any, routing_key: str, mandatory: bool = True) -> None:
        rang = len(self.publies)
        self.publies.append((message, routing_key, mandatory))
        if rang in self.erreurs:
            raise self.erreurs[rang]


class FauxCanal:
    def __init__(self, leve: bool = False) -> None:
        self.leve = leve
        self.declares: list[tuple[str, Any, dict[str, Any]]] = []
        self.default_exchange = FauxEchange()

    async def declare_exchange(self, nom: str, type: Any = "direct", **kw: Any) -> FauxEchange:
        if self.leve:
            raise RuntimeError("PRECONDITION_FAILED")
        self.declares.append((nom, type, kw))
        return FauxEchange()


class FauxConnexionAmqp:
    def __init__(self, canal_leve: bool = False) -> None:
        self.canal = FauxCanal(leve=canal_leve)
        self.fermee = False

        class _Transport:
            class connection:  # noqa: N801 — imite aiormq
                server_properties = {"product": "RabbitMQ", "version": "4.1.0"}

        self.transport = _Transport()

    async def channel(self, publisher_confirms: bool = False) -> FauxCanal:
        assert publisher_confirms is True
        return self.canal

    async def close(self) -> None:
        self.fermee = True


def rmq(**options: Any) -> RabbitMqOutput:
    return RabbitMqOutput({"exchange": "logs", "exchange_type": "topic", **options})


def test_rabbitmq_options_requises_et_proprietes() -> None:
    with pytest.raises(ValueError):
        RabbitMqOutput({"exchange": "logs"})
    with pytest.raises(ValueError):
        RabbitMqOutput({"exchange_type": "topic"})
    with pytest.raises(ValueError, match="inconnue"):
        rmq(message_properties={"couleur": "bleu"})
    with pytest.raises(ValueError, match="vide"):
        rmq(host=[])
    s = rmq(message_properties={"priority": "5", "content_type": "text/plain"})
    assert s._proprietes == {"priority": 5, "content_type": "text/plain"}
    message = s._message_amqp(s._preparer({"m": 1}))
    assert message.priority == 5 and message.content_type == "text/plain"
    assert message.delivery_mode.value == 2
    assert rmq(persistent=False)._message_amqp(s._preparer({})).delivery_mode.value == 1
    assert rmq()._preparer({}).destination == "logstash"


def test_rabbitmq_args_connexion() -> None:
    args = rmq(host="mq", user="u", password="p", vhost="/v", port=5673)._args_connexion("mq")
    assert args == {
        "host": "mq",
        "port": 5673,
        "login": "u",
        "password": "p",
        "virtualhost": "/v",
        "timeout": 5.0,
    }
    assert rmq()._args_connexion("mq:5999")["port"] == 5999
    tls = rmq(ssl_enabled=True, heartbeat=30)._args_connexion("mq")
    assert tls["ssl"] is True and isinstance(tls["ssl_context"], ssl.SSLContext) and tls["heartbeat"] == 30
    assert rmq(hosts=["a", "b"])._hotes == ["a", "b"]


def test_rabbitmq_connexion_premier_noeud_et_echange(monkeypatch: pytest.MonkeyPatch) -> None:
    connexions: list[FauxConnexionAmqp] = []

    async def connecter(**kw: Any) -> FauxConnexionAmqp:
        if kw["host"] == "mort":
            raise AMQPConnectionError("refusé")
        connexion = FauxConnexionAmqp()
        connexions.append(connexion)
        return connexion

    monkeypatch.setattr(courtier_rabbitmq.aio_pika, "connect", connecter)
    s = rmq(host=["mort", "vif"], durable=False)
    client = run(s._connecter())
    assert isinstance(client, ClientAmqp)
    assert connexions[0].canal.declares == [("logs", "topic", {"durable": False, "passive": False})]

    with pytest.raises(ConnectionError, match="aucun nœud"):
        run(rmq(host="mort")._connecter())


def test_rabbitmq_echec_du_canal_ferme_la_connexion(monkeypatch: pytest.MonkeyPatch) -> None:
    connexion = FauxConnexionAmqp(canal_leve=True)

    async def connecter(**kw: Any) -> FauxConnexionAmqp:
        return connexion

    monkeypatch.setattr(courtier_rabbitmq.aio_pika, "connect", connecter)
    with pytest.raises(RuntimeError):
        run(rmq()._connecter())
    assert connexion.fermee


def test_rabbitmq_echange_par_defaut() -> None:
    canal = FauxCanal()
    s = RabbitMqOutput({"exchange": "", "exchange_type": "direct"})
    assert run(s._echange(canal)) is canal.default_exchange


def test_rabbitmq_publication_nack_et_perte_de_canal() -> None:
    echange = FauxEchange(erreurs={1: DeliveryError(None, type("Trame", (), {"name": "Basic.Nack"})())})
    connexion = FauxConnexionAmqp()
    client = ClientAmqp(connexion=connexion, canal=connexion.canal, echange=echange)
    s = rmq(key="logs.%{app}", batch_size=2)
    s._client, s._ouvert = client, True
    issues = run(s._publier_lot(client, [s._preparer({"app": "a"}), s._preparer({"app": "b"})]))
    assert issues[0] is None and isinstance(issues[1], EchecTransitoire) and issues[1].erreur.startswith("nack")
    assert [p[1] for p in echange.publies] == ["logs.a", "logs.b"]
    assert all(p[2] is False for p in echange.publies)
    assert s._client is client  # un nack ne coupe pas la connexion

    echange.erreurs = {2: RuntimeError("canal fermé")}
    issues = run(s._publier_lot(client, [s._preparer({"app": "c"})]))
    assert isinstance(issues[0], EchecTransitoire) and "RuntimeError" in issues[0].erreur
    assert s._client is None and connexion.fermee


def test_rabbitmq_sante() -> None:
    connexion = FauxConnexionAmqp()
    client = ClientAmqp(connexion=connexion, canal=connexion.canal, echange=FauxEchange())
    s = rmq(passive=True)
    s._client, s._ouvert = client, True
    assert run(s.health()) == {"product": "RabbitMQ", "version": "4.1.0", "exchange": "logs"}
    assert connexion.canal.declares[-1] == ("logs", "direct", {"passive": True})
    vide = RabbitMqOutput({"exchange": "", "exchange_type": "direct"})
    connexion2 = FauxConnexionAmqp()
    del connexion2.transport
    vide._client, vide._ouvert = ClientAmqp(connexion2, connexion2.canal, None), True
    assert run(vide.health()) == {"product": None, "version": None, "exchange": ""}
    run(s.close())
    assert connexion.fermee


# --------------------------------------------------------------------------
# nats
# --------------------------------------------------------------------------


class FauxNats:
    def __init__(self, erreurs: dict[int, Exception] | None = None) -> None:
        self.erreurs = erreurs or {}
        self.publies: list[tuple[str, bytes]] = []
        self.flushes = 0
        self.fermee = False
        self.connected_url = None
        self.connected_server_version = "2.12.0"

    async def publish(self, sujet: str, charge: bytes) -> None:
        rang = len(self.publies)
        self.publies.append((sujet, charge))
        if rang in self.erreurs:
            raise self.erreurs[rang]

    async def flush(self, timeout: float = 10) -> None:
        self.flushes += 1

    async def close(self) -> None:
        self.fermee = True

    def jetstream(self) -> FauxJetStream:
        return FauxJetStream({})


class FauxJetStream:
    def __init__(self, erreurs: dict[int, Exception]) -> None:
        self.erreurs = erreurs
        self.publies: list[tuple[str, Any]] = []

    async def publish(self, sujet: str, charge: bytes, timeout: float = 5, stream: Any = None) -> Any:
        rang = len(self.publies)
        self.publies.append((sujet, stream))
        if rang in self.erreurs:
            raise self.erreurs[rang]
        return object()

    async def account_info(self) -> Any:
        class _Info:
            streams = 3

        return _Info()


def test_nats_options_et_args_connexion() -> None:
    with pytest.raises(ValueError):
        NatsOutput({})
    s = NatsOutput({"subject": "logs"})
    args = s._args_connexion()
    assert args["servers"] == ["nats://127.0.0.1:4222"]
    assert args["max_reconnect_attempts"] == 1 and args["allow_reconnect"] is False
    assert "tls" not in args and "user" not in args
    s = NatsOutput(
        {
            "subject": "logs",
            "hosts": ["nats://a:4222", "nats://b:4222"],
            "user": "u",
            "password": "p",
            "token": "t",
            "name": "pst",
            "ssl_enabled": True,
            "timeout": 2,
        }
    )
    args = s._args_connexion()
    assert args["servers"] == ["nats://a:4222", "nats://b:4222"] and args["connect_timeout"] == 2.0
    assert (args["user"], args["password"], args["token"], args["name"]) == ("u", "p", "t", "pst")
    assert isinstance(args["tls"], ssl.SSLContext)
    assert NatsOutput({"subject": "s", "url": "nats://x:1"})._serveurs == ["nats://x:1"]
    run(s._erreur_client(RuntimeError("bruit")))


def test_nats_connexion_coeur_et_jetstream(monkeypatch: pytest.MonkeyPatch) -> None:
    async def connecter(**kw: Any) -> FauxNats:
        return FauxNats()

    monkeypatch.setattr(courtier_nats.nats, "connect", connecter)
    coeur = run(NatsOutput({"subject": "s"})._connecter())
    assert isinstance(coeur, ClientNats) and coeur.jetstream is None
    js = run(NatsOutput({"subject": "s", "jetstream": True})._connecter())
    assert isinstance(js.jetstream, FauxJetStream)


def test_nats_coeur_flush_et_message_trop_gros() -> None:
    connexion = FauxNats(erreurs={1: MaxPayloadError()})
    client = ClientNats(connexion=connexion)
    s = NatsOutput({"subject": "logs.%{app}", "batch_size": 3}, client=client)
    issues = run(envoyer_tout(s, [{"app": "a"}, {"app": "b"}, {"app": "c"}]))
    assert issues == [SendOutcome.EN_ATTENTE, SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
    assert [p[0] for p in connexion.publies] == ["logs.a", "logs.b", "logs.c"]
    assert connexion.flushes == 1 and connexion.fermee


def test_nats_jetstream_classement_des_erreurs() -> None:
    erreurs = {
        1: NoStreamResponseError(),
        2: NoRespondersError(),
        3: APIError(code=503, description="indisponible"),
        4: APIError(code=400, description="flux attendu différent"),
        5: TimeoutError(),
    }
    jetstream = FauxJetStream(erreurs)
    client = ClientNats(connexion=FauxNats(), jetstream=jetstream)
    s = NatsOutput({"subject": "js.logs", "jetstream": True, "stream": "LOGS"}, client=client)
    issues = run(s._publier_lot(client, [s._preparer({"m": i}) for i in range(6)]))
    assert issues[0] is None
    assert isinstance(issues[1], EchecDefinitif) and isinstance(issues[2], EchecDefinitif)
    assert isinstance(issues[3], EchecTransitoire) and "503" in issues[3].erreur
    assert isinstance(issues[4], EchecDefinitif) and "400" in issues[4].erreur
    assert isinstance(issues[5], EchecTransitoire)
    assert {stream for _, stream in jetstream.publies} == {"LOGS"}


def test_nats_sante() -> None:
    from urllib.parse import urlparse

    connexion = FauxNats()
    s = NatsOutput({"subject": "s"}, client=ClientNats(connexion=connexion))
    assert run(s.health()) == {"url": None, "version": "2.12.0"}
    connexion.connected_url = urlparse("nats://n:4222")
    js = NatsOutput({"subject": "s", "jetstream": True}, client=ClientNats(connexion, FauxJetStream({})))
    assert run(js.health()) == {"url": "nats://n:4222", "version": "2.12.0", "streams": 3}
