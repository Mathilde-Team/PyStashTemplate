# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la sortie kafka (issue #29), faux producteur.

`bootstrap_servers`/`topic_id` requis, sujet et clé rendus par gabarit
sprintf, valeur encodée selon le `codec`, retentes sur échec transitoire
(seuls les messages en échec sont renvoyés), DLQ des transitoires
épuisés avec `dlq_transitoires`.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.kafka import KafkaOutput


class FauxProducteur:
    """Échoue `echecs` fois puis réussit ; garde les messages reçus."""

    def __init__(self, echecs: int = 0) -> None:
        self.echecs_restants = echecs
        self.envoyes: list[tuple[str, bytes, bytes | None]] = []
        self.demarrages = 0
        self.arrets = 0

    async def start(self) -> None:
        self.demarrages += 1

    async def stop(self) -> None:
        self.arrets += 1

    async def send(self, topic: str, value: bytes | None = None, key: bytes | None = None) -> Any:
        self.envoyes.append((topic, value, key))
        if self.echecs_restants > 0:
            self.echecs_restants -= 1
            raise RuntimeError("courtier injoignable")
        return object()

    class _Client:
        async def fetch_all_metadata(self) -> Any:
            class _Broker:
                host, port = "kafka", 9092

            class _Meta:
                def brokers(self) -> set[_Broker]:
                    return {_Broker()}

            return _Meta()

    client = _Client()


def sortie(producteur: FauxProducteur, **options: Any) -> KafkaOutput:
    """Sortie branchée sur le faux producteur (options minimales fournies)."""
    return KafkaOutput(
        {"bootstrap_servers": "kafka:9092", "topic_id": "logs", **options},
        producer=producteur,
    )


def envoyer(s: KafkaOutput, documents: list[dict[str, Any]]) -> list[SendOutcome]:
    """Envoie les documents puis ferme la sortie dans la même boucle."""

    async def scenario() -> list[SendOutcome]:
        try:
            return [await s.envoyer(doc) for doc in documents]
        finally:
            await s.close()

    return asyncio.run(scenario())


def test_inscrite_au_registre() -> None:
    assert REGISTRY["kafka"] is KafkaOutput


def test_capacites_insertions_seules_sans_id_ni_accuse_par_element() -> None:
    capacites = KafkaOutput.capacites
    assert capacites.actions == {"index"}
    assert not capacites.accepte_id
    assert not capacites.accuse_par_evenement


def test_bootstrap_servers_requis() -> None:
    with pytest.raises(ValueError, match="bootstrap_servers"):
        KafkaOutput({"topic_id": "logs"}, producer=FauxProducteur())
    with pytest.raises(ValueError, match="bootstrap_servers"):
        KafkaOutput({"bootstrap_servers": [], "topic_id": "logs"}, producer=FauxProducteur())


def test_topic_id_requis() -> None:
    with pytest.raises(ValueError, match="topic_id"):
        KafkaOutput({"bootstrap_servers": "kafka:9092"}, producer=FauxProducteur())


def test_bootstrap_liste_acceptee() -> None:
    sortie_ = sortie(FauxProducteur(), bootstrap_servers=["a:9092", "b:9092"])
    assert sortie_._bootstrap == ["a:9092", "b:9092"]


def test_option_inconnue_refusee() -> None:
    with pytest.raises(ValueError, match="inconnue"):
        sortie(FauxProducteur(), topic="logs")


def test_sujet_rendu_par_gabarit() -> None:
    producteur = FauxProducteur()
    doc = {"niveau": "alerte"}
    assert envoyer(sortie(producteur, topic_id="logs-%{niveau}"), [doc]) == [SendOutcome.INDEXED]
    assert producteur.envoyes[0][0] == "logs-alerte"


def test_cle_rendue_par_gabarit() -> None:
    producteur = FauxProducteur()
    doc = {"hote": "web-1"}
    envoyer(sortie(producteur, message_key="%{hote}"), [doc])
    assert producteur.envoyes[0][2] == b"web-1"


def test_sans_message_key_cle_absente() -> None:
    producteur = FauxProducteur()
    envoyer(sortie(producteur), [{"message": "m"}])
    assert producteur.envoyes[0][2] is None


def test_valeur_json_par_defaut() -> None:
    producteur = FauxProducteur()
    doc = {"message": "m", "n": 1}
    envoyer(sortie(producteur), [doc])
    topic, valeur, _ = producteur.envoyes[0]
    assert json.loads(valeur.decode("utf-8")) == doc


def test_codec_rubydebug() -> None:
    producteur = FauxProducteur()
    envoyer(sortie(producteur, codec="rubydebug"), [{"message": "m"}])
    assert b'"message" => "m"' in producteur.envoyes[0][1]


def test_batch_size_tamponne_puis_envoie() -> None:
    producteur = FauxProducteur()
    s = sortie(producteur, batch_size=2)

    async def scenario() -> list[SendOutcome]:
        premiers = [await s.envoyer({"n": i}) for i in range(2)]
        await s.flush()
        return premiers

    assert asyncio.run(scenario()) == [SendOutcome.EN_ATTENTE, SendOutcome.INDEXED]
    assert len(producteur.envoyes) == 2


def test_echec_transitoire_retabli_au_bout_des_retentes() -> None:
    producteur = FauxProducteur(echecs=1)
    s = sortie(producteur, retry_max=1, retry_backoff=0)

    async def scenario() -> SendOutcome:
        return await s.envoyer({"message": "m"})

    assert asyncio.run(scenario()) == SendOutcome.INDEXED
    # Le message en échec a été renvoyé : deux envois au total.
    assert len(producteur.envoyes) == 2


def test_echec_transitoire_epuise_rendu_a_l_appelant() -> None:
    producteur = FauxProducteur(echecs=5)
    s = sortie(producteur, retry_max=1, retry_backoff=0)

    async def scenario() -> SendOutcome:
        return await s.envoyer({"message": "m"})

    assert asyncio.run(scenario()) == SendOutcome.RETRYABLE


def test_echec_transitoire_epuise_en_dlq(tmp_path: Path) -> None:
    producteur = FauxProducteur(echecs=5)
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    s = KafkaOutput(
        {"bootstrap_servers": "kafka:9092", "topic_id": "logs", "dlq_transitoires": True},
        producer=producteur,
        dlq=dlq,
    )

    async def scenario() -> SendOutcome:
        return await s.envoyer({"message": "m"})

    assert asyncio.run(scenario()) == SendOutcome.DEAD_LETTERED
    entree = json.loads(dlq._path.read_text(encoding="utf-8"))
    assert entree["@dlq_index"] == "logs"
    assert entree["@dlq_output"] == "kafka"


def test_send_et_flush_contrat_output() -> None:
    producteur = FauxProducteur()

    async def scenario() -> None:
        s = sortie(producteur)
        await s.open()
        await s.send([{"message": "a"}, {"message": "b"}])
        await s.flush()
        await s.close()
        await s.close()  # idempotent

    asyncio.run(scenario())
    assert [t for t, _, _ in producteur.envoyes] == ["logs", "logs"]
    assert producteur.arrets == 1


def test_open_demarre_le_producteur_une_fois() -> None:
    producteur = FauxProducteur()

    async def scenario() -> None:
        s = sortie(producteur)
        await s.open()
        await s.open()
        await s.close()

    asyncio.run(scenario())
    assert producteur.demarrages == 1


def test_health_liste_les_noeuds() -> None:
    producteur = FauxProducteur()

    async def scenario() -> dict[str, Any]:
        s = sortie(producteur)
        return await s.health()

    assert asyncio.run(scenario()) == {"brokers": ["kafka:9092"]}


def test_sasl_sans_utilisateur_refuse() -> None:
    with pytest.raises(ValueError, match="user"):
        sortie(FauxProducteur(), security_protocol="SASL_SSL")


def test_producteur_construit_avec_options() -> None:
    s = KafkaOutput(
        {
            "bootstrap_servers": "kafka:9092",
            "topic_id": "logs",
            "acks": "all",
            "compression_type": "snappy",
            "client_id": "banc",
        },
    )
    args = s._producer_args()
    assert args["acks"] == "all"
    assert args["compression_type"] == "snappy"
    assert args["security_protocol"] == "PLAINTEXT"


def test_lot_vide_ne_produit_rien() -> None:
    producteur = FauxProducteur()

    async def scenario() -> None:
        s = sortie(producteur)
        await s.flush()

    asyncio.run(scenario())
    assert producteur.envoyes == []


def test_dlq_path_construit_la_dlq(tmp_path: Path) -> None:
    s = KafkaOutput(
        {
            "bootstrap_servers": "kafka:9092",
            "topic_id": "logs",
            "dlq_path": str(tmp_path / "dlq.jsonl"),
        },
    )
    assert s._dlq is not None


def test_ssl_avec_cacert_prepare_le_contexte(tmp_path: Path) -> None:
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
    s = sortie(FauxProducteur(), security_protocol="SSL", cacert=str(pem))
    contexte = s._args_producer["ssl_context"]
    assert isinstance(contexte, ssl.SSLContext)


def test_sasl_plain_authentifie() -> None:
    s = sortie(FauxProducteur(), security_protocol="SASL_PLAINTEXT", user="alice", password="secret")
    args = s._args_producer
    assert args["security_protocol"] == "SASL_PLAINTEXT"
    assert args["sasl_plain_username"] == "alice"
    assert args["sasl_plain_password"] == "secret"


def test_dlq_configuree_sans_dlq_transitoires_rendu_a_l_appelant(tmp_path: Path) -> None:
    producteur = FauxProducteur(echecs=5)
    s = KafkaOutput(
        {"bootstrap_servers": "kafka:9092", "topic_id": "logs"},
        producer=producteur,
        dlq=LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl")),
    )

    async def scenario() -> SendOutcome:
        return await s.envoyer({"message": "m"})

    assert asyncio.run(scenario()) == SendOutcome.RETRYABLE


def test_producteur_reel_construit_sans_connexion() -> None:
    s = KafkaOutput({"bootstrap_servers": "kafka:9092", "topic_id": "logs"})

    async def scenario() -> None:
        producteur = s._producteur()
        assert type(producteur).__name__ == "AIOKafkaProducer"

    asyncio.run(scenario())


def test_lot_vide_issue_vide() -> None:
    s = sortie(FauxProducteur())

    async def scenario() -> list[SendOutcome]:
        return await s._issues_du_lot([])

    assert asyncio.run(scenario()) == []
