# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie kafka : producteur asynchrone vers Apache Kafka (et compatibles).

Issue #29. Comme le plugin `logstash-output-kafka` :

- `bootstrap_servers` (nœuds `hôte:port`, chaîne ou liste) et `topic_id`
  (sujet de destination) sont requis ;
- le sujet et la clé (`message_key`, gabarit sprintf) sont rendus contre
  chaque document : les documents d'un même lot peuvent partir sur des
  sujets différents ;
- `codec` choisit le rendu de la valeur (`json`, `json_lines`,
  `rubydebug`), `acks` (0, 1, all), `compression_type`
  (none, gzip, snappy, lz4, zstd) et `client_id` passent au producteur ;
- `security_protocol` (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL) porte
  la sécurité — avec SASL, `user`/`password` (mécanisme PLAIN).

Kafka ne rejette pas un message pour son contenu (pas d'équivalent du
statut `_bulk` par élément) : un échec d'envoi est toujours transitoire
— retentes exponentielles (`retry_max`/`retry_backoff`), puis lot rendu
à l'appelant (`RETRYABLE`) ou en DLQ avec `dlq_transitoires` (#7). La
sémantique est au moins une fois : un message dont l'acquittement
s'est perdu peut être renvoyé en double, comme tout producteur Kafka.

Le banc de conformité (conteneur Redpanda) lit les messages avec un
consommateur `aiokafka` et vérifie sujet, clé et valeur.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from typing import Any, ClassVar

from aiokafka import AIOKafkaProducer
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

__all__ = ["KafkaOutput"]

# Protocoles de sécurité acceptés par aiokafka, comme le plugin Logstash.
PROTOCOLLES = ("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL")


@enregistrer
class KafkaOutput(Output):
    """Producteur Kafka asynchrone, branché sur le socle des sorties (#4, #8)."""

    plugin = "kafka"
    capacites = Capacites(actions=frozenset({"index"}), accepte_id=False, accuse_par_evenement=False)

    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = {
        "bootstrap_servers": OptionSpec(
            "bootstrap_servers",
            (str, list, tuple),
            description="Nœud(s) Kafka hôte:port (chaîne ou liste)",
        ),
        "topic_id": OptionSpec(
            "topic_id",
            (str,),
            description="Sujet de destination (gabarit sprintf)",
            requis=True,
        ),
        "message_key": OptionSpec(
            "message_key",
            (str,),
            description="Clé des messages (gabarit sprintf, partitionnement)",
        ),
        "codec": OptionSpec(
            "codec",
            (str,),
            "json",
            valeurs=("json", "json_lines", "rubydebug"),
            description="Rendu de la valeur (json par défaut)",
        ),
        "acks": OptionSpec(
            "acks",
            (str,),
            "1",
            valeurs=("0", "1", "all"),
            description="Niveau d'acquittement Kafka (1 par défaut)",
        ),
        "compression_type": OptionSpec(
            "compression_type",
            (str,),
            "none",
            valeurs=("none", "gzip", "snappy", "lz4", "zstd"),
            description="Compression des messages (none par défaut)",
        ),
        "client_id": OptionSpec("client_id", (str,), description="Identifiant du client Kafka"),
        "security_protocol": OptionSpec(
            "security_protocol",
            (str,),
            "PLAINTEXT",
            valeurs=PROTOCOLLES,
            description="Protocole de sécurité (PLAINTEXT par défaut)",
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
        producer: Any = None,
        dlq: Any = None,
    ) -> None:
        """Prépare le producteur et le tampon depuis les options du bloc.

        Args:
            options: Options du bloc `output{}` : connexion
                (`bootstrap_servers`, `security_protocol`, `user`,
                `password`, `cacert`), envoi (`topic_id`, `message_key`,
                `codec`, `acks`, `compression_type`, `client_id`,
                `batch_size`, `retry_max`, `retry_backoff`) et DLQ
                (`dlq_path`, `dlq_transitoires`).
            producer: Producteur de remplacement (couture de test — un
                faux producteur). `None` construit un `AIOKafkaProducer`
                au premier `open()`.
            dlq: DLQ toute construite (tests), prime sur `dlq_path`.

        Raises:
            ValueError: `bootstrap_servers` ou `topic_id` absent,
                `batch_size`/`retry_max` négatif, option inconnue ou
                valeur invalide.
        """
        logger.debug("KafkaOutput.__init__ | entrée")
        super().__init__(options)
        self._bootstrap = self._bootstrap_de(options.get("bootstrap_servers"))
        self._topic_gabarit = compile_template(str(options["topic_id"]))
        self._cle_gabarit = compile_template(str(options["message_key"])) if options.get("message_key") else None
        self._codec = str(options.get("codec", "json"))
        self._batch_size = max(1, int(options.get("batch_size", 1)))
        self._retry_max = max(0, int(options.get("retry_max", 0)))
        self._retry_backoff = float(options.get("retry_backoff", 1.0))
        self._dlq_transitoires = bool(options.get("dlq_transitoires", False))
        self._dlq = dlq
        if self._dlq is None and options.get("dlq_path"):
            from pystashtemplate.dlq import LocalDeadLetterQueue

            self._dlq = LocalDeadLetterQueue(str(options["dlq_path"]))
        self._producer = producer
        self._producteur_demarre = False
        self._args_producer = self._producer_args()  # valide SASL/TLS à la compilation
        self._pending: list[dict[str, Any]] = []
        logger.debug("KafkaOutput.__init__ | sortie")

    @traced
    def _bootstrap_de(self, serveurs: Any) -> list[str]:
        """Normalise `bootstrap_servers` (chaîne seule ou liste) en liste.

        Raises:
            ValueError: `bootstrap_servers` absent ou liste vide.
        """
        if isinstance(serveurs, str) and serveurs:
            return [serveurs]
        if isinstance(serveurs, (list, tuple)) and serveurs:
            return [str(serveur) for serveur in serveurs]
        raise ValueError(_("sortie kafka : bootstrap_servers est requis (ex. kafka:9092)"))

    @traced
    def _producer_args(self) -> dict[str, Any]:
        """Arguments du producteur déduits des options (sécurité comprise)."""
        acks = str(self.options.get("acks", "1"))
        compression = str(self.options.get("compression_type", "none"))
        args: dict[str, Any] = {
            "bootstrap_servers": self._bootstrap,
            # aiokafka n'accepte que des entiers (0, 1, -1) ou « all »
            "acks": "all" if acks == "all" else int(acks),
            # et « none » n'est pas une compression : None
            "compression_type": compression if compression != "none" else None,
        }
        if self.options.get("client_id"):
            args["client_id"] = str(self.options["client_id"])
        protocole = str(self.options.get("security_protocol", "PLAINTEXT")).upper()
        args["security_protocol"] = protocole
        if protocole in ("SSL", "SASL_SSL"):
            import ssl

            contexte = ssl.create_default_context()
            if self.options.get("cacert"):
                contexte.load_verify_locations(str(self.options["cacert"]))
            args["ssl_context"] = contexte
        if protocole in ("SASL_PLAINTEXT", "SASL_SSL"):
            if not self.options.get("user"):
                raise ValueError(_("sortie kafka : user est requis avec SASL"))
            args.update(
                sasl_mechanism="PLAIN",
                sasl_plain_username=str(self.options["user"]),
                sasl_plain_password=str(self.options.get("password", "")),
            )
        return args

    @traced
    def _producteur(self) -> Any:
        """Producteur partagé, construit paresseusement (jamais avant `open`)."""
        if self._producer is None:
            self._producer = AIOKafkaProducer(**self._args_producer)
        return self._producer

    @atraced
    async def open(self) -> None:
        """Démarre le producteur (idempotent : sans effet si déjà démarré)."""
        logger.debug("KafkaOutput.open | entrée")
        if not self._producteur_demarre:
            producteur = self._producteur()
            await producteur.start()
            self._producteur_demarre = True
        logger.debug("KafkaOutput.open | sortie")

    @atraced
    async def close(self) -> None:
        """Arrête le producteur (flush interne, idempotent)."""
        logger.debug("KafkaOutput.close | entrée")
        if self._producteur_demarre and self._producer is not None:
            await self._producer.stop()
            self._producteur_demarre = False
        logger.debug("KafkaOutput.close | sortie")

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge le cluster : les nœuds du méta-annuaire, ou lève l'erreur.

        Le producteur tient déjà la connexion : les nœuds connus
        suffisent à prouver la joignabilité (pas d'endpoint de santé
        standard dans Kafka).
        """
        producteur = self._producteur()
        await self.open()  # le client n'interroge le cluster que démarré
        metadata = await producteur.client.fetch_metadata()
        return {"brokers": [f"{broker.host}:{broker.port}" for broker in metadata.brokers.values()]}

    @traced
    def _rendre(self, gabarit: Any, doc: dict[str, Any]) -> str:
        """Rend un gabarit sprintf contre le document (déjà rendu = no-op)."""
        return render(gabarit, doc)

    @traced
    def _valeur(self, doc: dict[str, Any]) -> bytes:
        """Encode la valeur du message selon le `codec` configuré."""
        texte = encoder_json_lines(doc) if self._codec in ("json", "json_lines") else encoder_rubydebug(doc)
        return texte.encode("utf-8")

    @atraced
    async def envoyer(self, doc: dict[str, Any]) -> SendOutcome:
        """Tamponne un document et envoie le lot quand il est plein.

        Args:
            doc: Document prêt à envoyer.

        Returns:
            `SendOutcome` de CE document : réel si le lot est parti avec
            lui, `EN_ATTENTE` s'il est seulement tamponné.
        """
        logger.debug("KafkaOutput.envoyer | entrée")
        pending = {
            "doc": doc,
            "topic": self._rendre(self._topic_gabarit, doc),
            "key": self._rendre(self._cle_gabarit, doc).encode("utf-8") if self._cle_gabarit is not None else None,
        }
        self._pending.append(pending)
        self.observateur.file(self.nom, len(self._pending))
        if len(self._pending) < self._batch_size:
            logger.debug("KafkaOutput.envoyer | document tamponné ({})", len(self._pending))
            logger.debug("KafkaOutput.envoyer | sortie")
            return SendOutcome.EN_ATTENTE
        outcomes = await self._flush()
        logger.debug("KafkaOutput.envoyer | sortie")
        return outcomes[-1]

    @atraced
    async def send(self, batch: Sequence[Document]) -> None:
        """Envoie un lot de documents (contrat `Output` #4).

        Le résultat de chaque document est journalisé ; les échecs
        suivent la politique de `envoyer` (transitoire épuisé → rendu à
        l'appelant, ou DLQ avec `dlq_transitoires`).
        """
        logger.debug("KafkaOutput.send | entrée ({} document(s))", len(batch))
        for doc in batch:
            outcome = await self.envoyer(dict(doc))
            logger.debug("KafkaOutput.send | {} → {}", outcome.value, doc)
        logger.debug("KafkaOutput.send | sortie")

    @atraced
    async def flush(self) -> None:
        """Pousse les documents en attente (contrat `Output` : arrêt propre)."""
        logger.debug("KafkaOutput.flush | entrée")
        await self._flush()
        logger.debug("KafkaOutput.flush | sortie")

    @atraced
    async def _flush(self) -> list[SendOutcome]:
        """Envoie le tampon et renvoie l'issue de chaque document.

        Un lot vide ne produit rien (`[]`). Les messages dont l'envoi a
        échoué sont seuls retentés (les acquittés ne sont pas renvoyés) ;
        après `retry_max` retentes, le reste suit la politique des
        transitoires épuisés (rendu à l'appelant ou DLQ).
        """
        if not self._pending:
            return []
        pendings = self._pending
        self._pending = []
        logger.debug("_flush | {} document(s)", len(pendings))
        self.observateur.file(self.nom, 0)
        outcomes = await self._issues_du_lot(pendings)
        for outcome in (SendOutcome.INDEXED, SendOutcome.DEAD_LETTERED, SendOutcome.RETRYABLE):
            nombre = outcomes.count(outcome)
            if nombre:
                self.observateur.evenements(self.nom, outcome.value, nombre)
        logger.debug("_flush | sortie")
        return outcomes

    @atraced
    async def _envoyer_un(self, producteur: Any, pending: dict[str, Any]) -> Any:
        """Envoie UN message : la future d'acquittement, ou l'erreur survenue.

        L'erreur de `send()` lui-même (levée avant toute future) est
        capturée ici pour homogénéiser le traitement du lot.
        """
        try:
            return await producteur.send(pending["topic"], value=self._valeur(pending["doc"]), key=pending["key"])
        except Exception as exc:  # noqa: BLE001 — rendue au traitement du lot
            logger.debug("KafkaOutput._envoyer_un | envoi en échec : {}", exc)
            return exc

    @atraced
    async def _issues_du_lot(self, pendings: list[dict[str, Any]]) -> list[SendOutcome]:
        """Envoie un lot (retentes comprises) et renvoie l'issue de chaque document.

        Seuls les messages en échec sont retentés : les acquittés ne
        sont jamais renvoyés (pas de double). Kafka n'a pas de rejet
        définitif de message : tout échec est transitoire.
        """
        await self.open()  # producteur démarré paresseusement au premier envoi
        essai = 0
        reste = list(pendings)
        erreur = _("sortie kafka : aucun envoi")
        while reste:
            producteur = self._producteur()
            debut = time.perf_counter()
            resultats = await asyncio.gather(*(self._envoyer_un(producteur, p) for p in reste))
            self.observateur.latence(self.nom, time.perf_counter() - debut)
            echecs = [
                (pending, resultat)
                for pending, resultat in zip(reste, resultats, strict=True)
                if isinstance(resultat, BaseException)
            ]
            if not echecs:
                return [SendOutcome.INDEXED] * len(pendings)
            reste = [pending for pending, _ in echecs]
            erreur = f"{type(echecs[0][1]).__name__} : {echecs[0][1]}"
            logger.warning(
                _("envoi kafka échoué erreur={error} documents={nombre}"),
                error=erreur,
                nombre=len(reste),
            )
            if essai >= self._retry_max:
                break
            essai += 1
            self.observateur.reessai(self.nom)
            await asyncio.sleep(self._retry_backoff * 2 ** (essai - 1))
        logger.warning(
            _("envoi kafka : réessais épuisés documents={nombre} erreur={error}"),
            nombre=len(reste),
            error=erreur,
        )
        return await self._transitoires_epuises(reste, erreur)

    @atraced
    async def _transitoires_epuises(self, pendings: list[dict[str, Any]], erreur: str) -> list[SendOutcome]:
        """Lot en échec transitoire, réessais épuisés : RETRYABLE, ou DLQ si `dlq_transitoires`."""
        if self._dlq is None or not self._dlq_transitoires:
            return [SendOutcome.RETRYABLE] * len(pendings)
        logger.warning(_("lot kafka en échec transitoire mis en DLQ documents={nombre}"), nombre=len(pendings))
        for pending in pendings:
            await self._dlq.write(
                pending["doc"],
                index=pending["topic"],
                error=erreur,
                output=self.nom,
                reason=REASON_TRANSIENT_EXHAUSTED,
            )
        return [SendOutcome.DEAD_LETTERED] * len(pendings)
