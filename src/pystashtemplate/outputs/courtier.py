# SPDX-License-Identifier: GPL-3.0-or-later
"""Socle commun des sorties « courtiers de messages » (issue #33).

Redis, RabbitMQ et NATS partagent le même cycle d'envoi, celui de la
sortie kafka (#29) :

- chaque document est **préparé** à l'arrivée (destination rendue par
  gabarit sprintf, charge utile encodée selon le `codec`) puis tamponné
  jusqu'à `batch_size` ;
- le lot part d'un bloc (`_publier_lot`, propre au courtier), qui rend
  **une issue par message** : `None` (accepté), `EchecTransitoire` ou
  `EchecDefinitif` ;
- seuls les messages en échec transitoire sont retentés
  (`retry_max`/`retry_backoff`, exponentiel) : un message accepté n'est
  jamais renvoyé ;
- réessais épuisés : lot rendu à l'appelant (`RETRYABLE`), ou en DLQ avec
  `dlq_transitoires` (#7) ;
- échec définitif (refus du courtier pour ce message : mauvais type de
  clé Redis, aucun flux JetStream sur le sujet…) : DLQ si configurée,
  sinon `RETRYABLE` — comme la famille `_bulk`.

La sémantique est **au moins une fois** : un message dont l'accusé s'est
perdu peut être renvoyé en double.
"""

from __future__ import annotations

import asyncio
import time
from abc import abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

from loguru import logger

from pystashtemplate.dlq import REASON_PERMANENT, REASON_TRANSIENT_EXHAUSTED
from pystashtemplate.i18n import _
from pystashtemplate.outputs.base import Capacites, Document, Output
from pystashtemplate.outputs.codecs import encoder_json_lines, encoder_rubydebug
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.templates import compile_template, render
from pystashtemplate.tracing import atraced, traced

__all__ = ["EchecDefinitif", "EchecTransitoire", "Message", "SortieCourtier"]


@dataclass(frozen=True)
class EchecTransitoire:
    """Le message n'est pas passé, mais un nouvel essai peut réussir (réseau, nack…)."""

    erreur: str


@dataclass(frozen=True)
class EchecDefinitif:
    """Le courtier refuse CE message : le renvoyer tel quel échouerait encore."""

    erreur: str


@dataclass(frozen=True)
class Message:
    """Un document prêt à publier : destination rendue et charge utile encodée."""

    doc: dict[str, Any]
    destination: str
    charge: bytes


# Options partagées par les trois courtiers, en plus des communes du
# schéma (#8) et des propres de chacun.
OPTIONS_COURTIER: dict[str, OptionSpec] = {
    "codec": OptionSpec(
        "codec",
        (str,),
        "json",
        valeurs=("json", "json_lines", "rubydebug"),
        description="Rendu de la charge utile (json par défaut)",
    ),
    "ssl_enabled": OptionSpec(
        "ssl_enabled",
        (bool,),
        False,
        description="Chiffrer la connexion au courtier en TLS (faux par défaut)",
    ),
    "dlq_transitoires": OptionSpec(
        "dlq_transitoires",
        (bool,),
        False,
        description="Les échecs transitoires épuisés partent aussi en DLQ (#7)",
    ),
}


class SortieCourtier(Output):
    """Tampon, retentes et DLQ communs ; le courtier ne fournit que connexion et publication."""

    capacites = Capacites(actions=frozenset({"index"}), accepte_id=False, accuse_par_evenement=False)

    # Option portant la destination (gabarit sprintf requis) : `key`
    # pour Redis, `exchange`… pour RabbitMQ, `subject` pour NATS.
    OPTION_DESTINATION: ClassVar[str]
    # Valeur de la destination quand l'option est absente (`None` : requise).
    DESTINATION_DEFAUT: ClassVar[str | None] = None

    def __init__(self, options: dict[str, Any], *, client: Any = None, dlq: Any = None) -> None:
        """Valide les options et prépare le tampon commun.

        Args:
            options: Options du bloc `output{}` (communes, du courtier et
                propres à la sortie).
            client: Client du courtier tout construit (couture de test —
                un faux client). `None` le construit au premier `open()`.
            dlq: DLQ toute construite (tests), prime sur `dlq_path`.

        Raises:
            ValueError: option inconnue, valeur invalide ou destination
                absente.
        """
        logger.debug("SortieCourtier.__init__ | entrée ({})", self.plugin)
        super().__init__(options)
        self._destination = compile_template(str(options.get(self.OPTION_DESTINATION, self.DESTINATION_DEFAUT)))
        self._codec = str(options.get("codec", "json"))
        self._batch_size = max(1, int(options.get("batch_size", 1)))
        self._retry_max = max(0, int(options.get("retry_max", 0)))
        self._retry_backoff = float(options.get("retry_backoff", 1.0))
        self._dlq_transitoires = bool(options.get("dlq_transitoires", False))
        self._dlq = dlq
        if self._dlq is None and options.get("dlq_path"):
            from pystashtemplate.dlq import LocalDeadLetterQueue

            self._dlq = LocalDeadLetterQueue(str(options["dlq_path"]))
        self._client = client
        self._ouvert = False
        self._pending: list[Message] = []
        logger.debug("SortieCourtier.__init__ | sortie")

    # -- à fournir par chaque courtier ------------------------------------

    @abstractmethod
    async def _connecter(self) -> Any:
        """Construit et connecte le client du courtier (appelé une fois par `open`)."""

    @abstractmethod
    async def _deconnecter(self, client: Any) -> None:
        """Ferme le client du courtier."""

    @abstractmethod
    async def _publier_lot(
        self, client: Any, messages: list[Message]
    ) -> list[None | EchecTransitoire | EchecDefinitif]:
        """Publie un lot ; une issue par message, dans l'ordre."""

    @abstractmethod
    async def _sante(self, client: Any) -> dict[str, Any]:
        """Interroge le courtier (lève l'erreur s'il est injoignable)."""

    # -- cycle de vie -----------------------------------------------------

    @atraced
    async def open(self) -> None:
        """Connecte le client (idempotent : sans effet si déjà ouvert)."""
        logger.debug("{}.open | entrée", type(self).__name__)
        if not self._ouvert:
            if self._client is None:
                self._client = await self._connecter()
            self._ouvert = True
        logger.debug("{}.open | sortie", type(self).__name__)

    @atraced
    async def close(self) -> None:
        """Ferme le client (idempotent) ; le tampon n'est PAS vidé (voir `flush`)."""
        logger.debug("{}.close | entrée", type(self).__name__)
        if self._ouvert and self._client is not None:
            await self._deconnecter(self._client)
            self._client = None
            self._ouvert = False
        logger.debug("{}.close | sortie", type(self).__name__)

    @atraced
    async def health(self) -> dict[str, Any]:
        """État du courtier, ou l'erreur levée s'il est injoignable."""
        await self.open()
        return await self._sante(self._client)

    @atraced
    async def _reconnecter(self) -> None:
        """Abandonne le client courant (connexion perdue) : le prochain essai rouvre."""
        if self._client is not None:
            try:
                await self._deconnecter(self._client)
            except Exception as exc:  # noqa: BLE001 — la connexion est déjà perdue
                logger.debug("_reconnecter | fermeture en échec ignorée : {}", exc)
        self._client = None
        self._ouvert = False

    # -- préparation et envoi ---------------------------------------------

    @traced
    def _charge(self, doc: dict[str, Any]) -> bytes:
        """Encode le document selon le `codec` configuré."""
        texte = encoder_rubydebug(doc) if self._codec == "rubydebug" else encoder_json_lines(doc)
        return texte.encode("utf-8")

    @traced
    def _preparer(self, doc: dict[str, Any]) -> Message:
        """Rend la destination contre le document et encode la charge utile."""
        return Message(doc=doc, destination=render(self._destination, doc), charge=self._charge(doc))

    @atraced
    async def envoyer(self, doc: dict[str, Any]) -> SendOutcome:
        """Tamponne un document et envoie le lot quand il est plein.

        Returns:
            `SendOutcome` de CE document : réel si le lot est parti avec
            lui, `EN_ATTENTE` s'il est seulement tamponné.
        """
        logger.debug("{}.envoyer | entrée", type(self).__name__)
        self._pending.append(self._preparer(doc))
        self.observateur.file(self.nom, len(self._pending))
        if len(self._pending) < self._batch_size:
            logger.debug("{}.envoyer | document tamponné ({})", type(self).__name__, len(self._pending))
            return SendOutcome.EN_ATTENTE
        outcomes = await self._flush()
        logger.debug("{}.envoyer | sortie", type(self).__name__)
        return outcomes[-1]

    @atraced
    async def send(self, batch: Sequence[Document]) -> None:
        """Envoie un lot de documents (contrat `Output` #4), issue de chacun journalisée."""
        logger.debug("{}.send | entrée ({} document(s))", type(self).__name__, len(batch))
        for doc in batch:
            outcome = await self.envoyer(dict(doc))
            logger.debug("{}.send | {} → {}", type(self).__name__, outcome.value, doc)
        logger.debug("{}.send | sortie", type(self).__name__)

    @atraced
    async def flush(self) -> None:
        """Pousse les documents en attente (contrat `Output` : arrêt propre)."""
        logger.debug("{}.flush | entrée", type(self).__name__)
        await self._flush()
        logger.debug("{}.flush | sortie", type(self).__name__)

    @atraced
    async def _flush(self) -> list[SendOutcome]:
        """Envoie le tampon et renvoie l'issue de chaque document (lot vide : `[]`)."""
        if not self._pending:
            return []
        messages = self._pending
        self._pending = []
        self.observateur.file(self.nom, 0)
        outcomes = await self._issues_du_lot(messages)
        for outcome in (SendOutcome.INDEXED, SendOutcome.DEAD_LETTERED, SendOutcome.RETRYABLE):
            nombre = outcomes.count(outcome)
            if nombre:
                self.observateur.evenements(self.nom, outcome.value, nombre)
        return outcomes

    @atraced
    async def _publier(self, messages: list[Message]) -> list[None | EchecTransitoire | EchecDefinitif]:
        """Un essai de publication ; une erreur levée par le client vaut échec transitoire de tout le lot.

        Le client est alors abandonné : la connexion a pu se perdre, le
        prochain essai en rouvre une.
        """
        try:
            await self.open()
            return await self._publier_lot(self._client, messages)
        except Exception as exc:  # noqa: BLE001 — rendue au traitement du lot
            erreur = f"{type(exc).__name__} : {exc}"
            logger.debug("_publier | lot en échec : {}", erreur)
            await self._reconnecter()
            return [EchecTransitoire(erreur)] * len(messages)

    @atraced
    async def _issues_du_lot(self, messages: list[Message]) -> list[SendOutcome]:
        """Envoie un lot (retentes comprises) et renvoie l'issue de chaque document, dans l'ordre."""
        issues: dict[int, SendOutcome] = {}
        reste = list(enumerate(messages))
        essai = 0
        erreur = ""
        while True:
            debut = time.perf_counter()
            resultats = await self._publier([message for _, message in reste])
            self.observateur.latence(self.nom, time.perf_counter() - debut)
            transitoires: list[tuple[int, Message]] = []
            for (rang, message), resultat in zip(reste, resultats, strict=True):
                if resultat is None:
                    issues[rang] = SendOutcome.INDEXED
                elif isinstance(resultat, EchecDefinitif):
                    issues[rang] = await self._definitif(message, resultat.erreur)
                else:
                    transitoires.append((rang, message))
                    erreur = resultat.erreur
            reste = transitoires
            if not reste:
                break
            logger.warning(
                _("envoi {plugin} échoué erreur={error} documents={nombre}"),
                plugin=self.plugin,
                error=erreur,
                nombre=len(reste),
            )
            if essai >= self._retry_max:
                logger.warning(
                    _("envoi {plugin} : réessais épuisés documents={nombre} erreur={error}"),
                    plugin=self.plugin,
                    nombre=len(reste),
                    error=erreur,
                )
                for rang, message in reste:
                    issues[rang] = await self._transitoire_epuise(message, erreur)
                break
            essai += 1
            self.observateur.reessai(self.nom)
            await asyncio.sleep(self._retry_backoff * 2 ** (essai - 1))
        return [issues[rang] for rang in range(len(messages))]

    @atraced
    async def _definitif(self, message: Message, erreur: str) -> SendOutcome:
        """Refus définitif d'un message : DLQ si configurée, sinon rendu à l'appelant."""
        logger.warning(_("{plugin} : message refusé définitivement erreur={error}"), plugin=self.plugin, error=erreur)
        self.observateur.evenements(self.nom, "rejected")
        if self._dlq is None:
            return SendOutcome.RETRYABLE
        await self._dlq.write(
            message.doc, index=message.destination, error=erreur, output=self.nom, reason=REASON_PERMANENT
        )
        return SendOutcome.DEAD_LETTERED

    @atraced
    async def _transitoire_epuise(self, message: Message, erreur: str) -> SendOutcome:
        """Échec transitoire aux réessais épuisés : RETRYABLE, ou DLQ avec `dlq_transitoires`."""
        if self._dlq is None or not self._dlq_transitoires:
            return SendOutcome.RETRYABLE
        await self._dlq.write(
            message.doc, index=message.destination, error=erreur, output=self.nom, reason=REASON_TRANSIENT_EXHAUSTED
        )
        return SendOutcome.DEAD_LETTERED

    @traced
    def _tls(self) -> Any:
        """Contexte TLS déduit de `ssl_enabled`, `ssl` (vérification), `cacert` et du certificat client."""
        if not self.options.get("ssl_enabled", False):
            return None
        import ssl

        contexte = ssl.create_default_context()
        if self.options.get("cacert"):
            contexte.load_verify_locations(str(self.options["cacert"]))
        if not self.options.get("ssl", True):
            contexte.check_hostname = False
            contexte.verify_mode = ssl.CERT_NONE
        if self.options.get("ssl_certificate"):
            contexte.load_cert_chain(str(self.options["ssl_certificate"]), self.options.get("ssl_key"))
        return contexte
