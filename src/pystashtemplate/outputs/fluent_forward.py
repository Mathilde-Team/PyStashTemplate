# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie fluent_forward : protocole forward v1 (Fluentd / Fluent Bit).

Issue #31. Les documents partent en MessagePack sur TCP (24224 par
défaut), comme le plugin `out_forward` de Fluentd et la sortie
`forward` de Fluent Bit :

- `tag` (gabarit sprintf) est rendu contre chaque document ; un lot est
  regroupé par étiquette, un message forward par étiquette ;
- `mode` choisit `forward` (défaut : `[tag, [[temps, doc], …], option]`)
  ou `packed_forward` (entrées concaténées dans un binaire) ; l'option
  commune `compression` envoie le binaire compressé en gzip
  (`CompressedPackedForward`) ;
- le temps de l'évènement est lu dans `time_key` (`@timestamp` par
  défaut, ISO 8601 ou nombre) et envoyé en `EventTime` (nanosecondes),
  ou en entier avec `time_as_integer` (anciens Fluentd) ;
- `require_ack` ajoute un identifiant `chunk` à chaque message et
  attend la réponse `{"ack": chunk}` du serveur (livraison au moins une
  fois) dans `ack_response_timeout` secondes ;
- `shared_key` déclenche la poignée de main HELO/PING/PONG de la
  spécification (avec `user`/`password` si le serveur exige
  l'authentification) ; `tls` chiffre la connexion (options communes
  `ssl`, `cacert`, `ssl_certificate`, `ssl_key`).

Le protocole ne rejette pas un évènement pour son contenu : un échec
(connexion, écriture, accusé absent) est toujours transitoire —
retentes exponentielles des seuls messages non acquittés, puis lot
rendu à l'appelant (`RETRYABLE`) ou en DLQ avec `dlq_transitoires`
(#7). Sans `require_ack`, un message écrit est réputé livré.

Le banc de conformité lance Fluent Bit en conteneur (entrée `forward`,
sortie fichier) et relit les évènements reçus.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import hashlib
import math
import os
import socket
import ssl
import struct
import time
from collections.abc import Sequence
from datetime import datetime
from typing import Any, ClassVar

import msgpack
from loguru import logger

from pystashtemplate.dlq import REASON_TRANSIENT_EXHAUSTED
from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.base import Document, Output
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.templates import compile_template, render
from pystashtemplate.tracing import atraced, traced

__all__ = ["FluentForwardOutput"]

# Type d'extension MessagePack de l'EventTime (spécification forward v1).
EXT_EVENT_TIME = 0
# En-tête fixext 8 de l'EventTime, écrit à la main : sous la sonde de
# couverture (Python 3.14), la classe ExtType connue de l'extension C de
# msgpack n'est plus `msgpack.ExtType`, qui part alors en simple liste.
# Encodé ainsi, l'EventTime ne dépend pas de cette identité de classe.
ENTETE_EVENT_TIME = bytes((0xD7, EXT_EVENT_TIME))


class ErreurForward(Exception):
    """Échec du protocole forward (poignée de main, accusé, connexion)."""


@enregistrer
class FluentForwardOutput(Output):
    """Client forward asynchrone, branché sur le socle des sorties (#4, #8)."""

    plugin = "fluent_forward"
    capacites = Capacites(actions=frozenset({"index"}), accepte_id=False, accuse_par_evenement=False)

    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = {
        "host": OptionSpec("host", (str,), "localhost", description="Hôte du serveur forward (localhost par défaut)"),
        "port": OptionSpec("port", (int,), 24224, minimum=1, description="Port TCP du serveur forward (24224)"),
        "tag": OptionSpec("tag", (str,), requis=True, description="Étiquette des évènements (gabarit sprintf)"),
        "mode": OptionSpec(
            "mode",
            (str,),
            "forward",
            valeurs=("forward", "packed_forward"),
            description="Mode du protocole : forward (défaut) ou packed_forward",
        ),
        "require_ack": OptionSpec(
            "require_ack",
            (bool,),
            False,
            description="Attendre l'accusé {ack: chunk} du serveur (faux par défaut)",
        ),
        "ack_response_timeout": OptionSpec(
            "ack_response_timeout",
            (int, float),
            190,
            minimum=0,
            description="Secondes d'attente de l'accusé (190 par défaut, comme Fluentd)",
        ),
        "time_key": OptionSpec(
            "time_key",
            (str,),
            "@timestamp",
            description="Champ du temps de l'évènement (@timestamp par défaut)",
        ),
        "time_as_integer": OptionSpec(
            "time_as_integer",
            (bool,),
            False,
            description="Temps en secondes entières au lieu d'EventTime (faux par défaut)",
        ),
        "shared_key": OptionSpec("shared_key", (str,), description="Clé partagée (poignée de main HELO/PING/PONG)"),
        "self_hostname": OptionSpec(
            "self_hostname",
            (str,),
            description="Nom annoncé dans le PING (nom d'hôte local par défaut)",
        ),
        "tls": OptionSpec("tls", (bool,), False, description="Chiffrer la connexion en TLS (faux par défaut)"),
        "connect_timeout": OptionSpec(
            "connect_timeout",
            (int, float),
            10,
            minimum=0,
            description="Secondes d'attente de la connexion TCP (10 par défaut)",
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
        connecteur: Any = None,
        dlq: Any = None,
    ) -> None:
        """Prépare la connexion et le tampon depuis les options du bloc.

        Args:
            options: Options du bloc `output{}` : connexion (`host`,
                `port`, `tls`, `ssl`, `cacert`, `ssl_certificate`,
                `ssl_key`, `connect_timeout`, `shared_key`,
                `self_hostname`, `user`, `password`), envoi (`tag`,
                `mode`, `compression`, `require_ack`,
                `ack_response_timeout`, `time_key`, `time_as_integer`,
                `batch_size`, `retry_max`, `retry_backoff`) et DLQ
                (`dlq_path`, `dlq_transitoires`).
            connecteur: Coroutine `(hôte, port, ssl) -> (reader, writer)`
                de remplacement (couture de test). `None` utilise
                `asyncio.open_connection`.
            dlq: DLQ toute construite (tests), prime sur `dlq_path`.

        Raises:
            ValueError: `tag` absent, `batch_size`/`retry_max` négatif,
                option inconnue ou valeur invalide.
        """
        logger.debug("FluentForwardOutput.__init__ | entrée")
        super().__init__(options)
        self._host = str(options.get("host", "localhost"))
        self._port = int(options.get("port", 24224))
        self._tag_gabarit = compile_template(str(options["tag"]))
        self._packed = str(options.get("mode", "forward")) == "packed_forward" or bool(options.get("compression"))
        self._gzip = bool(options.get("compression", False))
        self._require_ack = bool(options.get("require_ack", False))
        self._ack_timeout = float(options.get("ack_response_timeout", 190))
        self._connect_timeout = float(options.get("connect_timeout", 10))
        self._time_key = str(options.get("time_key", "@timestamp"))
        self._time_as_integer = bool(options.get("time_as_integer", False))
        self._shared_key = str(options["shared_key"]) if options.get("shared_key") else None
        self._self_hostname = str(options.get("self_hostname") or socket.gethostname())
        self._batch_size = max(1, int(options.get("batch_size", 1)))
        self._retry_max = max(0, int(options.get("retry_max", 0)))
        self._retry_backoff = float(options.get("retry_backoff", 1.0))
        self._dlq_transitoires = bool(options.get("dlq_transitoires", False))
        self._dlq = dlq
        if self._dlq is None and options.get("dlq_path"):
            from pystashtemplate.dlq import LocalDeadLetterQueue

            self._dlq = LocalDeadLetterQueue(str(options["dlq_path"]))
        self._connecteur = connecteur or asyncio.open_connection
        self._ssl = self._contexte_tls() if options.get("tls") else None
        self._reader: Any = None
        self._writer: Any = None
        self._unpacker = msgpack.Unpacker(raw=True)
        self._pending: list[dict[str, Any]] = []
        logger.debug("FluentForwardOutput.__init__ | sortie")

    @traced
    def _contexte_tls(self) -> ssl.SSLContext:
        """Contexte TLS du client : vérification (`ssl`), CA et mTLS."""
        contexte = ssl.create_default_context()
        if self.options.get("cacert"):
            contexte.load_verify_locations(str(self.options["cacert"]))
        if self.options.get("ssl_certificate"):
            contexte.load_cert_chain(str(self.options["ssl_certificate"]), self.options.get("ssl_key"))
        if not self.options.get("ssl", True):
            contexte.check_hostname = False
            contexte.verify_mode = ssl.CERT_NONE
        return contexte

    # -- connexion et poignée de main ------------------------------------

    @atraced
    async def open(self) -> None:
        """Ouvre la connexion (et la poignée de main) ; sans effet si ouverte."""
        logger.debug("FluentForwardOutput.open | entrée")
        if self._writer is None:
            self._unpacker = msgpack.Unpacker(raw=True)
            self._reader, self._writer = await asyncio.wait_for(
                self._connecteur(self._host, self._port, ssl=self._ssl), self._connect_timeout
            )
            try:
                if self._shared_key is not None:
                    await self._poignee_de_main()
            except BaseException as exc:
                logger.debug("FluentForwardOutput.open | poignée de main en échec : {}", exc)
                await self.close()
                raise
        logger.debug("FluentForwardOutput.open | sortie")

    @atraced
    async def close(self) -> None:
        """Ferme la connexion (idempotent)."""
        logger.debug("FluentForwardOutput.close | entrée")
        writer, self._reader, self._writer = self._writer, None, None
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except (OSError, ssl.SSLError) as exc:  # connexion déjà rompue : rien à libérer de plus
                logger.debug("FluentForwardOutput.close | fermeture en erreur : {}", exc)
        logger.debug("FluentForwardOutput.close | sortie")

    @atraced
    async def health(self) -> dict[str, Any]:
        """Ouvre la connexion (poignée de main comprise) et la décrit.

        Le protocole forward n'a pas de requête de santé : une connexion
        établie, authentifiée si `shared_key` est fixée, suffit.
        """
        await self.open()
        return {"host": self._host, "port": self._port, "authentifie": self._shared_key is not None}

    @atraced
    async def _lire(self, timeout: float) -> Any:
        """Lit UN objet MessagePack du serveur, en `timeout` secondes au plus.

        Raises:
            ErreurForward: connexion fermée par le serveur.
            TimeoutError: rien reçu à temps.
        """
        async with asyncio.timeout(timeout):
            while True:
                for objet in self._unpacker:
                    return objet
                donnees = await self._reader.read(65536)
                if not donnees:
                    raise ErreurForward(_("sortie fluent_forward : connexion fermée par le serveur"))
                self._unpacker.feed(donnees)

    @staticmethod
    @traced
    def _texte(valeur: Any) -> Any:
        """Décode un octet brut reçu (`raw=True`) en texte ; le reste tel quel.

        Les réponses sont lues en brut : Fluent Bit envoie le `nonce`
        du HELO en type *str* MessagePack avec des octets non UTF-8.
        """
        return valeur.decode("utf-8", "replace") if isinstance(valeur, bytes) else valeur

    @staticmethod
    @traced
    def _empreinte(*morceaux: bytes | str) -> str:
        """SHA-512 hexadécimal de la concaténation (spécification forward v1)."""
        sha = hashlib.sha512()
        for morceau in morceaux:
            sha.update(morceau.encode("utf-8") if isinstance(morceau, str) else morceau)
        return sha.hexdigest()

    @atraced
    async def _poignee_de_main(self) -> None:
        """HELO du serveur → PING du client → PONG vérifié.

        Raises:
            ErreurForward: message inattendu, authentification refusée
                ou empreinte du serveur invalide.
        """
        logger.debug("FluentForwardOutput._poignee_de_main | entrée")
        helo = await self._lire(self._connect_timeout)
        if not (isinstance(helo, list) and len(helo) == 2 and helo[0] == b"HELO" and isinstance(helo[1], dict)):
            raise ErreurForward(_("sortie fluent_forward : HELO attendu, reçu {recu}").format(recu=str(helo)[:80]))
        nonce = helo[1].get(b"nonce") or b""
        sel_auth = helo[1].get(b"auth") or b""
        # Sel en hexadécimal ASCII, envoyé en type *str* : Fluent Bit
        # refuse un sel *bin* (« Invalid shared_key_salt type »).
        sel = os.urandom(16).hex().encode("ascii")
        cle = str(self._shared_key)
        utilisateur = str(self.options.get("user") or "") if sel_auth else ""
        mot_de_passe = (
            self._empreinte(sel_auth, utilisateur, str(self.options.get("password") or "")) if sel_auth else ""
        )
        ping = ["PING", self._self_hostname, sel, self._empreinte(sel, self._self_hostname, nonce, cle)]
        ping += [utilisateur, mot_de_passe]
        self._writer.write(msgpack.packb(ping, use_bin_type=False))
        await self._writer.drain()
        pong = await self._lire(self._connect_timeout)
        if not (isinstance(pong, list) and len(pong) == 5 and pong[0] == b"PONG"):
            raise ErreurForward(_("sortie fluent_forward : PONG attendu, reçu {recu}").format(recu=str(pong)[:80]))
        if not pong[1]:
            raison = self._texte(pong[2])
            raise ErreurForward(_("sortie fluent_forward : authentification refusée ({raison})").format(raison=raison))
        hote_serveur = pong[3] if isinstance(pong[3], bytes) else str(pong[3]).encode("utf-8")
        if self._texte(pong[4]) != self._empreinte(sel, hote_serveur, nonce, cle):
            raise ErreurForward(_("sortie fluent_forward : empreinte du serveur invalide"))
        logger.debug("FluentForwardOutput._poignee_de_main | sortie (serveur {})", self._texte(pong[3]))

    # -- encodage ---------------------------------------------------------

    @traced
    def _temps(self, doc: dict[str, Any]) -> bytes:
        """Temps de l'évènement, déjà encodé en MessagePack.

        Lu dans `time_key` du document (ISO 8601, `datetime` ou nombre),
        sinon maintenant. Rend un `EventTime` (extension 0 : secondes et
        nanosecondes sur 32 bits) ou, avec `time_as_integer`, des
        secondes entières.
        """
        valeur = doc.get(self._time_key)
        if isinstance(valeur, str):
            try:
                valeur = datetime.fromisoformat(valeur.replace("Z", "+00:00"))
            except ValueError:
                logger.debug("FluentForwardOutput._temps | {}={!r} illisible : maintenant", self._time_key, valeur)
        if isinstance(valeur, datetime):
            # Exact à la microseconde : pas de passage par un flottant.
            entier = math.floor(valeur.replace(microsecond=0).timestamp())
            nanos = valeur.microsecond * 1000
        else:
            nombre = valeur if isinstance(valeur, (int, float)) and not isinstance(valeur, bool) else time.time()
            entier = math.floor(nombre)
            nanos = min(round((nombre - entier) * 1_000_000) * 1000, 999_999_000)
        if self._time_as_integer:
            return msgpack.packb(entier)
        return ENTETE_EVENT_TIME + struct.pack(">II", entier, nanos)

    @traced
    def _entree(self, pending: dict[str, Any]) -> bytes:
        """Encode UNE entrée `[temps, document]` (le temps est déjà encodé)."""
        return b"\x92" + pending["time"] + msgpack.packb(pending["doc"], use_bin_type=True, default=str)

    @traced
    def _message(self, tag: str, pendings: list[dict[str, Any]], chunk: str | None) -> bytes:
        """Encode UN message forward (Forward, PackedForward ou compressé)."""
        option: dict[str, Any] = {"size": len(pendings)}
        if chunk is not None:
            option["chunk"] = chunk
        entrees = b"".join(self._entree(pending) for pending in pendings)
        if self._packed:
            if self._gzip:
                entrees = gzip.compress(entrees)
                option["compressed"] = "gzip"
            corps = msgpack.packb(entrees, use_bin_type=True)
        else:
            corps = msgpack.Packer().pack_array_header(len(pendings)) + entrees
        return b"\x93" + msgpack.packb(tag) + corps + msgpack.packb(option)

    # -- envoi ------------------------------------------------------------

    @atraced
    async def envoyer(self, doc: dict[str, Any]) -> SendOutcome:
        """Tamponne un document et envoie le lot quand il est plein.

        Args:
            doc: Document prêt à envoyer.

        Returns:
            `SendOutcome` de CE document : réel si le lot est parti avec
            lui, `EN_ATTENTE` s'il est seulement tamponné.
        """
        logger.debug("FluentForwardOutput.envoyer | entrée")
        self._pending.append({"doc": doc, "tag": render(self._tag_gabarit, doc), "time": self._temps(doc)})
        self.observateur.file(self.nom, len(self._pending))
        if len(self._pending) < self._batch_size:
            logger.debug("FluentForwardOutput.envoyer | document tamponné ({})", len(self._pending))
            logger.debug("FluentForwardOutput.envoyer | sortie")
            return SendOutcome.EN_ATTENTE
        outcomes = await self._flush()
        logger.debug("FluentForwardOutput.envoyer | sortie")
        return outcomes[-1]

    @atraced
    async def send(self, batch: Sequence[Document]) -> None:
        """Envoie un lot de documents (contrat `Output` #4)."""
        logger.debug("FluentForwardOutput.send | entrée ({} document(s))", len(batch))
        for doc in batch:
            outcome = await self.envoyer(dict(doc))
            logger.debug("FluentForwardOutput.send | {} → {}", outcome.value, doc)
        logger.debug("FluentForwardOutput.send | sortie")

    @atraced
    async def flush(self) -> None:
        """Pousse les documents en attente (contrat `Output` : arrêt propre)."""
        logger.debug("FluentForwardOutput.flush | entrée")
        await self._flush()
        logger.debug("FluentForwardOutput.flush | sortie")

    @atraced
    async def _flush(self) -> list[SendOutcome]:
        """Envoie le tampon et renvoie l'issue de chaque document (lot vide : `[]`)."""
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
    async def _envoyer_groupe(self, tag: str, pendings: list[dict[str, Any]]) -> None:
        """Envoie UN message (une étiquette) et attend son accusé si demandé.

        Raises:
            ErreurForward: accusé absent ou différent du `chunk` envoyé.
            OSError, TimeoutError: connexion ou écriture en échec.
        """
        await self.open()
        chunk = base64.b64encode(os.urandom(16)).decode("ascii") if self._require_ack else None
        self._writer.write(self._message(tag, pendings, chunk))
        await self._writer.drain()
        if chunk is not None:
            reponse = await self._lire(self._ack_timeout)
            if not (isinstance(reponse, dict) and self._texte(reponse.get(b"ack")) == chunk):
                raise ErreurForward(_("sortie fluent_forward : accusé inattendu {recu}").format(recu=str(reponse)[:80]))

    @atraced
    async def _issues_du_lot(self, pendings: list[dict[str, Any]]) -> list[SendOutcome]:
        """Envoie un lot (retentes comprises) et renvoie l'issue de chaque document.

        Un message par étiquette, dans l'ordre d'arrivée ; seuls les
        messages non acquittés sont retentés, sur une connexion neuve.
        """
        groupes: dict[str, list[dict[str, Any]]] = {}
        for pending in pendings:
            groupes.setdefault(pending["tag"], []).append(pending)
        reste = list(groupes.items())
        essai = 0
        erreur = ""
        while True:
            debut = time.perf_counter()
            echecs: list[tuple[str, list[dict[str, Any]]]] = []
            for position, (tag, groupe) in enumerate(reste):
                try:
                    await self._envoyer_groupe(tag, groupe)
                except (OSError, TimeoutError, ErreurForward, ssl.SSLError) as exc:
                    erreur = f"{type(exc).__name__} : {exc}"
                    logger.warning(
                        _("envoi fluent_forward échoué erreur={error} documents={nombre}"),
                        error=erreur,
                        nombre=sum(len(g) for _t, g in reste[position:]),
                    )
                    await self.close()  # flux désynchronisé : connexion neuve au prochain essai
                    echecs = reste[position:]
                    break
            self.observateur.latence(self.nom, time.perf_counter() - debut)
            if not echecs:
                return [SendOutcome.INDEXED] * len(pendings)
            reste = echecs
            if essai >= self._retry_max:
                break
            essai += 1
            self.observateur.reessai(self.nom)
            await asyncio.sleep(self._retry_backoff * 2 ** (essai - 1))
        en_echec = [pending for _t, groupe in reste for pending in groupe]
        logger.warning(
            _("envoi fluent_forward : réessais épuisés documents={nombre} erreur={error}"),
            nombre=len(en_echec),
            error=erreur,
        )
        echoues = {id(p) for p in en_echec}
        issues_echec = await self._transitoires_epuises(en_echec, erreur)
        issue_echec = issues_echec[0]
        return [issue_echec if id(p) in echoues else SendOutcome.INDEXED for p in pendings]

    @atraced
    async def _transitoires_epuises(self, pendings: list[dict[str, Any]], erreur: str) -> list[SendOutcome]:
        """Messages en échec, réessais épuisés : RETRYABLE, ou DLQ si `dlq_transitoires`."""
        if self._dlq is None or not self._dlq_transitoires:
            return [SendOutcome.RETRYABLE] * len(pendings)
        logger.warning(_("lot fluent_forward en échec transitoire mis en DLQ documents={nombre}"), nombre=len(pendings))
        for pending in pendings:
            await self._dlq.write(
                pending["doc"],
                index=pending["tag"],
                error=erreur,
                output=self.nom,
                reason=REASON_TRANSIENT_EXHAUSTED,
            )
        return [SendOutcome.DEAD_LETTERED] * len(pendings)
