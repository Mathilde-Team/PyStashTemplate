# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie rabbitmq : publication AMQP 0-9-1 asynchrone vers RabbitMQ (et compatibles : LavinMQ).

Issue #33. Options reprises du plugin `logstash-output-rabbitmq`,
défauts compris :

- `host` (chaîne ou liste, `localhost` par défaut ; à défaut, l'option
  commune `hosts`) : les nœuds sont essayés dans l'ordre à la connexion ;
  `port` (5672), `vhost` (`/`), `user`/`password` (`guest`/`guest`) ;
- `exchange` (**requis**) et `exchange_type` (**requis** : `direct`,
  `fanout`, `topic`, `headers`, `x-consistent-hash`, `x-modulus-hash`) ;
  l'échange est déclaré à la connexion (`durable`, vrai par défaut), ou
  seulement vérifié avec `passive` ;
- `key` : clé de routage, gabarit sprintf rendu contre chaque document
  (`logstash` par défaut) ;
- `persistent` (vrai) : messages en mode de livraison persistant ;
- `message_properties` : propriétés AMQP (`content_type`, `priority`,
  `app_id`, `type`…), `content_type` vaut `application/json` par défaut ;
- `ssl_enabled` chiffre la connexion (`cacert`, `ssl`, `ssl_certificate`
  et `ssl_key` comme les autres sorties) ; `heartbeat` en secondes.

Le canal est ouvert avec les **confirmations d'éditeur** : chaque
publication attend l'accusé du courtier. Un `nack` est transitoire (le
courtier n'a pas pu prendre le message), comme toute erreur de canal ou
de connexion — la connexion est alors rouverte au prochain essai. Comme
Logstash (`mandatory` non positionné), un message sans file liée à sa
clé de routage est accepté puis écarté par le courtier : lier une file à
l'échange avant de publier.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, ClassVar

import aio_pika
from aio_pika.exceptions import DeliveryError
from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.outputs import enregistrer
from pystashtemplate.outputs.courtier import (
    OPTIONS_COURTIER,
    EchecDefinitif,
    EchecTransitoire,
    Message,
    SortieCourtier,
)
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.tracing import atraced, traced

__all__ = ["RabbitMqOutput"]

TYPES_ECHANGE = ("direct", "fanout", "topic", "headers", "x-consistent-hash", "x-modulus-hash")

# Propriétés AMQP de `message_properties` acceptées (noms de aio_pika.Message).
PROPRIETES = ("content_type", "content_encoding", "priority", "correlation_id", "reply_to", "expiration")
PROPRIETES += ("message_id", "type", "user_id", "app_id")


@dataclass(frozen=True)
class Nack(EchecTransitoire):
    """Le courtier a refusé de prendre le message (`basic.nack`) : la connexion reste saine."""


@dataclass
class ClientAmqp:
    """Connexion, canal (confirmations d'éditeur) et échange déclaré."""

    connexion: Any
    canal: Any
    echange: Any


@enregistrer
class RabbitMqOutput(SortieCourtier):
    """Éditeur AMQP asynchrone avec confirmations, branché sur le socle des courtiers."""

    plugin = "rabbitmq"
    OPTION_DESTINATION = "key"
    DESTINATION_DEFAUT = "logstash"

    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = {
        **OPTIONS_COURTIER,
        "host": OptionSpec("host", (str, list, tuple), description="Nœud(s) RabbitMQ (localhost par défaut)"),
        "port": OptionSpec("port", (int,), 5672, minimum=1, description="Port AMQP (5672 par défaut)"),
        "vhost": OptionSpec("vhost", (str,), "/", description="Hôte virtuel (/ par défaut)"),
        "exchange": OptionSpec("exchange", (str,), description="Échange de destination", requis=True),
        "exchange_type": OptionSpec(
            "exchange_type",
            (str,),
            valeurs=TYPES_ECHANGE,
            description="Type de l'échange (direct, fanout, topic…)",
            requis=True,
        ),
        "key": OptionSpec("key", (str,), "logstash", description="Clé de routage (gabarit sprintf, logstash)"),
        "durable": OptionSpec("durable", (bool,), True, description="Échange durable (vrai par défaut)"),
        "passive": OptionSpec(
            "passive", (bool,), False, description="Vérifier l'échange sans le déclarer (faux par défaut)"
        ),
        "persistent": OptionSpec(
            "persistent", (bool,), True, description="Messages en livraison persistante (vrai par défaut)"
        ),
        "message_properties": OptionSpec(
            "message_properties", (dict,), description="Propriétés AMQP des messages (content_type…)"
        ),
        "heartbeat": OptionSpec("heartbeat", (int,), minimum=0, description="Battement de cœur AMQP (secondes)"),
        "timeout": OptionSpec(
            "timeout", (int, float), 5, minimum=0, description="Délai de connexion en secondes (5 par défaut)"
        ),
    }

    def __init__(self, options: dict[str, Any], *, client: Any = None, dlq: Any = None) -> None:
        """Valide les options AMQP et prépare le tampon.

        Args:
            options: Options du bloc `output{}` (voir l'en-tête du module).
            client: Faux `ClientAmqp` (tests) ; `None` en connecte un au
                premier `open()`.
            dlq: DLQ toute construite (tests), prime sur `dlq_path`.

        Raises:
            ValueError: `exchange`/`exchange_type` absent, propriété AMQP
                inconnue…
        """
        logger.debug("RabbitMqOutput.__init__ | entrée")
        super().__init__(options, client=client, dlq=dlq)
        hotes = options.get("host", options.get("hosts", "localhost"))
        self._hotes = [hotes] if isinstance(hotes, str) else [str(hote) for hote in hotes]
        if not self._hotes:
            logger.debug("RabbitMqOutput.__init__ | host vide")
            raise ValueError(_("sortie rabbitmq : host ne peut pas être une liste vide"))
        self._proprietes = dict(options.get("message_properties") or {})
        inconnues = sorted(set(self._proprietes) - set(PROPRIETES))
        if inconnues:
            logger.debug("RabbitMqOutput.__init__ | propriétés inconnues {}", inconnues)
            raise ValueError(
                _("sortie rabbitmq : propriété(s) de message inconnue(s) {inconnues} (connues : {connues})").format(
                    inconnues=", ".join(inconnues), connues=", ".join(PROPRIETES)
                )
            )
        self._proprietes.setdefault("content_type", "application/json")
        if "priority" in self._proprietes:
            self._proprietes["priority"] = int(self._proprietes["priority"])
        logger.debug("RabbitMqOutput.__init__ | sortie ({} nœud(s))", len(self._hotes))

    @traced
    def _args_connexion(self, hote: str) -> dict[str, Any]:
        """Arguments de `aio_pika.connect` pour un nœud (`hôte` ou `hôte:port`)."""
        nom, separateur, port = hote.rpartition(":")
        args: dict[str, Any] = {
            "host": nom if separateur and port.isdigit() else hote,
            "port": int(port) if separateur and port.isdigit() else int(self.options.get("port", 5672)),
            "login": str(self.options.get("user", "guest")),
            "password": str(self.options.get("password", "guest")),
            "virtualhost": str(self.options.get("vhost", "/")),
            "timeout": float(self.options.get("timeout", 5)),
        }
        contexte = self._tls()
        if contexte is not None:
            args.update(ssl=True, ssl_context=contexte)
        if self.options.get("heartbeat") is not None:
            args["heartbeat"] = int(self.options["heartbeat"])
        return args

    @atraced
    async def _connecter(self) -> Any:
        """Connecte le premier nœud joignable, ouvre le canal et déclare l'échange.

        Raises:
            ConnectionError: aucun nœud joignable (dernière erreur chaînée).
        """
        derniere: Exception | None = None
        for hote in self._hotes:
            try:
                connexion = await aio_pika.connect(**self._args_connexion(hote))
            except (OSError, asyncio.TimeoutError, aio_pika.exceptions.AMQPError) as exc:
                logger.debug("RabbitMqOutput._connecter | {} injoignable : {}", hote, exc)
                derniere = exc
                continue
            try:
                canal = await connexion.channel(publisher_confirms=True)
                echange = await self._echange(canal)
            except BaseException as exc:
                logger.debug("RabbitMqOutput._connecter | canal ou échange en échec : {}", exc)
                await connexion.close()
                raise
            return ClientAmqp(connexion=connexion, canal=canal, echange=echange)
        raise ConnectionError(_("sortie rabbitmq : aucun nœud joignable ({})").format(derniere)) from derniere

    @atraced
    async def _echange(self, canal: Any) -> Any:
        """Échange de destination : celui par défaut (nom vide), vérifié (`passive`) ou déclaré."""
        nom = str(self.options["exchange"])
        if not nom:
            return canal.default_exchange
        return await canal.declare_exchange(
            nom,
            str(self.options["exchange_type"]),
            durable=bool(self.options.get("durable", True)),
            passive=bool(self.options.get("passive", False)),
        )

    @atraced
    async def _deconnecter(self, client: Any) -> None:
        """Ferme la connexion AMQP (le canal avec elle)."""
        logger.debug("RabbitMqOutput._deconnecter | entrée")
        await client.connexion.close()
        logger.debug("RabbitMqOutput._deconnecter | sortie")

    @atraced
    async def _sante(self, client: Any) -> dict[str, Any]:
        """Produit et version annoncés par le courtier, échange vérifié (déclaration passive)."""
        proprietes = getattr(getattr(client.connexion, "transport", None), "connection", None)
        serveur = dict(getattr(proprietes, "server_properties", None) or {})
        if self.options["exchange"]:
            await client.canal.declare_exchange(str(self.options["exchange"]), passive=True)
        return {
            "product": serveur.get("product"),
            "version": serveur.get("version"),
            "exchange": str(self.options["exchange"]),
        }

    @traced
    def _message_amqp(self, message: Message) -> Any:
        """Message AMQP : charge utile, propriétés et mode de livraison."""
        mode = aio_pika.DeliveryMode.PERSISTENT if self.options.get("persistent", True) else None
        return aio_pika.Message(message.charge, delivery_mode=mode, **self._proprietes)

    @atraced
    async def _publier_un(self, echange: Any, message: Message) -> None | EchecTransitoire | EchecDefinitif:
        """Publie un message et attend sa confirmation ; l'erreur est rendue, pas levée."""
        try:
            await echange.publish(self._message_amqp(message), routing_key=message.destination, mandatory=False)
        except DeliveryError as exc:
            logger.debug("RabbitMqOutput._publier_un | nack : {}", type(exc).__name__)
            return Nack(f"nack : {type(exc).__name__}")
        except Exception as exc:  # noqa: BLE001 — canal ou connexion perdus : rendue au lot
            logger.debug("RabbitMqOutput._publier_un | publication en échec : {}", exc)
            return EchecTransitoire(f"{type(exc).__name__} : {exc}")
        return None

    @atraced
    async def _publier_lot(
        self, client: Any, messages: list[Message]
    ) -> list[None | EchecTransitoire | EchecDefinitif]:
        """Publie le lot en parallèle (confirmations attendues ensemble).

        Une erreur autre qu'un `nack` signale un canal ou une connexion
        perdus : le client est abandonné, le prochain essai le rouvre.
        """
        issues = list(await asyncio.gather(*(self._publier_un(client.echange, message) for message in messages)))
        if any(isinstance(issue, EchecTransitoire) and not isinstance(issue, Nack) for issue in issues):
            await self._reconnecter()
        return issues
