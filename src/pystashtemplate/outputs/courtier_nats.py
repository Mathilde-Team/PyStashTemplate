# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie nats : publication asynchrone vers NATS, cœur ou JetStream.

Issue #33. Pas de sortie NATS officielle dans Logstash : les options
suivent le plugin communautaire `logstash-output-nats` et le client
`nats-py` :

- `url` (chaîne ou liste, `nats://127.0.0.1:4222` par défaut ; à défaut,
  l'option commune `hosts`) : les serveurs du groupe, dans l'ordre ;
- `subject` (**requis**, gabarit sprintf) : sujet rendu contre chaque
  document ;
- authentification : `user`/`password`, ou `token` ;
- `jetstream` (faux par défaut) : publication **JetStream**, chaque
  message attend l'accusé (`PubAck`) du flux qui couvre le sujet ;
  `stream` impose le nom du flux attendu ;
- `name` : nom du client vu par le serveur, `timeout` (5 s) ;
- `ssl_enabled` chiffre la connexion (`cacert`, `ssl`, `ssl_certificate`
  et `ssl_key` comme les autres sorties).

Sans JetStream, NATS ne garde rien (au plus une fois, rien n'est stocké
sans abonné) : le lot est confirmé par un `flush` (aller-retour
PING/PONG) qui prouve seulement que le serveur a tout reçu. Avec
JetStream, un sujet couvert par aucun flux (« no responders »), un
message trop gros ou une erreur d'API non 503 sont des refus définitifs
(DLQ si configurée) ; un délai dépassé ou un 503 sont transitoires.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, ClassVar

import nats
from loguru import logger
from nats.errors import MaxPayloadError, NoRespondersError
from nats.js.errors import APIError, NoStreamResponseError

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

__all__ = ["NatsOutput"]


@dataclass
class ClientNats:
    """Connexion NATS et, avec `jetstream`, son contexte JetStream."""

    connexion: Any
    jetstream: Any = None


@enregistrer
class NatsOutput(SortieCourtier):
    """Éditeur NATS asynchrone (cœur ou JetStream), branché sur le socle des courtiers."""

    plugin = "nats"
    OPTION_DESTINATION = "subject"

    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = {
        **OPTIONS_COURTIER,
        "url": OptionSpec("url", (str, list, tuple), description="Serveur(s) NATS (nats://127.0.0.1:4222 par défaut)"),
        "subject": OptionSpec("subject", (str,), description="Sujet de destination (gabarit sprintf)", requis=True),
        "token": OptionSpec("token", (str,), description="Jeton d'authentification NATS"),
        "jetstream": OptionSpec(
            "jetstream", (bool,), False, description="Publier en JetStream, avec accusé (faux par défaut)"
        ),
        "stream": OptionSpec("stream", (str,), description="Flux JetStream attendu pour le sujet"),
        "name": OptionSpec("name", (str,), description="Nom du client vu par le serveur"),
        "timeout": OptionSpec(
            "timeout", (int, float), 5, minimum=0, description="Délai réseau en secondes (5 par défaut)"
        ),
    }

    def __init__(self, options: dict[str, Any], *, client: Any = None, dlq: Any = None) -> None:
        """Valide les options NATS et prépare le tampon.

        Args:
            options: Options du bloc `output{}` (voir l'en-tête du module).
            client: Faux `ClientNats` (tests) ; `None` en connecte un au
                premier `open()`.
            dlq: DLQ toute construite (tests), prime sur `dlq_path`.
        """
        logger.debug("NatsOutput.__init__ | entrée")
        super().__init__(options, client=client, dlq=dlq)
        serveurs = options.get("url", options.get("hosts", "nats://127.0.0.1:4222"))
        self._serveurs = [serveurs] if isinstance(serveurs, str) else [str(serveur) for serveur in serveurs]
        self._jetstream = bool(options.get("jetstream", False))
        self._delai = float(options.get("timeout", 5))
        logger.debug("NatsOutput.__init__ | sortie ({} serveur(s))", len(self._serveurs))

    @traced
    def _args_connexion(self) -> dict[str, Any]:
        """Arguments de `nats.connect` : serveurs, authentification, TLS, délais."""
        args: dict[str, Any] = {
            "servers": self._serveurs,
            "connect_timeout": self._delai,
            # Les retentes sont celles de la sortie (retry_max) : chaque
            # serveur n'est essayé que deux fois à la connexion (0 vaudrait
            # « sans fin » pour nats-py), sans reconnexion en arrière-plan.
            "max_reconnect_attempts": 1,
            "reconnect_time_wait": 0,
            "allow_reconnect": False,
            "dont_randomize": True,
            "error_cb": self._erreur_client,
        }
        for option in ("user", "password", "token", "name"):
            if self.options.get(option):
                args[option] = str(self.options[option])
        contexte = self._tls()
        if contexte is not None:
            args["tls"] = contexte
        return args

    @atraced
    async def _erreur_client(self, exc: Exception) -> None:
        """Rappel d'erreur de nats-py : journalisée en debug (l'erreur remonte aussi à l'appel)."""
        logger.debug("NatsOutput._erreur_client | {} : {}", type(exc).__name__, exc)

    @atraced
    async def _connecter(self) -> Any:
        """Connecte le groupe de serveurs (et prépare le contexte JetStream)."""
        connexion = await nats.connect(**self._args_connexion())
        return ClientNats(connexion=connexion, jetstream=connexion.jetstream() if self._jetstream else None)

    @atraced
    async def _deconnecter(self, client: Any) -> None:
        """Vide les envois en cours puis ferme la connexion."""
        logger.debug("NatsOutput._deconnecter | entrée")
        await client.connexion.close()
        logger.debug("NatsOutput._deconnecter | sortie")

    @atraced
    async def _sante(self, client: Any) -> dict[str, Any]:
        """Serveur connecté et sa version ; avec JetStream, le compte doit y avoir accès."""
        await client.connexion.flush(timeout=self._delai)
        sante: dict[str, Any] = {
            "url": str(client.connexion.connected_url.geturl()) if client.connexion.connected_url else None,
            "version": str(client.connexion.connected_server_version),
        }
        if client.jetstream is not None:
            info = await client.jetstream.account_info()
            sante["streams"] = info.streams
        return sante

    @atraced
    async def _publier_lot(
        self, client: Any, messages: list[Message]
    ) -> list[None | EchecTransitoire | EchecDefinitif]:
        """Publie le lot : cœur (puis `flush`) ou JetStream (accusés attendus en parallèle)."""
        if client.jetstream is None:
            return await self._publier_coeur(client.connexion, messages)
        return list(await asyncio.gather(*(self._publier_js(client.jetstream, message) for message in messages)))

    @atraced
    async def _publier_coeur(self, connexion: Any, messages: list[Message]) -> list[None | EchecDefinitif]:
        """NATS cœur : publications tamponnées par le client, confirmées ensemble par un `flush`.

        Un message plus gros que `max_payload` est refusé avant tout envoi
        (définitif) ; une erreur du `flush` lève et vaut pour le lot.
        """
        issues: list[None | EchecDefinitif] = []
        for message in messages:
            try:
                await connexion.publish(message.destination, message.charge)
            except MaxPayloadError as exc:
                logger.debug("NatsOutput._publier_coeur | message trop gros : {}", exc)
                issues.append(EchecDefinitif(f"MaxPayloadError : {exc}"))
                continue
            issues.append(None)
        await connexion.flush(timeout=self._delai)
        return issues

    @atraced
    async def _publier_js(self, jetstream: Any, message: Message) -> None | EchecTransitoire | EchecDefinitif:
        """JetStream : un message, son accusé ; l'erreur est classée, pas levée."""
        try:
            await jetstream.publish(
                message.destination, message.charge, timeout=self._delai, stream=self.options.get("stream")
            )
        except (NoStreamResponseError, NoRespondersError, MaxPayloadError) as exc:
            logger.debug("NatsOutput._publier_js | refus : {}", type(exc).__name__)
            return EchecDefinitif(f"{type(exc).__name__} : {exc}")
        except APIError as exc:
            logger.debug("NatsOutput._publier_js | erreur d'API {} : {}", exc.code, exc.description)
            if exc.code == 503:
                return EchecTransitoire(f"APIError 503 : {exc.description}")
            return EchecDefinitif(f"APIError {exc.code} : {exc.description}")
        except Exception as exc:  # noqa: BLE001 — délai dépassé, connexion perdue : transitoire
            logger.debug("NatsOutput._publier_js | échec transitoire : {}", exc)
            return EchecTransitoire(f"{type(exc).__name__} : {exc}")
        return None
