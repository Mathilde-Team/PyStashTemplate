# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie redis : publication asynchrone vers Redis (et compatibles : Valkey, KeyDB).

Issue #33. Options reprises du plugin `logstash-output-redis`, défauts
compris :

- `host` (chaîne ou liste de `hôte` / `hôte:port`, `127.0.0.1` par
  défaut ; à défaut, l'option commune `hosts`) : les nœuds sont essayés
  dans l'ordre à la connexion, le premier qui répond à `PING` sert ;
- `port` (6379), `db` (0), `user`/`password` (ACL Redis 6+, ou mot de
  passe seul), `timeout` (5 s) ;
- `key` (**requise**, gabarit sprintf) : nom de la liste, du canal ou
  du flux, rendu contre chaque document ;
- `data_type` : `list` (défaut, `RPUSH` — la file que relit l'entrée
  `redis` de Logstash), `channel` (`PUBLISH`, feu-et-oubli : rien n'est
  gardé sans abonné) ou `stream` (`XADD`, champ `message`, extension
  absente de Logstash ; `stream_maxlen` borne le flux, approximativement) ;
- `ssl_enabled` chiffre la connexion (`cacert`, `ssl`, `ssl_certificate`
  et `ssl_key` comme les autres sorties).

Un lot part dans **un seul pipeline** Redis (aller-retour unique, sans
transaction) ; la réponse est analysée commande par commande : une
erreur de commande (`WRONGTYPE` : la clé existe avec un autre type…) est
un refus définitif de ce document — DLQ si configurée —, une erreur de
connexion un échec transitoire de tout le lot.
"""

from __future__ import annotations

from typing import Any, ClassVar

from loguru import logger
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

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

__all__ = ["RedisOutput"]

TYPES_DE_DONNEES = ("list", "channel", "stream")


@enregistrer
class RedisOutput(SortieCourtier):
    """Producteur Redis asynchrone (liste, canal ou flux), branché sur le socle des courtiers."""

    plugin = "redis"
    OPTION_DESTINATION = "key"

    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = {
        **OPTIONS_COURTIER,
        "host": OptionSpec(
            "host",
            (str, list, tuple),
            description="Nœud(s) Redis hôte ou hôte:port (127.0.0.1 par défaut)",
        ),
        "port": OptionSpec("port", (int,), 6379, minimum=1, description="Port par défaut des nœuds (6379)"),
        "db": OptionSpec("db", (int,), 0, minimum=0, description="Numéro de base Redis (0 par défaut)"),
        "timeout": OptionSpec(
            "timeout", (int, float), 5, minimum=0, description="Délai réseau en secondes (5 par défaut)"
        ),
        "key": OptionSpec(
            "key",
            (str,),
            description="Liste, canal ou flux de destination (gabarit sprintf)",
            requis=True,
        ),
        "data_type": OptionSpec(
            "data_type",
            (str,),
            "list",
            valeurs=TYPES_DE_DONNEES,
            description="list (RPUSH, défaut), channel (PUBLISH) ou stream (XADD)",
        ),
        "stream_maxlen": OptionSpec(
            "stream_maxlen",
            (int,),
            minimum=1,
            description="Longueur maximale approximative du flux (data_type stream)",
        ),
    }

    def __init__(self, options: dict[str, Any], *, client: Any = None, dlq: Any = None) -> None:
        """Valide les options Redis et prépare le tampon.

        Args:
            options: Options du bloc `output{}` (voir l'en-tête du module).
            client: Faux client Redis (tests) ; `None` en construit un au
                premier `open()`.
            dlq: DLQ toute construite (tests), prime sur `dlq_path`.

        Raises:
            ValueError: `key` absente, `data_type` inconnu, nœud mal formé…
        """
        logger.debug("RedisOutput.__init__ | entrée")
        super().__init__(options, client=client, dlq=dlq)
        self._data_type = str(options.get("data_type", "list"))
        self._noeuds = self._noeuds_de(options.get("host", options.get("hosts")), int(options.get("port", 6379)))
        logger.debug("RedisOutput.__init__ | sortie ({} nœud(s))", len(self._noeuds))

    @traced
    def _noeuds_de(self, hotes: Any, port: int) -> list[tuple[str, int]]:
        """Normalise `host` (chaîne ou liste, `hôte` ou `hôte:port`) en couples (hôte, port).

        Raises:
            ValueError: port non numérique ou liste vide.
        """
        if hotes is None:
            hotes = ["127.0.0.1"]
        if isinstance(hotes, str):
            hotes = [hotes]
        noeuds: list[tuple[str, int]] = []
        for hote in hotes:
            texte = str(hote).strip()
            nom, separateur, port_texte = texte.rpartition(":")
            if separateur and not texte.endswith("]"):
                if not port_texte.isdigit():
                    raise ValueError(
                        _("sortie redis : nœud {noeud!r} invalide (hôte ou hôte:port)").format(noeud=texte)
                    )
                noeuds.append((nom.strip("[]"), int(port_texte)))
            else:
                noeuds.append((texte.strip("[]"), port))
        if not noeuds:
            raise ValueError(_("sortie redis : host ne peut pas être une liste vide"))
        return noeuds

    @traced
    def _args_client(self, hote: str, port: int) -> dict[str, Any]:
        """Arguments de `redis.asyncio.Redis` pour un nœud (authentification et TLS compris)."""
        delai = float(self.options.get("timeout", 5))
        args: dict[str, Any] = {
            "host": hote,
            "port": port,
            "db": int(self.options.get("db", 0)),
            "socket_timeout": delai,
            "socket_connect_timeout": delai,
        }
        if self.options.get("password"):
            args["password"] = str(self.options["password"])
        if self.options.get("user"):
            args["username"] = str(self.options["user"])
        if self.options.get("ssl_enabled", False):
            verifie = bool(self.options.get("ssl", True))
            args.update(ssl=True, ssl_cert_reqs="required" if verifie else "none", ssl_check_hostname=verifie)
            if self.options.get("cacert"):
                args["ssl_ca_certs"] = str(self.options["cacert"])
            if self.options.get("ssl_certificate"):
                args["ssl_certfile"] = str(self.options["ssl_certificate"])
                args["ssl_keyfile"] = self.options.get("ssl_key")
        return args

    @atraced
    async def _connecter(self) -> Any:
        """Essaie les nœuds dans l'ordre ; garde le premier qui répond à `PING`.

        Raises:
            ConnectionError: aucun nœud joignable (la dernière erreur est
                chaînée).
        """
        derniere: Exception | None = None
        for hote, port in self._noeuds:
            client = Redis(**self._args_client(hote, port))
            try:
                await client.ping()
            except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
                logger.debug("RedisOutput._connecter | {}:{} injoignable : {}", hote, port, exc)
                derniere = exc
                await client.aclose()
                continue
            return client
        raise ConnectionError(_("sortie redis : aucun nœud joignable ({})").format(derniere)) from derniere

    @atraced
    async def _deconnecter(self, client: Any) -> None:
        """Ferme le client Redis et son pool."""
        logger.debug("RedisOutput._deconnecter | entrée")
        await client.aclose()
        logger.debug("RedisOutput._deconnecter | sortie")

    @atraced
    async def _sante(self, client: Any) -> dict[str, Any]:
        """`PING` puis version du serveur (`INFO server`)."""
        await client.ping()
        info = await client.info("server")
        return {"ping": True, "version": info.get("redis_version") or info.get("valkey_version")}

    @atraced
    async def _publier_lot(
        self, client: Any, messages: list[Message]
    ) -> list[None | EchecTransitoire | EchecDefinitif]:
        """Un pipeline pour tout le lot ; chaque réponse de commande analysée.

        Une erreur de commande (`ResponseError`) refuse définitivement son
        document ; toute autre exception renvoyée dans la réponse est
        transitoire. Une erreur de connexion lève et vaut pour le lot
        entier (voir `SortieCourtier._publier`).
        """
        pipeline = client.pipeline(transaction=False)
        for message in messages:
            if self._data_type == "list":
                pipeline.rpush(message.destination, message.charge)
            elif self._data_type == "channel":
                pipeline.publish(message.destination, message.charge)
            else:
                maxlen = self.options.get("stream_maxlen")
                pipeline.xadd(
                    message.destination,
                    {"message": message.charge},
                    maxlen=int(maxlen) if maxlen else None,
                    approximate=True,
                )
        reponses = await pipeline.execute(raise_on_error=False)
        issues: list[None | EchecTransitoire | EchecDefinitif] = []
        for reponse in reponses:
            if isinstance(reponse, ResponseError):
                issues.append(EchecDefinitif(f"ResponseError : {reponse}"))
            elif isinstance(reponse, Exception):
                issues.append(EchecTransitoire(f"{type(reponse).__name__} : {reponse}"))
            else:
                issues.append(None)
        return issues
