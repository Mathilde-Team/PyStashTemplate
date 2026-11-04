# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie Graylog GELF (issue #26) : HTTP, TCP/TLS ou UDP.

Un document devient un message GELF 1.1, comme le plugin
`logstash-output-gelf` :

| Champ GELF | Source | Option |
|---|---|---|
| `version` | `"1.1"` | — |
| `host` | gabarit, `%{host}` par défaut (`host.name` si `host` est un objet) | `sender` |
| `short_message` | champ nommé, `short_message` par défaut, repli sur `message` | `short_message` |
| `full_message` | gabarit, `%{message}` par défaut (omis s'il ne se résout pas) | `full_message` |
| `timestamp` | `@timestamp`, en secondes depuis l'epoch | — |
| `level` | premier gabarit résolu, `["%{level}", "info"]` par défaut | `level` |
| `_<champ>` | tous les autres champs (`ship_metadata`), `_tags` (`ship_tags`), `custom_fields` | — |

Champs additionnels : les champs commençant par `@` et ceux déjà
portés par un champ GELF (`message`, `host`, `level`…) ne sont pas
répétés ; un `_` initial est retiré, `id` devient `__id` (`_id` est
réservé par GELF) ; les objets imbriqués sont aplatis (`a_b`), les
listes jointes par `, `, les booléens écrits `true`/`false` (GELF
n'accepte que chaînes et nombres) et tout caractère hors de
`[A-Za-z0-9_.-]` d'un nom devient `_`. Les niveaux nommés suivent
syslog (`emerg` 0 … `debug` 7).

## Transports

- `http` (défaut) : `POST /gelf` sur l'entrée GELF HTTP de Graylog. Un
  message par requête avec `batch_size => 1` ; au-delà, les messages
  partent en NDJSON et l'entrée doit avoir **Enable Bulk Receiving**
  activé (chaque message est terminé par un saut de ligne : l'entrée
  découpe le flux de la connexion, pas la requête). Graylog n'accuse pas réception élément par élément (202 sans
  corps) : statut HTTP global, même politique que les autres sorties
  (`accuse_par_evenement=False`) — 2xx = lot reçu ; 400/404 = échec
  permanent (DLQ si configurée) ; réseau, 429, 5xx = transitoire ;
  401/403 = lot rendu à l'appelant.
- `tcp` : connexion persistante, messages terminés par un octet nul,
  sans compression (limite de GELF TCP). TLS quand `hosts` commence par
  `tls://` (`ssl`, `cacert`, `ssl_certificate`/`ssl_key` comme les
  autres sorties).
- `udp` : un datagramme par message, compressé en gzip avec
  `compression => true`, découpé en morceaux GELF (en-tête `0x1e 0x0f`,
  identifiant de 8 octets, numéro et nombre de morceaux) au-delà de
  `chunksize` octets (1420 par défaut, comme Logstash) ; plus de 128
  morceaux = échec permanent.

TCP et UDP n'ont pas d'accusé applicatif : un message écrit sans
erreur de socket est compté reçu. Une erreur de socket est transitoire
(retentes `retry_max`/`retry_backoff`, puis lot rendu à l'appelant ou
DLQ avec `dlq_transitoires`) ; en TCP, la connexion est rouverte et le
reste du lot renvoyé — sémantique au moins une fois.

## Gabarit de pipeline d'exemple

```
output {
  gelf {
    hosts => ["http://graylog:12201"]
    transport => "http"   # ou "tcp", "udp"
  }
}
```
"""

from __future__ import annotations

import asyncio
import gzip
import json
import os
import socket
import ssl
import time
from datetime import datetime
from typing import Any, ClassVar
from urllib.parse import urlsplit

import httpx
from loguru import logger

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput, SendOutcome
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.templates import compile_template, render
from pystashtemplate.tracing import atraced, traced

__all__ = ["GelfOutput", "decouper_udp", "message_gelf"]

# Niveaux syslog (RFC 5424) : nom (et alias usuels) → valeur GELF.
NIVEAUX: dict[str, int] = {
    "emerg": 0,
    "emergency": 0,
    "panic": 0,
    "alert": 1,
    "crit": 2,
    "critical": 2,
    "fatal": 2,
    "err": 3,
    "error": 3,
    "warn": 4,
    "warning": 4,
    "notice": 5,
    "info": 6,
    "informational": 6,
    "debug": 7,
    "trace": 7,
}

# Champs du document déjà portés par un champ GELF : jamais répétés en `_xxx`.
_CHAMPS_GELF = frozenset({"version", "host", "short_message", "full_message", "timestamp", "level", "message"})

# En-tête d'un morceau GELF UDP (« chunked GELF ») et limites du format.
_MAGIE_MORCEAU = b"\x1e\x0f"
_ENTETE_MORCEAU = 12  # magie (2) + identifiant (8) + numéro (1) + nombre (1)
_MORCEAUX_MAX = 128

# Ports GELF usuels par transport, quand `hosts` n'en donne pas.
_PORT_DEFAUT = 12201


@traced
def _nom_champ(nom: str) -> str:
    """Nom de champ additionnel GELF valide (`[A-Za-z0-9_.-]`), sans `_` initial."""
    propre = "".join(c if c.isascii() and (c.isalnum() or c in "_.-") else "_" for c in nom)
    return propre.lstrip("_") or "champ"


@traced
def _valeur_gelf(valeur: Any) -> str | int | float:
    """Valeur d'un champ additionnel : chaîne ou nombre (seuls types GELF)."""
    if isinstance(valeur, bool):
        return "true" if valeur else "false"
    if isinstance(valeur, (int, float)):
        return valeur
    if isinstance(valeur, (list, tuple)):
        return ", ".join(str(_valeur_gelf(element)) for element in valeur)
    return str(valeur)


@traced
def _aplatir(prefixe: str, valeur: Any, sortie: dict[str, Any]) -> None:
    """Ajoute `valeur` sous `_<prefixe>`, en aplatissant les objets (`a_b`)."""
    if isinstance(valeur, dict):
        for cle, sous_valeur in valeur.items():
            _aplatir(f"{prefixe}_{_nom_champ(str(cle))}", sous_valeur, sortie)
        return
    if valeur is None:
        return
    sortie[f"_{prefixe}"] = _valeur_gelf(valeur)


@traced
def _horodatage(valeur: Any) -> float | None:
    """`@timestamp` (ISO 8601, `datetime` ou nombre) en secondes depuis l'epoch."""
    if isinstance(valeur, datetime):
        return valeur.timestamp()
    if isinstance(valeur, (int, float)) and not isinstance(valeur, bool):
        return float(valeur)
    if isinstance(valeur, str) and valeur:
        try:
            return datetime.fromisoformat(valeur.replace("Z", "+00:00")).timestamp()
        except ValueError:
            logger.warning(_("gelf : @timestamp illisible, champ timestamp omis : {}"), valeur)
    return None


@traced
def _niveau(valeur: str) -> int | None:
    """Niveau GELF d'une valeur rendue : nombre 0-7 ou nom syslog ; `None` sinon."""
    texte = valeur.strip().lower()
    if not texte or "%{" in texte:
        return None
    if texte.lstrip("-").isdigit():
        return min(7, max(0, int(texte)))
    return NIVEAUX.get(texte)


@traced
def message_gelf(
    doc: dict[str, Any],
    *,
    sender: Any,
    short_message: str = "short_message",
    full_message: Any = None,
    level: list[Any] | None = None,
    custom_fields: dict[str, Any] | None = None,
    ship_metadata: bool = True,
    ship_tags: bool = True,
) -> dict[str, Any]:
    """Message GELF 1.1 d'un document (fonction pure, voir la table du module).

    Args:
        doc: Document de l'événement.
        sender: Gabarit compilé du champ `host` (option `sender`).
        short_message: Nom du champ porté en `short_message` (repli `message`).
        full_message: Gabarit compilé de `full_message`, ou `None`.
        level: Gabarits compilés de `level`, essayés dans l'ordre.
        custom_fields: Champs ajoutés tels quels (préfixés `_`).
        ship_metadata: Envoyer les autres champs du document en `_xxx`.
        ship_tags: Envoyer `tags` en `_tags` (joints par `, `).

    Returns:
        Le message GELF, prêt à sérialiser en JSON.
    """
    if sender.source == "%{host}" and isinstance(doc.get("host"), dict):
        hote = str(doc["host"].get("name") or "")  # ECS : `host` est un objet
    else:
        hote = render(sender, doc, on_missing="keep")
    if not hote or "%{" in hote:
        hote = socket.gethostname()
    court = doc.get(short_message)
    if court in (None, ""):
        court = doc.get("message")
    message: dict[str, Any] = {
        "version": "1.1",
        "host": hote,
        "short_message": str(court) if court not in (None, "") else "-",
    }
    if full_message is not None:
        complet = render(full_message, doc, on_missing="keep")
        if complet and "%{" not in complet:
            message["full_message"] = complet
    horodatage = _horodatage(doc.get("@timestamp"))
    if horodatage is not None:
        message["timestamp"] = horodatage
    for gabarit in level or []:
        valeur = _niveau(render(gabarit, doc, on_missing="keep"))
        if valeur is not None:
            message["level"] = valeur
            break
    if ship_metadata:
        for cle, valeur in doc.items():
            if cle.startswith("@") or cle in _CHAMPS_GELF or cle == short_message or cle == "tags":
                continue
            nom = _nom_champ(cle)
            if nom == "id":
                nom = "_id"  # `_id` est réservé par GELF : `__id`, comme Logstash
            _aplatir(nom, valeur, message)
    if ship_tags and doc.get("tags"):
        message["_tags"] = _valeur_gelf(doc["tags"])
    for cle, valeur in (custom_fields or {}).items():
        _aplatir(_nom_champ(str(cle)), valeur, message)
    return message


@traced
def decouper_udp(donnees: bytes, chunksize: int, identifiant: bytes | None = None) -> list[bytes]:
    """Datagrammes GELF UDP d'un message : tel quel, ou en morceaux GELF.

    Args:
        donnees: Message sérialisé (et compressé le cas échéant).
        chunksize: Taille maximale d'un datagramme, en-tête compris.
        identifiant: Identifiant de 8 octets du message (aléatoire par défaut).

    Returns:
        Un datagramme si le message tient dans `chunksize`, sinon les
        morceaux (128 au plus).

    Raises:
        ValueError: plus de 128 morceaux nécessaires (message trop gros).
    """
    if len(donnees) <= chunksize:
        return [donnees]
    utile = chunksize - _ENTETE_MORCEAU
    nombre = -(-len(donnees) // utile)
    if nombre > _MORCEAUX_MAX:
        raise ValueError(
            _("sortie gelf : message de {} octets trop gros pour UDP ({} morceaux, {} au plus)").format(
                len(donnees), nombre, _MORCEAUX_MAX
            )
        )
    ident = identifiant if identifiant is not None else os.urandom(8)
    return [
        _MAGIE_MORCEAU + ident + bytes((numero, nombre)) + donnees[numero * utile : (numero + 1) * utile]
        for numero in range(nombre)
    ]


class _ProtocoleUdp(asyncio.DatagramProtocol):
    """Protocole datagramme minimal : mémorise la dernière erreur de socket."""

    @traced
    def __init__(self) -> None:
        """Aucune erreur au départ."""
        self.erreur: Exception | None = None

    @traced
    def error_received(self, exc: Exception) -> None:
        """Erreur de socket signalée par la boucle (ICMP port injoignable…)."""
        logger.debug("_ProtocoleUdp.error_received | {}", exc)
        self.erreur = exc


@enregistrer
class GelfOutput(ElasticsearchBulkOutput):
    """Sortie Graylog GELF : HTTP (`POST /gelf`), TCP/TLS (octet nul) ou UDP (morceaux)."""

    plugin = "gelf"
    capacites = Capacites(
        actions=frozenset({"index"}),
        accepte_id=False,
        accuse_par_evenement=False,
    )
    # Options du client _bulk sans objet pour GELF : `index`, `pipeline`,
    # `routing`, `doc_as_upsert` (spécifiques à l'API _bulk).
    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = dict(
        {
            nom: spec
            for nom, spec in ElasticsearchBulkOutput.OPTIONS_PROPRES.items()
            if nom not in ("index", "pipeline", "routing", "doc_as_upsert")
        },
        action=OptionSpec("action", (str,), "index", valeurs=("index",), description="Envois seuls (index)"),
        transport=OptionSpec(
            "transport",
            (str,),
            "http",
            valeurs=("http", "tcp", "udp"),
            description="Transport GELF (http par défaut, tcp, udp)",
        ),
        sender=OptionSpec("sender", (str,), "%{host}", description="Gabarit du champ host GELF"),
        short_message=OptionSpec(
            "short_message",
            (str,),
            "short_message",
            description="Champ porté en short_message (repli sur message)",
        ),
        full_message=OptionSpec("full_message", (str,), "%{message}", description="Gabarit de full_message"),
        level=OptionSpec(
            "level",
            (str, list, tuple),
            ["%{level}", "info"],
            description="Gabarit(s) du niveau, essayés dans l'ordre (nom syslog ou 0-7)",
        ),
        custom_fields=OptionSpec("custom_fields", (dict,), {}, description="Champs additionnels fixes"),
        ship_metadata=OptionSpec(
            "ship_metadata", (bool,), True, description="Envoyer les autres champs en _xxx (vrai par défaut)"
        ),
        ship_tags=OptionSpec("ship_tags", (bool,), True, description="Envoyer tags en _tags (vrai par défaut)"),
        chunksize=OptionSpec(
            "chunksize",
            (int,),
            1420,
            minimum=_ENTETE_MORCEAU + 1,
            description="UDP : taille maximale d'un datagramme (1420 par défaut)",
        ),
    )

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        dlq: LocalDeadLetterQueue | None = None,
        transport: Any = None,
    ) -> None:
        """Valide le transport et compile les gabarits du message GELF.

        Args:
            options: Options du bloc `output{}` (communes + propres, voir
                le module).
            dlq: DLQ toute construite (tests), prime sur `dlq_path`.
            transport: Transport httpx de remplacement (couture de test,
                transport `http` seulement).

        Raises:
            ValueError: `hosts` absent ou sans hôte, option inconnue ou
                valeur invalide (schéma commun, #8).
        """
        super().__init__(options, dlq=dlq, transport=transport)
        self._mode = str(options.get("transport", "http"))
        self._sender = compile_template(str(options.get("sender", "%{host}")))
        self._short_message = str(options.get("short_message", "short_message"))
        full = options.get("full_message", "%{message}")
        self._full_message = compile_template(str(full)) if full else None
        niveaux = options.get("level", ["%{level}", "info"])
        niveaux = [niveaux] if isinstance(niveaux, str) else list(niveaux)
        self._level = [compile_template(str(n)) for n in niveaux]
        self._custom_fields = dict(options.get("custom_fields") or {})
        self._ship_metadata = bool(options.get("ship_metadata", True))
        self._ship_tags = bool(options.get("ship_tags", True))
        self._chunksize = int(options.get("chunksize", 1420))
        self._compression = bool(options.get("compression", False))
        url = urlsplit(self._hosts if "://" in self._hosts else f"{self._mode}://{self._hosts}")
        if not url.hostname:
            logger.warning(_("sortie gelf : hôte absent de hosts {}"), self._hosts)
            raise ValueError(_("sortie gelf : hosts doit nommer un hôte (ex. http://graylog:12201)"))
        self._hote = url.hostname
        self._port = url.port or _PORT_DEFAUT
        self._tls = url.scheme in ("tls", "ssl")
        self._ecrivain: asyncio.StreamWriter | None = None
        self._udp: asyncio.DatagramTransport | None = None
        self._protocole_udp: _ProtocoleUdp | None = None
        logger.debug(
            "GelfOutput : transport {}, hôte {}:{}, TLS {}, compression {}",
            self._mode,
            self._hote,
            self._port,
            self._tls,
            self._compression,
        )

    @traced
    def _message(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Message GELF du document, selon les options du bloc."""
        return message_gelf(
            doc,
            sender=self._sender,
            short_message=self._short_message,
            full_message=self._full_message,
            level=self._level,
            custom_fields=self._custom_fields,
            ship_metadata=self._ship_metadata,
            ship_tags=self._ship_tags,
        )

    @traced
    def _json(self, doc: dict[str, Any]) -> bytes:
        """Message GELF sérialisé sur une seule ligne (exigé par le mode bulk)."""
        return json.dumps(self._message(doc), ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    @traced
    def _chemin_envoi(self) -> str:
        """`/gelf` (entrée GELF HTTP de Graylog)."""
        return "/gelf"

    @traced
    def _corps_requete(self, pendings: list[dict[str, Any]]) -> str:
        """Messages GELF du lot, chacun terminé par un saut de ligne (NDJSON du mode bulk).

        Le saut de ligne final compte : en réception par lot, Graylog
        découpe le flux de la connexion sur les sauts de ligne ; sans lui,
        le dernier message d'une requête reste en attente et se colle au
        premier de la requête suivante sur la même connexion (perdu).
        """
        return "".join(self._json(pending["doc"]).decode("utf-8") + "\n" for pending in pendings)

    @traced
    def _client_http(self) -> httpx.AsyncClient:
        """Client du client commun, en-tête `Content-Type: application/json`."""
        client = super()._client_http()
        client.headers["Content-Type"] = "application/json"
        return client

    @traced
    def _contexte_tls(self) -> ssl.SSLContext:
        """Contexte TLS du transport `tcp` (`tls://`) depuis les options communes."""
        contexte = ssl.create_default_context(
            cafile=str(self.options["cacert"]) if self.options.get("cacert") else None
        )
        if not bool(self.options.get("ssl", True)):
            contexte.check_hostname = False
            contexte.verify_mode = ssl.CERT_NONE
        if self.options.get("ssl_certificate"):
            contexte.load_cert_chain(str(self.options["ssl_certificate"]), str(self.options["ssl_key"]))
        return contexte

    @atraced
    async def _connexion_tcp(self) -> asyncio.StreamWriter:
        """Connexion TCP (ou TLS) persistante, ouverte au besoin."""
        if self._ecrivain is None or self._ecrivain.is_closing():
            _lecteur, self._ecrivain = await asyncio.wait_for(
                asyncio.open_connection(self._hote, self._port, ssl=self._contexte_tls() if self._tls else None),
                timeout=float(self.options.get("timeout", 30.0)),
            )
        return self._ecrivain

    @atraced
    async def _fermer_tcp(self) -> None:
        """Ferme la connexion TCP (après une erreur ou à la fermeture), sans lever."""
        ecrivain, self._ecrivain = self._ecrivain, None
        if ecrivain is None:
            return
        ecrivain.close()
        try:
            await ecrivain.wait_closed()
        except OSError as exc:
            logger.warning(_("gelf : fermeture de la connexion tcp en erreur : {}"), exc)

    @atraced
    async def _envoyer_tcp(self, pending: dict[str, Any]) -> None:
        """Écrit UN message terminé par un octet nul (sans compression : limite GELF TCP)."""
        ecrivain = await self._connexion_tcp()
        ecrivain.write(self._json(pending["doc"]) + b"\0")
        await ecrivain.drain()

    @atraced
    async def _socket_udp(self) -> asyncio.DatagramTransport:
        """Socket UDP « connectée » à la cible, ouverte au besoin."""
        if self._udp is None or self._udp.is_closing():
            boucle = asyncio.get_running_loop()
            self._udp, self._protocole_udp = await boucle.create_datagram_endpoint(
                _ProtocoleUdp, remote_addr=(self._hote, self._port)
            )
        return self._udp

    @atraced
    async def _envoyer_udp(self, pending: dict[str, Any]) -> None:
        """Envoie UN message en datagramme(s), compressé si `compression`.

        Raises:
            ValueError: message trop gros (plus de 128 morceaux).
            OSError: erreur de socket signalée par la boucle.
        """
        donnees = self._json(pending["doc"])
        if self._compression:
            donnees = gzip.compress(donnees)
        morceaux = decouper_udp(donnees, self._chunksize)
        socket_udp = await self._socket_udp()
        for morceau in morceaux:
            socket_udp.sendto(morceau)
        await asyncio.sleep(0)  # laisse la boucle remonter une erreur de socket
        if self._protocole_udp is not None and self._protocole_udp.erreur is not None:
            erreur, self._protocole_udp.erreur = self._protocole_udp.erreur, None
            raise OSError(str(erreur))

    @atraced
    async def _issues_du_lot(self, pendings: list[dict[str, Any]]) -> list[SendOutcome]:
        """HTTP : client commun ; TCP/UDP : envoi message par message, avec retentes."""
        if self._mode == "http":
            return await super()._issues_du_lot(pendings)
        envoyer = self._envoyer_tcp if self._mode == "tcp" else self._envoyer_udp
        outcomes: dict[int, SendOutcome] = {}
        reste = list(enumerate(pendings))
        essai = 0
        erreur = ""
        while True:
            debut = time.perf_counter()
            echecs: list[tuple[int, dict[str, Any]]] = []
            for position, (rang, pending) in enumerate(reste):
                try:
                    await envoyer(pending)
                except ValueError as exc:  # message trop gros : jamais valide tel quel
                    logger.warning(_("envoi gelf {transport} refusé erreur={error}"), transport=self._mode, error=exc)
                    outcomes[rang] = await self._permanent(pending, str(exc))
                except OSError as exc:
                    erreur = f"{type(exc).__name__} : {exc}"
                    logger.warning(
                        _("envoi gelf {transport} échoué erreur={error}"), transport=self._mode, error=erreur
                    )
                    if self._mode == "tcp":
                        await self._fermer_tcp()
                        # flux coupé : le reste du lot repart avec la connexion rouverte
                        echecs.extend(reste[position:])
                        break
                    echecs.append((rang, pending))
                else:
                    outcomes[rang] = SendOutcome.INDEXED
            self.observateur.latence(self.nom, time.perf_counter() - debut)
            reste = echecs
            if not reste or essai >= self._retry_max:
                break
            essai += 1
            self.observateur.reessai(self.nom)
            await asyncio.sleep(self._retry_backoff * 2 ** (essai - 1))
        if reste:
            logger.warning(
                _("envoi gelf : réessais épuisés documents={nombre} erreur={error}"), nombre=len(reste), error=erreur
            )
            epuises = await self._transitoires_epuises([pending for _rang, pending in reste], erreur)
            outcomes.update(zip((rang for rang, _pending in reste), epuises, strict=True))
        return [outcomes[rang] for rang in range(len(pendings))]

    @atraced
    async def open(self) -> None:
        """Ouvre la sortie : client HTTP paresseux (`http`), rien d'autre avant le premier envoi."""
        logger.debug("GelfOutput.open | entrée ({})", self._mode)
        if self._mode == "http":
            await super().open()
        logger.debug("GelfOutput.open | sortie")

    @atraced
    async def close(self) -> None:
        """Ferme client HTTP, connexion TCP et socket UDP (idempotent)."""
        logger.debug("GelfOutput.close | entrée")
        await super().close()
        await self._fermer_tcp()
        if self._udp is not None:
            self._udp.close()
            self._udp = None
        logger.debug("GelfOutput.close | sortie")

    @atraced
    async def health(self) -> dict[str, Any]:
        """Joignabilité de l'entrée : `HEAD /gelf` (HTTP) ou connexion (TCP) ; UDP sans contrôle.

        Returns:
            `{"transport": ..., "status": "ok" | "unchecked"}`.

        Raises:
            httpx.HTTPError: entrée HTTP injoignable ou en erreur (5xx).
            OSError: connexion TCP refusée.
        """
        if self._mode == "http":
            reponse = await self._client_http().head("/gelf")
            if reponse.status_code >= 500:
                reponse.raise_for_status()
            return {"transport": "http", "status": "ok"}
        if self._mode == "tcp":
            await self._connexion_tcp()
            return {"transport": "tcp", "status": "ok"}
        return {"transport": "udp", "status": "unchecked"}
