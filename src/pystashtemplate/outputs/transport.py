# SPDX-License-Identifier: GPL-3.0-or-later
"""Transport HTTP asynchrone commun des sorties (chantier #5).

Un seul transport pour toutes les sorties HTTP (famille `_bulk`
Elasticsearch/OpenSearch, Loki, OTLP…) :

- pool de connexions `aiohttp` en keep-alive, tailles et délais
  configurables ;
- TLS (autorité de certification) et mTLS (certificat client) ;
- authentifications : basique, `Authorization: Bearer`, `ApiKey`
  (Elasticsearch), en-têtes libres (`X-P-Stream`, `X-Scope-OrgID`…) ;
- compression gzip de la requête, en option ;
- réessais avec backoff exponentiel et jitter, sur délai dépassé, erreur
  de connexion, `429` et `5xx` ; `Retry-After` (secondes ou date HTTP)
  est respecté quand il dépasse le backoff ; tout autre `4xx` est
  définitif : aucun réessai, la réponse remonte telle quelle (c'est à
  l'appelant — DLQ et réessais, #7 — d'en décider) ;
- épuisement des réessais réseau : `TransportErreur` (échec transitoire,
  l'appelant peut rejouer).

Les authentifications ne passent jamais dans un journal ; les messages
d'erreur ne mentionnent que des codes et des hôtes. Les secrets
viennent de la substitution `${VAR}` du `.conf` (#122), résolue avant.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import random
import ssl
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Callable

import aiohttp
from loguru import logger

from pystashtemplate.i18n import _


class TransportErreur(Exception):
    """Échec réseau (délai dépassé, connexion impossible) après tous les réessais."""


@dataclass(frozen=True)
class AuthBasique:
    """Authentification HTTP basique (le mot de passe ne doit jamais être journalisé)."""

    utilisateur: str
    mot_de_passe: str


@dataclass(frozen=True)
class AuthBearer:
    """Authentification `Authorization: Bearer <jeton>`."""

    jeton: str


@dataclass(frozen=True)
class AuthApiKey:
    """Authentification `ApiKey` Elasticsearch : `id:cle` encodé en base64."""

    identifiant: str
    cle: str


Authentification = AuthBasique | AuthBearer | AuthApiKey


@dataclass(frozen=True)
class ReponseHTTP:
    """Réponse du serveur, aussi bien en succès qu'en échec définitif.

    Args:
        statut: Code HTTP (200, 400…).
        corps: Corps de la réponse, en octets.
        en_tetes: En-têtes de réponse (dont `Retry-After`).
    """

    statut: int
    corps: bytes
    en_tetes: dict[str, str]


class TransportHTTP:
    """Client HTTP asynchrone partagé par les sorties (voir le module).

    Un `TransportHTTP` est ouvert (`open()`), sert plusieurs `envoyer()`,
    puis est fermé (`close()`). Il est utilisable en `async with`.
    """

    def __init__(
        self,
        *,
        timeout_total: float = 30.0,
        timeout_connexion: float = 10.0,
        connexions_max: int = 20,
        keepalive: float = 75.0,
        verification_tls: bool = True,
        ca_certificats: str | None = None,
        certificat_client: str | None = None,
        cle_client: str | None = None,
        essais_max: int = 3,
        delai_initial: float = 1.0,
        delai_max: float = 60.0,
        alea: Callable[[], float] | None = None,
    ) -> None:
        """Configure le transport ; rien n'est connecté avant `open()`.

        Args:
            timeout_total: Délai maximal d'une requête complète (réessais
                non compris).
            timeout_connexion: Délai maximal d'établissement d'une connexion.
            connexions_max: Taille du pool de connexions.
            keepalive: Durée de vie d'une connexion inactive du pool.
            verification_tls: `False` équivaut à
                `ssl_verification_mode => none` (certificats non vérifiés).
            ca_certificats: Fichier PEM des autorités
                (`ssl_certificate_authorities`) ; le système sinon.
            certificat_client: Certificat client mTLS (`ssl_certificate`),
                avec sa clé `cle_client`.
            cle_client: Clé privée PEM du certificat client (`ssl_key`).
            essais_max: Nombre total de tentatives (réseau et statuts
                réessayables compris).
            delai_initial: Base du backoff exponentiel (secondes).
            delai_max: Plafond du délai entre deux tentatives : même un
                `Retry-After` énorme ne bloque pas la sortie au-delà.
            alea: Source du jitter (0–1), injectée par les tests ; `random`
                sinon.
        """
        logger.debug("TransportHTTP.__init__ | pool {}, essais {}", connexions_max, essais_max)
        self._timeout_total = timeout_total
        self._timeout_connexion = timeout_connexion
        self._connexions_max = connexions_max
        self._keepalive = keepalive
        self._verification_tls = verification_tls
        self._ca_certificats = ca_certificats
        self._certificat_client = certificat_client
        self._cle_client = cle_client
        self._essais_max = essais_max
        self._delai_initial = delai_initial
        self._delai_max = delai_max
        self._alea: Callable[[], float] = alea if alea is not None else random.random
        self._session: aiohttp.ClientSession | None = None
        logger.debug("TransportHTTP.__init__ | configuré")

    def _contexte_ssl(self) -> ssl.SSLContext:
        """Contexte TLS du pool : vérification, autorités, certificat client."""
        logger.debug("_contexte_ssl | verification={}", self._verification_tls)
        if self._verification_tls:
            contexte = ssl.create_default_context(cafile=self._ca_certificats)
        else:
            contexte = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            contexte.check_hostname = False
            contexte.verify_mode = ssl.CERT_NONE
        if self._certificat_client:
            if not self._cle_client:
                logger.debug("_contexte_ssl | certificat client sans clé : refusé")
                raise ValueError("mTLS : certificat client sans clé privée")
            contexte.load_cert_chain(self._certificat_client, self._cle_client)
        logger.debug("_contexte_ssl | contexte prêt")
        return contexte

    async def open(self) -> None:
        """Ouvre le pool de connexions (keep-alive) ; sans effet si déjà ouvert."""
        logger.debug("open |")
        if self._session is not None:
            logger.debug("open | déjà ouvert")
        else:
            connecteur = aiohttp.TCPConnector(
                limit=self._connexions_max,
                keepalive_timeout=self._keepalive,
                ssl=self._contexte_ssl(),
            )
            delais = aiohttp.ClientTimeout(total=self._timeout_total, connect=self._timeout_connexion)
            self._session = aiohttp.ClientSession(connector=connecteur, timeout=delais)
            logger.debug("open | pool ouvert ({} connexions max)", self._connexions_max)

    async def close(self) -> None:
        """Ferme le pool ; les requêtes en cours attendent leur fin."""
        logger.debug("close |")
        if self._session is not None:
            await self._session.close()
            self._session = None
            logger.debug("close | pool fermé")
        else:
            logger.debug("close | déjà fermé")

    async def __aenter__(self) -> "TransportHTTP":
        """`open()` puis sert de gestionnaire asynchrone."""
        logger.debug("__aenter__ |")
        await self.open()
        logger.debug("__aenter__ | ouvert")
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        """`close()` dans tous les cas (l'exception, s'il y en a une, remonte)."""
        logger.debug("__aexit__ | exception : {}", type(exc).__name__ if exc else "aucune")
        await self.close()
        logger.debug("__aexit__ | fermé")

    def _entetes_auth(self, authentification: Authentification) -> dict[str, str]:
        """En-têtes d'authentification (jamais journalisés : secrets)."""
        logger.debug("_entetes_auth | type {}", type(authentification).__name__)
        if isinstance(authentification, AuthBasique):
            brut = f"{authentification.utilisateur}:{authentification.mot_de_passe}".encode("utf-8")
            entetes = {"Authorization": f"Basic {base64.b64encode(brut).decode('ascii')}"}
            logger.debug("_entetes_auth | Basic prêt")
            return entetes
        if isinstance(authentification, AuthBearer):
            entetes = {"Authorization": f"Bearer {authentification.jeton}"}
            logger.debug("_entetes_auth | Bearer prêt")
            return entetes
        brut = f"{authentification.identifiant}:{authentification.cle}".encode("utf-8")
        entetes = {"Authorization": f"ApiKey {base64.b64encode(brut).decode('ascii')}"}
        logger.debug("_entetes_auth | ApiKey prêt")
        return entetes

    def _secondes_retry_after(self, valeur: str) -> float | None:
        """`Retry-After` en secondes, ou `None` s'il est illisible/périmé."""
        logger.debug("_secondes_retry_after |")
        try:
            secondes = float(valeur)
        except ValueError:
            logger.debug("_secondes_retry_after | pas un nombre")
            secondes = None
        if secondes is not None:
            logger.debug("_secondes_retry_after | {} s", secondes)
            return secondes
        try:
            date = parsedate_to_datetime(valeur)
        except (TypeError, ValueError):
            logger.debug("_secondes_retry_after | date HTTP illisible")
            return None
        reste = (date - datetime.now(UTC)).total_seconds()
        if reste < 0:
            logger.debug("_secondes_retry_after | date déjà passée")
            return 0.0
        logger.debug("_secondes_retry_after | {} s", reste)
        return reste

    def _delai(self, tentative: int, retry_after: str | None) -> float:
        """Délai avant la tentative suivante : backoff, jitter, `Retry-After`."""
        logger.debug("_delai | tentative {}", tentative)
        brut = self._delai_initial * (2**tentative)
        delai = brut * (0.5 + 0.5 * self._alea())
        if retry_after:
            demande = self._secondes_retry_after(retry_after)
            if demande is not None and demande > delai:
                logger.debug("_delai | Retry-After {} s retenu", demande)
                delai = demande
        delai = min(delai, self._delai_max)
        logger.debug("_delai | {} s retenu", delai)
        return delai

    async def envoyer(
        self,
        url: str,
        *,
        corps: bytes,
        type_contenu: str = "application/json",
        authentification: Authentification | None = None,
        en_tetes: dict[str, str] | None = None,
        compression: bool = False,
    ) -> ReponseHTTP:
        """Envoie un corps (une requête `_bulk` par exemple) avec les réessais.

        Args:
            url: URL complète (`https://hote:port/chemin`).
            corps: Corps de la requête, en octets.
            type_contenu: En-tête `Content-Type`.
            authentification: Basic, Bearer ou ApiKey (en-têtes libres
                sinon).
            en_tetes: En-têtes supplémentaires (`X-P-Stream`…).
            compression: `True` compresse le corps en gzip et annonce
                `Content-Encoding: gzip`.

        Returns:
            La réponse du serveur, même en échec `4xx` définitif (l'appelant
            décide : DLQ, métriques).

        Raises:
            TransportErreur: Réseau injoignable ou délai dépassé après tous
                les réessais (échec transitoire).
            RuntimeError: Le transport n'a pas été ouvert (`open()`).
            ValueError: `essais_max` n'est pas positif (aucun envoi possible).
        """
        logger.debug("envoyer | {} ({} octet(s))", type_contenu, len(corps))
        session = self._session
        if session is None:
            logger.debug("envoyer | transport non ouvert : refusé")
            raise RuntimeError("transport non ouvert : appeler open() d'abord")
        entetes: dict[str, str] = {"Content-Type": type_contenu}
        if authentification is not None:
            entetes.update(self._entetes_auth(authentification))
        if en_tetes:
            entetes.update(en_tetes)
        corps_envoye = corps
        if compression:
            corps_envoye = gzip.compress(corps)
            entetes["Content-Encoding"] = "gzip"
        tentative = 0
        while tentative < self._essais_max:
            try:
                async with session.post(url, data=corps_envoye, headers=entetes) as reponse:
                    statut = reponse.status
                    corps_lu = await reponse.read()
                    en_tetes_lus = {str(cle): str(valeur) for cle, valeur in reponse.headers.items()}
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.debug("envoyer | échec réseau (essai {}/{})", tentative + 1, self._essais_max)
                if tentative == self._essais_max - 1:
                    logger.warning(_("transport : échec réseau après {} essai(s)"), self._essais_max)
                    logger.debug("envoyer | abandon (réseau)")
                    raise TransportErreur(f"échec réseau après {self._essais_max} essai(s)") from exc
                await asyncio.sleep(self._delai(tentative, None))
                tentative += 1
                continue
            if statut != 429 and not 500 <= statut < 600:
                logger.debug("envoyer | statut {}", statut)
                return ReponseHTTP(statut=statut, corps=corps_lu, en_tetes=en_tetes_lus)
            if tentative == self._essais_max - 1:
                logger.debug("envoyer | statut réessayable épuisé : {}", statut)
                return ReponseHTTP(statut=statut, corps=corps_lu, en_tetes=en_tetes_lus)
            logger.debug("envoyer | statut réessayable ({}) : nouvel essai", statut)
            await asyncio.sleep(self._delai(tentative, en_tetes_lus.get("Retry-After")))
            tentative += 1
        logger.debug("envoyer | aucun essai configuré (essais_max non positif)")
        raise ValueError("essais_max doit être supérieur à 0")
