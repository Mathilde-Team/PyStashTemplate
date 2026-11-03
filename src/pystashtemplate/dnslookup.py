# SPDX-License-Identifier: GPL-3.0-or-later
"""Filtre `dns` façon Logstash pour les pipelines `.conf` (issue #121).

`filter { dns { reverse => ["[host][ip]"] action => "replace" } }` reprend
logstash-filter-dns (`dns.rb`) :

- `resolve` (nom → première adresse) et `reverse` (adresse → premier nom),
  champ par champ, `resolve` d'abord ;
- `action => append` (défaut) : le champ devient `[valeur, résultat]` (ou
  le tableau reçoit le résultat) ; `replace` : le résultat remplace la
  valeur (dans un tableau si c'en était un) ;
- un champ absent est sauté ; plusieurs valeurs, une valeur non textuelle,
  une adresse invalide (`reverse`), un échec ou une erreur arrêtent le
  filtre, sans options communes ;
- résolveurs interrogés dans l'ordre, le premier qui répond l'emporte :
  fichiers `hostsfile`, puis `/etc/hosts`, puis les serveurs `nameserver`
  (chaîne, liste, ou hash `address`/`search`/`ndots`). Sans `nameserver`
  ni `hostsfile` : `/etc/hosts` puis la configuration DNS du système ; avec
  `hostsfile` seul, aucun serveur DNS ;
- `timeout` (0,5 s par essai), `max_retries` (2 nouveaux essais après un
  délai dépassé ou une erreur réseau), `tag_on_timeout` (`_dnstimeout`) ;
- caches `hit_cache_size`/`hit_cache_ttl` (0, 60 s) et
  `failed_cache_size`/`failed_cache_ttl` (0, 5 s), LRU avec durée de vie ;
- noms internationalisés convertis (IDNA) dans les deux sens.

`PYSTASHTEMPLATE_DNS=off` coupe les requêtes DNS réseau (les fichiers hosts
restent lus) : utile pour les tests et les environnements sans réseau.

Différences : dnspython remplace `Resolv` de Ruby ; IDNA 2003 de la
bibliothèque standard au lieu de `java.net.IDN`.
"""

from __future__ import annotations

import ipaddress
import os
import time
from collections import OrderedDict
from collections.abc import Callable
from typing import Any, Protocol

import dns.asyncresolver
import dns.exception
import dns.name
import dns.resolver
import dns.reversename
from loguru import logger

from pystashtemplate.templates import _MISSING, assign_field, parse_field_path, resolve_field
from pystashtemplate.tracing import atraced, traced
from pystashtemplate.util import add_failure_tags

HOSTS_SYSTEME = "/etc/hosts"
VARIABLE_DESACTIVATION = "PYSTASHTEMPLATE_DNS"
# Compatibilité : ancien nom avant le découpage #111 (issue RsysLogstach #111).
VARIABLE_DESACTIVATION_ANCIENNE = "RSYSLOGSTACH_DNS"


class DelaiDepasse(Exception):
    """Délai dépassé (`Resolv::ResolvTimeout`)."""


class Resolveur(Protocol):
    """Source de résolution : fichier hosts ou serveur DNS.

    Asynchrone depuis #35 (chantier full async #1) : `dns.asyncresolver`
    fournit la vraie concurrence réseau côté serveurs DNS ; les fichiers
    hosts (lectures locales rapides) portent la même interface.
    """

    async def adresses(self, nom: str) -> list[str]:
        """Adresses du nom, dans l'ordre."""

    async def noms(self, adresse: str) -> list[str]:
        """Noms de l'adresse, dans l'ordre."""


class CacheTTL:
    """Cache LRU dont les entrées expirent après `ttl` secondes (`LruRedux::TTL`)."""

    @traced
    def __init__(self, taille: int, ttl: float, horloge: Callable[[], float] = time.monotonic) -> None:
        """Cache vide de `taille` entrées au plus."""
        self._taille = taille
        self._ttl = ttl
        self._horloge = horloge
        self._entrees: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    @traced
    def lire(self, cle: str) -> Any:
        """Valeur encore valide de `cle`, ou None."""
        entree = self._entrees.get(cle)
        if entree is None:
            return None
        if self._horloge() - entree[0] > self._ttl:
            del self._entrees[cle]
            logger.debug("CacheTTL.lire({}) | expiré", cle)
            return None
        self._entrees.move_to_end(cle)
        return entree[1]

    @traced
    def ecrire(self, cle: str, valeur: Any) -> None:
        """Mémorise `valeur` ; la plus ancienne entrée sort si le cache est plein."""
        self._entrees[cle] = (self._horloge(), valeur)
        self._entrees.move_to_end(cle)
        while len(self._entrees) > self._taille:
            self._entrees.popitem(last=False)
        logger.debug("CacheTTL.ecrire({}) | {} entrée(s)", cle, len(self._entrees))


class FichierHosts:
    """`Resolv::Hosts` : fichier au format `/etc/hosts`, lu au premier usage."""

    @traced
    def __init__(self, chemin: str) -> None:
        """Mémorise le chemin ; le fichier n'est lu qu'à la première requête."""
        self._chemin = chemin
        self._noms_vers_adresses: dict[str, list[str]] | None = None
        self._adresses_vers_noms: dict[str, list[str]] = {}

    @traced
    def _charger(self) -> dict[str, list[str]]:
        """Lit le fichier (absent ou illisible : aucune entrée, comme Ruby)."""
        if self._noms_vers_adresses is not None:
            return self._noms_vers_adresses
        self._noms_vers_adresses = {}
        try:
            with open(self._chemin, encoding="utf-8", errors="replace") as fichier:
                lignes = fichier.read().splitlines()
        except OSError as exc:
            logger.debug("hosts illisible {} : {}", self._chemin, exc)
            lignes = []
        for ligne in lignes:
            champs = ligne.split("#", 1)[0].split()
            if len(champs) < 2:
                continue
            adresse = _normaliser_adresse(champs[0])
            for nom in champs[1:]:
                self._noms_vers_adresses.setdefault(nom, []).append(adresse)
                self._adresses_vers_noms.setdefault(adresse, []).append(nom)
        logger.debug("hosts {} : {} nom(s)", self._chemin, len(self._noms_vers_adresses))
        return self._noms_vers_adresses

    @atraced
    async def adresses(self, nom: str) -> list[str]:
        """Adresses de `nom` dans l'ordre du fichier."""
        return list(self._charger().get(nom, []))

    @atraced
    async def noms(self, adresse: str) -> list[str]:
        """Noms de `adresse` (nom canonique d'abord)."""
        self._charger()
        return list(self._adresses_vers_noms.get(_normaliser_adresse(adresse), []))


class ServeurDns:
    """`Resolv::DNS` : requêtes A puis AAAA, ou PTR, avec dnspython."""

    @traced
    def __init__(self, timeout: float, serveurs: dict[str, Any] | None = None) -> None:
        """Configuration du système (`/etc/resolv.conf`) ou serveurs donnés.

        Args:
            timeout: Durée d'un essai, en secondes.
            serveurs: `{"nameserver": [...], "search": [...], "ndots": n}`
                ou None pour la configuration du système.
        """
        self._serveurs = serveurs
        self._timeout = timeout
        self._resolveur: dns.asyncresolver.Resolver | None = None

    @traced
    def _client(self) -> dns.asyncresolver.Resolver:
        """Résolveur dnspython asynchrone, construit au premier usage."""
        if self._resolveur is None:
            if self._serveurs is None:
                resolveur = dns.asyncresolver.Resolver(configure=True)
            else:
                resolveur = dns.asyncresolver.Resolver(configure=False)
                resolveur.nameservers = list(self._serveurs["nameserver"])
                resolveur.search = [dns.name.from_text(domaine) for domaine in self._serveurs["search"]]
                resolveur.ndots = self._serveurs["ndots"]
            resolveur.timeout = self._timeout
            resolveur.lifetime = self._timeout
            self._resolveur = resolveur
        return self._resolveur

    @atraced
    async def _requete(self, nom: str, type_: str) -> list[str]:
        """Réponses d'une requête ; aucune si le nom n'existe pas ou sans réponse."""
        if (
            os.environ.get(VARIABLE_DESACTIVATION) or os.environ.get(VARIABLE_DESACTIVATION_ANCIENNE, "")
        ).strip().lower() == "off":
            logger.debug("dns : requêtes réseau coupées ({}=off)", VARIABLE_DESACTIVATION)
            return []
        try:
            reponse = await self._client().resolve(nom, type_, search=True)
        except dns.exception.Timeout as exc:
            logger.debug("dns : délai dépassé pour {} {} : {}", nom, type_, exc)
            raise DelaiDepasse(str(exc)) from exc
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.NoNameservers) as exc:
            logger.debug("dns : pas de réponse pour {} {} : {}", nom, type_, exc)
            return []
        resultats = [rdata.to_text() for rdata in reponse]
        logger.debug("dns : {} {} | {}", nom, type_, resultats)
        return resultats

    @atraced
    async def adresses(self, nom: str) -> list[str]:
        """Adresses IPv4 puis IPv6."""
        return [*await self._requete(nom, "A"), *await self._requete(nom, "AAAA")]

    @atraced
    async def noms(self, adresse: str) -> list[str]:
        """Noms PTR, sans point final."""
        return [nom.rstrip(".") for nom in await self._requete(dns.reversename.from_address(adresse).to_text(), "PTR")]


@traced
def _normaliser_adresse(adresse: str) -> str:
    """Forme canonique d'une adresse IP (texte inchangé si ce n'en est pas une)."""
    try:
        return str(ipaddress.ip_address(adresse))
    except ValueError:
        logger.debug("_normaliser_adresse : pas une adresse : {!r}", adresse)
        return adresse


@traced
def est_adresse(texte: str) -> bool:
    """`Resolv::AddressRegex` : adresse IPv4 ou IPv6, sans zone (`%eth0`)."""
    if "%" in texte:
        return False
    try:
        ipaddress.ip_address(texte)
    except ValueError:
        logger.debug("est_adresse({!r}) | non", texte)
        return False
    return True


@traced
def _vers_ascii(nom: str) -> str:
    """`IDN.toASCII` : nom internationalisé en ACE (`xn--`).

    Raises:
        ValueError: nom non convertible (`IllegalArgumentException`).
    """
    try:
        return nom.encode("idna").decode("ascii")
    except UnicodeError as exc:
        logger.debug("_vers_ascii({!r}) : {}", nom, exc)
        raise ValueError(f"nom invalide {nom!r}") from exc


@traced
def _vers_unicode(nom: str) -> str:
    """`IDN.toUnicode` : forme lisible, nom inchangé s'il n'est pas convertible."""
    try:
        return nom.encode("ascii").decode("idna")
    except UnicodeError as exc:
        logger.debug("_vers_unicode({!r}) : {}", nom, exc)
        return nom


@traced
def _liste(valeur: Any, nom: str) -> list[str] | None:
    """Option `:array` de Logstash : chaîne seule ou liste de chaînes."""
    if valeur is None:
        return None
    valeurs = valeur if isinstance(valeur, list) else [valeur]
    if not all(isinstance(v, str) for v in valeurs):
        raise ValueError(f"dns : {nom!r} attend des chaînes")
    return valeurs


@traced
def _nombre(options: dict[str, Any], nom: str, defaut: float) -> float:
    """Option `:number` (texte accepté) ; un `${VAR}` non résolu est une erreur."""
    valeur = options.get(nom, defaut)
    try:
        return float(valeur)
    except (TypeError, ValueError) as exc:
        logger.debug("dns : {} invalide : {!r}", nom, valeur)
        raise ValueError(f"dns : {nom!r} doit être un nombre (reçu {valeur!r})") from exc


@traced
def serveurs_normalises(nameserver: Any) -> dict[str, Any]:
    """`normalised_nameserver` : adresses, domaines de recherche (6 au plus) et ndots.

    Raises:
        ValueError: hash sans `address`, liste vide, plus de 6 domaines,
            ndots non entier ou nul, clé inconnue.
    """
    options = dict(nameserver) if isinstance(nameserver, dict) else {"address": nameserver}
    adresses = options.pop("address", None)
    if adresses is None:
        raise ValueError("dns : le hash 'nameserver' doit contenir 'address'")
    adresses = [str(a) for a in (adresses if isinstance(adresses, list) else [adresses])]
    if not adresses:
        raise ValueError("dns : 'nameserver' ne peut pas être vide")
    recherche = options.pop("search", None) or []
    recherche = [str(d) for d in (recherche if isinstance(recherche, list) else [recherche])]
    if len(recherche) > 6:
        raise ValueError("dns : 6 domaines 'search' au plus")
    try:
        ndots = int(options.pop("ndots", None) or 1)
    except (TypeError, ValueError) as exc:
        logger.debug("dns : ndots invalide : {}", exc)
        raise ValueError("dns : 'ndots' doit être un entier") from exc
    if ndots <= 0:
        raise ValueError("dns : 'ndots' doit être positif")
    if options:
        raise ValueError(f"dns : argument(s) 'nameserver' inconnu(s) : {sorted(options)}")
    resultat = {"nameserver": adresses, "search": recherche, "ndots": ndots}
    logger.debug("serveurs_normalises | {}", resultat)
    return resultat


class DnsFilter:
    """Bloc `dns {}` d'un `.conf` (voir la docstring du module)."""

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        resolveurs: list[Resolveur] | None = None,
        horloge: Callable[[], float] = time.monotonic,
    ) -> None:
        """Valide les options et prépare les résolveurs et les caches.

        Args:
            options: Options du bloc.
            resolveurs: Résolveurs à utiliser à la place de ceux déduits de
                `hostsfile` et `nameserver` (tests).
            horloge: Horloge des caches (tests).

        Raises:
            ValueError: option mal typée ou incohérente.
        """
        self._resolve = _liste(options.get("resolve"), "resolve")
        self._reverse = _liste(options.get("reverse"), "reverse")
        self._action = str(options.get("action", "append"))
        if self._action not in ("append", "replace"):
            raise ValueError(f"dns : action inconnue {self._action!r}")
        timeout = _nombre(options, "timeout", 0.5)
        self._max_retries = int(_nombre(options, "max_retries", 2))
        self._tags_delai = _liste(options.get("tag_on_timeout", ["_dnstimeout"]), "tag_on_timeout") or []
        self._cache_succes = self._cache(options, "hit_cache", 60, horloge)
        self._cache_echecs = self._cache(options, "failed_cache", 5, horloge)
        if resolveurs is None:
            resolveurs = self._resolveurs(options, timeout)
        self._resolveurs_actifs = resolveurs

    @traced
    def _cache(self, options: dict[str, Any], nom: str, ttl: float, horloge: Callable[[], float]) -> CacheTTL | None:
        """Cache `<nom>_size`/`<nom>_ttl`, ou None si la taille est nulle."""
        taille = int(_nombre(options, f"{nom}_size", 0))
        duree = _nombre(options, f"{nom}_ttl", ttl)
        return CacheTTL(taille, duree, horloge) if taille > 0 else None

    @traced
    def _resolveurs(self, options: dict[str, Any], timeout: float) -> list[Resolveur]:
        """`register` : ordre hosts utilisateur, `/etc/hosts`, puis DNS."""
        nameserver = options.get("nameserver")
        hostsfile = _liste(options.get("hostsfile"), "hostsfile")
        if nameserver is None and hostsfile is None:
            return [FichierHosts(HOSTS_SYSTEME), ServeurDns(timeout)]
        resolveurs: list[Resolveur] = [FichierHosts(chemin) for chemin in hostsfile or []]
        resolveurs.append(FichierHosts(HOSTS_SYSTEME))
        if nameserver not in (None, [], ""):
            resolveurs.append(ServeurDns(timeout, serveurs_normalises(nameserver)))
        logger.debug("dns : {} résolveur(s)", len(resolveurs))
        return resolveurs

    @atraced
    async def apply(self, doc: dict[str, Any]) -> bool:
        """Applique `resolve` puis `reverse` ; True si le filtre est allé au bout.

        Asynchrone depuis #35 (chantier full async #1) : la résolution
        réseau passe par `dns.asyncresolver`.
        """
        for champs, inverse in ((self._resolve, False), (self._reverse, True)):
            if champs is not None and not await self._traiter(doc, champs, inverse=inverse):
                logger.debug("dns : filtre arrêté")
                return False
        logger.debug("dns : terminé")
        return True

    @atraced
    async def _traiter(self, doc: dict[str, Any], champs: list[str], *, inverse: bool) -> bool:
        """`resolve(event)` / `reverse(event)` : False quand Ruby fait `return`."""
        for champ in champs:
            chemin = parse_field_path(champ)
            brut = resolve_field(doc, chemin)
            if brut is _MISSING or brut is None:
                logger.debug("dns : champ absent {}", champ)
                continue
            tableau = isinstance(brut, list)
            if tableau:
                if len(brut) > 1:
                    logger.debug("dns : plusieurs valeurs dans {}", champ)
                    return False
                brut = brut[0] if brut else None
            if not isinstance(brut, str):
                logger.debug("dns : valeur non textuelle dans {}", champ)
                return False
            if inverse and not est_adresse(brut):
                logger.debug("dns : pas une adresse : {!r}", brut)
                return False
            resultat = await self._chercher(doc, brut, inverse=inverse)
            if resultat is None:
                return False
            self._ecrire(doc, chemin, resultat, tableau=tableau)
        return True

    @atraced
    async def _chercher(self, doc: dict[str, Any], brut: str, *, inverse: bool) -> str | None:
        """Résultat (caches compris) ; None si échec, délai dépassé ou erreur."""
        if self._cache_echecs is not None and self._cache_echecs.lire(brut):
            logger.debug("dns : échec récent en cache : {}", brut)
            return None
        try:
            resultat = self._cache_succes.lire(brut) if self._cache_succes is not None else None
            if resultat is None:
                resultat = await self._avec_essais(brut, inverse=inverse)
                if resultat is not None and self._cache_succes is not None:
                    self._cache_succes.ecrire(brut, resultat)
        except DelaiDepasse:
            logger.debug("dns : délai dépassé pour {}", brut)
            self._noter_echec(brut)
            add_failure_tags(doc, self._tags_delai)
            return None
        except (OSError, ValueError, dns.exception.DNSException) as exc:
            logger.debug("dns : erreur pour {} : {}", brut, exc)
            return None
        if resultat is None:
            logger.debug("dns : aucune réponse pour {}", brut)
            self._noter_echec(brut)
        return resultat

    @traced
    def _noter_echec(self, brut: str) -> None:
        """Mémorise l'échec si le cache des échecs est actif."""
        if self._cache_echecs is not None:
            self._cache_echecs.ecrire(brut, True)

    @atraced
    async def _avec_essais(self, brut: str, *, inverse: bool) -> str | None:
        """`retriable_request` : nouveaux essais après un délai dépassé ou une erreur réseau."""
        essais = 0
        while True:
            try:
                return await (self._nom(brut) if inverse else self._adresse(brut))
            except (DelaiDepasse, OSError) as exc:
                if essais >= self._max_retries:
                    logger.debug("dns : abandon après {} essai(s) : {}", essais + 1, exc)
                    raise
                essais += 1
                logger.debug("dns : nouvel essai {} pour {}", essais, brut)

    @atraced
    async def _adresse(self, nom: str) -> str | None:
        """Première adresse du premier résolveur qui répond."""
        ascii_ = _vers_ascii(nom)
        for resolveur in self._resolveurs_actifs:
            for adresse in await resolveur.adresses(ascii_):
                return adresse
        return None

    @atraced
    async def _nom(self, adresse: str) -> str | None:
        """Premier nom du premier résolveur qui répond, en Unicode."""
        for resolveur in self._resolveurs_actifs:
            for nom in await resolveur.noms(adresse):
                return _vers_unicode(nom)
        return None

    @traced
    def _ecrire(self, doc: dict[str, Any], chemin: list[str], resultat: str, *, tableau: bool) -> None:
        """`action` : `replace` remplace, `append` ajoute à la valeur d'origine."""
        if self._action == "replace":
            assign_field(doc, chemin, [resultat] if tableau else resultat)
        elif tableau:
            assign_field(doc, chemin, [*resolve_field(doc, chemin), resultat])
        else:
            assign_field(doc, chemin, [resolve_field(doc, chemin), resultat])
        logger.debug("dns : {} | {}", chemin, resultat)
