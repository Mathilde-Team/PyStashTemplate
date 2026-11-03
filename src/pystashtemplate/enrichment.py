# SPDX-License-Identifier: GPL-3.0-or-later
"""Enrichissement de documents (dictionnaire + géolocalisation MaxMind).

Extrait d'omprog.py (session 9, réorganisation selon le squelette de
Mathilde). Ne dépend que de `tracing` — indépendant de `filters.py`, de
`templates.py` et de `indexer.py` (voir PATTERNS.md, "Absence de
dépendances circulaires entre modules").

`TranslateDictionary`/`GenericMmdbLookup` (session 16) : équivalents des
plugins `translate`/`mmdb` d'un pipeline `.conf` Logstash, consommés par
`interpreter.py::PipelineInterpreter` (voir FEATURES.md, item "Proposé"
#7). Ajoutés ici plutôt que dans `interpreter.py` pour la même raison que
`DictEnricher`/`GeoIPEnricher` : ce module regroupe toute lecture d'une
source de données EXTERNE (fichier de dictionnaire, base mmdb), qu'elle
soit pilotée par la CLI d'`omprog` (`--enrich-dict`/`--geoip-db`) ou par
un `.conf` Logstash interprété. Toutes deux restent des feuilles du
graphe de dépendances (ne dépendent que de `tracing`, comme
`DictEnricher`/`GeoIPEnricher`) : la résolution de chemin de champ
(`%{[a][b]}`) et le rendu de gabarit sprintf du `fallback` de `translate`
restent une responsabilité d'`interpreter.py` (qui dépend déjà de
`templates.py`), pas de ce module — voir sa docstring pour le partage des
responsabilités exact.

`GeoIPLookup` (session 21) : même principe pour le plugin `geoip` officiel
de Logstash, resté non interprété par `interpreter.py` depuis la
session 11 (structure normalisée seulement, voir FEATURES.md, "Limites
connues"). Réutilise `_build_geoip_record`, extraite de
`GeoIPEnricher.enrich` pour être partagée sans dupliquer le format de
sortie (mêmes champs, alignés sur le filtre `geoip` officiel — voir sa
docstring) entre l'enrichissement CLI (`--geoip-db`, une paire ip_field/
out_field fixe pour tout le processus) et l'enrichissement piloté par
`.conf` (`source`/`target` propres à chaque bloc `geoip {}`, résolus par
`interpreter.py` comme pour `mmdb`).
"""

from __future__ import annotations

import csv
import json
import os
from typing import Any

import geoip2.database
import geoip2.errors
import maxminddb
import yaml
from loguru import logger

from pystashtemplate.tracing import traced


class DictEnricher:
    """Enrichit un document à partir d'un dictionnaire chargé en mémoire.

    Équivalent du filtre `translate` de Logstash.
    """

    @traced
    def __init__(self, mapping: dict[str, dict], key_field: str, out_field: str) -> None:
        """Initialise l'enrichisseur.

        Args:
            mapping: Dictionnaire {valeur de key_field: champs à fusionner}.
            key_field: Nom du champ du document utilisé comme clé de recherche.
            out_field: Nom du sous-objet dans lequel placer les champs trouvés.
        """
        self._mapping = mapping
        self._key_field = key_field
        self._out_field = out_field

    @traced
    def enrich(self, doc: dict) -> None:
        """Ajoute les champs correspondants à doc, en place, si la clé est connue."""
        key = doc.get(self._key_field)
        extra = self._mapping.get(key) if key is not None else None
        if extra:
            doc[self._out_field] = extra


@traced
def _build_geoip_record(ip: str, response: Any) -> dict[str, Any]:
    """Construit le dict de géolocalisation à partir d'une réponse `geoip2.models.City`.

    Fonction pure extraite de `GeoIPEnricher.enrich` (session 21) pour être
    partagée avec `GeoIPLookup` (équivalent du plugin `geoip` officiel d'un
    pipeline `.conf` Logstash, `interpreter.py::PipelineInterpreter`) — même
    motif que `filters.apply_mutate`/`convert_value` (session 14, partagés
    entre `--filters` et `--pipeline-conf`) : une seule implémentation du
    format de sortie, consommée par les deux mécanismes de géolocalisation
    du projet plutôt que deux copies.

    Champs alignés sur ceux du filtre geoip officiel de Logstash pour une
    base GeoLite2 City (city_name, continent_code, country_code2,
    country_name, postal_code, region_name, timezone, latitude, longitude,
    location) — à deux exceptions près, non reproductibles fidèlement à
    partir de geoip2 : country_code3 (Logstash le dérive d'une table de
    correspondance interne, absente de la base MaxMind elle-même) et
    dma_code (non peuplé par GeoLite2 City).
    """
    geo: dict[str, Any] = {"ip": ip}
    if response.city.name is not None:
        geo["city_name"] = response.city.name
    if response.continent.code is not None:
        geo["continent_code"] = response.continent.code
    if response.country.iso_code is not None:
        geo["country_code2"] = response.country.iso_code
    if response.country.name is not None:
        geo["country_name"] = response.country.name
    if response.postal.code is not None:
        geo["postal_code"] = response.postal.code
    region_name = response.subdivisions.most_specific.name
    if region_name is not None:
        geo["region_name"] = region_name
    if response.location.time_zone is not None:
        geo["timezone"] = response.location.time_zone
    if response.location.latitude is not None and response.location.longitude is not None:
        geo["latitude"] = response.location.latitude
        geo["longitude"] = response.location.longitude
        # [lon, lat] : convention GeoJSON reprise par le filtre geoip
        # officiel de Logstash, compatible geo_point Elasticsearch/OpenSearch.
        geo["location"] = [response.location.longitude, response.location.latitude]
    return geo


class GeoIPEnricher:
    """Enrichit un document avec la géolocalisation d'une IP (base MaxMind mmdb)."""

    @traced
    def __init__(self, mmdb_path: str, ip_field: str, out_field: str) -> None:
        """Ouvre la base mmdb.

        Args:
            mmdb_path: Chemin du fichier .mmdb (ex. GeoLite2-City.mmdb).
            ip_field: Nom du champ du document contenant l'IP à géolocaliser.
            out_field: Nom du sous-objet dans lequel placer le résultat.
        """
        self._reader = geoip2.database.Reader(mmdb_path)
        self._ip_field = ip_field
        self._out_field = out_field

    @traced
    def enrich(self, doc: dict) -> None:
        """Ajoute la géolocalisation à doc, en place, si l'IP est trouvée.

        Voir `_build_geoip_record` pour le détail des champs produits.
        Sans effet sur les IP privées/réservées : les bases MaxMind ne les
        couvrent pas (attendu pour du matériel réseau interne).
        """
        ip = doc.get(self._ip_field)
        if not ip:
            return
        try:
            response = self._reader.city(ip)
        except (geoip2.errors.AddressNotFoundError, ValueError) as exc:
            logger.debug("geoip : adresse introuvable ou invalide ({}), champ non rempli : {}", ip, exc)
            return
        doc[self._out_field] = _build_geoip_record(ip, response)

    @traced
    def close(self) -> None:
        """Ferme la base mmdb."""
        self._reader.close()


class GeoIPLookup:
    """Interroge une base MaxMind GeoIP2 City par IP, sans champ source/cible fixes.

    Équivalent du plugin `geoip` officiel d'un pipeline `.conf` Logstash
    (`interpreter.py::PipelineInterpreter._apply_geoip`, session 21) —
    contrairement à `GeoIPEnricher` (`ip_field`/`out_field` fixés une fois
    pour toutes à la construction, piloté par `--geoip-db` côté CLI
    `omprog`), un bloc `geoip {}` de `.conf` porte SES PROPRES `source`/
    `target` par occurrence : `interpreter.py` les résout lui-même (comme
    il le fait déjà pour `mmdb`/`translate`), cette classe se contente
    d'interroger la base et de restituer le même dict que `GeoIPEnricher`
    (via `_build_geoip_record`, partagée — pas de second format de sortie).

    Ouvre la base une seule fois à la construction (comme `GeoIPEnricher`/
    `GenericMmdbLookup`) ; `interpreter.py` met en cache une instance par
    chemin de base rencontré dans le pipeline (comme `_mmdb_lookups`), pour
    ne jamais rouvrir le même fichier à chaque message.
    """

    @traced
    def __init__(self, mmdb_path: str) -> None:
        """Ouvre la base mmdb.

        Args:
            mmdb_path: Chemin du fichier .mmdb (ex. GeoLite2-City.mmdb).

        Raises:
            OSError: Fichier introuvable ou illisible.
            maxminddb.InvalidDatabaseError: Fichier présent mais pas un MaxMind DB valide.
        """
        self._reader = geoip2.database.Reader(mmdb_path)

    @traced
    def lookup(self, ip: str) -> dict[str, Any] | None:
        """Renvoie le dict de géolocalisation pour `ip`, ou `None` si absente/invalide.

        Testé sur une vraie base GeoLite2-City générée par les tests
        (`tests/conftest.py`, `mmdb-writer`, issue #73) : IP présente,
        champs partiels, IP absente ou invalide.
        """
        if not ip:
            return None
        try:
            response = self._reader.city(ip)
        except (geoip2.errors.AddressNotFoundError, ValueError) as exc:
            logger.debug("geoip : adresse introuvable ou invalide ({}), aucun enregistrement : {}", ip, exc)
            return None
        return _build_geoip_record(ip, response)

    @traced
    def close(self) -> None:
        """Ferme la base mmdb."""
        self._reader.close()


class TranslateDictionary:
    """Charge un dictionnaire externe YAML, JSON ou CSV (équivalent du plugin `translate` de Logstash).

    Contrairement à `DictEnricher` (dictionnaire JSON, clé/sortie fixes,
    champs trouvés toujours fusionnés sous un même sous-objet), le plugin
    `translate` de Logstash lit un fichier YAML plat {clé: valeur} — le
    format réellement utilisé par le pipeline de production de Mathilde
    (`ifName.yml`/`ifNamevlan.yml`/`ifSup.yml`, voir
    `pipeline.example.d/25_filter_logstash-rsyslog-dns-geoip.conf`) — et
    sa cible (`target`) est un chemin de champ arbitraire choisi PAR bloc
    `translate {}`, pas un sous-objet fixe pour tout le pipeline. Cette
    classe se contente de charger le fichier et d'exposer une recherche
    clé -> valeur ; `filters.TranslateFilter` (partagé par `--filters` et
    `--pipeline-conf`) reste seul responsable de résoudre `source`/`target` (chemins
    `%{[a][b]}`, via `templates.py`) et de rendre le gabarit sprintf d'un
    `fallback` — cette classe n'a besoin de connaître ni l'un ni l'autre,
    d'où sa dépendance à `tracing` seule (voir la docstring du module).

    Formats (#119), choisis par l'extension comme le plugin officiel :
    `.yml`/`.yaml` (YAML), `.json` (objet JSON plat) et `.csv` (deux
    colonnes, la première est la clé). Une autre extension est lue comme du
    YAML (le plugin la refuse ; ce choix garde les dictionnaires existants).
    Un fichier vide ou dont la racine n'est pas un mapping donne un
    dictionnaire vide.

    `recharger()` relit le fichier si sa date de modification a changé,
    selon `refresh_behaviour` : `merge` (défaut du plugin, les entrées
    retirées du fichier sont gardées) ou `replace`. Un fichier disparu ou
    illisible au rechargement laisse l'ancien dictionnaire en place, comme
    Logstash. `version` augmente à chaque rechargement effectif.
    """

    @traced
    def __init__(self, dictionary_path: str) -> None:
        """Charge le fichier.

        Args:
            dictionary_path: Chemin d'un fichier YAML, JSON ou CSV {clé: valeur}.

        Raises:
            OSError: Fichier introuvable ou illisible.
            yaml.YAMLError: Contenu YAML malformé.
            ValueError: Contenu JSON malformé (`json.JSONDecodeError`).
            csv.Error: Contenu CSV malformé.
        """
        self._path = dictionary_path
        self._mtime = os.path.getmtime(dictionary_path)
        self._mapping: dict[Any, Any] = self._lire()
        self.version = 0
        logger.debug("TranslateDictionary : {} entrée(s) lues dans {}", len(self._mapping), dictionary_path)

    @property
    def mapping(self) -> dict[Any, Any]:
        """Entrées courantes du dictionnaire (clé -> valeur), sans copie."""
        return self._mapping

    @traced
    def _lire(self) -> dict[Any, Any]:
        """Lit le fichier selon son extension ; racine non mapping -> dictionnaire vide."""
        chemin = self._path.lower()
        with open(self._path, encoding="utf-8-sig", newline="") as handle:
            if chemin.endswith(".json"):
                contenu = handle.read()
                loaded = json.loads(contenu) if contenu.strip() else {}
            elif chemin.endswith(".csv"):
                loaded = {ligne[0]: (ligne[1] if len(ligne) > 1 else None) for ligne in csv.reader(handle) if ligne}
            else:
                loaded = yaml.safe_load(handle)
        # Un fichier vide (`safe_load` renvoie None) ou dont la racine n'est
        # pas un mapping (ex. une simple liste) donne un dictionnaire vide
        # plutôt qu'un échec — même philosophie que `DictEnricher`.
        resultat = loaded if isinstance(loaded, dict) else {}
        logger.debug("TranslateDictionary._lire : {} entrée(s)", len(resultat))
        return resultat

    @traced
    def recharger(self, comportement: str = "merge") -> bool:
        """Relit le fichier s'il a changé (`refresh_behaviour` : `merge` ou `replace`).

        Returns:
            True si le dictionnaire a été rechargé.
        """
        try:
            mtime = os.path.getmtime(self._path)
            if mtime == self._mtime:
                logger.debug("TranslateDictionary.recharger : {} inchangé", self._path)
                return False
            nouveau = self._lire()
        except (OSError, yaml.YAMLError, ValueError, csv.Error) as exc:
            logger.debug("translate : rechargement impossible, ancien dictionnaire gardé : {} ({})", self._path, exc)
            return False
        self._mtime = mtime
        self._mapping = nouveau if comportement == "replace" else {**self._mapping, **nouveau}
        self.version += 1
        logger.debug("TranslateDictionary.recharger : {} rechargé ({}, v{})", self._path, comportement, self.version)
        return True

    @traced
    def lookup(self, key: Any) -> Any:
        """Renvoie la valeur associée à `key`, ou `None` si absente (ou clé non hachable).

        `None` plutôt que la sentinelle `templates._MISSING` : ce module
        ne dépend pas de `templates.py` (voir la docstring du module) —
        c'est à l'appelant de décider quoi faire d'une absence (ex.
        rendre son propre `fallback`).
        """
        try:
            return self._mapping.get(key)
        except TypeError as exc:
            logger.debug("translate : clé non hachable, traitée comme absence : {}", exc)
            # Clé non hachable (ex. `source` résolu en liste/dict) : pas
            # une entrée valide d'un dictionnaire translate, absence comme
            # une autre plutôt qu'une exception qui ferait échouer le
            # document entier.
            return None


class GenericMmdbLookup:
    """Lit un enregistrement brut dans une base MaxMind DB, sans imposer de schéma.

    Équivalent du plugin `mmdb` d'un pipeline `.conf` Logstash pour une
    base personnalisée — ex. la base `XXXX.mmdb` (chemin volontairement
    anonymisé par Mathilde) du pipeline réel de
    `pipeline.example.d/25_filter_logstash-rsyslog-dns-geoip.conf`.

    Contrairement à `GeoIPEnricher` (schéma GeoIP2 City figé —
    city_name/country_name/timezone/etc., voir sa docstring), une base
    mmdb personnalisée peut associer une IP à N'IMPORTE QUEL
    enregistrement : vérifié concrètement (session 16) sur ce pipeline
    réel, dont le bloc `mmdb` est suivi de références à
    `[host][longitude]`/`[host][latitude]`/`[host][tz]`/`[host][WWW]` —
    "tz" et "WWW" n'existent pas dans le schéma GeoIP2 City (qui
    produirait "timezone", pas "tz", et n'a aucune notion de "WWW").
    FEATURES.md (item "Proposé" #7, session 15) supposait la sémantique
    de `mmdb` "très proche" de `GeoIPEnricher" — ce n'est vrai que pour
    `source`/`database`/`target` (les trois options), pas pour le
    résultat produit : rattacher `mmdb` directement à `GeoIPEnricher`
    aurait donc silencieusement produit les MAUVAIS noms de champs pour
    ce pipeline réel. Cette classe restitue l'enregistrement BRUT de la
    base (via `maxminddb`, la bibliothèque bas niveau dont dépend déjà
    `geoip2` — ajoutée en dépendance directe cette session), champ par
    champ, sans supposer de schéma : `interpreter.py` les fusionne
    ensuite sous le chemin `target` du bloc `mmdb {}`.

    Ouvre la base une seule fois à la construction (comme
    `GeoIPEnricher`) ; `interpreter.py` met en cache une instance par
    chemin de base rencontré dans le pipeline (comme son
    `_dissect_cache`), pour ne jamais rouvrir le même fichier à chaque
    message.
    """

    @traced
    def __init__(self, mmdb_path: str) -> None:
        """Ouvre la base mmdb.

        Args:
            mmdb_path: Chemin du fichier .mmdb.

        Raises:
            OSError: Fichier introuvable ou illisible.
            maxminddb.InvalidDatabaseError: Fichier présent mais pas un MaxMind DB valide.
        """
        self._reader = maxminddb.open_database(mmdb_path)

    @traced
    def lookup(self, ip: str) -> dict[str, Any] | None:
        """Renvoie l'enregistrement brut associé à `ip`, ou `None` si absent/IP invalide.

        Testé sur une vraie base personnalisée générée par les tests
        (`tests/conftest.py`, `mmdb-writer`, issue #73), directement et
        via le plugin `mmdb` de `PipelineInterpreter`.
        """
        try:
            record = self._reader.get(ip)
        except (ValueError, TypeError) as exc:
            logger.debug("geoip : adresse invalide, aucun enregistrement : {}", exc)
            return None
        return record if isinstance(record, dict) else None

    @traced
    def close(self) -> None:
        """Ferme la base mmdb."""
        self._reader.close()
