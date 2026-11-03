# SPDX-License-Identifier: GPL-3.0-or-later
"""Filtre `useragent` façon Logstash pour les pipelines `.conf` (issue #121).

`filter { useragent { source => "[user_agent][original]" } }` analyse une
chaîne User-Agent avec ua-parser (bibliothèque Python de uap-core, la même
base de motifs que le parseur Java embarqué par Logstash), puis écrit les
mêmes champs que logstash-filter-useragent (`useragent.rb`).

Champs écrits, mode ECS (défaut de Logstash 8, cible `[user_agent]`) :

- `[cible][name]`, `[cible][version]`, `[cible][device][name]` ;
- `[cible][os][name]`, `[cible][os][version]`, `[cible][os][full]` ;
- `[@metadata][filter][user_agent][version][major|minor|patch]` et
  `[@metadata][filter][user_agent][os][version][major|minor|patch]`.

Mode `ecs_compatibility => disabled` : cible vide par défaut (racine),
champs `name`, `device`, `major`, `minor`, `patch`, `version`, `os`,
`os_name`, `os_full`, `os_version`, `os_major`, `os_minor`, `os_patch`,
précédés de `prefix`.

Options : `source` (requis), `target`, `prefix` (ignoré en mode ECS, comme
Logstash), `regexes` (fichier `regexes.yaml` de uap-core à la place des
motifs intégrés), `lru_cache_size` (100 000). Si `target` égale `source`,
le champ source est retiré avant l'écriture. Un tableau donne son premier
élément ; une valeur absente ou vide ne fait rien.

Différences : les motifs intégrés sont ceux du paquet `ua-parser-builtins`
figé dans `pyproject.toml`, pas ceux du jar de Logstash ; les résultats
peuvent donc différer pour des User-Agent récents ou rares.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from loguru import logger
from ua_parser import BasicResolver, CachingResolver, Parser, loaders
from ua_parser.caching import Lru

from pystashtemplate.templates import _MISSING, assign_field, parse_field_path, resolve_field
from pystashtemplate.tracing import traced

_CHAMPS_ECS = {
    "name": ["name"],
    "device": ["device", "name"],
    "version": ["version"],
    "os_full": ["os", "full"],
    "os_name": ["os", "name"],
    "os_version": ["os", "version"],
}
_METADONNEES_ECS = ["@metadata", "filter", "user_agent"]


@lru_cache(maxsize=8)
@traced
def _parseur(regexes: str | None, taille_cache: int) -> Parser:
    """Parseur ua-parser (motifs intégrés ou fichier `regexes`), partagé par configuration."""
    motifs = loaders.load_builtins() if regexes is None else loaders.load_yaml(regexes)
    resolveur = BasicResolver(motifs)
    if taille_cache > 0:
        resolveur = CachingResolver(resolveur, Lru(taille_cache))
    logger.debug("useragent : parseur prêt (regexes={}, cache={})", regexes, taille_cache)
    return Parser(resolveur)


@traced
def _version_os(major: str | None, minor: str | None, patch: str | None, patch_minor: str | None) -> str | None:
    """`build_os_version` : `10.0.1`, ou `Vista SP2` quand le major n'est pas numérique."""
    if major is None:
        return None
    separateur = "." if _entier_ruby(major) else " "
    version = major
    if minor is not None:
        version += separateur + minor
        if patch is not None:
            version += "." + patch
            if patch_minor is not None:
                version += "." + patch_minor
    return version


@traced
def _entier_ruby(texte: str) -> bool:
    """Vrai si `texte.to_i.to_s == texte` en Ruby (entier écrit sans zéro de tête ni espace)."""
    resultat = texte.lstrip("-").isdigit() and str(int(texte)) == texte
    logger.debug("_entier_ruby({!r}) | {}", texte, resultat)
    return resultat


@traced
def _version_ua(source: str, major: str | None, minor: str | None, patch: str | None, patch_minor: str | None):
    """`set_version` + `check_and_adjust_version` : version complète, retrouvée dans la chaîne si besoin."""
    if major is None:
        return None
    version = major
    if minor is not None:
        version += "." + minor
        if patch is not None:
            version += "." + patch
            if patch_minor is not None:
                version += "." + patch_minor
            else:
                version = _ajuster_version(source, version)
    return version


@traced
def _ajuster_version(source: str, version: str) -> str:
    """Ajoute un 4e numéro présent dans la chaîne (`89.0.4389.90`) et ignoré par les motifs."""
    debut = source.find(version)
    if debut < 0:
        return version
    fin = debut + len(version)
    if source[fin : fin + 1] == ".":
        espace = source.find(" ", fin + 1)
        if espace >= 0:
            suite = source[fin + 1 : espace]
            if _entier_ruby(suite):
                version = f"{version}.{suite}"
    logger.debug("_ajuster_version | {}", version)
    return version


class UserAgentFilter:
    """Bloc `useragent {}` d'un `.conf` (voir la docstring du module)."""

    @traced
    def __init__(self, options: dict[str, Any]) -> None:
        """Valide les options et prépare le parseur.

        Raises:
            ValueError: `source` absent, option mal typée, ou fichier
                `regexes` illisible.
        """
        source = options.get("source")
        if not isinstance(source, str) or not source:
            raise ValueError("useragent : 'source' (chaîne) est requis")
        self._source = source
        self._chemin_source = parse_field_path(source)
        target = options.get("target")
        if target is not None and not isinstance(target, str):
            raise ValueError("useragent : 'target' doit être une chaîne")
        self._ecs = str(options.get("ecs_compatibility", "v8")).strip().lower() != "disabled"
        prefixe = options.get("prefix", "")
        if not isinstance(prefixe, str):
            raise ValueError("useragent : 'prefix' doit être une chaîne")
        if self._ecs and prefixe:
            logger.debug("useragent : prefix ignoré en mode ECS : {!r}", prefixe)
            prefixe = ""
        self._prefixe = prefixe
        self._retirer_source = target == source
        cible = target if target is not None else ("[user_agent]" if self._ecs else "")
        self._chemin_cible = parse_field_path(cible) if cible else []
        try:
            taille = int(options.get("lru_cache_size", 100_000))
        except (TypeError, ValueError) as exc:
            logger.debug("useragent : lru_cache_size invalide : {}", exc)
            raise ValueError(f"useragent : lru_cache_size invalide {options.get('lru_cache_size')!r}") from exc
        regexes = options.get("regexes")
        if regexes is not None and not isinstance(regexes, str):
            raise ValueError("useragent : 'regexes' doit être une chaîne")
        try:
            self._parseur = _parseur(regexes, taille)
        except (OSError, ValueError, TypeError, KeyError) as exc:
            logger.debug("useragent : fichier regexes illisible {} : {}", regexes, exc)
            raise ValueError(f"useragent : fichier regexes illisible {regexes} ({exc})") from exc

    @traced
    def _chemin(self, nom: str) -> list[str]:
        """Chemin complet du champ `nom` (noms du mode `disabled`) selon le mode ECS."""
        if not self._ecs:
            return [*self._chemin_cible, self._prefixe + nom]
        if nom in _CHAMPS_ECS:
            return [*self._chemin_cible, *_CHAMPS_ECS[nom]]
        if nom.startswith("os_"):
            return [*_METADONNEES_ECS, "os", "version", nom[3:]]
        return [*_METADONNEES_ECS, "version", nom]

    @traced
    def apply(self, doc: dict[str, Any]) -> bool:
        """Analyse le User-Agent et écrit les champs ; True si l'analyse a eu lieu."""
        valeur = resolve_field(doc, self._chemin_source)
        if isinstance(valeur, list):
            valeur = valeur[0] if valeur else None
        if valeur is _MISSING or valeur is None or valeur == "":
            logger.debug("useragent : source absente ou vide : {}", self._source)
            return False
        chaine = valeur if isinstance(valeur, str) else str(valeur)
        resultat = self._parseur.parse(chaine).with_defaults()
        if self._retirer_source:
            parent = resolve_field(doc, self._chemin_source[:-1]) if len(self._chemin_source) > 1 else doc
            if isinstance(parent, dict):
                parent.pop(self._chemin_source[-1], None)
        ua, os_, appareil = resultat.user_agent, resultat.os, resultat.device
        self._ecrire(doc, "name", ua.family)
        self._ecrire(doc, "device", appareil.family)
        self._ecrire(doc, "major", ua.major)
        self._ecrire(doc, "minor", ua.minor)
        self._ecrire(doc, "patch", ua.patch)
        self._ecrire(doc, "version", _version_ua(chaine, ua.major, ua.minor, ua.patch, ua.patch_minor))
        self._ecrire(doc, "os_major", os_.major)
        self._ecrire(doc, "os_minor", os_.minor)
        self._ecrire(doc, "os_patch", os_.patch)
        version_os = _version_os(os_.major, os_.minor, os_.patch, os_.patch_minor)
        self._ecrire(doc, "os_version", version_os)
        self._ecrire(doc, "os_name", os_.family)
        if not self._ecs:
            self._ecrire(doc, "os", os_.family)
        self._ecrire(doc, "os_full", os_.family + (" " + version_os if version_os else ""))
        logger.debug("useragent : {} analysé ({})", self._source, ua.family)
        return True

    @traced
    def _ecrire(self, doc: dict[str, Any], nom: str, valeur: str | None) -> None:
        """Écrit le champ `nom` s'il a une valeur (`if ua.major` de Logstash)."""
        if valeur is not None:
            assign_field(doc, self._chemin(nom), valeur)
        logger.debug("_ecrire({}) | {!r}", nom, valeur)
