# SPDX-License-Identifier: GPL-3.0-or-later
"""Empreinte déterministe d'un document, pour un document_id OpenSearch idempotent.

Feuille du graphe de dépendances comme `templates`/`enrichment`/`util`/`dlq` :
dépend seulement de `templates` (résolution des chemins de champs imbriqués
via `render_template`, même syntaxe `%{...}`/`%{[a][b]}` que partout
ailleurs dans le projet) et `tracing`. Aucune dépendance dans l'autre sens —
voir `tests/test_no_cross_imports.py::test_templates_has_no_dependency_on_fingerprint`.

Répond à l'item "Proposé" #4 de FEATURES.md (session 17, jusqu'ici
partiellement couvert depuis la session 9 par `--document-id-template`
seul) : un `_id` OpenSearch dérivé d'un HASH du contenu (façon filtre
`fingerprint` de Logstash), pour les documents qui n'ont pas de champ
identifiant naturel — utile surtout en cas de ré-émission par rsyslog après
un accusé de réception perdu (`confirmMessages="on"`, voir `rsyslog.conf`) :
le même document renvoie toujours la même empreinte, donc une ré-indexation
écrase l'original au lieu de le dupliquer, plutôt que de dépendre d'un `_id`
aléatoire (une entrée en double par ré-émission) ou d'un champ pivot que
les logs HPE Comware qui motivent ce projet ne fournissent pas toujours.

Portée volontairement réduite par rapport au filtre `fingerprint` officiel
de Logstash (voir FEATURES.md, "Limites connues") : algorithmes couverts
`sha256`/`sha1`/`md5` (`hashlib`), plus `MURMUR3` (issue #28 : entier 32
bits non signé en décimal via `mmh3`, non cryptographique — déduplication
seulement) et le mode HMAC (issue #28 : clé secrète, `--fingerprint-key`
ou variable d'environnement `FINGERPRINT_KEY` — empreinte dépendant du
déploiement). Restent exclus : `IPV4_NETWORK` (portée trop spécifique à un
cas d'usage réseau que rsyslogstach ne rencontre pas ici) et le HMAC
MURMUR3 (comme le filtre Logstash, qui ne le propose pas non plus).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import ipaddress
import json
import uuid
from typing import Any

import mmh3
from loguru import logger

from pystashtemplate.templates import _MISSING, assign_field, parse_field_path, render_template, resolve_field
from pystashtemplate.tracing import traced

_HASH_FACTORIES = {
    "sha256": hashlib.sha256,
    "sha1": hashlib.sha1,
    "md5": hashlib.md5,
}

# MURMUR3 (issue #28) : hash non cryptographique 32 bits très rapide —
# comme le filtre `fingerprint` de Logstash. Format `_id` DIFFÉRENT d'un
# hex digest : l'entier non signé en décimal (str(int)).
_MURMUR3_METHOD = "murmur3"

# Séparateur entre les valeurs de champs concaténées avant hachage — un
# caractère de contrôle improbable dans un log réel (unit separator ASCII),
# pour éviter qu'une simple concaténation ne fasse se confondre deux
# découpages différents des mêmes caractères (ex. champs "ab"/"c" vs
# "a"/"bc" produiraient sinon la même empreinte).
_FIELD_SEPARATOR = "\x1f"


@traced
def compute_fingerprint(
    doc: dict, fields: list[str] | None, method: str = "sha256", key: str | None = None
) -> str | None:
    """Calcule une empreinte déterministe d'un document (hex digest, ou entier pour MURMUR3).

    Args:
        doc: Document à empreindre.
        fields: Chemins de champs (même syntaxe que l'intérieur d'une
            référence `%{...}`, ex. "host", "[host][ip]") dont les valeurs
            sont concaténées puis hachées, DANS L'ORDRE fourni — l'ordre
            fait partie de l'empreinte : `fields=["a", "b"]` et
            `fields=["b", "a"]` produisent des empreintes différentes pour
            le même document, même sémantique qu'une liste `source` du
            filtre `fingerprint` officiel de Logstash. `None` ou liste
            vide : empreinte du document ENTIER (JSON à clés triées, forme
            canonique — indépendante de l'ordre d'insertion des clés en
            Python), pas d'une sélection de champs.
        method: Algorithme de hachage (`sha256` par défaut, `sha1`, `md5`,
            ou `murmur3` — entier 32 bits non signé en décimal, NON
            cryptographique : déduplication seulement ; voir la docstring
            du module).
        key: Clé secrète du mode HMAC (issue #28) : si fournie,
            `hmac.new(key, payload, method)` au lieu du hash simple —
            l'empreinte dépend alors de la clé (deux déploiements avec des
            clés différentes produisent des `_id` incomparables). `None`
            (défaut) : comportement historique sans clé. Ignorée pour
            `murmur3` (pas de HMAC MURMUR3, comme le filtre Logstash).

    Returns:
        Le hex digest (chaîne hexadécimale ; entier décimal pour
            `murmur3`), ou `None` si TOUS les champs demandés sont absents du document — une empreinte sans aucune
        valeur distinctive n'aurait pas de sens, même convention que
        `templates.render` pour un gabarit qui rend une chaîne vide (voir
        `OpenSearchIndexer._document_id`). Un champ présent mais `null` est
        DISTINGUÉ d'un champ absent (décision issue #29) : il se rend en
        chaîne vide mais compte comme présent — l'empreinte est alors celle
        du contenu vide plutôt que `None`. S'écarte volontairement de
        Logstash 8, qui ne distingue pas les deux (`StringInterpolation`
        renvoie le gabarit littéral dans les deux cas) — voir FEATURES.md.

    Raises:
        ValueError: `method` non reconnu.
    """
    factory = _HASH_FACTORIES.get(method)
    if factory is None and method != _MURMUR3_METHOD:
        msg = f"méthode de fingerprint inconnue: {method!r} (attendu: {sorted([*_HASH_FACTORIES, _MURMUR3_METHOD])})"
        raise ValueError(msg)

    if not fields:
        payload = json.dumps(doc, sort_keys=True, default=str, ensure_ascii=False)
    else:
        # Distinction null/absent (décision issue #29) : un champ présent
        # mais `null` contribue sa valeur rendue (chaîne vide) et COMPTE
        # comme présent — seule l'absence d'un champ l'exclut. L'empreinte
        # n'est `None` que si TOUS les champs demandés sont absents.
        rendered = [
            render_template(f"%{{{field}}}", doc, on_missing="empty")
            for field in fields
            if resolve_field(doc, parse_field_path(field)) is not _MISSING
        ]
        if not rendered:
            return None
        payload = _FIELD_SEPARATOR.join(rendered)

    if method == _MURMUR3_METHOD:
        # Entier 32 bits non signé, en décimal — format `_id` différent
        # d'un hex digest (comme le filtre `fingerprint` de Logstash).
        # MURMUR3 n'est PAS cryptographique : déduplication seulement.
        return str(mmh3.hash(payload.encode("utf-8"), signed=False))
    if key is not None:
        # Mode HMAC (issue #28) : l'empreinte dépend d'une clé secrète —
        # deux déploiements avec des clés différentes produisent des `_id`
        # incomparables (empreinte "signée").
        return hmac.new(key.encode("utf-8"), payload.encode("utf-8"), factory).hexdigest()
    return factory(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Filtre `fingerprint` natif (#114)
# ---------------------------------------------------------------------------

#: Méthodes du filtre `fingerprint` de Logstash prises en charge.
FILTER_METHODS = ("SHA1", "SHA256", "SHA384", "SHA512", "MD5", "MURMUR3", "UUID", "IPV4_NETWORK")


@traced
def _valeur_texte(valeur: Any) -> str:
    """Valeur rendue comme `"#{v}"` en Ruby : texte tel quel, objets en JSON trié."""
    if isinstance(valeur, str):
        return valeur
    if isinstance(valeur, bool):
        return "true" if valeur else "false"
    if valeur is None:
        return ""
    if isinstance(valeur, (dict, list)):
        return json.dumps(valeur, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return str(valeur)


class FingerprintFilter:
    """`fingerprint { source target method key base64encode concatenate_sources concatenate_all_fields }`.

    Comportement de `logstash-filter-fingerprint` :

    - `source` (`["message"]` par défaut), `target` (`"fingerprint"`),
      `method` (`SHA256` par défaut ; `SHA1`, `SHA384`, `SHA512`, `MD5`,
      `MURMUR3`, `UUID`, `IPV4_NETWORK`) ;
    - sans concaténation, chaque champ source présent est empreint et écrit
      dans `target` (le dernier l'emporte) ; un tableau donne un tableau
      d'empreintes ;
    - `concatenate_sources` : `|champ|valeur` pour chaque source triée,
      puis `|` final ; `concatenate_all_fields` : idem sur tout le document ;
    - `key` : HMAC pour les SHA/MD5 ; masque (préfixe CIDR, ex. 24) pour
      `IPV4_NETWORK` ; `base64encode` : condensat en Base64 plutôt qu'en hexa.

    Écart : une valeur hash ou tableau est rendue en JSON trié, pas avec
    l'`inspect` Ruby ; l'empreinte diffère donc de Logstash pour ces
    valeurs (identique pour du texte et des nombres).
    """

    @traced
    def __init__(self, options: dict[str, Any]) -> None:
        """Lit les options ; une méthode inconnue est signalée par ValueError."""
        source = options.get("source", ["message"])
        self.sources = [source] if isinstance(source, str) else [s for s in source if isinstance(s, str)]
        self.target = str(options.get("target", "fingerprint"))
        self.method = str(options.get("method", "SHA256")).upper()
        if self.method not in FILTER_METHODS:
            raise ValueError(f"méthode de fingerprint inconnue : {self.method!r} (attendu : {list(FILTER_METHODS)})")
        key = options.get("key")
        self.key = None if key is None else str(key)
        self.base64 = options.get("base64encode", False) is True
        self.concat = options.get("concatenate_sources", False) is True
        self.concat_all = options.get("concatenate_all_fields", False) is True
        logger.debug("FingerprintFilter: method={} sources={}", self.method, self.sources)

    @traced
    def empreinte(self, donnee: Any) -> Any:
        """Empreinte d'une valeur selon `method` ; None si `IPV4_NETWORK` ne lit pas d'IPv4."""
        texte = _valeur_texte(donnee)
        if self.method == "UUID":
            return str(uuid.uuid4())
        if self.method == "MURMUR3":
            return mmh3.hash(texte.encode("utf-8"), signed=False)
        if self.method == "IPV4_NETWORK":
            try:
                reseau = ipaddress.IPv4Network(f"{texte}/{self.key or 32}", strict=False)
            except ValueError as exc:
                logger.debug("fingerprint IPV4_NETWORK : valeur ignorée ({})", exc)
                return None
            return str(reseau.network_address)
        nom = self.method.lower()
        if self.key is not None:
            condensat = hmac.new(self.key.encode("utf-8"), texte.encode("utf-8"), nom).digest()
        else:
            condensat = hashlib.new(nom, texte.encode("utf-8")).digest()
        return base64.b64encode(condensat).decode("ascii") if self.base64 else condensat.hex()

    @traced
    def apply(self, doc: dict[str, Any]) -> bool:
        """Écrit l'empreinte dans `target` ; True si une empreinte a été écrite."""
        chemin = parse_field_path(self.target)
        if self.concat_all or self.concat:
            if self.concat_all:
                paires = sorted((k, v) for k, v in doc.items())
            else:
                paires = [(s, resolve_field(doc, parse_field_path(s))) for s in sorted(self.sources)]
            texte = "".join(f"|{k}|{'' if v is _MISSING else _valeur_texte(v)}" for k, v in paires) + "|"
            assign_field(doc, chemin, self.empreinte(texte))
            logger.debug("FingerprintFilter.apply: concaténation")
            return True
        ecrit = False
        for source in self.sources:
            valeur = resolve_field(doc, parse_field_path(source))
            if valeur is _MISSING:
                continue
            resultat = [self.empreinte(v) for v in valeur] if isinstance(valeur, list) else self.empreinte(valeur)
            assign_field(doc, chemin, resultat)
            ecrit = True
        logger.debug("FingerprintFilter.apply: ecrit={}", ecrit)
        return ecrit
