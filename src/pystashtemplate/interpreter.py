# SPDX-License-Identifier: GPL-3.0-or-later
"""Interprète un vrai fichier de pipeline Logstash (`input{}`/`filter{}`/`output{}`)
et l'applique concrètement à un document événement.

Session 11 — suite explicitement laissée ouverte par la session 10
(CLAUDE.md : « aucun wiring dans filters.py/indexer.py/src/omprog, aucun
nouveau module conditions.py [...] Prochaine étape naturelle si
l'intégration est validée : écrire l'évaluateur qui consomme l'AST de
logstash-pipeline-parser »). Ce module EST cet évaluateur. Contrairement à
ce que suggérait FEATURES.md (« conditions.py ? »), les conditions ne sont
qu'une partie du travail — dispatcher aussi vers `dissect`/`mutate` et
rendre les gabarits sprintf d'un output nécessite le même AST — d'où un
seul module `interpreter.py`, plutôt que `conditions.py` isolé.

Ce module est le point de rencontre des deux bibliothèques évaluées
séparément :
    - `logstash_pipeline_parser` (tierce, désormais une dépendance réelle
      — voir la note Python 3.11 plus bas) : parse la *structure* d'un
      pipeline (blocs, plugins imbriqués génériques, conditions) en AST.
    - `templates` (`src/templates.py`, session 9, INCHANGÉ) : rend un
      gabarit sprintf `%{...}` contre un document. `templates.py` exclut
      délibérément ce DSL de conditions de sa propre grammaire (voir sa
      docstring) — c'est précisément ce que ce module lui apporte, sans
      toucher à son code.

Couverture (vérifiée concrètement par `tests/test_interpreter.py`, pas
seulement lue) :
    - `input{}`/`filter{}`/`output{}`, plusieurs blocs du même type admis
      (traités dans l'ordre du fichier) ;
    - plugins imbriqués génériques : toute option de plugin (scalaire,
      hash imbriqué, tableau) est normalisée en structure Python native
      par `_normalize_value`, pour N'IMPORTE QUEL plugin — pas seulement
      ceux gérés spécifiquement ci-dessous ;
    - conditions `if`/`else if`/`else`, opérateurs de comparaison
      (`==`/`!=`/`<`/`<=`/`>`/`>=`), regex (`=~`/`!~`), appartenance
      (`in`/`not in`), négation (`!champ` et `!(condition)`), chaînage
      booléen (`and`/`or`/`xor`/`nand`, évalué strictement de gauche à
      droite — la grammaire de référence de Logstash elle-même ne définit
      aucune priorité entre opérateurs booléens, donc ce choix n'est pas
      une simplification par rapport au comportement réel) ;
    - `filter { dissect { mapping => {...} } }` — réutilise
      `filters.Dissector` (même algorithme, pas de duplication) ;
    - `filter { kv { ... } }` — réutilise `filters.KeyValueFilter`
      (plugin `kv` natif, issue #113) : `source`, `target`,
      `field_split`, `value_split`, `include_keys`, `exclude_keys`,
      `prefix`, `trim_key`, `trim_value`, `allow_duplicate_values` ;
    - `filter { mutate { ... } }` — `convert`/`remove_field` (la demande
      initiale) plus `rename`/`gsub`/`strip`/`add_field`/`add_tag`/
      `remove_tag` (item "Proposé" #1 de FEATURES.md, à portée de main une
      fois le dispatch mutate écrit) ; le reste des options `mutate`
      officielles (`uppercase`/`lowercase`/`capitalize`/`split`/`join`/
      `merge`/`copy`/`update`/`replace`/`coerce`/`rename` en masse...) est
      normalisé dans les options mais délibérément pas interprété — voir
      "Limites assumées" ;
    - `output { ... }` : générique, pas seulement `elasticsearch` — toute
      valeur de type chaîne dans les options d'un plugin de sortie
      (imbriquée ou non) est rendue via `templates.render_template`
      avant d'être restituée dans un `OutputAction`. C'est le point qui
      répond à « je veux un "full interpreter" des templates sur mes
      documents » : la structure vient de `logstash_pipeline_parser`, le
      rendu vient de `templates.py`, ce module ne réimplémente ni l'un ni
      l'autre.
    - `filter { translate { ... } }` / `filter { mmdb { ... } }`
      (session 16, item "Proposé" #7 de FEATURES.md) — réutilisent
      `enrichment.TranslateDictionary`/`GenericMmdbLookup` (nouveaux),
      chargés/ouverts une seule fois par chemin de fichier rencontré
      (mis en cache, comme `_dissect_cache`) : `translate` résout
      `source` (chemin `%{[a][b]}`), cherche la valeur dans le
      dictionnaire YAML de `dictionary_path`, écrit le résultat à
      `target` — ou rend `fallback` (gabarit sprintf éventuel) si la clé
      est absente. `mmdb` résout `source`, interroge la base de
      `database`, fusionne CHAQUE champ de l'enregistrement brut trouvé
      sous `target` (pas de schéma GeoIP2 imposé — voir la docstring de
      `GenericMmdbLookup`, la sémantique réelle diffère de `GeoIPEnricher`
      malgré ce que supposait FEATURES.md avant cette session). `dns`
      (résolution réseau réelle) reste hors de portée, comme documenté
      depuis la session 15 — pas traité ici.
    - `filter { geoip { source => ... [target => "geoip"] database => ...
      [fields => [...]] [tag_on_failure => [...]] } }` (session 21,
      `fields`/`tag_on_failure` honorés depuis la session 22) — LE filtre
      `geoip` officiel de Logstash cette fois (contrairement à `mmdb`,
      extension pour base personnalisée sans schéma) : réutilise
      `enrichment.GeoIPLookup`, construite sur la même fonction pure
      (`_build_geoip_record`) que `GeoIPEnricher` (`--geoip-db`, CLI
      d'`omprog`) — une seule implémentation du format de sortie GeoIP2
      City pour les deux mécanismes de géolocalisation du projet.
      `target` prend `"geoip"` par défaut si absent (comme documenté par
      Logstash) ; `database` reste requis explicitement (pas de base
      GeoLite2 embarquée dans ce projet, contrairement au plugin officiel
      qui en propose une par défaut). Résultat écrit à `target` comme un
      REMPLACEMENT (schéma autonome), contrairement à la fusion champ par
      champ de `mmdb` (voir sa docstring). `fields` (session 22) restreint
      l'enregistrement produit aux clés listées, dans l'ordre de
      `_build_geoip_record` (pas celui de `fields`) ; absent ou vide,
      tous les champs produits sont conservés (comportement historique,
      comme le défaut documenté par Logstash). `tag_on_failure`
      (session 22, `["_geoip_lookup_failure"]` par défaut, comme
      Logstash) ajoute des tags à `doc["tags"]` (sans doublon, réutilise
      `util.add_failure_tags`, partagée avec `filters.py` depuis la
      session 24 — voir `_apply_date`) quand `source` ne résout
      pas à une IP (chaîne) ou qu'aucune correspondance n'est trouvée
      dans la base — PAS quand le plugin lui-même est mal formé
      (`source`/`database`/`target` absents ou de mauvais type) ni quand
      la base est illisible : ces deux derniers cas restent une erreur de
      CONFIGURATION, ignorée silencieusement comme avant, distincte d'un
      échec de résolution au niveau d'UN document (voir "Limites
      assumées" plus bas pour cette même distinction chez `filters_json`/
      `translate`).
    - `filter { filters_json { path => "..." [on_missing => "keep"|
      "empty"] } }` (session 18, item "Proposé" #3 de FEATURES.md) —
      PAS un plugin Logstash officiel (nom délibérément absent de tout
      catalogue réel, pour ne jamais entrer en collision avec un futur
      plugin qui porterait vraiment ce nom) : c'est le pont, resté
      manquant depuis la session 9, entre les deux mécanismes de
      filtrage conditionnel du projet. Charge et compile un
      `filters.FilterPipeline` depuis le fichier JSON `path` (même
      format que `--filters`/`filters.example.json`), une seule fois par
      chemin rencontré (mis en cache, comme `_dissect_cache`/
      `_translate_dictionaries`), puis l'applique au document EN PLACE,
      comme une étape de plus du `filter{}` englobant — dissect/regex/kv/
      mutate/drop de ce fichier JSON s'exécutent alors sous condition
      `if`/`else if`/`else` du `.conf`, ce qu'un `--filters` seul, câblé
      en dehors de toute condition côté `src/omprog`, ne permet pas. Un
      step `"drop"` qui correspond À L'INTÉRIEUR de ce `FilterPipeline`
      abandonne le document entier (voir `apply()` : renvoie alors `None`
      plutôt qu'une liste, y compris pour un pipeline SANS `output{}` —
      voir "Limites assumées" plus bas pour la distinction avec `[]`).
    - `filter { date { match => [champ, motif, ...] [target] [timezone]
      [tag_on_failure] } }` (session 19) — le filtre `date` officiel de
      Logstash : parse `champ` selon le premier motif de `match` qui
      correspond (`"ISO8601"`, `"UNIX"`/`"UNIX_MS"`, ou un motif Java/
      Joda traduit par `filters.translate_date_pattern`, portée réduite — voir
      sa docstring), écrit le résultat (toujours ramené en UTC, toujours
      une chaîne ISO8601, jamais un objet `datetime` brut) à `target`
      (`"@timestamp"` par défaut). Échec (aucun motif ne correspond) :
      ajoute les tags de `tag_on_failure` (`["_dateparsefailure"]` par
      défaut) à `doc["tags"]`, comme Logstash — `target` reste alors
      inchangé. Justifié par le pipeline réel de Mathilde lui-même : son
      `output{}` route déjà vers un index `%{+YYYY.MM.dd}`, un gabarit
      qui dérive sa date de `"@timestamp"` (voir `templates.
      _event_timestamp`) — sans `date`, ce champ ne peut jamais refléter
      l'horodatage embarqué DANS un message de switch.
    - `filter { if [x] == "bruit" { drop { [percentage => N] } } }` (#34)
      — le plugin `drop` officiel : écarte le document (`apply()` renvoie
      `None`, comme pour un step `"drop"` d'un `filters_json`), sous la
      condition du `if` englobant. `percentage` n'écarte qu'une part
      aléatoire des documents, comme dans Logstash.

Limites assumées (à documenter dans FEATURES.md, section "Limites
connues") :
    - `input{}` est parsé (structurellement) mais jamais exécuté par
      `apply()` : un plugin d'entrée est une SOURCE d'événements, pas une
      transformation par document — il n'y a rien à "appliquer" à un
      document déjà reçu. `apply()` ignore silencieusement les sections
      `input`.
    - Options `mutate` génériques d'ordre 2 (`add_tag`/`remove_tag`/
      `add_field`/`remove_field` disponibles en réalité sur N'IMPORTE
      QUEL plugin filter Logstash, pas seulement `mutate`) : seules celles
      DANS un bloc `mutate {}` sont interprétées ici ; un
      `dissect { add_tag => [...] }` ne déclenchera pas l'ajout du tag.
    - Plugins filter autres que `dissect`/`mutate`/`translate`/`mmdb`/
      `geoip`/`filters_json`/`date`/`drop` (ex. `grok`, `kv` NATIF Logstash,
      `dns`) : structure normalisée (accessible si besoin) mais aucun
      comportement appliqué au document — un `logger.debug` le signale,
      le document n'est pas modifié pour cette étape. `filters.py` couvre
      `dissect`/`regex`/`kv`/`drop`/`mutate` via SON propre format de
      configuration JSON, invocable DEPUIS un `.conf` via `filters_json`
      (session 18, voir plus haut) — mais un plugin filter Logstash natif
      non listé ci-dessus (ex. un vrai bloc `kv { field_split => "," }`
      écrit directement dans un `.conf`, PAS via `filters_json`) reste
      normalisé sans être exécuté, comme avant cette session.
    - `geoip` (session 21) : `database` est TOUJOURS requis explicitement,
      contrairement au plugin officiel de Logstash qui replie sur une
      base GeoLite2 embarquée si absent — ce projet n'en embarque aucune
      (même choix que `--geoip-db`/`GeoIPEnricher` depuis la session 5).
      `fields`/`tag_on_failure` sont honorées depuis la session 22 (voir
      la section "Fait" de FEATURES.md) ; les options communes restantes
      (`add_field`/`add_tag`/`remove_field`/`remove_tag`, disponibles en
      réalité sur N'IMPORTE QUEL plugin filter Logstash) restent, elles,
      dans le cas déjà documenté juste au-dessus (seules celles DANS un
      bloc `mutate {}` sont interprétées).
    - `filters_json` (session 18) : ne fusionne PAS architecturalement
      `FilterPipeline`/`--pipeline-conf` au-delà de ce pont explicite — un
      `.conf` doit toujours nommer `filters_json { path => ... }` pour
      invoquer un fichier `--filters` existant ; l'inverse (un
      `FilterPipeline`/`--filters` capable d'invoquer un `.conf` complet
      comme step) n'est pas proposé et n'aurait pas de sens symétrique
      (un `.conf` porte ses propres conditions/output{}, un step JSON
      n'en a pas). `on_missing` de `filters_json` (comportement d'un
      gabarit sprintf de step `mutate.add_field` À L'INTÉRIEUR du
      `FilterPipeline` invoqué) reste indépendant de l'`on_missing` de
      `apply()` (rendu des gabarits d'`output{}` et de `mutate` du
      `.conf` lui-même) — même choix d'indépendance que
      `--filters-on-missing`/`--pipeline-on-missing` côté `src/omprog`
      (session 14).
    - `date` (session 19) : portée volontairement réduite par rapport au
      filtre officiel de Logstash — voir `filters.translate_date_pattern`, ni
      `SSS` (millisecondes Java, incompatibles avec `%f`, MICROsecondes
      Python) ni nom de fuseau textuel (`z`/`zzz`, ex. "CET", ambigu :
      plusieurs fuseaux IANA peuvent partager la même abréviation). Un
      motif Java/Joda hors de `filters.DATE_PATTERN_TOKENS` échoue silencieusement
      pour CE motif (essaie le suivant de `match`, ou `tag_on_failure` si
      aucun ne convient) plutôt que de lever une exception — cohérent avec
      le reste de ce module (une configuration imparfaite ne doit jamais
      faire planter le traitement d'un message). `timezone` n'est consulté
      QUE si le motif ne porte lui-même aucune information de fuseau —
      comme Logstash, jamais pour re-décaler un horodatage déjà zoné.
    - `translate` (#119) : délégué à `filters.TranslateFilter` ; le
      rechargement (`refresh_interval`/`refresh_behaviour`) a lieu au
      premier document après l'intervalle, pas sur une minuterie de fond.
      Les clés sont comparées en texte (Ruby `to_s`).
    - `mmdb` : aucun schéma de champs n'est imposé (voir
      `enrichment.GenericMmdbLookup`) — l'enregistrement brut de la base
      est fusionné tel quel sous `target`, un niveau de profondeur
      (`target[clé] = valeur`), sans imbrication récursive d'un
      enregistrement lui-même imbriqué.
    - Comparaisons avec coercition de type minimale (chaîne <-> nombre
      seulement, ex. `[severity] >= 5` avec `doc["severity"] == "7"`
      fonctionne) : pas de coercition générale (ex. IP vs chaîne) — à
      l'exception, depuis la session 16, des littéraux `pathlib.PurePath`/
      `ipaddress.IPv4Address`/`IPv6Address` que `logstash_pipeline_parser`
      produit pour certaines valeurs reconnues comme un chemin ou une
      adresse IP (ex. `dictionary_path`/`database` de `translate`/`mmdb`,
      ou un littéral `"127.0.0.1"` dans une condition) : `_coerce_literal`
      les convertit systématiquement en chaîne — voir sa docstring, ce
      module travaille uniquement avec des documents JSON (chaînes,
      nombres, booléens, `null`), jamais avec ces types Python.
    - `_normalize_value` distingue un hash imbriqué d'un tableau par forme
      structurelle (chaque entrée `[clé, [valeur]]`) plutôt que par un
      marqueur explicite de la grammaire (qui n'en fournit pas) — cas
      pathologique non rencontré en pratique : un tableau dont EXACTEMENT
      2 éléments sont eux-mêmes des paires `[scalaire, [valeur]]serait
      mal interprété comme un hash. Non rencontré dans les pipelines HPE
      Comware qui motivent ce projet.

Note Python 3.11 : `logstash_pipeline_parser` exige `>=3.11,<3.14`
(bloquant identifié en session 10). Son adoption ici tranche donc le bump
`>=3.10` -> `>=3.11` de `pyproject.toml` — fait dans cette session, à la
demande explicite ("ok python >3.11").
"""

from __future__ import annotations

import json
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime
from ipaddress import IPv4Address, IPv6Address
from pathlib import PurePath
from typing import Any

import maxminddb
from logstash_pipeline_parser import Pipeline
from logstash_pipeline_parser import tree as _parser_tree
from loguru import logger

from pystashtemplate.dnslookup import DnsFilter
from pystashtemplate.enrichment import GenericMmdbLookup, GeoIPLookup, TranslateDictionary
from pystashtemplate.filters import (
    DateFilter,
    Dissector,
    FilterPipeline,
    KeyValueFilter,
    TranslateFilter,
    apply_common_options,
    apply_mutate,
)
from pystashtemplate.fingerprint import FingerprintFilter
from pystashtemplate.grok import GrokFilter
from pystashtemplate.i18n import _
from pystashtemplate.multievents import CloneFilter, SplitFilter
from pystashtemplate.simples import PLUGINS as PLUGINS_SIMPLES
from pystashtemplate.simples import appliquer_cidr, reseaux_cidr
from pystashtemplate.templates import _MISSING, assign_field, parse_field_path, render_template, resolve_field
from pystashtemplate.tracing import traced
from pystashtemplate.useragent import UserAgentFilter
from pystashtemplate.util import add_failure_tags, load_json_file
from pystashtemplate.xmlfilter import XmlFilter

_COMPARE_OPS = {"==", "!=", "<=", ">=", "<", ">"}
_REGEX_OPS = {"=~", "!~"}
_BOOL_OPS = {"and", "or", "xor", "nand"}


@traced
def _chaine_brute(source: str, position: int, tokens: Any) -> str:
    """Action d'analyse : texte d'une chaîne entre guillemets, guillemets retirés, antislashs gardés.

    Signature complète `(source, position, jetons)` de pyparsing : le
    décorateur `traced` masque l'arité réelle, pyparsing doit donc recevoir
    la forme à trois arguments sans avoir à la deviner.
    """
    texte = tokens[0][1:-1]
    logger.debug("_chaine_brute() | {} caractère(s)", len(texte))
    return texte


@traced
def _conserver_antislashs() -> None:
    r"""Chaînes `.conf` lues brutes, comme Logstash (`config.support_escapes: false`), issue #128.

    `logstash-pipeline-parser==0.0.3` déclare les chaînes avec
    `pp.QuotedString(..., esc_char="\\")` : pyparsing retire alors
    l'antislash devant tout caractère (`"a\.b"` → `a.b`, `"\d+"` → `d+`),
    ce qui casse les regex de `gsub`, des conditions `=~ "..."` et des
    motifs `grok`. Logstash, lui, garde le texte entre guillemets tel quel
    (`a\.b`, `\d+`, `\"`). Les deux éléments de grammaire sont des
    attributs de classe de `tree.PEG`, partagés par toutes les analyses :
    on désactive leur déséchappement une seule fois (idempotent) et on se
    contente de retirer les guillemets. Les regex `/.../` ne sont pas
    concernées (déjà brutes).
    """
    for element in (_parser_tree.PEG.double_quoted_string, _parser_tree.PEG.single_quoted_string):
        if getattr(element, "_rsyslogstach_brut", False):
            continue
        element.unquote_results = False
        element.add_parse_action(_chaine_brute)
        element._rsyslogstach_brut = True
    logger.debug("_conserver_antislashs() | chaînes .conf lues brutes")


_conserver_antislashs()


# `${VAR}` / `${VAR:défaut}` : nom de variable d'environnement (lettre ou
# `_` puis lettres/chiffres/`_`) et, optionnellement, valeur par défaut
# jusqu'à la première `}` fermante (pas d'accolade imbriquée, pas
# d'échappement — Logstash n'en propose pas non plus).
_VAR_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")


@traced
def substituer_variables_environnement(source: str) -> str:
    """Remplace les `${VAR}` et `${VAR:défaut}` d'un `.conf` (#122).

    Comme Logstash, la substitution se fait au chargement du texte du
    `.conf`, AVANT l'analyse : elle vaut donc aussi bien pour les valeurs
    entre guillemets que pour le reste, et `%{champ}` (gabarit sprintf,
    rendu par événement) n'est pas concerné — seul `${...}` est touché.

    Sémantique du plugin officiel (documentation Elastic) : la variable
    définie prime toujours sur la valeur par défaut ; `${VAR:défaut}` ne
    sert que si `VAR` n'est pas dans l'environnement ; et une variable
    non définie SANS valeur par défaut est une erreur de configuration —
    `ValueError` qui nomme les variables fautives, plutôt qu'un pont
    qui démarre avec des blocs silencieusement faux. Les occurrences au
    nom invalide (`${1x}`, `${}`) sont laissées telles quelles : le
    parseur les verra comme du texte ordinaire.

    Args:
        source: Texte complet du `.conf` (fichier ou dossier concaténé).

    Returns:
        Le texte substitué ; les valeurs substituées sont des chaînes,
        le type attendu (nombre, booléen…) est rétabli par la
        normalisation des options (`_coerce_literal`).
    """
    manquantes: list[str] = []

    @traced
    def _remplace(match: re.Match[str]) -> str:
        nom, defaut = match.group(1), match.group(2)
        valeur = os.environ.get(nom)
        if valeur is None:
            if defaut is None:
                manquantes.append(nom)
                return match.group(0)
            return defaut
        return valeur

    resultat = _VAR_ENV_RE.sub(_remplace, source)
    if manquantes:
        noms = ", ".join(sorted(set(manquantes)))
        raise ValueError(f".conf : variable(s) d'environnement non définie(s) sans valeur par défaut : {noms}")
    return resultat


@dataclass(frozen=True)
class OutputAction:
    """Un plugin de sortie dont les conditions englobantes se sont toutes évaluées à vrai.

    `options` a déjà ses gabarits sprintf rendus (`templates.render_template`)
    contre le document — prêt à l'emploi par l'appelant (ex. passer
    `options["hosts"]`/`options["index"]`/`options["document_id"]` à
    `indexer.OpenSearchIndexer`), sans que l'appelant ait à connaître l'AST.
    """

    plugin: str
    options: dict[str, Any]


class _DocumentDropped(Exception):
    """Signal interne : un `drop` a demandé l'abandon du document.

    Levée par `_apply_drop` (plugin `drop {}` natif, #34) et par
    `_apply_filters_json` (step `"drop"` d'un fichier JSON), capturée par `apply()` (voir sa
    docstring) — jamais propagée hors de `PipelineInterpreter`. Un simple
    booléen de retour n'aurait pas suffi ici : `_run_filter_body` peut
    être plusieurs niveaux d'appels récursifs à l'intérieur d'un `if`/
    `else if` imbriqué au moment où le drop survient (voir
    `_branch_chain`), et propager un booléen jusqu'à `apply()` à travers
    cette récursion aurait demandé de faire porter ce booléen par chaque
    fonction intermédiaire, alors qu'aucune d'elles n'a de décision à
    prendre à son sujet — seule `apply()` en a une.
    """


@traced
def _convertir_dissect(doc: dict[str, Any], champ: str, type_cible: Any) -> None:
    """`convert_datatype` de dissect (#117) : `int` ou `float`, valeur laissée telle quelle si impossible."""
    chemin = parse_field_path(champ)
    valeur = resolve_field(doc, chemin)
    if valeur is _MISSING:
        return
    convertisseurs = {"int": int, "float": float}
    if type_cible not in convertisseurs:
        logger.warning(_("dissect : type {} inconnu pour convert_datatype"), type_cible)
        return
    try:
        assign_field(doc, chemin, convertisseurs[type_cible](valeur))
    except (TypeError, ValueError):
        logger.warning(_("dissect : conversion impossible de {} en {}"), champ, type_cible)


class PipelineInterpreter:
    """Interprète un pipeline Logstash complet, structure via `logstash_pipeline_parser`.

    Compile (parse) le pipeline une seule fois à la construction — comme
    `templates.CompiledTemplate` — puis `apply()` peut être appelé sur de
    nombreux documents sans reparser le texte du pipeline à chaque fois.
    """

    @traced
    def __init__(self, pipeline_source: str) -> None:
        """Parse le texte du pipeline.

        Les `${VAR}` / `${VAR:défaut}` sont d'abord substitués
        (`substituer_variables_environnement`, #122), comme Logstash au
        chargement du `.conf`.

        Args:
            pipeline_source: Contenu d'un fichier `.conf` Logstash complet
                (typiquement `input{}` optionnel, `filter{}`, `output{}`).

        Raises:
            ValueError: Variable d'environnement non définie sans valeur
                par défaut.
            logstash_pipeline_parser.tree.ParseError: Pipeline malformé.
        """
        self._ast: list = Pipeline(substituer_variables_environnement(pipeline_source)).parse()
        self._dissect_cache: dict[str, Dissector] = {}
        # #114 : un FingerprintFilter par bloc fingerprint (clé id(entry)).
        self._fingerprint_cache: dict[int, FingerprintFilter | None] = {}
        # #120 : réseaux d'un bloc cidr, lus une fois (clé id(entry)).
        self._cidr_cache: dict[int, list[Any]] = {}
        # `grok` (#112) : un GrokFilter compilé par jeu d'options rencontré
        # (motifs développés et regex compilées une seule fois).
        self._grok_cache: dict[str, GrokFilter] = {}
        # Chargées/ouvertes une seule fois par chemin rencontré dans le
        # pipeline, comme _dissect_cache (session 16, translate/mmdb).
        self._translate_dictionaries: dict[str, TranslateDictionary] = {}
        # #119 : un TranslateFilter par jeu d'options (None : bloc ignoré).
        self._translate_filters: dict[str, TranslateFilter | None] = {}
        # #121 : un UserAgentFilter par jeu d'options (None : bloc ignoré).
        self._useragent_filters: dict[str, UserAgentFilter | None] = {}
        # #121 : un XmlFilter par jeu d'options (None : bloc ignoré).
        self._xml_filters: dict[str, XmlFilter | None] = {}
        # #121 : un DnsFilter par jeu d'options (None : bloc ignoré) ; ses
        # caches durent donc autant que l'interpréteur, comme dans Logstash.
        self._dns_filters: dict[str, DnsFilter | None] = {}
        self._clone_filters: dict[str, CloneFilter | None] = {}
        self._split_filters: dict[str, SplitFilter | None] = {}
        self._mmdb_lookups: dict[str, GenericMmdbLookup] = {}
        # Idem pour `geoip` (session 21) : une base GeoIP2 City ouverte
        # une seule fois par chemin de fichier rencontré, comme
        # `_mmdb_lookups` (schéma de sortie différent — voir
        # `enrichment.GeoIPLookup` — mais même stratégie de cache).
        self._geoip_lookups: dict[str, GeoIPLookup] = {}
        # Idem pour `filters_json` (session 18) : un `FilterPipeline`
        # compilé une seule fois par chemin de fichier JSON rencontré.
        self._filter_pipelines: dict[str, FilterPipeline] = {}
        # Fabriques remplaçables (tests) — mêmes rôles que
        # `enrichment.TranslateDictionary`/`GenericMmdbLookup`/`GeoIPLookup`,
        # injectées plutôt qu'importées en dur dans `_apply_translate`/
        # `_apply_mmdb`/`_apply_geoip` pour permettre un double de test
        # sans fichier YAML/mmdb réel (voir tests/test_interpreter.py,
        # même principe que `tests/test_indexer.py::_FakeAsyncOpenSearchClient`
        # remplaçant `OpenSearchIndexer._client`).
        self._translate_dictionary_factory: Any = TranslateDictionary
        self._mmdb_lookup_factory: Any = GenericMmdbLookup
        self._geoip_lookup_factory: Any = GeoIPLookup

    @property
    def output_plugins(self) -> list[tuple[str, dict[str, Any]]]:
        """Plugins de tous les blocs `output{}`, branches comprises, avec leurs options brutes (#125).

        Options normalisées mais non rendues (pas de gabarit sprintf, pas de
        document) : sert à lire la configuration statique d'une sortie, par
        exemple la connexion `hosts`/`user`/`ssl` d'un `opensearch{}`.
        """
        logger.debug("output_plugins | {} section(s)", len(self._ast))
        plugins: list[tuple[str, dict[str, Any]]] = []
        for section in self._ast:
            if section[0] == "output":
                plugins.extend(_output_plugins(section[1]))
        logger.debug("output_plugins | {} plugin(s)", len(plugins))
        return plugins

    @property
    def has_output(self) -> bool:
        """`True` si le pipeline compilé contient au moins un bloc `output{}`.

        Utilisé par l'appelant (`src/omprog`, session 12) pour distinguer
        deux cas : un pipeline SANS `output{}` (le `.conf` ne sert alors
        qu'à piloter `filter{}` — mutate/dissect — et la destination
        d'indexation reste la configuration statique de l'appelant) d'un
        pipeline AVEC `output{}` dont aucune branche n'a matché pour un
        document donné (`apply()` renvoie alors `[]` intentionnellement —
        à distinguer de « pas de section `output{}` du tout »).
        """
        return any(section[0] == "output" for section in self._ast)

    @traced
    def close(self) -> None:
        """Ferme toute base mmdb/geoip ouverte par un plugin `mmdb {}`/`geoip {}` du pipeline.

        Ferme aussi les bases ouvertes par les steps "geoip" des fichiers
        `filters_json` chargés (`filters.FilterPipeline.close`). Sans effet si
        le pipeline ne contient aucun plugin `mmdb`/`geoip`/`filters_json`.
        Symétrique de `enrichment.GeoIPEnricher.close()` pour `--geoip-db`
        — à appeler par l'appelant (`src/omprog`) dans son bloc `finally`,
        une seule fois à l'arrêt. `TranslateDictionary` n'a rien à fermer
        (un dictionnaire chargé une fois en mémoire, pas un descripteur de
        fichier conservé).
        """
        for lookup in self._mmdb_lookups.values():
            lookup.close()
        self._mmdb_lookups.clear()
        for lookup in self._geoip_lookups.values():
            lookup.close()
        self._geoip_lookups.clear()
        # Un `filters_json` peut lui-même ouvrir une base (step "geoip",
        # `filters.GeoipFilter`) : sans ceci, elle restait ouverte jusqu'à la
        # fin du processus (issue #73, relevé en écrivant les tests).
        for pipeline in self._filter_pipelines.values():
            pipeline.close()
        self._filter_pipelines.clear()

    async def apply(
        self,
        doc: dict[str, Any],
        *,
        now: datetime | None = None,
        on_missing: str = "keep",
    ) -> list[OutputAction] | None:
        """Applique le pipeline à `doc` : exécute `filter{}` (en place), évalue `output{}`.

        Args:
            doc: Document événement, MODIFIÉ EN PLACE par les étapes
                `filter{}` (même convention que `filters.FilterPipeline.apply`).
            now: Horodatage transmis à `templates.render_template` pour les
                gabarits de date (`%{+FORMAT}`/`%{{FORMAT}}`) — voir sa
                docstring.
            on_missing: Transmis tel quel à `templates.render_template`
                ("keep" ou "empty") pour le rendu des options d'`output{}`
                (et pour les valeurs `%{...}` d'un `add_field` `mutate`).

        Returns:
            `None` si un step `"drop"` d'un `filters_json` (session 18) a
            abandonné ce document — DANS CE CAS, aucune action de sortie
            ne doit être considérée, MÊME si le pipeline n'a aucun bloc
            `output{}` du tout (à la différence de `[]`, qui laisse alors
            l'appelant retomber sur sa configuration d'indexation statique
            — voir `has_output`). Sinon, la liste ordonnée des actions de
            sortie dont les conditions englobantes se sont évaluées à vrai
            (une par plugin `output` atteint, dans l'ordre du pipeline),
            options déjà rendues — `[]` si le pipeline a un `output{}` mais
            qu'aucune branche n'a matché, ou s'il n'a pas de section
            `output{}` du tout (voir `has_output` pour les distinguer).
            Les sections `input{}` sont ignorées (voir la docstring du
            module, "Limites assumées"). Ne concerne que `doc` : `None`
            aussi si un `split` l'a remplacé, et les événements créés par
            `clone` / `split` ne sont pas rendus — voir `apply_events`
            (#141), qu'utilise omprog.

        Asynchrone depuis #34 (chantier full async, #1) : sémantique
        inchangée, l'appelant doit désormais `await` (la migration du
        connecteur RsysLogstach suit, #159).
        """
        logger.debug("apply | doc={}", doc)
        for event, actions in await self.apply_events(doc, now=now, on_missing=on_missing):
            if event is doc:
                logger.debug("apply | {} action(s)", len(actions))
                return actions
        logger.debug("apply | document d'origine écarté (drop ou split)")
        return None

    @traced
    async def apply_events(
        self,
        doc: dict[str, Any],
        *,
        now: datetime | None = None,
        on_missing: str = "keep",
    ) -> list[tuple[dict[str, Any], list[OutputAction]]]:
        """Applique le pipeline et renvoie chaque événement produit avec ses actions de sortie (#141).

        `doc` est modifié en place et reste en tête de liste s'il n'a pas
        été écarté (`drop`) ni remplacé (`split`). Les événements créés par
        `clone` ou `split` poursuivent le pipeline à partir du filtre
        suivant, dans la même branche, comme Logstash ; un `drop` n'écarte
        que l'événement qu'il atteint. Comme Logstash, toutes les sections
        `filter{}` passent avant les sections `output{}`.

        Returns:
            Liste (événement, actions de sortie rendues pour lui), dans
            l'ordre de production ; `[]` si tout a été écarté. Les actions
            sont `[]` si aucune sortie n'est atteinte (voir `has_output`).
            Asynchrone depuis #34 (chantier full async, #1) : sémantique
            inchangée, l'appelant doit désormais `await`.
        """
        events = [doc]
        for section in self._ast:
            if section[0] == "filter":
                # #35 : _run_filter_body est asynchrone (filtre dns réseau).
                produits: list[dict[str, Any]] = []
                for event in events:
                    produits.extend(await self._run_filter_body(section[1], event, now=now, on_missing=on_missing))
                events = produits
            # "input" : voir la docstring du module, "Limites assumées" — rien à appliquer.
        results: list[tuple[dict[str, Any], list[OutputAction]]] = []
        for event in events:
            actions: list[OutputAction] = []
            for section in self._ast:
                if section[0] == "output":
                    actions.extend(self._run_output_body(section[1], event, now=now, on_missing=on_missing))
            results.append((event, actions))
        logger.debug("apply_events | {} événement(s)", len(results))
        return results

    # ------------------------------------------------------------------
    # filter{}
    # ------------------------------------------------------------------

    @traced
    async def _run_filter_body(
        self, body: list, doc: dict[str, Any], *, now: datetime | None, on_missing: str
    ) -> list[dict[str, Any]]:
        """Exécute séquentiellement les entrées d'un corps `filter{}` (ou d'une branche).

        Asynchrone depuis #35 (chantier full async #1) : le plugin `dns`
        attend la résolution réseau (`dns.asyncresolver`).

        Returns:
            Les événements qui sortent du corps : `[doc]` en général, plus
            les copies de `clone`, les morceaux de `split` à la place de
            `doc`, ou `[]` après un `drop` (#141).
        """
        events = [doc]
        i = 0
        while i < len(body):
            entry = body[i]
            produced: list[dict[str, Any]] = []
            if entry[0] == "if":
                end = i
                for event in events:
                    end, matched = self._branch_chain(body, i, event)
                    if matched is None:
                        produced.append(event)
                    else:
                        produced.extend(await self._run_filter_body(matched, event, now=now, on_missing=on_missing))
                i = end if events else _skip_chain_tail(body, i + 1)
            else:
                for event in events:
                    try:
                        produced.extend(await self._run_filter_plugin(entry, event, now=now, on_missing=on_missing))
                    except _DocumentDropped as exc:
                        logger.debug("événement écarté par un filtre : {}", exc)
                i += 1
            events = produced
        logger.debug("_run_filter_body | {} événement(s)", len(events))
        return events

    @traced
    async def _run_filter_plugin(
        self, entry: list, doc: dict[str, Any], *, now: datetime | None, on_missing: str
    ) -> list[dict[str, Any]]:
        """Dispatche un plugin `filter{}` vers son comportement (ou l'ignore, tracé).

        Returns:
            Les événements qui en sortent : `[doc]`, sauf `clone` (`doc` puis
            ses copies) et `split` (les morceaux à la place de `doc`).

        Raises:
            _DocumentDropped: `drop` (natif ou step d'un `filters_json`).
        """
        name, attrs = entry
        options = _normalize_attrs(attrs)
        reussi: bool | None = None
        if name in ("clone", "split"):
            return self._apply_multi(name, doc, options, now=now, on_missing=on_missing)
        if name == "dissect":
            reussi = self._apply_dissect(doc, options)
        elif name == "kv":
            reussi = self._apply_kv(doc, options)
        elif name == "mutate":
            self._apply_mutate(doc, options, now=now, on_missing=on_missing)  # options communes incluses
        elif name == "translate":
            reussi = self._apply_translate(doc, options, now=now, on_missing=on_missing)
        elif name == "mmdb":
            reussi = self._apply_mmdb(doc, options)
        elif name == "geoip":
            reussi = self._apply_geoip(doc, options)
        elif name == "filters_json":
            self._apply_filters_json(doc, options, now=now, on_missing=on_missing)
        elif name == "date":
            reussi = self._apply_date(doc, options)
        elif name == "drop":
            self._apply_drop(options)
        elif name == "fingerprint":
            reussi = self._apply_fingerprint(entry, doc, options)
        elif name == "grok":
            reussi = self._apply_grok(doc, options)
        elif name == "json":
            reussi = self._apply_json(doc, options)
        elif name == "useragent":
            reussi = self._apply_useragent(doc, options)
        elif name == "xml":
            reussi = self._apply_xml(doc, options)
        elif name == "dns":
            reussi = await self._apply_dns(doc, options)
        elif name in PLUGINS_SIMPLES:
            reussi = PLUGINS_SIMPLES[name](doc, options)
        elif name == "cidr":
            if id(entry) not in self._cidr_cache:
                self._cidr_cache[id(entry)] = reseaux_cidr(options)
            reussi = appliquer_cidr(doc, options, self._cidr_cache[id(entry)])
        else:
            logger.debug("plugin filter non interprété (structure seule) : {}", name)
        # #116 : options communes (add_field/add_tag/remove_field/remove_tag)
        # après un filtre réussi, comme `filter_matched` de Logstash.
        if reussi:
            apply_common_options(doc, options, now=now, on_missing=on_missing)
        logger.debug("_run_filter_plugin: {} réussi={}", name, reussi)
        return [doc]

    @traced
    def _apply_multi(
        self, name: str, doc: dict[str, Any], options: dict[str, Any], *, now: datetime | None, on_missing: str
    ) -> list[dict[str, Any]]:
        """`clone { … }` / `split { … }` — délègue à `multievents` (#141).

        Construit une fois par jeu d'options (clé JSON triée) ; options
        invalides : bloc ignoré, tracé. Les options communes (#116)
        s'appliquent à chaque nouvel événement, pas à l'original, comme
        `filter_matched` dans `clone.rb` / `split.rb`.
        """
        cache: dict[str, Any] = self._clone_filters if name == "clone" else self._split_filters
        cle = json.dumps(options, sort_keys=True, default=str)
        if cle not in cache:
            try:
                cache[cle] = CloneFilter(options) if name == "clone" else SplitFilter(options)
            except ValueError as exc:
                logger.debug("plugin {} ignoré : {}", name, exc)
                cache[cle] = None
        filtre = cache[cle]
        if filtre is None:
            return [doc]
        nouveaux = filtre.apply(doc)
        if nouveaux is None:
            logger.debug("_apply_multi | {} sans nouvel événement", name)
            return [doc]
        for event in nouveaux:
            apply_common_options(event, options, now=now, on_missing=on_missing)
        resultat = [doc, *nouveaux] if name == "clone" else nouveaux
        logger.debug("_apply_multi | {} -> {} événement(s)", name, len(resultat))
        return resultat

    @traced
    def _apply_fingerprint(self, entry: list, doc: dict[str, Any], options: dict[str, Any]) -> bool:
        """`fingerprint { … }` natif (#114) — voir `fingerprint.FingerprintFilter`.

        Une méthode inconnue est une erreur de configuration : journalisée
        une fois, le bloc est ensuite sans effet (pas de tag).
        """
        if id(entry) not in self._fingerprint_cache:
            try:
                self._fingerprint_cache[id(entry)] = FingerprintFilter(options)
            except ValueError as exc:
                logger.warning(_("fingerprint ignoré : {}"), exc)
                self._fingerprint_cache[id(entry)] = None
        filtre = self._fingerprint_cache[id(entry)]
        reussi = filtre is not None and filtre.apply(doc)
        logger.debug("_apply_fingerprint: {}", reussi)
        return reussi

    @traced
    def _apply_drop(self, options: dict[str, Any]) -> None:
        """`filter { [if ...] drop { [percentage => N] } }` — plugin `drop` natif (#34).

        Comme Logstash, écarte le document courant : lève `_DocumentDropped`,
        que `apply()` convertit en `None`, exactement comme un step `"drop"`
        d'un `filters_json`. La condition se place sur le `if` englobant.
        `percentage` (entier 0-100, 100 par défaut) n'écarte qu'une part
        aléatoire des documents, comme l'option officielle ; une valeur non
        numérique est ignorée (on écarte, comme sans l'option).
        """
        logger.debug("_apply_drop() called | options={}", options)
        brut = options.get("percentage", 100)
        try:
            pourcentage = float(brut)
        except (TypeError, ValueError):
            logger.debug("drop : percentage non numérique ignoré : {!r}", brut)
            pourcentage = 100.0
        if pourcentage < 100 and random.random() * 100 >= pourcentage:
            logger.debug("_apply_drop() returning | document conservé (percentage={})", pourcentage)
            return
        logger.debug("_apply_drop() raising | _DocumentDropped (percentage={})", pourcentage)
        raise _DocumentDropped("drop natif")

    @traced
    def _apply_grok(self, doc: dict[str, Any], options: dict[str, Any]) -> bool:
        """`filter { grok { match => { champ => motif } ... } }` — délègue à `grok.GrokFilter` (#112).

        Compilé une fois par jeu d'options (clé JSON triée), comme
        `_dissect_cache`. Options, défauts et différences avec Logstash :
        docstring du module `grok`.

        Returns:
            True si un motif a correspondu (options communes, #116).
        """
        cle = json.dumps(options, sort_keys=True, default=str)
        filtre = self._grok_cache.get(cle)
        if filtre is None:
            filtre = GrokFilter(options)
            self._grok_cache[cle] = filtre
        return filtre.apply(doc)

    @traced
    def _apply_useragent(self, doc: dict[str, Any], options: dict[str, Any]) -> bool:
        """`filter { useragent { source => ... } }` — délègue à `useragent.UserAgentFilter` (#121).

        Construit une fois par jeu d'options (clé JSON triée). Options
        invalides ou fichier `regexes` illisible : bloc ignoré, tracé,
        sans nouvel essai.

        Returns:
            True si le User-Agent a été analysé (options communes, #116).
        """
        cle = json.dumps(options, sort_keys=True, default=str)
        if cle not in self._useragent_filters:
            try:
                filtre: UserAgentFilter | None = UserAgentFilter(options)
            except ValueError as exc:
                logger.debug("plugin useragent ignoré : {}", exc)
                filtre = None
            self._useragent_filters[cle] = filtre
        filtre = self._useragent_filters[cle]
        if filtre is None:
            return False
        return filtre.apply(doc)

    @traced
    def _apply_xml(self, doc: dict[str, Any], options: dict[str, Any]) -> bool:
        """`filter { xml { source => ... target => ... } }` — délègue à `xmlfilter.XmlFilter` (#121).

        Construit une fois par jeu d'options (clé JSON triée). Options
        invalides (`target` absent avec `store_xml`, `parse_options` refusé…) :
        bloc ignoré, tracé, sans nouvel essai.

        Returns:
            True si une valeur a été extraite (options communes, #116).
        """
        cle = json.dumps(options, sort_keys=True, default=str)
        if cle not in self._xml_filters:
            try:
                filtre: XmlFilter | None = XmlFilter(options)
            except ValueError as exc:
                logger.debug("plugin xml ignoré : {}", exc)
                filtre = None
            self._xml_filters[cle] = filtre
        filtre = self._xml_filters[cle]
        if filtre is None:
            return False
        return filtre.apply(doc)

    @traced
    async def _apply_dns(self, doc: dict[str, Any], options: dict[str, Any]) -> bool:
        """`filter { dns { reverse => [...] resolve => [...] } }` — délègue à `dnslookup.DnsFilter` (#121).

        Asynchrone depuis #35 (chantier full async #1).

        Construit une fois par jeu d'options (clé JSON triée). Options
        invalides (par exemple un `${VAR}` non résolu à la place d'un
        nombre) : bloc ignoré, tracé, sans nouvel essai.

        Returns:
            True si le filtre est allé au bout (options communes, #116).
        """
        cle = json.dumps(options, sort_keys=True, default=str)
        if cle not in self._dns_filters:
            try:
                filtre: DnsFilter | None = DnsFilter(options)
            except ValueError as exc:
                logger.debug("plugin dns ignoré : {}", exc)
                filtre = None
            self._dns_filters[cle] = filtre
        filtre = self._dns_filters[cle]
        if filtre is None:
            return False
        return await filtre.apply(doc)

    @traced
    def _apply_json(self, doc: dict[str, Any], options: dict[str, Any]) -> bool:
        """`json { source target skip_on_invalid_json tag_on_failure }` natif (#115).

        Comme le filtre officiel : source absente, rien ; JSON invalide,
        `tag_on_failure` (défaut `_jsonparsefailure`) sauf si
        `skip_on_invalid_json` ; sans `target`, l'objet décodé est fusionné
        à la racine (un JSON non objet est alors un échec).

        Returns:
            True si le document a été modifié (options communes, #116).
        """
        source = options.get("source")
        if not isinstance(source, str):
            logger.warning(_("json ignoré : option source manquante"))
            return False
        texte = resolve_field(doc, parse_field_path(source))
        if texte is _MISSING or texte is None:
            return False
        echec = options.get("tag_on_failure", ["_jsonparsefailure"])
        try:
            if not isinstance(texte, str):
                raise TypeError(type(texte).__name__)
            decode = json.loads(texte)
        except (ValueError, TypeError) as exc:
            if options.get("skip_on_invalid_json", False) is not True:
                add_failure_tags(doc, echec)
                logger.warning(_("json : analyse impossible du champ {} : {}"), source, exc)
            return False
        cible = options.get("target")
        if isinstance(cible, str):
            assign_field(doc, parse_field_path(cible), decode)
        elif isinstance(decode, dict):
            doc.update(decode)
        else:
            add_failure_tags(doc, echec)
            logger.warning(_("json : un JSON non objet exige l'option target"))
            return False
        logger.debug("_apply_json: {} décodé", source)
        return True

    @traced
    def _apply_dissect(self, doc: dict[str, Any], options: dict[str, Any]) -> bool:
        """`dissect { mapping convert_datatype tag_on_failure }` — réutilise `filters.Dissector`.

        Comme le filtre officiel (#117) : un champ source absent, vide ou non
        texte, ou un patron qui ne s'applique pas, ajoute `tag_on_failure`
        (défaut `_dissectfailure`) et coupe `convert_datatype` et les options
        communes. Sinon `convert_datatype` (`int`, `float`) convertit les
        champs indiqués.

        Returns:
            True sans échec et avec au moins `mapping` ou `convert_datatype`
            (options communes, #116).
        """
        mapping = options.get("mapping")
        conversions = options.get("convert_datatype")
        mapping = mapping if isinstance(mapping, dict) else {}
        conversions = conversions if isinstance(conversions, dict) else {}
        echec = False
        for source_field, pattern in mapping.items():
            dissector = self._dissector(pattern)
            if dissector is None:
                continue
            value = resolve_field(doc, parse_field_path(source_field))
            extracted = dissector.apply(value) if isinstance(value, str) and value else None
            if extracted is None:
                logger.warning(_("dissect : échec sur le champ {}"), source_field)
                echec = True
                continue
            for champ, texte in extracted.items():
                assign_field(doc, parse_field_path(champ), texte)
        if echec:
            add_failure_tags(doc, options.get("tag_on_failure", ["_dissectfailure"]))
            return False
        for champ, type_cible in conversions.items():
            _convertir_dissect(doc, champ, type_cible)
        return bool(mapping or conversions)

    @traced
    def _dissector(self, pattern: Any) -> Dissector | None:
        """Compile (avec cache) un patron dissect ; None si le patron est invalide."""
        if not isinstance(pattern, str):
            return None
        dissector = self._dissect_cache.get(pattern)
        if dissector is None:
            try:
                dissector = Dissector(pattern)
            except ValueError:
                logger.debug("patron dissect invalide, ignoré : {}", pattern)
                return None
            self._dissect_cache[pattern] = dissector
        return dissector

    @traced
    def _apply_kv(self, doc: dict[str, Any], options: dict[str, Any]) -> bool:
        """`kv { source => ... [target => ...] ... }` — plugin `kv` natif (#113).

        Réutilise `filters.KeyValueFilter` (même algorithme que le step
        `kv` de `--filters`, pas de duplication). Options du plugin
        officiel : `source` (champ à découper, `"message"` par défaut),
        `target` (hash de destination des paires, racine du document par
        défaut — fusionné dans un hash existant, comme Logstash),
        `field_split`, `value_split`, `include_keys`, `exclude_keys`,
        `prefix`, `trim_key`, `trim_value`, `allow_duplicate_values`.
        Champ source absent ou non texte : rien (comme `dissect`), tracé.

        Returns:
            True si au moins une paire a été extraite (options communes, #116).
        """
        source = options.get("source", "message")
        if not isinstance(source, str):
            logger.debug("kv : source non texte, ignoré : {!r}", source)
            return False
        value = doc.get(source)
        if not isinstance(value, str):
            logger.debug("kv : champ source absent ou non texte, ignoré : {}", source)
            return False
        include_keys = options.get("include_keys")
        exclude_keys = options.get("exclude_keys")
        kv = KeyValueFilter(
            field_split=options.get("field_split", " ") if isinstance(options.get("field_split"), str) else " ",
            value_split=options.get("value_split", "=") if isinstance(options.get("value_split"), str) else "=",
            prefix=options.get("prefix", "") if isinstance(options.get("prefix"), str) else "",
            include_keys=[k for k in include_keys if isinstance(k, str)] if isinstance(include_keys, list) else None,
            exclude_keys=[k for k in exclude_keys if isinstance(k, str)] if isinstance(exclude_keys, list) else None,
            trim_key=options.get("trim_key", "") if isinstance(options.get("trim_key"), str) else "",
            trim_value=options.get("trim_value", "") if isinstance(options.get("trim_value"), str) else "",
            allow_duplicate_values=bool(options.get("allow_duplicate_values", True)),
        )
        pairs = kv.apply(value)
        if not pairs:
            return False
        target = options.get("target")
        if isinstance(target, str) and target:
            path = parse_field_path(target)
            base = resolve_field(doc, path)
            if isinstance(base, dict):
                base.update(pairs)
            else:
                assign_field(doc, path, dict(pairs))
        else:
            doc.update(pairs)
        return True

    @traced
    def _apply_mutate(
        self, doc: dict[str, Any], options: dict[str, Any], *, now: datetime | None, on_missing: str
    ) -> None:
        """`mutate { ... }` — délègue à `filters.apply_mutate` (session 14, plus de duplication).

        Avant la session 14, ce module réimplémentait sa propre version de
        cette logique (`_convert_value`/`_apply_gsub` locaux) tandis que
        `FilterPipeline` (`filters.py`, `--filters`) n'avait AUCUN
        équivalent `mutate` — deux gaps signalés par FEATURES.md, item
        "Proposé" #1. Les deux mécanismes de filtrage partagent désormais
        une seule implémentation (`filters.apply_mutate`) : voir la
        docstring de cette fonction pour le format exact des options.
        """
        apply_mutate(doc, options, now=now, on_missing=on_missing)

    @traced
    def _apply_translate(
        self, doc: dict[str, Any], options: dict[str, Any], *, now: datetime | None, on_missing: str
    ) -> bool:
        """`translate { source => ... [target => ...] [dictionary(_path) => ...] ... }` (#119).

        Délègue à `filters.TranslateFilter` (même code que le step
        `translate` de `--filters`), construit au premier document concerné
        et mis en cache par jeu d'options. Les dictionnaires fichiers sont
        partagés par chemin (`_translate_dictionaries`, chargés une seule
        fois). Options incohérentes ou fichier illisible : bloc ignoré,
        tracé, sans nouvel essai.

        Returns:
            True si les options communes (#116) doivent suivre.
        """
        cle = json.dumps(options, sort_keys=True, default=str)
        if cle not in self._translate_filters:
            try:
                filtre: TranslateFilter | None = TranslateFilter(options, dictionary_factory=self._translate_dictionary)
            except ValueError as exc:
                logger.debug("plugin translate ignoré : {}", exc)
                filtre = None
            self._translate_filters[cle] = filtre
        filtre = self._translate_filters[cle]
        if filtre is None:
            return False
        return filtre.apply(doc, now=now, on_missing=on_missing)

    @traced
    def _translate_dictionary(self, dictionary_path: str) -> TranslateDictionary:
        """Dictionnaire `translate` du chemin, chargé une seule fois (`_translate_dictionary_factory`)."""
        dictionary = self._translate_dictionaries.get(dictionary_path)
        if dictionary is None:
            dictionary = self._translate_dictionary_factory(dictionary_path)
            self._translate_dictionaries[dictionary_path] = dictionary
        return dictionary

    @traced
    def _apply_mmdb(self, doc: dict[str, Any], options: dict[str, Any]) -> bool:
        """`mmdb { source => ... database => ... target => ... }`.

        Résout `source` (chemin `%{[a][b]}`), interroge la base de
        `database` (ouverte une seule fois, mise en cache par chemin —
        voir `enrichment.GenericMmdbLookup`) et fusionne CHAQUE champ de
        l'enregistrement brut trouvé sous `target`, un niveau de
        profondeur (`target[clé] = valeur`) — sans écraser les champs déjà
        présents sous `target` qui ne sont pas retournés par la base
        (contrairement à une simple affectation `doc[target] = record`,
        qui aurait détruit ex. `[host][ip]`/`[host][domain]` posés par un
        `mutate` précédent dans le pipeline réel). Aucun schéma de champs
        n'est imposé — voir la docstring de `GenericMmdbLookup` pour
        pourquoi ce n'est délibérément PAS un simple rattachement à
        `enrichment.GeoIPEnricher`. IP absente/invalide ou aucune
        correspondance : `target` n'est pas touché.
        """
        source = options.get("source")
        database = options.get("database")
        target = options.get("target")
        if not (isinstance(source, str) and isinstance(database, str) and isinstance(target, str)):
            logger.debug("plugin mmdb mal formé (source/database/target requis), ignoré")
            return False
        ip = resolve_field(doc, parse_field_path(source))
        if not isinstance(ip, str):
            return False
        lookup = self._mmdb_lookups.get(database)
        if lookup is None:
            try:
                lookup = self._mmdb_lookup_factory(database)
            except (OSError, maxminddb.InvalidDatabaseError) as exc:
                logger.debug("base mmdb illisible, ignorée : {} ({})", database, exc)
                return False
            self._mmdb_lookups[database] = lookup
        record = lookup.lookup(ip)
        if not record:
            return False
        target_path = parse_field_path(target)
        for field_name, field_value in record.items():
            assign_field(doc, [*target_path, str(field_name)], field_value)
        return True

    @traced
    def _apply_geoip(self, doc: dict[str, Any], options: dict[str, Any]) -> bool:
        """`geoip { source => ... [target => "geoip"] database => ... [fields] [tag_on_failure] }`.

        LE filtre officiel de Logstash. Contrairement à `mmdb` (base
        personnalisée, enregistrement brut fusionné sans schéma), `geoip`
        a un format de sortie FIGÉ, celui du filtre officiel de Logstash
        pour une base GeoLite2 City — voir `enrichment._build_geoip_record`,
        partagée avec `GeoIPEnricher` (`--geoip-db`, CLI d'`omprog`).
        `target` prend la valeur par défaut de Logstash (`"geoip"`) si
        absent, contrairement à `translate`/`mmdb` où `target` est requis
        explicitement — Logstash documente ce défaut précis pour `geoip`,
        contrairement aux deux autres. `database` reste, lui, TOUJOURS
        requis explicitement : Logstash le rend optionnel en repliant sur
        une base GeoLite2 embarquée dans le plugin officiel — ce projet
        n'en embarque aucune (voir `--geoip-db`/`GeoIPEnricher`, déjà
        exigé explicitement depuis la session 5). Résultat écrit à
        `target` comme un REMPLACEMENT (pas une fusion champ par champ
        comme `mmdb` — le schéma geoip officiel est un sous-objet
        autonome, jamais partagé avec d'autres champs posés par un
        `mutate` précédent sous le même chemin, contrairement au cas réel
        qui motive la fusion de `mmdb`, voir sa docstring).

        `fields` (session 22, optionnel — tous les champs par défaut,
        comme Logstash) : restreint l'enregistrement à ces clés
        (`enrichment._build_geoip_record` en produit jusqu'à neuf — `ip`,
        `city_name`, `continent_code`, `country_code2`, `country_name`,
        `postal_code`, `region_name`, `timezone`, `latitude`/`longitude`/
        `location`), dans l'ordre où `_build_geoip_record` les produit
        (pas celui de `fields`) — une clé de `fields` absente de
        l'enregistrement (ex. jamais produite pour cette IP, comme
        `postal_code` souvent absent) est simplement sans effet, pas une
        erreur. `fields` mal typé (pas une liste, ou une liste vide) est
        traité comme absent : aucune restriction.

        `tag_on_failure` (session 22, `["_geoip_lookup_failure"]` par
        défaut, comme Logstash — voir `_geoip_lookup_failure`) : IP
        absente/invalide OU aucune correspondance dans la base, désormais
        signalés par un tag dans `doc["tags"]` (`target` reste alors
        inchangé, comme avant) plutôt que silencieusement, cohérent avec
        `tag_on_failure` de `_apply_date`. Un plugin mal formé (`source`/
        `database`/`target` absents ou de mauvais type) ou une base
        illisible restent, eux, des erreurs de CONFIGURATION ignorées
        SANS tag — `tag_on_failure` signale un échec de résolution POUR
        UN document donné, pas un pipeline mal écrit (même distinction
        que `filters_json`, voir sa docstring plus haut dans ce module).
        """
        source = options.get("source")
        database = options.get("database")
        target = options.get("target", "geoip")
        if not (isinstance(source, str) and isinstance(database, str) and isinstance(target, str)):
            logger.debug("plugin geoip mal formé (source/database requis), ignoré")
            return False
        ip = resolve_field(doc, parse_field_path(source))
        if not isinstance(ip, str):
            self._geoip_lookup_failure(doc, options)
            return False
        lookup = self._geoip_lookups.get(database)
        if lookup is None:
            try:
                lookup = self._geoip_lookup_factory(database)
            except (OSError, maxminddb.InvalidDatabaseError) as exc:
                logger.debug("base geoip illisible, ignorée : {} ({})", database, exc)
                return False
            self._geoip_lookups[database] = lookup
        record = lookup.lookup(ip)
        if not record:
            self._geoip_lookup_failure(doc, options)
            return False
        fields = options.get("fields")
        if isinstance(fields, list) and fields:
            record = {key: value for key, value in record.items() if key in fields}
        assign_field(doc, parse_field_path(target), record)
        return True

    @staticmethod
    @traced
    def _geoip_lookup_failure(doc: dict[str, Any], options: dict[str, Any]) -> None:
        """`tag_on_failure` du plugin `geoip` (défaut `["_geoip_lookup_failure"]`, comme Logstash)."""
        add_failure_tags(doc, options.get("tag_on_failure", ["_geoip_lookup_failure"]))

    @traced
    def _apply_filters_json(
        self, doc: dict[str, Any], options: dict[str, Any], *, now: datetime | None, on_missing: str
    ) -> None:
        """`filters_json { path => "..." [on_missing => "keep"|"empty"] }` (session 18).

        Voir la docstring du module ("filters_json") pour le contexte —
        c'est le pont entre `--pipeline-conf` et `--filters` resté
        manquant depuis la session 9 (item "Proposé" #3 de FEATURES.md).
        Charge et compile un `filters.FilterPipeline` depuis le fichier
        JSON `path` (mis en cache par chemin, comme `_dissect_cache`), lui
        applique `doc` EN PLACE. Si ce `FilterPipeline` comporte un step
        `"drop"` qui correspond, lève `_DocumentDropped` — capturée par
        `apply()`, qui renvoie alors `None` pour tout le pipeline (pas
        seulement cette étape).

        `on_missing` de l'option `filters_json` (rendu d'un éventuel
        gabarit sprintf de step `mutate.add_field` À L'INTÉRIEUR du
        `FilterPipeline` chargé) est délibérément indépendant du
        paramètre `on_missing` de cette méthode (rendu des gabarits
        d'`output{}`/`mutate` du `.conf` lui-même) — comme
        `--filters-on-missing` et `--pipeline-on-missing` restent deux
        réglages indépendants côté `src/omprog` depuis la session 14.
        """
        path = options.get("path")
        if not isinstance(path, str):
            logger.debug("plugin filters_json sans 'path' valide, ignoré")
            return
        pipeline = self._filter_pipelines.get(path)
        if pipeline is None:
            try:
                pipeline = FilterPipeline(load_json_file(path))
            except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
                # OSError/JSONDecodeError : fichier illisible ou JSON invalide
                # (util.load_json_file). ValueError/KeyError : type de step
                # inconnu, ou step bien formé JSON mais sans une clé requise
                # par FilterPipeline._compile_step (ex. "source" manquant) —
                # contrairement à --filters au démarrage (src/omprog, échec
                # franc voulu), ce fichier est référencé PAR un pipeline .conf
                # déjà en cours d'exécution : une seule étape mal configurée
                # ne doit pas faire planter tout le traitement des messages.
                logger.debug("fichier filters_json illisible/invalide, ignoré : {} ({})", path, exc)
                return
            self._filter_pipelines[path] = pipeline
        step_on_missing = options.get("on_missing", "keep")
        if not pipeline.apply(doc, now=now, on_missing=step_on_missing):
            raise _DocumentDropped

    @traced
    def _apply_date(self, doc: dict[str, Any], options: dict[str, Any]) -> bool:
        """`filter { date { match => [champ, motif, ...] [target] [timezone] [tag_on_failure] } }`.

        Session 19 — item du nouvel audit Context7 (voir CLAUDE.md,
        session 19) : le filtre `date` officiel de Logstash, absent
        jusqu'ici (structure normalisée, aucun comportement appliqué).
        Justifié par le pipeline réel de Mathilde lui-même : son
        `output{}` route vers un index nommé `%{+YYYY.MM.dd}`
        (`pipeline.example.d/30_output_...conf`), un gabarit qui dérive
        sa date de `"@timestamp"` (voir `templates._event_timestamp`) —
        sans `date`, ce champ ne peut jamais refléter l'horodatage
        embarqué DANS le message d'un switch (ex. extrait par un
        `dissect` en amont), seulement l'heure de traitement.

        `match` : `[champ, motif1, motif2, ...]` — chaque motif est
        essayé dans l'ordre jusqu'au premier qui correspond (comme
        Logstash). Trois formes de motif : `"ISO8601"` (`datetime.
        fromisoformat`, `Z` accepté), `"UNIX"`/`"UNIX_MS"` (epoch
        secondes/millisecondes, `float`/`int`), ou un motif Java/Joda
        traduit en motif `strptime` par `filters.translate_date_pattern` — voir
        sa docstring pour la portée volontairement réduite (PAS `SSS`,
        PAS de nom de fuseau textuel). Un motif non reconnu ou qui ne
        correspond pas à la valeur est traité comme un échec de CE motif
        seulement (comme un motif Logstash qui ne correspond pas à la
        valeur) : les motifs suivants de `match` sont encore essayés.

        `target` (`"@timestamp"` par défaut) : chemin d'écriture (syntaxe
        `%{[a][b]}` déjà normalisée par `templates.parse_field_path`).
        `timezone` (nom IANA, ex. `"Europe/Paris"`) : fuseau appliqué
        SEULEMENT si le motif ne porte lui-même aucune information de
        fuseau (ex. un motif sans `Z`/`XXX`) — sinon UTC par défaut,
        comme Logstash. Le résultat est toujours ramené en UTC et écrit
        comme chaîne ISO8601 (`datetime.isoformat()`), jamais un objet
        `datetime` brut : `doc` doit rester sérialisable en JSON tel quel
        pour `indexer.OpenSearchIndexer.send` — voir PATTERNS.md.

        Aucune correspondance : ajoute les tags de `tag_on_failure`
        (`["_dateparsefailure"]` par défaut, comme Logstash) à
        `doc["tags"]` sans doublon, plutôt que de laisser `target`
        inchangé silencieusement — signale l'échec de façon visible et
        cohérente avec `add_tag`/`remove_tag` de `filters.apply_mutate`.

        Depuis #34, toute la logique vit dans `filters.DateFilter`, partagée
        avec le step `"date"` de `--filters` (`FilterPipeline`) : une seule
        implémentation pour les deux mécanismes, comme `apply_mutate`
        (session 14), `GeoipFilter` (session 24) et `TranslateFilter`
        (session 25). Un `match` mal formé reste ici ignoré (log DEBUG) :
        un `.conf` imparfait ne doit pas faire planter le traitement.
        """
        return DateFilter(options).apply(doc)

    # ------------------------------------------------------------------
    # output{}
    # ------------------------------------------------------------------

    @traced
    def _run_output_body(
        self, body: list, doc: dict[str, Any], *, now: datetime | None, on_missing: str
    ) -> list[OutputAction]:
        """Évalue séquentiellement les entrées d'un corps `output{}` (ou d'une branche)."""
        actions: list[OutputAction] = []
        i = 0
        while i < len(body):
            entry = body[i]
            if entry[0] == "if":
                i, matched = self._branch_chain(body, i, doc)
                if matched is not None:
                    actions.extend(self._run_output_body(matched, doc, now=now, on_missing=on_missing))
            else:
                name, attrs = entry
                options = _normalize_attrs(attrs)
                rendered = _render_templates(options, doc, now=now, on_missing=on_missing)
                actions.append(OutputAction(plugin=name, options=rendered))
                i += 1
        return actions

    # ------------------------------------------------------------------
    # if / else if / else — commun à filter{} et output{}
    # ------------------------------------------------------------------

    @traced
    def _branch_chain(self, body: list, index: int, doc: dict[str, Any]) -> tuple[int, list | None]:
        """Évalue une chaîne `if`/`else if`*/`else`? démarrant en `body[index]`.

        Returns:
            Un couple (index après la chaîne entière dans `body`, corps de
            la branche retenue — `None` si aucune condition n'a matché et
            qu'il n'y a pas de `else`).
        """
        entry = body[index]
        condition, branch = entry[1][:-1], entry[1][-1][0]
        j = index + 1
        if _evaluate_condition(condition, doc):
            return _skip_chain_tail(body, j), branch
        while j < len(body) and body[j][0] == "else if":
            elseif_entry = body[j]
            elseif_condition, elseif_branch = elseif_entry[1][:-1], elseif_entry[1][-1][0]
            if _evaluate_condition(elseif_condition, doc):
                return _skip_chain_tail(body, j + 1), elseif_branch
            j += 1
        if j < len(body) and body[j][0] == "else":
            # `else` n'a pas le même niveau d'imbrication que if/else if dans
            # l'AST de la bibliothèque (pas de [-1][0] à défaire) — vérifié
            # concrètement, voir tests/test_interpreter.py.
            return j + 1, body[j][1]
        return j, None


@traced
def _output_plugins(body: list) -> list[tuple[str, dict[str, Any]]]:
    """Plugins d'un corps `output{}`, en descendant dans `if`/`else if`/`else` (voir `_branch_chain`)."""
    plugins: list[tuple[str, dict[str, Any]]] = []
    for entry in body:
        if entry[0] in ("if", "else if"):
            plugins.extend(_output_plugins(entry[1][-1][0]))
        elif entry[0] == "else":
            plugins.extend(_output_plugins(entry[1]))
        else:
            plugins.append((entry[0], _normalize_attrs(entry[1])))
    logger.debug("_output_plugins | {}", len(plugins))
    return plugins


@traced
def _skip_chain_tail(body: list, index: int) -> int:
    """Avance `index` après le reste d'une chaîne if/else if/else déjà tranchée."""
    while index < len(body) and body[index][0] in ("else if", "else"):
        index += 1
    return index


# ----------------------------------------------------------------------
# Normalisation générique des options de plugin (AST -> structures Python)
# ----------------------------------------------------------------------


@traced
def _coerce_literal(value: Any) -> Any:
    """Convertit un littéral AST typé (chemin, adresse IP) en chaîne — forme JSON valide.

    `logstash_pipeline_parser` reconnaît certaines valeurs littérales QUI
    RESSEMBLENT à un chemin de fichier ou une adresse IP et les type en
    conséquence (`pathlib.PurePath`, `ipaddress.IPv4Address`/`IPv6Address`)
    plutôt qu'en chaîne — vérifié concrètement en session 16, sur deux cas
    distincts du même pipeline réel (`pipeline.example.d/`) :
        - `dictionary_path`/`database` (options `translate`/`mmdb`) :
          `PosixPath('/etc/.../ifName.yml')`, pas `'/etc/.../ifName.yml'`.
        - un littéral de condition (`if [host][ip] == "127.0.0.1"`) :
          `IPv4Address('127.0.0.1')`, pas `'127.0.0.1'`.
    Sans cette conversion, une comparaison chaîne <-> IPv4Address est
    TOUJOURS fausse en Python (aucune coercition automatique, quelle que
    soit l'égalité réelle des adresses) — un bug de cohérence resté
    invisible jusqu'ici dans ce module : `mutate.add_field`/`.replace` ont
    le même effet dans cette implémentation (voir `apply_mutate`, sa
    docstring), donc les deux branches du if/else de ce pipeline réel
    produisaient silencieusement le même document, sans qu'un test
    existant ne puisse le distinguer. Corrigé ici plutôt que laissé en
    l'état : `rsyslogstach` ne travaille qu'avec des documents JSON
    (chaînes, nombres, booléens, `null`) — un chemin ou une IP n'a jamais
    de raison d'y être un type Python autre qu'une chaîne.
    """
    if isinstance(value, (PurePath, IPv4Address, IPv6Address)):
        return str(value)
    return value


@traced
def _normalize_attrs(attrs: list) -> dict[str, Any]:
    """Normalise la liste d'attributs d'un plugin (`[[nom, [valeur]], ...]`) en dict.
    Un même nom d'attribut PEUT apparaître plusieurs fois dans un bloc
    plugin — ce n'est PAS un pipeline mal formé (contrairement à ce que
    documentait cette fonction avant la session 15) : un `mutate {
    add_field => {"a" => "1"} add_field => {"b" => "2"} }` avec plusieurs
    `add_field` successifs est un motif Logstash authentique, rencontré
    tel quel dans un pipeline de production réel transmis par Mathilde
    (session 15). Avant cette session, une dict comprehension ne gardait
    que la DERNIÈRE occurrence de chaque nom, perdant silencieusement les
    autres — confirmé en rejouant ce pipeline (voir CLAUDE.md, session 15).

    Politique de fusion, générique (ne dépend pas de connaître chaque
    plugin, seulement de la FORME de la valeur normalisée) :
        - deux occurrences dont la valeur est un dict : fusionnées
          (`dict.update`, la dernière occurrence gagne sur une clé en
          commun) — couvre `add_field`/`replace`/`convert`/`rename`
          répétés.
        - deux occurrences dont la valeur est une liste : concaténées
          (`list.extend`, dans l'ordre d'apparition) — couvre
          `remove_field`/`add_tag`/`remove_tag`/`strip` répétés.
        - toute autre valeur (scalaire) : la dernière occurrence
          l'emporte — comportement historique inchangé pour ce cas, qui
          reste un motif limite (répéter une option scalaire n'a pas de
          sens fusionnable évident).
    """
    result: dict[str, Any] = {}
    for name, value_wrapper in attrs:
        value = _normalize_value(value_wrapper[0])
        existing = result.get(name)
        if isinstance(existing, dict) and isinstance(value, dict):
            existing.update(value)
        elif isinstance(existing, list) and isinstance(value, list):
            existing.extend(value)
        else:
            result[name] = value
    return result


@traced
def _normalize_value(raw: Any) -> Any:
    """Normalise récursivement une valeur d'AST en structure Python native.

    Générique : fonctionne pour N'IMPORTE QUEL plugin, pas seulement ceux
    interprétés spécifiquement par ce module (voir la docstring du module,
    "plugins imbriqués génériques"). Distingue un hash imbriqué (options
    façon `convert => {"a" => "b"}`) d'un tableau (`remove_field =>
    ["a"]`) par forme structurelle — voir "Limites assumées".
    """
    if not isinstance(raw, list):
        return _coerce_literal(raw)
    if raw and all(_looks_like_hash_entry(item) for item in raw):
        return {item[0]: _normalize_value(item[1][0]) for item in raw}
    return [_normalize_value(item) for item in raw]


@traced
def _looks_like_hash_entry(item: Any) -> bool:
    """Une entrée `[clé, [valeur]]` (forme produite par `hash_entry`/`attribute` dans l'AST)."""
    return (
        isinstance(item, list)
        and len(item) == 2
        and isinstance(item[0], (str, int, float))
        and isinstance(item[1], list)
    )


@traced
def _render_templates(value: Any, doc: dict[str, Any], *, now: datetime | None, on_missing: str) -> Any:
    """Rend récursivement tout gabarit sprintf `%{...}` trouvé dans `value` contre `doc`.

    C'est le point de jonction avec `templates.py` : `value` vient de
    `_normalize_value` (structure générique, n'importe quel plugin
    `output`), le rendu proprement dit est entièrement délégué à
    `templates.render_template` — aucune réimplémentation ici.
    """
    if isinstance(value, str):
        return render_template(value, doc, now=now, on_missing=on_missing)
    if isinstance(value, dict):
        return {key: _render_templates(item, doc, now=now, on_missing=on_missing) for key, item in value.items()}
    if isinstance(value, list):
        return [_render_templates(item, doc, now=now, on_missing=on_missing) for item in value]
    return value


# ----------------------------------------------------------------------
# Conditions : comparaison / regex / in / négation / chaînage booléen
# ----------------------------------------------------------------------


@traced
def _evaluate_condition(elements: list, doc: dict[str, Any]) -> bool:
    """Évalue une séquence `expression (bool_op expression)*` déjà extraite de l'AST."""
    if not elements:
        return True
    result = _evaluate_expression(elements[0], doc)
    i = 1
    while i + 1 < len(elements):
        op, rhs_node = elements[i], elements[i + 1]
        result = _combine_bool(op, result, _evaluate_expression(rhs_node, doc))
        i += 2
    return result


@traced
def _evaluate_expression(expr: Any, doc: dict[str, Any]) -> bool:
    """Évalue un nœud "expression" isolé (comparaison, regex, in, négation, ou rvalue nue)."""
    if not isinstance(expr, list):
        return _truthy(expr)
    if expr and expr[0] == "!":
        rest = expr[1:]
        if len(rest) == 1 and not isinstance(rest[0], list):
            return not _truthy(_resolve_rvalue(rest[0], doc))
        return not _evaluate_condition(rest, doc)
    if len(expr) == 3 and expr[1] in _COMPARE_OPS:
        return _compare(_resolve_rvalue(expr[0], doc), expr[1], _resolve_rvalue(expr[2], doc))
    if len(expr) == 3 and expr[1] in _REGEX_OPS:
        return _regex_match(_resolve_rvalue(expr[0], doc), expr[1], expr[2])
    if len(expr) == 3 and expr[1] == "in":
        return _in_test(_resolve_rvalue(expr[0], doc), _resolve_rvalue(expr[2], doc))
    if len(expr) == 4 and expr[1] == "not" and expr[2] == "in":
        return not _in_test(_resolve_rvalue(expr[0], doc), _resolve_rvalue(expr[3], doc))
    if len(expr) >= 3 and isinstance(expr[1], str) and expr[1] in _BOOL_OPS:
        # `(cond1 and cond2 [...])` — parenthèses autour d'une condition à
        # plusieurs membres : voir la docstring du module et
        # tests/test_interpreter.py pour la forme exacte de l'AST.
        return _evaluate_condition(expr, doc)
    if len(expr) == 1:
        inner = expr[0]
        return _evaluate_expression(inner, doc) if isinstance(inner, list) else _truthy(_resolve_rvalue(inner, doc))
    # Repli : forme de liste inattendue (grammaire évoluée ?) — vérité de la valeur résolue.
    return _truthy(_resolve_rvalue(expr, doc))


@traced
def _resolve_rvalue(node: Any, doc: dict[str, Any]) -> Any:
    """Résout un nœud rvalue de condition (sélecteur `[a][b]`, littéral, ou tableau) contre `doc`.

    Réutilise `templates.parse_field_path`/`templates.resolve_field` (même
    format de chemin `[a][b]` que la syntaxe sprintf des gabarits) plutôt
    que de dupliquer la résolution de chemin.
    """
    if isinstance(node, str) and node.startswith("[") and node.endswith("]"):
        return resolve_field(doc, parse_field_path(node))
    if isinstance(node, list):
        return [_resolve_rvalue(item, doc) for item in node]
    return _coerce_literal(node)


@traced
def _truthy(value: Any) -> bool:
    """Vérité façon Logstash : absent ou `null` -> faux ; `false` -> faux ; tout le reste -> vrai.

    Une chaîne vide ou `0` sont VRAIS (présence de champ, pas valeur
    "vide" façon Python) — comportement réel de Logstash, pas une
    simplification.
    """
    if value is _MISSING or value is None:
        return False
    if isinstance(value, bool):
        return value
    return True


@traced
def _compare(lhs: Any, op: str, rhs: Any) -> bool:
    """Comparaison façon Logstash : un opérande absent n'est jamais égal à rien (`!=` vrai, reste faux)."""
    if lhs is _MISSING or rhs is _MISSING:
        return op == "!="
    lhs, rhs = _coerce_comparable(lhs, rhs)
    try:
        if op == "==":
            return lhs == rhs
        if op == "!=":
            return lhs != rhs
        if op == "<":
            return lhs < rhs
        if op == "<=":
            return lhs <= rhs
        if op == ">":
            return lhs > rhs
        if op == ">=":
            return lhs >= rhs
    except TypeError as exc:
        logger.debug("comparaison : opérande non comparable, faux retourné : {}", exc)
        return False
    return False  # pragma: no cover (opérateur hors de _COMPARE_OPS, inatteignable par construction)


@traced
def _coerce_comparable(lhs: Any, rhs: Any) -> tuple[Any, Any]:
    """Coercition minimale chaîne <-> nombre pour comparer ex. `doc["severity"] == "7"` à `5`.

    N'essaie PAS de coercition plus générale (IP, booléen vs chaîne...) —
    voir la docstring du module, "Limites assumées".
    """
    if isinstance(lhs, (int, float)) and not isinstance(lhs, bool) and isinstance(rhs, str):
        try:
            return lhs, type(lhs)(rhs)
        except ValueError as exc:
            logger.debug("comparaison : nombre attendu, opérandes inchangés : {}", exc)
            return lhs, rhs
    if isinstance(rhs, (int, float)) and not isinstance(rhs, bool) and isinstance(lhs, str):
        try:
            return type(rhs)(lhs), rhs
        except ValueError as exc:
            logger.debug("comparaison : nombre attendu, opérandes inchangés : {}", exc)
            return lhs, rhs
    return lhs, rhs


@traced
def _regex_match(value: Any, op: str, pattern: str) -> bool:
    """`=~`/`!~` — un champ absent ne correspond jamais (`=~` faux, `!~` vrai)."""
    matched = value is not _MISSING and re.search(pattern, str(value)) is not None
    return matched if op == "=~" else not matched


@traced
def _in_test(needle: Any, haystack: Any) -> bool:
    """`in` — sous-chaîne si `haystack` est une chaîne, appartenance sinon (tableau)."""
    if haystack is _MISSING or needle is _MISSING:
        return False
    if isinstance(haystack, str):
        return str(needle) in haystack
    if isinstance(haystack, (list, tuple)):
        return needle in haystack
    return False


@traced
def _combine_bool(op: str, lhs: bool, rhs: bool) -> bool:
    """`and`/`or`/`xor`/`nand`, évalués sans court-circuit (les deux membres déjà évalués)."""
    if op == "and":
        return lhs and rhs
    if op == "or":
        return lhs or rhs
    if op == "xor":
        return lhs != rhs
    return not (lhs and rhs)  # nand
