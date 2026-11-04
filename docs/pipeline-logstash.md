<!-- SPDX-License-Identifier: GPL-3.0-or-later -->
# Référence : filtres et pipelines Logstash

Référence des formats interprétés par PyStashTemplate : chaîne de filtres
JSON (`pystashtemplate.filters.FilterPipeline`), enrichissement par
dictionnaire (`pystashtemplate.enrichment`) et pipelines `.conf` Logstash
(`pystashtemplate.interpreter.PipelineInterpreter`).

Cette page vient du README de
[RsysLogstach](https://github.com/Mathilde-Team/RsysLogstach), où elle
documentait les options `--filters`, `--enrich-dict` et `--pipeline-conf`
du connecteur rsyslog, avant le découpage
[#111](https://github.com/Mathilde-Team/RsysLogstach/issues/111) (rapatriée
par #57). Les exemples cités (`filters.example.json`,
`blocklist.example.txt`, `enrich-dict.example.json`, `pipeline.example.d/`)
sont à la racine du dépôt.

Depuis le chantier « full async » (#1), `FilterPipeline.apply` et
`PipelineInterpreter.apply` / `apply_events` sont des coroutines.

## Chaîne de filtres JSON (`FilterPipeline`)

Voir `filters.example.json` :

```json
[
  {"type": "drop", "source": "msg", "words_file": "/etc/omprog/blocklist.txt", "mode": "contains"},
  {"type": "regex", "source": "msg", "pattern": "^%%\\d+(?P<module>[A-Z_]+)/..."},
  {"type": "kv", "source": "detail", "field_split": " ", "value_split": "=", "prefix": "kv_"},
  {"type": "mutate", "convert": {"severity_code": "integer"}, "remove_field": ["msg"]}
]
```

Step `date` (#34), mêmes options que le plugin `date` de Logstash : `{"type": "date", "match": ["horodatage", "ISO8601", "MMM dd HH:mm:ss"], "timezone": "Europe/Paris"}` écrit `@timestamp` (ou `target`) en ISO8601 UTC, et ajoute `_dateparsefailure` (ou `tag_on_failure`) aux tags si aucun motif ne correspond. Motifs `SSS` (millisecondes) et `z`/`ZZZ` (fuseau textuel : « CET », « Europe/Paris ») reconnus, ainsi que l'option `locale` (`fr` : noms de mois/jours français, #118). `grok` n'est pas proposé : un step `regex` à groupes nommés le remplace.

## Enrichissement par dictionnaire (`DictEnricher`)

```json
{
  "sw-core-01": {"site": "Besançon - DR", "role": "core", "owner_team": "Infra Réseau"}
}
```

## Pipeline Logstash `.conf` (`PipelineInterpreter`)

Voir `pipeline.example.d/` : input UDP → filter (DNS, GeoIP) → output OpenSearch.

Fonctions Logstash prises en charge (gabarits `%{...}`, conditions, plugins
`filter{}`/`output{}`) et limites : [audit-fonctions-logstash.md](audit-fonctions-logstash.md).

Variables d'environnement du `.conf` (#122) : `${VAR}` et `${VAR:défaut}` sont
substitués au chargement, comme Logstash — la variable définie prime sur la
valeur par défaut ; une variable non définie sans défaut refuse le démarrage
(`ValueError` qui la nomme). `%{champ}` (rendu par événement) n'est pas
concerné.
Sorties `opensearch` (#126) : `action` (`index`, `create`, `update`, `delete`), `pipeline` et `routing` acceptent des gabarits, par exemple `opensearch { index => "logs" action => "update" document_id => "%{id}" doc_as_upsert => true routing => "%{host}" }`.

Nom de champ contenant des crochets (#124) : avec `--field-reference-escape-style percent`, `%{[map%5Bk%5D]}` lit le champ `map[k]` ; `ampersand` utilise `&#91;` et `&#93;`. Défaut `none`, comme Logstash.

Plugins simples natifs (#120) : `csv`, `cidr`, `prune`, `urldecode`, `uuid`, `syslog_pri`, `truncate` ; par exemple `cidr { address => ["%{ip}"] network => ["10.0.0.0/8"] add_tag => ["interne"] }`. Plusieurs événements par message (#141) : `clone { clones => ["copie"] }` garde l'original et ajoute une copie par nom (ajouté à `tags`, ou écrit dans `type` avec `ecs_compatibility => disabled`) ; `split { field => "message" terminator => "\n" target => "ligne" }` remplace l'événement par un événement par morceau non vide (une liste est parcourue élément par élément ; champ absent ou d'un autre type : tag `_splitparsefailure`). Les options communes (`add_field`, `add_tag`…) s'appliquent aux nouveaux événements ; ceux-ci poursuivent le pipeline à partir du filtre suivant, et chacun est indexé selon ses propres sorties. Un `drop` n'écarte que l'événement qu'il atteint ; rsyslog reçoit un échec transitoire si l'indexation de l'un d'eux échoue, et renvoie alors le message entier.

Un `dissect` qui échoue (source absente ou sans correspondance) ajoute `_dissectfailure` aux `tags`, ou les tags de `tag_on_failure` (#117) ; `convert_datatype => { "port" => "int" }` convertit ensuite les champs extraits.
Plugin `kv` natif (#113) : `filter { kv { source => "message" target => "pairs" ... } }`
réutilise le même extracteur que le step `kv` de `--filters` (options
`include_keys`, `exclude_keys`, `trim_key`, `trim_value`,
`allow_duplicate_values` comprises ; clé répétée → liste de valeurs).

Les chaînes entre guillemets sont lues brutes, comme Logstash par défaut
(`config.support_escapes: false`) : `"\d+"` reste `\d+` (#128).

### `translate` (#119)

```
filter {
  translate {
    source => "[ifTable][index]"
    target => "[ifTable][name]"          # facultatif : traduction en place
    dictionary_path => "/etc/rsyslogstach/ifName.yml"   # ou .json, .csv ; ou dictionary => { "1" => "Uplink" }
    fallback => "index: %{[ifTable][index]}"
    refresh_interval => 300              # relu s'il a changé ; refresh_behaviour => "merge" ou "replace"
  }
}
```

- Comme le plugin officiel : `override` (faux, sauf en place), `exact`,
  `regex` (clés vues comme regex), `exact => false` (remplacement de
  chaque clé trouvée dans la valeur), `iterate_on` (tableau de valeurs ou
  tableau d'objets), anciens noms `field`/`destination`.
- Le même code sert le step `translate` de `--filters`.
- Différences : rechargement au premier document après l'intervalle (pas
  de minuterie) ; clés comparées en texte ; une extension de fichier
  inconnue est lue comme du YAML.

### `grok` (#112)

```
filter {
  grok {
    match => { "message" => "%{SYSLOGBASE} %{GREEDYDATA:texte}" }
    # ou plusieurs motifs : { "message" => ["%{IP:client} %{NUMBER:octets:int}", "%{GREEDYDATA:brut}"] }
  }
}
```

- Motifs `%{SYNTAXE}`, `%{SYNTAXE:champ}`, `%{SYNTAXE:champ:int|float}`, champs
  imbriqués (`%{IP:[client][ip]}`) et groupes nommés `(?<champ>...)`.
- Bibliothèque de base de Logstash (`USERNAME`, `INT`, `NUMBER`, `WORD`,
  `NOTSPACE`, `DATA`, `GREEDYDATA`, `QUOTEDSTRING`, `UUID`, `MAC`, `IP`,
  `IPV4`, `IPV6`, `HOSTNAME`, `IPORHOST`, `URI`, `PATH`, `TIMESTAMP_ISO8601`,
  `SYSLOGTIMESTAMP`, `SYSLOGBASE`, `HTTPDATE`, `LOGLEVEL`…, 70 motifs).
- Options : `break_on_match` (vrai), `overwrite`, `tag_on_failure`
  (`_grokparsefailure`), `patterns_dir` + `patterns_files_glob`,
  `pattern_definitions`, `keep_empty_captures`, `target`.
- Sans `overwrite`, une capture vers un champ texte existant donne un
  tableau `[ancien, nouveau]`, comme Logstash.
- Différences : moteur `re` de Python (3.11+) au lieu d'Oniguruma — groupes
  nommés, classes POSIX et `\z` sont traduits ; `named_captures_only`
  toujours vrai ; `timeout_millis` non appliqué. Un motif invalide ou
  inconnu est ignoré (journal DEBUG), les autres motifs restent actifs.

### `useragent` (#121)

```
filter {
  useragent {
    source => "[user_agent][original]"
    # target => "[user_agent]"  (défaut en mode ECS)
  }
}
```

- Analyse par [ua-parser](https://github.com/ua-parser/uap-python) (motifs
  uap-core, comme le parseur Java de Logstash). Champs ECS écrits :
  `[name]`, `[version]` (`89.0.4389.90`), `[device][name]`, `[os][name]`,
  `[os][version]`, `[os][full]` sous la cible ; major, minor et patch dans
  `[@metadata][filter][user_agent][version]` et `[…][os][version]`.
- `ecs_compatibility => disabled` : cible vide (racine) par défaut et anciens
  champs `name`, `device`, `major`, `minor`, `patch`, `version`, `os`,
  `os_name`, `os_full`, `os_version`, `os_major`, `os_minor`, `os_patch`,
  précédés de `prefix`.
- Options : `source` (requis), `target`, `prefix`, `regexes` (fichier
  `regexes.yaml` de uap-core), `lru_cache_size` (100 000). Un tableau donne
  son premier élément ; source absente ou vide : rien. Si `target` égale
  `source`, la source est remplacée.
- Différences : motifs du paquet `ua-parser-builtins` figé dans
  `pyproject.toml`, pas ceux du jar de Logstash ; un User-Agent récent ou
  rare peut être classé autrement. Fichier `regexes` illisible : bloc ignoré
  (journal DEBUG).

### `xml` (#121)

```
filter {
  xml {
    source => "message"
    target => "doc"
    xpath => { "/evenement/@id" => "[evenement][id]" }
  }
}
```

- `store_xml` (vrai par défaut, `target` requis) : conversion façon
  XmlSimple, comme Logstash — attributs et enfants dans le même objet,
  enfants en tableaux (`force_array`), texte seul en chaîne ou sous
  `content` (`force_content`, ou en présence d'attributs), éléments vides
  omis (`suppress_empty`). L'élément racine n'apparaît pas. Analyse stricte.
- `xpath => { "expression" => "[champ]" }` : analyse tolérante (ou selon
  `parse_options`, par ex. `strict`), résultats en tableau (texte seul avec
  `force_array => false` et un résultat). Préfixes de la racine (défaut sous
  `xmlns`) ou `namespaces` ; `remove_namespaces`.
- Échec d'analyse, expression invalide, tableau de longueur différente de 1
  ou valeur non textuelle : tag `_xmlparsefailure`.
- Sécurité : ni entités externes, ni DTD, ni XInclude, ni réseau ;
  `parse_options` `noent`, `dtdload`, `dtdattr`, `dtdvalid` ou `xinclude`
  rend le bloc invalide (ignoré, journal DEBUG).
- Différences : lxml (libxml2) remplace Nokogiri et REXML ; un document
  sans élément racine, même en mode tolérant, ne donne aucun résultat XPath.

### `dns` (#121)

```
filter {
  dns {
    reverse => [ "[host][ip]" ]
    action => "replace"
    nameserver => { address => ["192.0.2.53"] search => ["lan"] }
    hit_cache_size => 4096
  }
}
```

- `resolve` (nom → première adresse, IPv4 puis IPv6) et `reverse`
  (adresse → premier nom) ; `action` `append` (défaut, le champ devient
  `[valeur, résultat]`) ou `replace`.
- Résolveurs dans l'ordre : `hostsfile`, `/etc/hosts`, puis `nameserver`
  (chaîne, liste, ou hash `address`/`search`/`ndots`). Sans les deux :
  `/etc/hosts` puis le DNS du système ; avec `hostsfile` seul : aucun DNS.
- `timeout` (0,5 s), `max_retries` (2), `tag_on_timeout` (`_dnstimeout`),
  caches `hit_cache_size`/`hit_cache_ttl` (0, 60 s) et
  `failed_cache_size`/`failed_cache_ttl` (0, 5 s), noms IDNA.
- Comme Logstash, un champ absent est sauté, mais plusieurs valeurs, une
  valeur non textuelle, une adresse invalide ou un échec arrêtent le
  filtre, sans options communes.
- `PYSTASHTEMPLATE_DNS=off` (ancien nom `RSYSLOGSTACH_DNS` toujours lu)
  coupe les requêtes réseau (fichiers hosts seulement) ; les tests le
  positionnent.
- Limite : les nombres écrits `"${VAR}"` ne sont pas encore remplacés
  (#122) : le bloc est alors ignoré (journal DEBUG), c'est le cas de
  `pipeline.example.d/25_…`.

Exemple de filtre `json` natif (#115) : `json { source => "message" target => "payload" skip_on_invalid_json => true }`.
