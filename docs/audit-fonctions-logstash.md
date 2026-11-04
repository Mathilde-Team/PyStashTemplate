<!-- SPDX-License-Identifier: GPL-3.0-or-later -->
# Audit des fonctions Logstash utilisables dans les templates

Issue RsysLogstach#101, état au 03/11/2026, rapatrié dans PyStashTemplate
par #57 (découpage RsysLogstach#111). Les numéros d'issues cités (#112,
#123…) sont ceux de [RsysLogstach](https://github.com/Mathilde-Team/RsysLogstach/issues),
où les fonctions ont été développées avant le découpage ; les chemins
`src/*.py` sont devenus les modules du paquet `pystashtemplate`. Ce tableau recense les fonctions
qu'un pipeline Logstash peut utiliser : gabarits sprintf `%{...}`, conditions,
plugins `filter{}` et `output{}`, options communes. Il indique ensuite ce que
PyStashTemplate interprète réellement.

Légende : **fait** = interprété avec le comportement Logstash ; **partiel** =
interprété avec des limites (voir commentaire) ; **pas fait** = accepté par le
parseur mais sans effet sur le document ; **hors périmètre** = sans objet pour
un pont rsyslog → OpenSearch.

Références : [documentation des filtres Logstash](https://www.elastic.co/guide/en/logstash/current/filter-plugins.html),
[références de champs et sprintf](https://www.elastic.co/guide/en/logstash/current/event-dependent-configuration.html),
[plugin logstash-output-opensearch](https://github.com/opensearch-project/logstash-output-opensearch).

## Gabarits sprintf (PyStashTemplate : `templates`)

| Fonction | Fait / pas fait | Commentaires |
|---|---|---|
| `%{champ}` | fait | Champ de premier niveau. |
| `%{[objet][champ]}` | fait | Un niveau par paire de crochets, profondeur libre. |
| `%{[@metadata][champ]}` | fait | `@metadata` traité comme un champ racine ordinaire. |
| `%{[tableau][0]}` | fait | Indice numérique de tableau. |
| `%{+YYYY.MM.dd}` (Joda) | fait | Date de `@timestamp`, ancienne syntaxe. |
| `%{{yyyy.MM.dd}}` (Java Time) | fait (#123) | Lettres `G u y Y Q q M L w W D d E e c a h K k H m s S n N A V O X x Z z` et texte entre quotes ; `%{+…}` suit les lettres Joda (`C`, `x` année de semaine…). Semaines ISO et textes en anglais (Java Time suit la locale de la JVM) ; `F` et `B` recopiées telles quelles. |
| `%{{TIME_NOW}}` | fait | Heure du traitement, pas `@timestamp`. |
| `%{+%s}` (epoch) | fait | Secondes depuis l'epoch de `@timestamp` (corrigé par #102 : rendait `%0`). |
| Champ absent | fait | `--pipeline-on-missing` / `--filters-on-missing` : `keep` conserve `%{...}`, `empty` rend une chaîne vide. Un champ `null` se rend vide. |
| Valeur hash ou tableau dans `%{...}` | fait | Rendue en JSON compact. |
| Échappement de crochets dans un nom de champ (`none`/`percent`/`ampersand`) | fait | `--field-reference-escape-style` (#124), équivalent de `field_reference.escape_style` de logstash.yml : `%5B`/`%5D` ou `&#91;`/`&#93;` désignent un crochet littéral, dans les gabarits comme dans les options des plugins. |
| `${VAR}` / `${VAR:défaut}` (variables d'environnement du `.conf`) | fait (#122) | Substitués au chargement du `.conf`, avant l'analyse : la variable définie prime sur la valeur par défaut ; non définie sans défaut → `ValueError` qui la nomme. `%{champ}` (rendu par événement) non concerné. Noms invalides (`${1x}`) laissés tels quels. |

## Conditions `if` / `else if` / `else` (PyStashTemplate : `interpreter`)

| Fonction | Fait / pas fait | Commentaires |
|---|---|---|
| `if [champ]` (présence) | fait | Absent, `null` ou `false` → faux ; chaîne vide et `0` → vrai, comme Logstash. |
| `==`, `!=`, `<`, `<=`, `>`, `>=` | fait | Un opérande absent n'est égal à rien (`!=` vrai, le reste faux). |
| `=~`, `!~` (regex) | fait | Expression régulière Python. |
| `in`, `not in` | fait | Sous-chaîne si la cible est une chaîne, appartenance si c'est un tableau. |
| `and`, `or`, `xor`, `nand` | fait | Sans court-circuit. |
| `!` (négation), parenthèses | fait | |
| `else if` en chaîne, `if` imbriqués | fait | Dans `filter{}` comme dans `output{}`. |

## Plugins `filter{}`

| Plugin | Fait / pas fait | Commentaires |
|---|---|---|
| `mutate` | fait | Ordre officiel : `coerce`, `rename`, `update`, `replace`, `convert`, `gsub`, `uppercase`, `capitalize`, `lowercase`, `strip`, `split`, `join`, `merge`, `copy`, puis options communes. `tag_on_failure` (défaut `_mutate_error`). |
| `dissect` | fait | `mapping` (sources imbriquées comprises), `convert_datatype` (`int`, `float`) et `tag_on_failure` (défaut `_dissectfailure` : source absente, vide ou sans correspondance) (#117). Pas de référence indirecte `%{&champ}`. |
| `date` | fait | `match` (ISO8601, UNIX, UNIX_MS, motifs Joda), `target`, `timezone`, `locale` (`fr`), `tag_on_failure`. `SSS` (millisecondes) et fuseaux textuels `z`/`ZZZ` (« CET », IDs IANA) reconnus (#118) ; les abréviations couvertes sont réduites (zones européennes et nord-américaines courantes). Les lettres Java Time des gabarits (#123) ne concernent pas `match`. |
| `geoip` | partiel | `source`, `target`, `fields`, `tag_on_failure`. `database` obligatoire (pas de base GeoLite2 embarquée). |
| `translate` | fait | #119 : `source`/`field`, `target`/`destination` (défaut en place, `translation` si `ecs_compatibility => disabled`), `dictionary` en ligne, `dictionary_path` (YAML, JSON, CSV), `exact`, `regex`, `override`, `fallback` (gabarit sprintf), `iterate_on` (tableau de valeurs ou d'objets), `refresh_interval`, `refresh_behaviour` (`merge`/`replace`). Rechargement au premier document après l'intervalle (pas de minuterie) ; `yaml_dictionary_code_point_limit` et `yaml_load_strategy` sans effet ; clés comparées en texte. |
| `drop` | fait | `percentage` compris. |
| `grok` | fait | Bloc natif (#112) : bibliothèque des 70 motifs de base Logstash, `%{SYNTAXE:champ:int}` et `:float`, `(?<champ>...)`, `break_on_match`, `overwrite`, `tag_on_failure`, `patterns_dir`, `patterns_files_glob`, `pattern_definitions`, `keep_empty_captures`, `target`. Moteur `re` Python : `named_captures_only` toujours vrai, `timeout_millis` non appliqué. |
| `kv` | fait | Bloc natif `filter { kv { ... } }` (#113) : `source`, `target` (fusion dans un hash existant), `field_split`, `value_split`, `include_keys`, `exclude_keys`, `prefix`, `trim_key`, `trim_value`, `allow_duplicate_values` (clé répétée → liste, dédup si `false`). Réutilise `filters.KeyValueFilter` (même algorithme que le step `--filters`). |
| `json` | fait | Bloc `filter{}` natif (#115) : `source`, `target` (sinon fusion à la racine), `skip_on_invalid_json`, `tag_on_failure` (défaut `_jsonparsefailure`). `@timestamp` décodé n'est pas converti en date. |
| `dns` | fait (#121) | `src/dnslookup.py`, dnspython : `resolve`, `reverse`, `action`, `nameserver`, `hostsfile`, `timeout`, `max_retries`, caches, `tag_on_timeout`, IDNA. Les `${VAR:défaut}` de l'exemple `pipeline.example.d/25_filter_…` sont substitués au chargement (#122). |
| `fingerprint` | fait | Bloc `filter{}` natif (#114) : `source`, `target`, `method` (SHA1, SHA256, SHA384, SHA512, MD5, MURMUR3, UUID, IPV4_NETWORK), `key` (HMAC ou masque IPv4), `base64encode`, `concatenate_sources`, `concatenate_all_fields`. Côté indexation : `--document-id-fingerprint`. |
| `csv`, `cidr`, `prune`, `urldecode`, `uuid`, `syslog_pri`, `truncate` | fait | Blocs natifs (#120, `src/simples.py`), options principales et défauts du plugin officiel ; `csv` : `convert` en `integer`, `float`, `boolean` seulement ; `prune` : champs de premier niveau, sans `interpolate`. |
| `clone`, `split` | fait (#141) | `clone` : `clones`, `type` ou tag selon `ecs_compatibility` ; `split` : `field`, `terminator`, `target`, tag `_splitparsefailure`. Les nouveaux événements poursuivent le pipeline après le filtre et sont tous indexés (`PipelineInterpreter.apply_events`). |
| `useragent` | fait (#121) | `src/useragent.py`, ua-parser : champs ECS et anciens champs (`ecs_compatibility => disabled`), `prefix`, `regexes`, `lru_cache_size`. |
| `xml` | fait (#121) | `src/xmlfilter.py`, lxml : `store_xml` façon XmlSimple, `xpath`, `namespaces`, `remove_namespaces`, `parse_options` (entités, DTD et XInclude refusés), `force_array`, `force_content`, `suppress_empty`. |
| `aggregate`, `ruby`, `elapsed`, `throttle`, `memcached`, `http`, `jdbc_*` | pas fait | Blocs acceptés par le parseur, sans effet. |
| `mmdb` (communautaire) | fait | Extension hors cœur Logstash : `database`, `source`, `target`. |
| `filters_json` (propre au projet) | fait | Invoque un fichier `--filters` depuis un `.conf`. |

## Options communes à tous les filtres

| Option | Fait / pas fait | Commentaires |
|---|---|---|
| `add_field`, `remove_field`, `add_tag`, `remove_tag` | fait | Après tout filtre natif réussi (`mutate`, `dissect`, `date`, `geoip`, `translate`, `mmdb`, `kv`, `grok`), jamais après un échec, comme `filter_matched` (#116). |
| `id`, `enable_metric`, `periodic_flush` | hors périmètre | Sans effet sur le document. |

## Plugins et options `output{}`

| Fonction | Fait / pas fait | Commentaires |
|---|---|---|
| `opensearch { … }` / `elasticsearch { … }` | partiel | Lues par document (gabarits sprintf) : `index`, `document_id`, `action`, `pipeline`, `routing`, `doc_as_upsert` (#126). |
| `hosts`, `user`, `password`, `ssl`, `ssl_certificate_verification` | fait (#125) | Lus dans le `.conf` (branches comprises) ; `--opensearch-host`, `OPENSEARCH_USER` / `OPENSEARCH_PASSWORD` et `--no-verify-certs` restent prioritaires. Une seule connexion : valeurs différentes d'un bloc à l'autre refusées. |
| `cacert`, `tls_certificate`, `tls_key` (mTLS) | fait | Mêmes noms que le plugin logstash-output-opensearch : `--cacert`, `--tls-certificate`, `--tls-key` (#100), ou dans le `.conf` (#125) ; la ligne de commande est prioritaire. |
| `keystore`, `truststore` (JKS / PKCS#12) | pas fait | Format Java ; utiliser des fichiers PEM. |
| `action`, `pipeline`, `routing` | fait | #126 : `action` `index` (défaut), `create` (`op_type=create`), `update` (`doc_as_upsert` possible) ou `delete` ; `update` et `delete` exigent `document_id`, sinon le document part en DLQ ; `pipeline` est ignoré pour `update` et `delete`, que l'API n'accepte pas. Un document par requête, pas de `_bulk`. |
| `ilm_*`, `template*` | pas fait | Le template d'index passe par `--index-template`. |
| `stdout`, `file`, `http`, autres sorties | pas fait | Ignorées (trace de débogage). |

## Sections `input{}`

| Fonction | Fait / pas fait | Commentaires |
|---|---|---|
| `input { … }` | hors périmètre | Parsé mais jamais exécuté : l'entrée est rsyslog (omprog). |
