# Sorties agnostiques

Le nom du plugin d'un bloc `output{}` du `.conf` désigne une sortie
inscrite au registre (`pystashtemplate.outputs.REGISTRY`). La
compilation du pipeline (`compiler_sorties`) instancie chaque sortie ;
une sortie inconnue échoue dès la compilation, avant toute connexion.

## Sorties disponibles

Matrice de compatibilité (système × fonctionnalité), lue dans les
capacités déclarées par chaque classe du registre ; « banc en
conteneur » : sortie testée contre le vrai système (job CI séparé,
`.github/workflows/conformite-<plugin>.yml`). Toutes les sorties HTTP
passent en plus le banc commun sans réseau
(`tests/test_conformite_sorties.py` : lot simple, erreur partielle, 429,
coupure réseau, arrêt propre).

<!-- matrice-sorties:debut (générée par scripts/matrice_sorties.py, ne pas éditer) -->
| Plugin | Actions | `_id` | Accusé par élément | Taille max. de requête | DLQ | Réessais | Banc en conteneur |
|---|---|---|---|---|---|---|---|
| `clickhouse` | `index` | non | non | — | oui | oui | oui |
| `elasticsearch` | toutes | oui | oui | — | oui | oui | oui |
| `file` | `index`, `create` | non | oui | — | oui | oui | non |
| `greptimedb` | `index`, `create` | non | oui | — | oui | oui | oui |
| `loki` | `index` | non | non | — | oui | oui | oui |
| `manticore` | toutes | oui | oui | — | oui | oui | oui |
| `null` | toutes | oui | oui | — | non | oui | non |
| `openobserve` | `index`, `create` | non | oui | — | oui | oui | oui |
| `opensearch` | toutes | oui | oui | — | oui | oui | oui |
| `otlp` | `index`, `create` | non | non | — | oui | oui | oui |
| `parseable` | `index` | non | non | 10 Mio | oui | oui | oui |
| `quickwit` | `create` | non | oui | 10 Mio | oui | oui | oui |
| `siglens` | `index`, `create` | non | oui | — | oui | oui | oui |
| `stdout` | `index`, `create` | non | oui | — | non | oui | non |
| `victorialogs` | `index`, `create` | non | non | — | oui | oui | oui |
| `zincsearch` | `index`, `create`, `delete` | oui | non | — | oui | oui | oui |
<!-- matrice-sorties:fin -->

La famille « API _bulk Elasticsearch » (issue #11) partage le client
`pystashtemplate.outputs.elasticsearch_bulk` : chaque produit n'est
plus qu'une sous-classe de quelques lignes (valeurs par défaut et
capacités). Les autres familles (Loki, ClickHouse, HEC, OTLP…) sont
des sous-issues du chantier #2.

## Schéma de configuration commun (issue #8)

Objectif : **seule la configuration du `output{}` diffère légèrement d'un
système à l'autre.** Les options communes à toutes les sorties sont
déclarées une seule fois (`pystashtemplate.outputs.schema`), validées
**à la compilation** du pipeline — option inconnue (faute de frappe),
type inattendu ou valeur hors bornes échouent franchement, avec un
message traduit nommant le plugin — avant toute connexion. Les noms
d'options reprennent ceux du plugin Logstash correspondant quand il
existe, pour qu'un `.conf` Logstash fonctionne sans retouche ; `${VAR}`
y est résolu par l'interpréteur.

### Options communes

| Option | Type | Défaut | Rôle |
|---|---|---|---|
| `hosts` | str ou liste | — | URL(s) du ou des nœuds cibles |
| `user` | str | — | utilisateur de l'authentification basique |
| `password` | str | — | mot de passe de l'authentification basique |
| `ssl` | bool | `true` | vérifier le certificat TLS du serveur |
| `cacert` | str | — | autorité de certification (PEM) du serveur |
| `ssl_certificate` | str | — | certificat client TLS (mTLS) |
| `ssl_key` | str | — | clé privée du certificat client |
| `compression` | bool | `false` | compresser les requêtes |
| `batch_size` | int ≥ 1 | `1` | documents tamponnés avant envoi |
| `batch_bytes` | int ≥ 1 | `10485760` | taille maximale d'un lot en octets |
| `flush_interval` | nombre ≥ 0 | `1.0` | secondes entre deux poussées du tampon |
| `max_concurrency` | int ≥ 1 | `4` | envois parallèles au maximum |
| `retry_max` | int ≥ 0 | `0` | réessais avant échec définitif |
| `retry_backoff` | nombre ≥ 0 | `1.0` | secondes initiales d'attente entre réessais |
| `dlq_path` | str | — | fichier de la DLQ locale (échecs définitifs) |

Une sortie qui n'utilise pas une option commune l'ignore : le même
pipeline peut basculer d'OpenSearch à VictoriaLogs en changeant **le
seul nom du plugin et l'URL**. Chaque sortie ajoute ensuite ses
options propres (schéma déclaré par classe, `OPTIONS_PROPRES`), validées
aux mêmes instants : `opensearch` (`index`, `action`, `pipeline`,
`routing`, `doc_as_upsert`, `timeout`, `headers`, `id`,
`dlq_transitoires`), `elasticsearch` (en plus : `api_key`, `token`,
`data_stream` et ses composants), `victorialogs` (en plus : `mode`,
`stream_fields`, `msg_field`, `time_field`), `file` (`path`, requis ;
`codec`), `stdout` (`codec`).

## Sortie `opensearch` (issue #12)

Sous-classe de référence du client commun : chemin `_bulk` natif
(`POST /_bulk`, sans préfixe), capacités complètes (les quatre actions,
`_id` accepté, accusé par élément). Le connecteur RsysLogstach y
migre son `indexer` (issue #160) : mêmes documents, même DLQ, mêmes
métriques.

### Gabarit de pipeline d'exemple

```
output {
  opensearch {
    hosts   => ["https://opensearch:9200"]
    index   => "logs-%Y.%m.%d"
    user    => "admin"
    password => "${OPENSEARCH_PASSWORD}"
    ssl     => true
    cacert  => "/etc/ssl/certs/opensearch-ca.pem"
    # facultatifs :
    # action => "index"          # index | create | update | delete
    # batch_size => 1            # documents par requête _bulk
    # retry_max => 0              # retentes d'un lot transitoire
    # dlq_path => "/var/lib/dlq/opensearch.jsonl"
  }
}
```

### Options

| Option | Rôle | Équivalent indexer RsysLogstach |
|---|---|---|
| `hosts` | URL(s) du cluster (première entrée) | `hosts` |
| `index` | motif strftime de l'index | `index_pattern` |
| `user`, `password` | authentification basique | `auth` |
| `ssl` | vérifier les certificats TLS | `use_ssl` + `verify_certs` |
| `cacert` | autorité de signature (PEM) | `ca_certs` |
| `ssl_certificate`, `ssl_key` | mTLS | `client_cert`, `client_key` |
| `action` | action par défaut du lot | `action` |
| `batch_size` | documents par requête `_bulk` | — |
| `retry_max`, `retry_backoff` | retentes d'un lot transitoire | — |
| `dlq_path` | DLQ locale des échecs permanents | DLQ (format identique) |
| `dlq_transitoires` | DLQ aussi pour un lot transitoire aux réessais épuisés (faux par défaut) | — (rsyslog retient) |
| `id` | nom de la sortie, inscrit dans la DLQ (`@dlq_output`) | — |

Les options rendues par document (`index` sprintf, `document_id`)
passent par l'API `envoyer()` du client — c'est celle qu'utilise le
connecteur (#160).

## Sortie `elasticsearch` (issue #13)

Le client commun, comme `opensearch` : chemin `_bulk` natif, les quatre
actions, `_id` accepté, erreurs analysées élément par élément
(`items[]`). Les autres options sont celles de `opensearch` ci-dessus.

```
output {
  elasticsearch {
    hosts   => ["https://elasticsearch:9200"]
    index   => "logs-%Y.%m.%d"
    api_key => "${ES_API_KEY}"
    cacert  => "/etc/ssl/certs/elasticsearch-ca.pem"
  }
}
```

| Option propre | Rôle |
|---|---|
| `api_key` | en-tête `Authorization: ApiKey …` ; forme encodée (champ `encoded` d'Elasticsearch) ou `id:api_key` (comme Logstash, encodée en base64 par la sortie) |
| `token` | en-tête `Authorization: Bearer …` (jeton de service, OAuth/OIDC) |
| `data_stream` | écrit dans un data stream : action `create` imposée |
| `data_stream_type`, `data_stream_dataset`, `data_stream_namespace` | nom du flux sans `index` : `<type>-<dataset>-<namespace>`, `logs-generic-default` par défaut |

- Une seule authentification : `user`/`password`, `api_key` et `token`
  s'excluent (erreur dès la configuration). Les secrets passent par
  `${VAR}` et ne sont jamais journalisés.
- Data stream : Elasticsearch n'accepte que `create`. L'action `index`
  (défaut) devient `create` ; `update` et `delete` sont refusées avant
  tout envoi (configuration ou `envoyer`). Les options de nommage sans
  `data_stream => true` sont une erreur.

Banc de conformité : `tests/test_integration_elasticsearch.py`, workflow
`conformite-elasticsearch.yml` (Elasticsearch 9.5.5 en conteneur,
sécurité active) — indexation, `_id` déterministe, clé d'API créée puis
utilisée sous ses deux formes, data stream créé par le modèle intégré
`logs-*-*`, rejet de mapping parti en DLQ.

## Sortie `victorialogs` (issue #14)

Ingestion seule (port 9428 par défaut), deux modes :

- `bulk` (défaut) : `POST /insert/elasticsearch/_bulk`, le NDJSON
  `_bulk` du client commun ;
- `jsonline` : `POST /insert/jsonline`, une ligne JSON par document.

```
output {
  victorialogs {
    hosts => ["http://victorialogs:9428"]
    mode => "bulk"   # ou "jsonline"
    stream_fields => ["host", "app"]
  }
}
```

| Option propre | Paramètre VictoriaLogs | Défaut |
|---|---|---|
| `mode` | — | `bulk` |
| `stream_fields` | `_stream_fields` (liste ou chaîne) | aucun (flux `{}`) |
| `msg_field` | `_msg_field` | `message` |
| `time_field` | `_time_field` | `@timestamp` |

Les défauts suivent les documents Logstash (`message`, `@timestamp`) ;
une valeur vide retire le paramètre (VictoriaLogs lit alors `_msg` et
`_time`). Les autres options sont celles du client commun.

Accusé de réception : VictoriaLogs ne rapporte pas d'erreur par
élément ; il s'arrête à la première erreur et la rend pour tout le lot.
La décision se fait donc sur le **statut HTTP global** : 2xx = lot
indexé ; 400/404 = permanent (DLQ si configurée) ; tout autre statut
(réseau, 429, 5xx, 401…) = lot rendu à l'appelant, ou DLQ avec
`dlq_transitoires`. En mode `jsonline`, VictoriaLogs ignore une ligne
JSON invalide sans faire échouer le lot. `_index` et `_id` sont ignorés
par le produit (`_id` refusé par la sortie). `health()` lit
`GET /health`.

Banc de conformité : `tests/test_integration_victorialogs.py`, workflow
`conformite-victorialogs.yml` (VictoriaLogs v1.53.0 en conteneur) —
modes `bulk` et `jsonline`, champs de flux et de date relus par LogsQL.

## Sortie `zincsearch` (issue #18)

API `_bulk` native de ZincSearch (port 4080 par défaut) :
`POST /api/_bulk`, authentification basic.

```
output {
  zincsearch {
    hosts => ["http://zincsearch:4080"]
    index => "logs"
    user => "${ZINC_USER}"
    password => "${ZINC_PASSWORD}"
  }
}
```

Compatibilité Elasticsearch partielle (lue dans le code de ZincSearch
0.4.10) :

- actions `index`, `create` et `delete` ; `update` est refusée à la
  compilation, car ZincSearch enregistre la ligne de données comme
  document complet (pas de mise à jour partielle `{"doc": …}`) ;
- `_id` transmis : un `index`/`create` avec le même `_id` remplace le
  document ;
- statut HTTP global seul : la réponse ne porte que `record_count`.
  Une erreur arrête le lot en HTTP 500 — les documents précédents sont
  déjà indexés ; le lot est retenté puis rendu à l'appelant. Sans
  `document_id`, un réessai peut donc créer des doublons. Une ligne JSON
  illisible est ignorée sans erreur par le serveur.

`health()` lit `GET /version`. Aucune option propre.

Banc de conformité : `tests/test_integration_zincsearch.py`, workflow
`conformite-zincsearch.yml` (ZincSearch 0.4.10 en conteneur) — lot
indexé, remplacement par `_id` et `delete`, relus par l'API de
recherche.

## Sortie `openobserve` (issue #16)

Sous-classe du client commun sur l'API compatible Elasticsearch
d'OpenObserve : `POST /api/{organization}/_bulk` (port 5080 par défaut),
le `_index` de chaque ligne étant le stream. Authentification basic
(`user`/`password` via `${VAR}`).

| Option propre | Rôle |
|---|---|
| `organization` | organisation, dans le chemin (défaut : `default`) |

- **Actions `index` et `create`** ; `update`/`delete` refusées dès la
  compilation (OpenObserve ignore `delete`, et génère ses identifiants :
  `_id` ignoré avec un avertissement).
- **Ordre des `items[]` non garanti** ([issue amont
  #15716](https://github.com/openobserve/openobserve/issues/15716)) :
  attribuer le N-ième élément au N-ième document pourrait déclarer
  indexé un document refusé. Appariement prudent (crochet `_apparier`
  du client commun) :
  1. réponse sans champ `errors` (429, 5xx…) : règle commune ;
  2. `errors: false` : tout le lot indexé, sans regarder l'ordre (couvre
     `ZO_BULK_API_RESPONSE_ERRORS_ONLY`, qui omet les succès) ;
  3. sinon, chaque échec est apparié par contenu (`originalRecord`) ;
  4. les échecs sans contenu (`originalRecord` nul : OpenObserve 1.1.2
     le perd pour les erreurs d'horodatage, et les liste **avant** les
     éléments réussis — observé contre le vrai serveur) ne sont
     attribués que s'il y en a autant que de documents non appariés, ou
     si le lot n'a qu'un document ;
  5. à défaut, chaque document non apparié est **renvoyé seul**, où
     l'attribution est exacte : doublon possible pour ceux qui avaient
     réussi, plutôt qu'une perte ou un rejeu sans fin du lot. Pour éviter
     ces doublons, `batch_size => 1`.

Santé : `health()` interroge `GET /healthz`.

Banc de conformité contre OpenObserve v1.1.2 en conteneur
(`.github/workflows/conformite-openobserve.yml`,
`tests/test_integration_openobserve.py`) : lot indexé puis retrouvé par
la recherche SQL ; un document trop ancien (au-delà de
`ZO_INGEST_ALLOWED_UPTO`) part seul en DLQ après renvoi un par un, les
autres documents indexés (en double).

## Sortie `manticore` (issue #17)

API `/_bulk` compatible Elasticsearch de Manticore Search (port HTTP
9308 par défaut), accusé de réception élément par élément (`items[]`).

```
output {
  manticore {
    hosts => ["http://manticore:9308"]
    index => "logs"
  }
}
```

Particularités (issue amont
[#4884](https://github.com/manticoresoftware/manticoresearch/issues/4884),
observées contre Manticore 29.9.0) :

- `create` en doublon : avant 29.9.0, HTTP 409 pour toute la requête et
  élément en 400 (« duplicate id ») ; depuis, HTTP 200 et élément en
  409. Les deux formes sont un **doublon** : document déjà présent,
  compté indexé (comme la sortie `elasticsearch` de Logstash avec un
  conflit 409), jamais envoyé en DLQ comme une erreur de mapping ;
- perte silencieuse : dans une suite de `create` consécutifs vers la
  même table, un doublon annule les documents qui le précèdent alors
  qu'ils sont rapportés `201`. La sortie les rend à l'appelant
  (`RETRYABLE`) pour renvoi — idempotent avec un `document_id` (au pire
  un nouveau doublon, compté indexé). Sans `_id`, préférer `index` ;
- `_id` non numérique haché par Manticore en entier 64 bits ;
- la table doit exister (ou le module Buddy la créer) : une table absente
  ou une colonne inconnue est une erreur d'élément 400, permanente (DLQ
  si configurée).

`health()` lit `GET /` (réponse de type Elasticsearch). Aucune option
propre.

Banc de conformité : `tests/test_integration_manticore.py`, workflow
`conformite-manticore.yml` (Manticore 29.9.0 en conteneur) — lot
indexé, doublon `create` et renvoi convergent, relus par SQL.

## Sortie `quickwit` (issue #15)

Sous-classe du client commun sur l'API compatible Elasticsearch de
Quickwit : `POST /api/v1/_elastic/_bulk` (port 7280 par défaut).

- **`create` seule** : c'est l'action par défaut ; toute autre action
  (`index`, `update`, `delete`) est refusée dès la compilation du
  pipeline (message traduit) et par `envoyer`.
- **Pas de `_id`** : ignoré avec un avertissement.
- **Accusé par élément** : la réponse porte un `items[]` (201, 404
  index absent, 400 document illisible…), mais chaque élément est
  rapporté sous la clé `index` même pour un `create` ; la sortie le
  relit sous l'action envoyée. Erreur d'élément 400/404 = permanente,
  DLQ si configurée ; réseau, 429, 5xx = transitoire.
- **Requête ≤ 10 Mio** (limite de Quickwit, déclarée dans les capacités).
- L'index doit exister : Quickwit ne le crée pas à l'ingestion.

| Option propre | Rôle |
|---|---|
| `refresh` | `true` (commit forcé), `wait_for` (attend le prochain commit), `false` ; absent par défaut |

Avec `refresh => "true"` ou `"wait_for"`, la requête attend le commit
(2 à 5 s observées) : sans `timeout` explicite, le délai passe à 30 s.

Santé : `health()` interroge `GET /api/v1/version`.

Banc de conformité contre Quickwit 0.9.3 en conteneur
(`.github/workflows/conformite-quickwit.yml`,
`tests/test_integration_quickwit.py`) : lot `create` indexé puis
retrouvé par la recherche, `_id` ignoré, index absent parti en DLQ.

## Sortie `parseable` (issue #25)

API d'ingestion JSON de Parseable (port 8000 par défaut) :
`POST /api/v1/ingest`, corps **tableau JSON** des documents du lot
(`Content-Type: application/json`), jeu de données visé dans l'en-tête
`X-P-Stream` — créé automatiquement s'il n'existe pas.

```
output {
  parseable {
    hosts => ["http://parseable:8000"]
    stream => "rsyslog"
    user => "${PARSEABLE_USER}"
    password => "${PARSEABLE_PASSWORD}"
    tags => { "env" => "prod" }
  }
}
```

| Option propre | Rôle |
|---|---|
| `stream` | Jeu de données Parseable (en-tête `X-P-Stream`) ; **obligatoire** |
| `api_key` | Clé d'API (en-tête `X-API-Key`, Parseable Cloud / Enterprise) ; absente par défaut |
| `tenant_id` | Locataire multi-tenant (en-tête `X-P-Tenant-ID`) ; absent par défaut |
| `tags` | Étiquettes ajoutées à chaque événement (en-têtes `X-P-Tag-<nom>`, nom en lettres, chiffres, `_`, `-`) ; `{}` par défaut |

Particularités :

- **authentification** : basique (`user`/`password`) pour l'édition
  open source, `api_key` sinon ;
- **schéma souple** : Parseable aplatit les champs imbriqués
  (`host.name` → colonne `host_name`) et ajoute `p_timestamp`
  (horodatage d'ingestion) ; un **conflit de type** n'est pas une
  erreur : la valeur est rangée dans une colonne suffixée (`n`
  numérique puis `"texte"` → colonne `n_utf8`), sans perte ;
- **insertions seules** : `index` est la seule action acceptée (les
  options `index`, `pipeline`, `routing`, `doc_as_upsert` de la famille
  `_bulk` sont refusées à la compilation) ; `_id` refusé ;
- **accusé global** : Parseable répond 200 sans corps, sans détail par
  élément (`accuse_par_evenement=False`) : 2xx = lot ingéré ; 400 (JSON
  invalide, élément qui n'est pas un objet, nom de jeu de données
  invalide) = permanent, DLQ si configurée ; réseau, 429, 5xx =
  transitoire, retenté puis rendu à l'appelant ; 401/403 (clé ou mot de
  passe refusés) = lot rendu à l'appelant, jamais en DLQ ;
- **taille** : 10 Mio au plus par requête (limite documentée de
  Parseable, à respecter avec `batch_bytes`) ;
- **lot vide jamais envoyé** : Parseable 3.2.6 panique et s'arrête sur
  un tableau vide `[]` (constaté au banc) — le client n'envoie jamais
  de lot vide.

Santé : `health()` interroge `GET /api/v1/readiness` puis
`GET /api/v1/about` (rend la version).

Banc de conformité contre Parseable 3.2.6 (binaire officiel, empreinte
vérifiée, dans un conteneur Debian éphémère : Parseable 3.x n'est pas
publié sur Docker Hub) (`.github/workflows/conformite-parseable.yml`,
`tests/test_integration_parseable.py`) : lot relu par l'API SQL, jeu de
données créé à la volée, champs aplatis, conflit de type rangé en
colonne suffixée, nom invalide en DLQ, mauvais mot de passe rendu à
l'appelant. Fixture du banc commun (#9) en plus.

## Sortie `otlp` (issue #22)

OpenTelemetry OTLP/HTTP, journaux : `POST {hosts}/v1/logs` (port 4318
par défaut), en protobuf (`application/x-protobuf`, défaut de la
spécification) ou en JSON. Un seul client pour tous les récepteurs
OTLP : OpenTelemetry Collector, SigNoz, Uptrace, HyperDX/ClickStack,
OpenObserve, Quickwit, GreptimeDB, VictoriaLogs, Parseable, Data
Prepper… Le protobuf est codé par `outputs/otlp_codec.py`, sans nouvelle
dépendance.

```
output {
  otlp {
    hosts => ["http://otel-collector:4318"]
    encoding => "protobuf"   # ou "json"
    resource => { "service.name" => "rsyslog" }
    fields => { "host" => "resource.host.name" "program" => "attributes.process.executable.name" }
  }
}
```

| Option propre | Rôle |
|---|---|
| `encoding` | `protobuf` (défaut) ou `json` |
| `resource` | attributs de ressource fixes (défaut : `service.name => "pystashtemplate"`) |
| `fields` | table de correspondance champ → cible, fusionnée avec la table par défaut |
| `scope` | nom de la portée d'instrumentation (défaut : `pystashtemplate`) |
| `timeout`, `headers`, `dlq_transitoires` | comme la famille `_bulk` |

`compression => true` compresse le corps en gzip.

Correspondance champ de l'événement → `LogRecord` (option `fields`) :

| Cible | Effet | Champ par défaut |
|---|---|---|
| `body` | corps du journal | `message` |
| `time` | `time_unix_nano` : date ISO 8601 (UTC si sans fuseau) ou nombre (s, ms, µs ou ns) | `@timestamp` |
| `severity` | `severity_text` + `severity_number` : nom usuel (`info`, `warning`, `err`, `crit`…) ou code syslog 0–7 | `severity` |
| `trace_id`, `span_id` | identifiants hexadécimaux (32 et 16 caractères) | champs de même nom |
| `resource.<nom>` | attribut de ressource ; les journaux sont groupés par ressource | — |
| `attributes.<nom>` | attribut du journal, renommé | — |
| `drop` | champ non transmis | — |

Tout autre champ devient un attribut du journal sous son nom ; une
valeur illisible pour sa cible (date, identifiant) reste un attribut.
`observed_time_unix_nano` est l'heure d'envoi.

Accusé de réception **global** (OTLP ne rapporte rien par journal) :

- 2xx accepté ; 400 refus définitif (DLQ) ; 429 et 5xx retentés
  (`retry_max`) puis rendus à l'appelant ;
- `partial_success` d'une réponse 2xx : tout le lot refusé → DLQ ;
  refus partiel → les journaux refusés ne sont pas identifiables et la
  spécification interdit de les renvoyer : ils sont comptés dans la
  métrique `rejected` et journalisés avec le message du serveur, le
  reste du lot compté indexé ;
- en JSON, un document impossible à coder (flottant NaN ou infini) part
  seul en DLQ, les autres sont envoyés.

`health()` envoie une requête OTLP vide (OTLP n'a pas de point de santé).

Banc de conformité : `tests/test_integration_otlp.py`, workflow
`conformite-otlp.yml` (OpenTelemetry Collector contrib 0.164.0 en
conteneur, exportateur `file`) — protobuf, JSON et gzip relus après
décodage par le Collector (ressources, dates, sévérités, identifiants,
attributs imbriqués).

## Sortie `siglens` (issue #20)

API `_bulk` compatible Elasticsearch de SigLens, sur le port
d'ingestion (8081 par défaut) : `POST /elastic/_bulk`. La recherche est
servie par un autre port (5122).

```
output {
  siglens {
    hosts => ["http://siglens:8081"]
    index => "logs"
  }
}
```

Compatibilité Elasticsearch partielle (observée contre SigLens 1.0.57) :

- actions `index` et `create` seules : `update` et `delete` font
  rejeter tout le lot (HTTP 400 « request body is required ») et sont
  refusées à la compilation ;
- `_id` ignoré : SigLens l'accepte sans le conserver (deux documents de
  même `_id` sont gardés tous deux, un `create` en double n'est pas
  refusé) — la sortie n'envoie donc pas d'`_id` et le signale ;
- accusé par élément : `items[]` (201, ou 400 `mapper_parse_exception`
  pour une ligne illisible), chaque élément rapporté sous la clé `index`
  même pour un `create`, le `status` d'un élément en échec placé hors de
  l'objet de l'action. La sortie relit chaque élément sous l'action
  envoyée, statut compris (`outputs/relecture.py`, partagé avec
  `quickwit`) ;
- conflit de type (un champ numérique puis texte) accepté et conservé ;
- index créé à l'ingestion ; santé : `GET /elastic/` (fiche
  « Elasticsearch » 7.9.3 de SigLens).

Banc de conformité : `tests/test_integration_siglens.py`, workflow
`conformite-siglens.yml` (SigLens 1.0.57 en conteneur) — lots `index`
et `create`, `_id` ignoré, ligne illisible seule en DLQ, relus par l'API
de recherche (`POST /api/search` du port 5122).

## Sortie `greptimedb` (issue #19)

API d'ingestion compatible Elasticsearch de GreptimeDB (port HTTP 4000
par défaut) : `POST /v1/elasticsearch/_bulk`, le `_index` de chaque ligne
étant la table (créée à la première écriture).

```
output {
  greptimedb {
    hosts => ["http://greptimedb:4000"]
    index => "logs"
  }
}
```

Particularités, observées contre GreptimeDB 1.3.0 :

- **insertions seules** : `index` (défaut) et `create` ; `update` et
  `delete` refusées dès la compilation ; `_id` ignoré avec un
  avertissement (non stocké) ;
- **éléments sous `create`** : chaque élément de `items[]` est rapporté
  sous `create`, même pour un `index` ; la sortie le relit sous l'action
  envoyée (sinon une erreur d'élément serait comptée indexée) ;
- **lot atomique** : une ligne invalide fait rejeter tout le lot en
  HTTP 400, sans rien écrire. La sortie renvoie alors chaque document
  seul — sans doublon possible — : seul le document fautif est refusé
  (DLQ si configurée), avec la raison donnée par le serveur ;
- **perte silencieuse sur conflit de type** : un document dont un champ
  contredit le type déjà connu de la colonne n'est pas écrit, alors que
  le serveur répond `201` ; indétectable dans la réponse. Garder des
  types stables par champ (`mutate { convert => … }`).

`health()` lit `GET /health` (`{}`). Aucune option propre.

Banc de conformité : `tests/test_integration_greptimedb.py`, workflow
`conformite-greptimedb.yml` (GreptimeDB 1.3.0 standalone en conteneur) —
lot relu par SQL, lot rejeté dont seul le document fautif part en DLQ.

## Sortie `clickhouse` (issue #23)

Interface HTTP de ClickHouse (port 8123 par défaut) :
`POST /?query=INSERT%20INTO%20<base>.<table>%20FORMAT%20JSONEachRow`,
le corps est le lot en JSON Each Row (une ligne JSON par document).

```
output {
  clickhouse {
    hosts => ["http://clickhouse:8123"]
    table => "logs"
    database => "rsyslog"
    async_insert => true
  }
}
```

| Option propre | Rôle |
|---|---|
| `table` | Table cible — **requise**, elle doit exister (ClickHouse ne la crée pas) |
| `database` | Base de la table (`default` par défaut) |
| `async_insert` | Insertions asynchrones côté serveur (recommandé pour les petits lots ; désactivé par défaut) |
| `champs_inconnus` | Champ sans colonne : `ignorer` (défaut, `input_format_skip_unknown_fields=1`) ou `erreur` |

Particularités :

- **schéma de table colonne par colonne** : le document est envoyé tel
  quel, ClickHouse fait la correspondance champ → colonne (les colonnes
  manquantes prennent leur défaut) ; un champ sans colonne est écarté
  ou fait échouer le lot (`champs_inconnus => "erreur"` : aucun
  document n'est isolé, le lot entier part en DLQ) ;
- **insertions seules** : `index` est la seule action (les options
  `index`, `action`, `pipeline`, `routing`, `doc_as_upsert` de la
  famille `_bulk` sont refusées à la compilation) ; `_id` refusé ;
- **accusé global** : l'INSERT est atomique, la décision se prend sur
  le statut HTTP global (`accuse_par_evenement=False`) : 2xx = lot
  ingéré ; 404 (table ou base absente), 400 = permanent (DLQ si
  configurée) ; réseau, 429, 5xx = transitoire ;
- l'authentification passe par les options communes (`user`,
  `password`, `ssl`, `cacert`…) ; les en-têtes HTTP additionnels
  (ex. `X-ClickHouse-Key`) passent par `headers`.

Santé : `health()` interroge `GET /ping` (ClickHouse répond `Ok.`).

Banc de conformité contre ClickHouse 26.3.62 en conteneur
(`.github/workflows/conformite-clickhouse.yml`,
`tests/test_integration_clickhouse.py`) : documents relus colonne par
colonne, champ sans colonne écarté, `async_insert` visible à la
lecture, table absente jamais comptée comme indexée. Fixture du banc
commun (#9) en plus.

## Sortie `loki` (issue #21)

API push de Grafana Loki (port 3100 par défaut), modèle **par labels** :
`POST /loki/api/v1/push` en JSON (`Content-Type: application/json`), le
protocole accepté aussi par VictoriaLogs, GreptimeDB, OpenObserve, SigLens
et Alloy.

```
output {
  loki {
    hosts => ["http://loki:3100"]
    labels => { "job" => "rsyslog" "host" => "%{host}" }
    tenant_id => "${LOKI_TENANT}"
  }
}
```

| Option propre | Rôle |
|---|---|
| `labels` | Dictionnaire label → valeur (gabarit sprintf `%{host}`), faible cardinalité ; `{}` par défaut |
| `tenant_id` | Locataire multi-tenant (en-tête `X-Scope-OrgID`) ; absent par défaut |

Particularités :

- **labels par flux** : la valeur de chaque label est rendue contre
  chaque document (gabarit sprintf) ; un label rendu vide est écarté
  (Loki refuse les valeurs vides) ; les documents sont regroupés en un
  flux par jeu de labels ;
- **ligne de log** : le document entier part en ligne (JSON, sans
  perte) ; l'horodatage vient du champ `@timestamp` du document (ISO
  8601), l'heure d'envoi sinon, en nanosecondes ; les lignes d'un même
  flux sont triées par horodatage ;
- **insertions seules** : `index` est la seule action acceptée (les
  options `index`, `pipeline`, `routing`, `doc_as_upsert` de la famille
  `_bulk` sont refusées à la compilation) ; `_id` refusé ;
- **accusé global** : Loki répond `204 No Content` sans détail par
  élément (`accuse_par_evenement=False`) : 2xx = lot ingéré ; 400 =
  permanent (DLQ si configurée) ; réseau, 429, 5xx = transitoire,
  retenté puis rendu à l'appelant (ou DLQ avec `dlq_transitoires`) ;
- **multi-tenant** : `tenant_id` devient l'en-tête `X-Scope-OrgID`.

Santé : `health()` interroge `GET /ready` (200 quand Loki accepte des
écritures).

Banc de conformité contre Grafana Loki 3.7.9 en conteneur
(`.github/workflows/conformite-loki.yml`, `tests/test_integration_loki.py`) :
documents retrouvés par LogQL avec leurs labels de flux, horodatage du
document respecté. Fixture du banc commun (#9) en plus.

## Sorties `file` et `stdout` (issue #32)

Sorties locales, sans client : chaque document part en texte, rendu par
un codec partagé — `json_lines` (une ligne JSON par document, NDJSON) ou
`rubydebug` (rendu « inspect Ruby » multi-lignes, celui du plugin
Logstash, simplifié : tableaux et sous-hachages restent sur une ligne).

### Sortie `file`

Écrit une ligne par document dans le fichier rendu par le gabarit
`path` — la rotation est le gabarit lui-même (rendu contre
`@timestamp`, comme `index_pattern`) : `%{+YYYY-MM-dd}` change de
fichier chaque jour, le précédent est fermé à la rotation. Le dossier
parent est créé au besoin ; un seul fichier reste ouvert.

```
output {
  file {
    path   => "/var/log/pystash/%{+YYYY-MM-dd}.ndjson"
    codec  => "json_lines"        # ou rubydebug
    # dlq_path => "/var/lib/dlq/file.jsonl"
  }
}
```

| Option | Rôle | Équivalent Logstash |
|---|---|---|
| `path` | gabarit du fichier (sprintf + date Joda) | `path` |
| `codec` | `json_lines` (défaut) ou `rubydebug` | `codec` |
| `dlq_path` | DLQ locale des échecs d'écriture (`OSError`) | — |

Échec d'écriture (disque plein, droits) : permanent — le document part
au DLQ si `dlq_path` est configuré, sinon l'exception remonte à
l'appelant.

### Sortie `stdout`

Écrit une ligne par document sur la sortie standard (injectable par le
connecteur). `codec` : `rubydebug` par défaut (comme le plugin
Logstash) ou `json_lines`. `flush()` vide la sortie ; `close()` ne
ferme pas la sortie standard.

## Dead letter queue et rejeu (issue #7)

Toute sortie qui a une DLQ (`dlq_path`) y écrit une ligne JSON par
document refusé :

```json
{"@dlq_timestamp": "2026-11-04T16:00:00+00:00", "@dlq_index": "logs-2026.11.04",
 "@dlq_error": "...", "@dlq_output": "opensearch", "@dlq_reason": "permanent",
 "document": {...}}
```

- `@dlq_output` : option `id` du bloc `output{}`, sinon nom du plugin ;
- `@dlq_reason` : `permanent` (4xx, mapping, écriture impossible) ou
  `transitoire_epuise` (réseau, 429, 5xx après `retry_max` réessais,
  avec `dlq_transitoires => true`) ;
- ces deux champs s'ajoutent au format historique de RsysLogstach : une
  ancienne DLQ se relit et se rejoue sans conversion.

Par défaut, un échec transitoire épuisé reste rendu à l'appelant
(`RETRYABLE`) — c'est ce qu'attend rsyslog, qui retient le message.
`dlq_transitoires` sert aux appelants qui ne retiennent rien. Un échec
transitoire d'un seul élément de la réponse `_bulk` n'est jamais retenté
individuellement : il reste `RETRYABLE`.

Rejeu, une fois la cause corrigée :

```python
import asyncio
from pystashtemplate.outputs.opensearch import OpenSearchOutput
from pystashtemplate.outputs.rejeu import rejouer_dlq

sortie = OpenSearchOutput({"hosts": "https://localhost:9200"})  # batch_size 1, sans dlq_path
rapport = asyncio.run(rejouer_dlq("/var/lib/dlq/opensearch.jsonl", sortie))
print(rapport.rejouees, rapport.conservees, rapport.erreurs)
```

Chaque document repart vers son index d'origine (`@dlq_index`). Les
entrées refusées restent dans le fichier, au format d'origine
(remplacement atomique) ; le fichier est supprimé quand tout est
rejoué. Un fichier rotaté `.gz` se rejoue aussi (le reste est réécrit à
côté, non compressé ; refusé si ce fichier existe déjà). Ne rejouez
pas le fichier courant d'une DLQ encore alimentée : faites-le tourner
(`LocalDeadLetterQueue.rotate()`) et rejouez le fichier rotaté. Les sorties sans résultat par document (`file`,
`stdout`, `null`) sont rejouées par `send()` : accepté si aucune
exception n'est levée.

### Banc de conformité

`tests/test_integration_opensearch.py` exerce la sortie `opensearch` contre un vrai
OpenSearch (conteneur éphémère en CI, workflow
`conformite-opensearch.yml`) : indexation effective, recherche, `_id`
déterministe, rejet de mapping parti en DLQ. Localement :

```bash
docker run -d -p 9200:9200 -e DISABLE_SECURITY_PLUGIN=true \
  -e "discovery.type=single-node" opensearchproject/opensearch:2.19.1
PYSTASHTEMPLATE_OPENSEARCH_URL=http://localhost:9200 \
  uv run pytest tests/test_integration_opensearch.py
```

## Métriques des sorties (issue #10)

Chaque sortie prévient son `observateur` (attribut d'instance, nul par
défaut : aucun coût sans métriques) — protocole
`pystashtemplate.outputs.metriques.ObservateurSortie` :

| Appel | Quand | Sorties |
|---|---|---|
| `evenements(sortie, "indexed", n)` | documents acceptés par la cible | HTTP, `file` |
| `evenements(sortie, "dead_lettered", n)` | documents écrits en DLQ | HTTP, `file` |
| `evenements(sortie, "retryable", n)` | documents rendus à l'appelant (transitoire épuisé) | HTTP |
| `evenements(sortie, "rejected", n)` | refus définitif de la cible (compté en plus de l'issue finale) | HTTP, `file` |
| `reessai(sortie)` | chaque retente d'une requête (429, 5xx, réseau) | HTTP |
| `latence(sortie, secondes)` | chaque requête vers la cible, réussie ou non | HTTP |
| `file(sortie, taille)` | taille du tampon (`batch_size`) après chaque ajout / envoi | HTTP |

`sortie` est `Output.nom` (option `id`, sinon le nom du plugin).
`CompteursSortie` tient ces compteurs en mémoire (`instantane()`) et les
expose au format texte Prometheus (`exposition()`, contenu d'un
`/metrics`) : `pystashtemplate_output_events_total{output,outcome}`,
`pystashtemplate_output_retries_total{output}`,
`pystashtemplate_output_request_duration_seconds{output}` (résumé
`_count`/`_sum`) et `pystashtemplate_output_queue_size{output}`. Le banc
commun de conformité vérifie ces métriques pour chaque sortie HTTP.

```python
from pystashtemplate.outputs.metriques import CompteursSortie

compteurs = CompteursSortie()
for sortie in compiler_sorties(pipeline):
    sortie.observateur = compteurs
...
print(compteurs.exposition())
```

### Connecteur RsysLogstach : mêmes métriques qu'aujourd'hui

Le connecteur compte déjà l'issue de chaque message
(`rsyslogstach_messages_total{outcome}`) et la durée de l'appel
`indexer.send` (`rsyslogstach_index_latency_seconds`). Pour n'en changer
aucun nom, il branche un adaptateur sur ses compteurs `prometheus_client`
existants plutôt que `CompteursSortie` :

```python
from prometheus_client import Counter, Gauge


class ObservateurRsyslogstach:
    """Mesures de la sortie sur le registre de `metrics.Metrics`, sous de nouveaux noms."""

    def __init__(self, metrics: Metrics) -> None:
        r = metrics.registry
        self.retries = Counter("rsyslogstach_output_retries", "Retentes de la sortie", registry=r)
        self.rejected = Counter("rsyslogstach_output_rejected", "Refus définitifs de la cible", registry=r)
        self.queue = Gauge("rsyslogstach_output_queue_size", "Tampon de la sortie", registry=r)

    def evenements(self, sortie: str, issue: str, nombre: int = 1) -> None:
        # indexed / dead_lettered / retryable : déjà comptés par message
        # par omprog (rsyslogstach_messages_total{outcome}) — pas de double compte.
        if issue == "rejected":
            self.rejected.inc(nombre)

    def reessai(self, sortie: str) -> None:
        self.retries.inc()

    def latence(self, sortie: str, secondes: float) -> None:
        pass  # rsyslogstach_index_latency_seconds reste observée autour de send

    def file(self, sortie: str, taille: int) -> None:
        self.queue.set(taille)
```

Les métriques existantes ne changent ni de nom ni de valeur ; les
nouvelles mesures (retentes, refus définitifs, taille du tampon)
s'ajoutent sous de nouveaux noms.
