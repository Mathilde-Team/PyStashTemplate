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
| `doris` | `index` | non | non | — | oui | oui | oui |
| `elasticsearch` | toutes | oui | oui | — | oui | oui | oui |
| `file` | `index`, `create` | non | oui | — | oui | oui | non |
| `fluent_forward` | `index` | non | non | — | oui | oui | oui |
| `gelf` | `index` | non | non | — | oui | oui | oui |
| `greptimedb` | `index`, `create` | non | oui | — | oui | oui | oui |
| `http` | `index` | non | non | — | oui | oui | oui |
| `kafka` | `index` | non | non | — | oui | oui | oui |
| `loki` | `index` | non | non | — | oui | oui | oui |
| `manticore` | toutes | oui | oui | — | oui | oui | oui |
| `nats` | `index` | non | non | — | oui | oui | oui |
| `null` | toutes | oui | oui | — | non | oui | non |
| `openobserve` | `index`, `create` | non | oui | — | oui | oui | oui |
| `opensearch` | toutes | oui | oui | — | oui | oui | oui |
| `otlp` | `index`, `create` | non | non | — | oui | oui | oui |
| `parseable` | `index` | non | non | 10 Mio | oui | oui | oui |
| `quickwit` | `create` | non | oui | 10 Mio | oui | oui | oui |
| `rabbitmq` | `index` | non | non | — | oui | oui | oui |
| `redis` | `index` | non | non | — | oui | oui | oui |
| `siglens` | `index`, `create` | non | oui | — | oui | oui | oui |
| `splunk_hec` | `index` | non | non | — | oui | oui | oui |
| `stdout` | `index`, `create` | non | oui | — | non | oui | non |
| `syslog` | `index` | non | non | — | oui | oui | oui |
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
`_id` accepté, accusé par élément). `pystashtemplate.indexer`
(`OpenSearchIndexer`, rapatrié de RsysLogstach par #110) s'appuie dessus :
voir [indexation-metriques.md](indexation-metriques.md).

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

## Sortie `doris` (issue #27)

Stream Load d'Apache Doris : `PUT /api/<database>/<table>/_stream_load`
sur le FE (port HTTP 8030 par défaut), qui redirige (`307`) vers un BE.
Chaque lot est **un** Stream Load : un tableau JSON (`format: json`,
`strip_outer_array: true`), dont les clés sont appariées aux colonnes
de la table par leur nom (les colonnes absentes du document prennent
leur défaut, les champs sans colonne sont ignorés).

```
output {
  doris {
    hosts => ["http://doris-fe:8030"]
    database => "logs"
    table => "evenements"
    user => "${DORIS_USER}"
    password => "${DORIS_PASSWORD}"
  }
}
```

| Option propre | Rôle |
|---|---|
| `database` | Base de la table — **requise** |
| `table` | Table cible — **requise**, elle doit exister (Doris ne la crée pas) |
| `label_prefix` | Préfixe du label de chaque lot (`pystashtemplate` par défaut ; lettres, chiffres, `-`, `_`) |
| `max_filter_ratio` | Part de lignes invalides tolérée par lot, de 0 (défaut : lot refusé dès une ligne) à 1 |

Particularités :

- **redirection FE → BE avec authentification** : httpx retire
  `Authorization` d'une requête redirigée vers un autre hôte (comme
  `curl` sans `--location-trusted`) ; la sortie suit elle-même les
  `307`/`308` (trois sauts au plus) en gardant l'authentification.
  `hosts` peut aussi viser un BE directement (port 8040). Le BE doit
  être joignable à l'adresse sous laquelle le FE le connaît ;
- **label par lot, rejeu sans doublon** : chaque lot porte un label
  `<label_prefix>_<32 chiffres hexadécimaux>`, réutilisé à chaque
  retente du même lot. Si un chargement a abouti sans que la réponse
  arrive (coupure), la retente répond `Label Already Exists` avec
  `ExistingJobStatus: FINISHED` : le lot est compté comme indexé, sans
  ligne en double. Un lot rendu à l'appelant (`retryable`) puis renvoyé
  repart, lui, avec un nouveau label ;
- **statut dans le corps** : Doris répond HTTP 200 même quand le
  chargement échoue ; la sortie lit `Status` : `Success` et
  `Publish Timeout` (données validées, visibles sous peu) = indexé ;
  `Label Already Exists` = indexé si `FINISHED`, rendu à l'appelant
  sinon ; `Fail` = refus ; corps illisible ou statut inconnu = rendu à
  l'appelant. Statuts HTTP : 400/404 = permanent, 429/5xx/réseau =
  transitoire (options communes de retente), autres (401, 403…) =
  rendu à l'appelant ;
- **lot atomique, ligne fautive isolée** : avec `max_filter_ratio => 0`,
  une seule ligne invalide fait échouer tout le lot sans rien écrire.
  La sortie renvoie alors chaque document **seul** : les bons sont
  chargés, le fautif est refusé définitivement (DLQ si configurée) avec
  le `Message` et l'`ErrorURL` du serveur ;
- **lignes filtrées** : avec `max_filter_ratio` > 0, Doris peut écarter
  des lignes et répondre `Success` sans dire lesquelles : le lot est
  compté comme indexé, un avertissement donne `NumberFilteredRows` et
  l'`ErrorURL` ;
- **insertions seules** : ni `_id`, ni `update`/`delete` ; les options
  `index`, `action`, `pipeline`, `routing`, `doc_as_upsert` sont
  refusées à la compilation. Les en-têtes Stream Load additionnels
  (`columns`, `timezone`, `strict_mode`…) passent par `headers`.

StarRocks expose la même interface Stream Load ; la sortie n'a été
vérifiée qu'au banc Doris.

Santé : `health()` interroge `GET /api/health` du FE (nombre de BE en
ligne).

Banc de conformité contre Doris 4.1.3 (image `all-in-one`, un FE et un
BE) en conteneur (`.github/workflows/conformite-doris.yml`,
`tests/test_integration_doris.py`) : lot chargé par le FE et relu
colonne par colonne, envoi direct au BE, même label rejoué sans
doublon, table absente en DLQ, ligne invalide isolée du lot. Fixture du
banc commun (#9) en plus.

## Sortie `splunk_hec` (issue #28)

Protocole **HTTP Event Collector** : `POST /services/collector/event`
(port 8088 chez Splunk, 8081 chez SigLens), en-tête
`Authorization: Splunk <jeton>`. Splunk n'est pas open source : la
sortie vise le **protocole**, accepté aussi par des systèmes open source
comme SigLens. Chaque document devient une enveloppe HEC ; les
enveloppes du lot sont concaténées, une par ligne.

```
output {
  splunk_hec {
    hosts => ["https://siglens:8088"]
    token => "${HEC_TOKEN}"
    sourcetype => "syslog"
  }
}
```

| Option propre | Champ HEC | Rôle |
|---|---|---|
| `token` | en-tête `Authorization: Splunk <jeton>` | jeton HEC ; **obligatoire** |
| `sourcetype` | `sourcetype` | type de source ; absent par défaut |
| `source` | `source` | source ; absente par défaut |
| `host` | `host` | hôte fixe ; sinon le champ `host` du document (chaîne, ou `host.name` ECS) |
| `index` | `index` | index Splunk (gabarit admis) ; absent : index par défaut du jeton |
| `time_field` | `time` | champ ISO 8601 converti en secondes epoch (à la milliseconde) ; `@timestamp` par défaut ; illisible ou absent : `time` omis, le serveur date l'événement |
| `fields` | `fields` | liste de champs du document recopiés en champs indexés (valeurs en chaînes) ; `[]` par défaut |
| `channel` | en-tête `X-Splunk-Request-Channel` | canal, exigé par Splunk quand l'accusé d'indexation est actif |

Enveloppe produite :

```
{"event": {...document...}, "time": 1793786400.123, "host": "h1",
 "source": "rsyslog", "sourcetype": "syslog", "index": "main",
 "fields": {"severity": "info"}}
```

Particularités :

- **jeton seul** : `user`/`password` sont refusés à la compilation
  (l'authentification basique remplacerait l'en-tête du jeton) ; un
  en-tête `Authorization` passé dans `headers` prime ;
- **insertions seules** : `index` est la seule action ; `pipeline`,
  `routing`, `doc_as_upsert` (propres à `_bulk`) refusés ; `_id` refusé ;
- **accusé global** : HEC répond pour le lot entier
  (`{"text": "Success", "code": 0}`), sans détail par événement
  (`invalid-event-number` ne désigne que le premier fautif) :
  `accuse_par_evenement=False`. 2xx = lot ingéré ; 400 (format invalide,
  pas de données) = permanent pour tout le lot, DLQ si configurée ;
  réseau, 429, 503 (serveur occupé) = transitoire, retenté puis rendu à
  l'appelant ; 401/403 (jeton absent ou refusé) = lot rendu à
  l'appelant, jamais en DLQ ;
- **non suivi** : l'accusé d'indexation différé
  (`/services/collector/ack`) et le point d'entrée brut
  (`/services/collector/raw`).

Santé : `health()` interroge `GET /services/collector/health` (rend le
corps JSON du serveur, ou `None`).

Banc de conformité contre SigLens 1.0.57 en conteneur
(`.github/workflows/conformite-splunk_hec.yml`,
`tests/test_integration_splunk_hec.py`) : santé, lot de trois
enveloppes relu par la recherche (`event.message`, `sourcetype`,
`source`, `host`, `fields`), caractères non ASCII. SigLens range
l'enveloppe entière et n'impose pas de jeton : le refus d'un jeton
(401/403) n'est couvert que par les tests unitaires. Fixture du banc
commun (#9) en plus.

## Sortie `http` générique (issue #24)

Repli pour **tout récepteur HTTP** — Vector (source `http_server`),
entrée `http` de Logstash, Fluent Bit, webhooks… — sans client dédié :
`POST` (ou `PUT`) vers une URL libre, options reprises de la sortie
`http` de Logstash.

```
output {
  http {
    url => "http://vector:8080/logs"
    http_method => "post"
    format => "json_batch"
    headers => { "Authorization" => "Bearer ${TOKEN}" }
  }
}
```

| Option propre | Rôle |
|---|---|
| `url` | URL libre du récepteur (schéma, hôte, chemin) — **requise**, prime sur `hosts` |
| `http_method` | `post` (défaut) ou `put` |
| `format` | `ndjson` (défaut : une ligne JSON par document), `json_batch` (tableau du lot), `json` (**un événement par requête**, lot retaillé à 1) |

Particularités :

- `url` est découpée : l'hôte alimente le client commun (options
  communes `user`/`password`/`ssl_*`/`headers`…), le chemin (et la
  requête d'URL) sert à chaque envoi ;
- **accusé global** : un récepteur générique n'accuse pas réception
  élément par élément (`accuse_par_evenement=False`) : 2xx = lot
  accepté ; 400/404 = permanent (DLQ si configurée) ; réseau, 429, 5xx
  = transitoire — un lot rejeté part en entier ;
- **insertions seules** : `index`, `_id` refusé, options de la famille
  `_bulk` (`index`/`action`/`pipeline`/`routing`/`doc_as_upsert`)
  refusées à la compilation ;
- **santé** : `health()` sonde le récepteur (`GET` sur le chemin
  d'`url`) — toute réponse inférieure à 5xx prouve la joignabilité (un
  récepteur générique n'a pas de protocole de santé standard).

Banc de conformité contre Vector 0.59.0 en conteneur (source
`http_server`, `.github/workflows/conformite-http.yml`,
`tests/test_integration_http.py`) : événements relus dans les journaux
du sink `console`, les trois formats vérifiés. Fixture du banc commun
(#9) en plus.

## Sortie `kafka` (issue #29)

Producteur asynchrone vers **Apache Kafka et compatibles** (Redpanda,
MSK…), comme le plugin `logstash-output-kafka` : `bootstrap_servers`
et `topic_id` sont requis, le sujet et la clé (`message_key`) sont
rendus par gabarit sprintf — les documents d'un même lot peuvent
partir sur des sujets différents.

```
output {
  kafka {
    bootstrap_servers => "kafka:9092"
    topic_id => "logs-%{+YYYY.MM.dd}"
    message_key => "%{host}"
    codec => "json"
    acks => "1"
  }
}
```

| Option propre | Rôle |
|---|---|
| `bootstrap_servers` | Nœud(s) `hôte:port` (chaîne ou liste) — **requise** |
| `topic_id` | Sujet de destination, gabarit sprintf — **requis** |
| `message_key` | Clé des messages (gabarit sprintf, partitionnement) |
| `codec` | `json` (défaut), `json_lines` ou `rubydebug` : rendu de la valeur |
| `acks` | `0`, `1` (défaut) ou `all` : niveau d'acquittement Kafka |
| `compression_type` | `none` (défaut), `gzip`, `snappy`, `lz4`, `zstd` |
| `client_id` | Identifiant du client auprès du cluster |
| `security_protocol` | `PLAINTEXT` (défaut), `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` (avec `user`/`password`, mécanisme PLAIN) |

Particularités :

- **accusé par message** : chaque envoi attend l'acquittement du
  courtier (selon `acks`), mais l'issue est rapportée par lot —
  `accuse_par_evenement=False` au sens du socle ;
- **tout échec est transitoire** : Kafka ne rejette pas un message
  pour son contenu — retentes exponentielles
  (`retry_max`/`retry_backoff`) sur les seuls messages en échec, puis
  lot rendu à l'appelant (`RETRYABLE`) ou en DLQ avec
  `dlq_transitoires` (#7) ; la sémantique est **au moins une fois** :
  un message dont l'acquittement s'est perdu peut partir en double ;
- **insertions seules** : `index`, `_id` refusé ;
- **santé** : `health()` relit le méta-annuaire du cluster (nœuds
  connus du producteur) — pas d'endpoint de santé standard dans Kafka.

Banc de conformité contre Redpanda en conteneur
(`.github/workflows/conformite-kafka.yml`,
`tests/test_integration_kafka.py`) : messages relus par un
consommateur `aiokafka`, sujet, clé et valeur vérifiés.

## Sortie `fluent_forward` (issue #31)

Client du **protocole forward v1** de Fluentd et Fluent Bit (TCP,
24224 par défaut, messages MessagePack), pour relayer vers un pipeline
Fluentd / Fluent Bit existant — comme `out_forward` de Fluentd.

```
output {
  fluent_forward {
    host => "fluentbit"
    port => 24224
    tag => "rsyslog.%{app}"
    require_ack => true
  }
}
```

| Option propre | Rôle |
|---|---|
| `host` / `port` | Serveur forward (`localhost`:`24224` par défaut) |
| `tag` | Étiquette des évènements, gabarit sprintf — **requise** |
| `mode` | `forward` (défaut : `[tag, [[temps, doc], …], option]`) ou `packed_forward` (entrées concaténées dans un binaire) |
| `require_ack` | Ajoute un `chunk` à chaque message et attend `{"ack": chunk}` (au moins une fois) |
| `ack_response_timeout` | Secondes d'attente de l'accusé (190 par défaut, comme Fluentd) |
| `time_key` | Champ du temps de l'évènement (`@timestamp` par défaut : ISO 8601, `datetime` ou nombre) |
| `time_as_integer` | Temps en secondes entières au lieu d'`EventTime` (anciens Fluentd) |
| `shared_key` | Clé partagée : poignée de main HELO/PING/PONG (avec `user`/`password` si le serveur authentifie) |
| `self_hostname` | Nom annoncé dans le PING (nom d'hôte local par défaut) |
| `tls` | Chiffre la connexion (options communes `ssl`, `cacert`, `ssl_certificate`, `ssl_key`) |
| `connect_timeout` | Secondes d'attente de la connexion et de la poignée de main (10 par défaut) |

L'option commune `compression => true` envoie le binaire compressé en
gzip (`CompressedPackedForward`, implique `packed_forward`).

Particularités :

- **un message par étiquette** : un lot est regroupé par étiquette
  rendue, dans l'ordre d'arrivée ; une connexion TCP est réutilisée
  d'un lot à l'autre ;
- **temps exact** : `EventTime` (extension MessagePack 0, secondes et
  nanosecondes) calculé sans passer par un flottant — à la microseconde
  près pour une date ISO 8601 ;
- **tout échec est transitoire** : le protocole ne rejette pas un
  évènement pour son contenu ; connexion refusée, coupure, accusé
  absent ou faux, poignée de main refusée → retentes exponentielles
  (`retry_max`/`retry_backoff`) des seuls messages non acquittés, sur
  une connexion neuve, puis lot rendu à l'appelant (`RETRYABLE`) ou en
  DLQ avec `dlq_transitoires` (#7). Sans `require_ack`, un message
  écrit est réputé livré ;
- **clé partagée** : Fluent Bit **ferme la connexion** sur une clé
  fausse (pas de PONG refusé) et attend un sel de type *str*
  MessagePack ; un mauvais mot de passe rend un PONG refusé
  (« username/password mismatch ») ;
- **insertions seules** : `index`, `_id` refusé ;
- **santé** : `health()` ouvre la connexion (poignée de main comprise)
  — le protocole n'a pas de requête de santé.

Banc de conformité contre Fluent Bit 5.1.5 en conteneur
(`.github/workflows/conformite-fluent_forward.yml`,
`tests/test_integration_fluent_forward.py`) : entrées `forward` ouverte
et authentifiée, évènements relus dans les fichiers de la sortie `file`
(étiquette, temps en nanosecondes, document), modes Forward,
PackedForward et gzip, clé partagée, mot de passe refusé.

## Sorties courtiers de messages : `redis`, `rabbitmq`, `nats` (issue #33)

Trois éditeurs asynchrones sur un **socle commun**
(`pystashtemplate.outputs.courtier`) : seul le `output{}` change d'un
courtier à l'autre, le cycle d'envoi est le même que celui de `kafka`.

- chaque document est préparé à l'arrivée (destination rendue par
  gabarit sprintf, charge utile encodée selon `codec` : `json` par
  défaut, `json_lines` ou `rubydebug`) puis tamponné jusqu'à
  `batch_size` ;
- le lot part d'un bloc ; l'issue est analysée **message par message**
  quand le courtier le permet ;
- seuls les messages en échec transitoire sont retentés
  (`retry_max`/`retry_backoff`), puis rendus à l'appelant (`RETRYABLE`)
  ou mis en DLQ avec `dlq_transitoires` (#7) ; une erreur de connexion
  vaut pour tout le lot et fait rouvrir la connexion au prochain essai ;
- un **refus définitif** d'un message (voir chaque courtier) part en DLQ
  si elle est configurée, sinon il est rendu à l'appelant ;
- sémantique **au moins une fois** ; `index` seulement, `_id` refusé ;
- TLS commun : `ssl_enabled` (faux par défaut) chiffre la connexion,
  `ssl` (vérification du certificat, vrai par défaut), `cacert`,
  `ssl_certificate`/`ssl_key` comme les autres sorties.

### `redis`

```
output {
  redis {
    host => ["redis-1:6379", "redis-2"]
    key => "logs-%{app}"
    data_type => "list"
    batch_size => 100
  }
}
```

| Option propre | Rôle |
|---|---|
| `host` | Nœud(s) `hôte` ou `hôte:port` (`[::1]:6379` en IPv6), essayés dans l'ordre (`127.0.0.1` par défaut, à défaut `hosts`) |
| `port` | Port des nœuds sans port explicite (6379) |
| `db` | Base Redis (0) |
| `key` | Liste, canal ou flux, gabarit sprintf — **requise** |
| `data_type` | `list` (défaut, `RPUSH`), `channel` (`PUBLISH`), `stream` (`XADD`, champ `message`, extension absente de Logstash) |
| `stream_maxlen` | Longueur maximale approximative du flux (`MAXLEN ~`) |
| `timeout` | Délai réseau en secondes (5) |

`user`/`password` portent l'authentification (ACL Redis 6+, ou mot de
passe seul). Un lot = **un pipeline** Redis (sans transaction) ; une
erreur de commande (`WRONGTYPE` : la clé existe avec un autre type) est
un refus définitif de ce seul document. `channel` est feu-et-oubli :
sans abonné, le message est perdu. Santé : `PING` et version du serveur.

### `rabbitmq`

```
output {
  rabbitmq {
    host => "rabbitmq"
    exchange => "logs"
    exchange_type => "topic"
    key => "%{app}"
  }
}
```

| Option propre | Rôle |
|---|---|
| `host` | Nœud(s) `hôte` ou `hôte:port`, essayés dans l'ordre (`localhost`) |
| `port` | Port AMQP (5672) |
| `vhost` | Hôte virtuel (`/`) |
| `exchange` | Échange de destination — **requis** (`""` : échange par défaut) |
| `exchange_type` | `direct`, `fanout`, `topic`, `headers`, `x-consistent-hash`, `x-modulus-hash` — **requis** |
| `key` | Clé de routage, gabarit sprintf (`logstash`) |
| `durable` | Échange déclaré durable (vrai) |
| `passive` | Vérifier l'échange sans le déclarer (faux) |
| `persistent` | Messages en livraison persistante (vrai) |
| `message_properties` | Propriétés AMQP : `content_type` (`application/json` par défaut), `priority`, `app_id`, `type`… |
| `heartbeat`, `timeout` | Battement de cœur AMQP ; délai de connexion (5 s) |

`user`/`password` valent `guest`/`guest` par défaut, comme Logstash. Le
canal utilise les **confirmations d'éditeur** : un `nack` est
transitoire, une erreur de canal ou de connexion aussi (la connexion est
rouverte). Comme Logstash, `mandatory` n'est pas positionné : un message
qu'aucune file ne reçoit est écarté par le courtier. Santé : produit et
version annoncés, échange vérifié.

### `nats`

Pas de sortie NATS officielle dans Logstash : options du plugin
communautaire `logstash-output-nats` et du client `nats-py`.

```
output {
  nats {
    url => ["nats://nats-1:4222", "nats://nats-2:4222"]
    subject => "logs.%{app}"
    jetstream => true
  }
}
```

| Option propre | Rôle |
|---|---|
| `url` | Serveur(s) du groupe, essayés dans l'ordre (`nats://127.0.0.1:4222`, à défaut `hosts`) |
| `subject` | Sujet, gabarit sprintf — **requis** |
| `token` | Jeton d'authentification (sinon `user`/`password`) |
| `jetstream` | Publier en JetStream, avec accusé par message (faux) |
| `stream` | Flux JetStream attendu pour le sujet |
| `name`, `timeout` | Nom du client vu par le serveur ; délai réseau (5 s) |

NATS cœur ne stocke rien : le lot est confirmé par un `flush`
(aller-retour PING/PONG), qui prouve seulement que le serveur a tout
reçu. En JetStream, un sujet couvert par aucun flux, un message plus
gros que `max_payload` ou une erreur d'API autre que 503 sont des refus
définitifs ; un délai dépassé ou un 503 sont transitoires. Santé : URL
et version du serveur, plus le nombre de flux du compte en JetStream.

Bancs de conformité en conteneur : Redis 8.4
(`conformite-redis.yml`, `tests/test_integration_redis.py`),
RabbitMQ 4.2 (`conformite-rabbitmq.yml`,
`tests/test_integration_rabbitmq.py`) et NATS 2.15 avec JetStream
(`conformite-nats.yml`, `tests/test_integration_nats.py`) : messages
relus par un client indépendant, refus définitifs vérifiés en DLQ.

## Sortie `syslog` (issue #30)

Émetteur syslog — **RFC 5424 ou RFC 3164**, vers UDP, TCP ou TCP+TLS
(rsyslog, syslog-ng, entrée syslog de VictoriaLogs…) — options reprises
de la sortie `syslog` de Logstash, défauts compris.

```
output {
  syslog {
    host => "rsyslog-relay"
    port => 6514
    protocol => "ssl-tcp"
    rfc => "rfc5424"
  }
}
```

| Option propre | Rôle |
|---|---|
| `host` | Récepteur syslog — **requise** |
| `port` | Port du récepteur (514 par défaut) |
| `protocol` | `udp` (défaut), `tcp` ou `ssl-tcp` (TLS, `cacert` des options communes) |
| `rfc` | `rfc3164` (défaut) ou `rfc5424` |
| `framing` | Cadrage TCP RFC 6587 : `octet-counting` (défaut) ou `lf` |
| `facility` / `severity` | Mots-clés RFC 5424 (`user-level`/`notice` par défaut) |
| `appname` / `procid` / `msgid` / `sourcehost` | Champs d'en-tête (`LOGSTASH`/`-`/`-`/`%{host}` par défaut, gabarits sprintf) |
| `codec` | Encodage du MESSAGE (`json` par défaut, `json_lines`, `rubydebug`) |

Particularités :

- **le document voyage dans le MESSAGE** : l'en-tête syslog porte le
  routage (PRI, facilité/sévérité, hôte, application), le MESSAGE
  porte le document encodé par le `codec` — les deux RFC sont
  émises mot pour mot (`<PRI>1 …` en 5424, BSD `<PRI>Mmm …` en
  3164) ;
- **pas d'accusé** : syslog n'accuse rien, ni UDP (feu-et-oubli) ni
  TCP (écriture du flux) — l'issue est globale au lot, tout échec de
  transport est transitoire (retentes exponentielles, puis lot rendu
  à l'appelant ou en DLQ avec `dlq_transitoires` #7) ;
- **santé** : TCP/ssl-tcp se connectent réellement ; UDP ne peut que
  ouvrir sa socket (documenté — un datagramme ne prouve rien) ;
- **insertions seules** : `index`, `_id` refusé.

Banc de conformité contre Vector 0.59.0 en conteneur (sources
`syslog` TCP et UDP, `.github/workflows/conformite-syslog.yml`,
`tests/test_integration_syslog.py`) : cadrage octet-counting et LF,
UDP, RFC 5424 et 3164 vérifiés dans les journaux du sink `console`.

## Sortie `gelf` (issue #26)

Messages **Graylog GELF 1.1**, comme le plugin `logstash-output-gelf`,
par trois transports au choix (`transport`) :

```
output {
  gelf {
    hosts => ["http://graylog:12201"]   # tcp://, tls://, udp://
    transport => "http"                 # défaut ; ou "tcp", "udp"
    batch_size => 100                   # HTTP : exige « Enable Bulk Receiving »
  }
}
```

| Option propre | Rôle |
|---|---|
| `transport` | `http` (défaut, `POST /gelf`), `tcp` (octet nul, TLS avec `tls://`) ou `udp` (morceaux GELF) |
| `sender` | Gabarit du champ `host` (`%{host}` par défaut, `host.name` si `host` est un objet, nom de la machine en repli) |
| `short_message` | Champ porté en `short_message` (`short_message` par défaut, repli sur `message`, puis `-`) |
| `full_message` | Gabarit de `full_message` (`%{message}` par défaut ; vide : omis) |
| `level` | Gabarit(s) du niveau, essayés dans l'ordre (`["%{level}", "info"]`) : nom syslog (`emerg`…`debug`) ou nombre ramené à 0-7 |
| `custom_fields` | Champs fixes ajoutés à chaque message (`_xxx`) |
| `ship_metadata` | Autres champs du document en `_xxx` (vrai par défaut) |
| `ship_tags` | `tags` en `_tags`, joints par `, ` (vrai par défaut) |
| `chunksize` | UDP : taille maximale d'un datagramme (1420 par défaut) |

Particularités :

- **champs additionnels** : objets aplatis (`service.name` →
  `_service_name`), listes jointes par `, `, booléens `true`/`false`,
  noms ramenés à `[A-Za-z0-9_.-]`, `id` envoyé en `__id` (`_id` est
  réservé par GELF), champs `@…` non envoyés ; `timestamp` vient de
  `@timestamp` (secondes depuis l'epoch) ;
- **HTTP** : un message par requête avec `batch_size => 1` ; au-delà,
  NDJSON (chaque message terminé par un saut de ligne, sinon Graylog
  colle le dernier message d'une requête au premier de la suivante sur
  la même connexion), à condition d'activer **Enable Bulk Receiving**
  sur l'entrée GELF HTTP. Graylog répond 202 sans détail :
  statut HTTP global (`accuse_par_evenement=False`), 400/404 en DLQ,
  429/5xx/réseau retentés, 401/403 rendus à l'appelant ;
- **TCP** : connexion persistante, messages terminés par un octet nul,
  jamais compressés (limite de GELF TCP) ; TLS avec `tls://` (`ssl`,
  `cacert`, `ssl_certificate`/`ssl_key`). Coupure : reconnexion et
  renvoi du reste du lot — sémantique **au moins une fois** ;
- **UDP** : gzip avec `compression => true` (ignorée en HTTP et TCP),
  découpage en morceaux GELF au-delà de `chunksize`, 128 morceaux au
  plus (au-delà : échec permanent, DLQ). Sans accusé : un datagramme
  parti est compté reçu, une erreur de socket signalée est retentée ;
- **insertions seules** : `index`, `_id` refusé ; `index`, `pipeline`,
  `routing`, `doc_as_upsert` refusées ;
- **santé** : `HEAD /gelf` (HTTP, erreur si 5xx), ouverture de la
  connexion (TCP) ; UDP non vérifiable (`unchecked`).

Banc de conformité contre Graylog 7.0 (MongoDB 7, OpenSearch 2.19) en
conteneurs (`.github/workflows/conformite-gelf.yml`,
`tests/test_integration_gelf.py`) : entrées GELF HTTP (réception par
lot), TCP et UDP créées par l'API, messages relus dans les index
`graylog_*` (champs `source`, `level`, `full_message`, champs
additionnels, morceaux UDP et gzip).

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
- **avertissement de cardinalité** : un label dont le gabarit référence
  un champ qui change à chaque document (`message`, `@timestamp`, `id`,
  `trace_id`, `request_id`, ports, IP… — liste
  `CHAMPS_FORTE_CARDINALITE` de `outputs/loki.py`) déclenche un
  avertissement au démarrage (un flux Loki par valeur) ; la
  configuration reste acceptée ;
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

## Sortie `greptimedb` (issue #19)

GreptimeDB (port HTTP 4000 par défaut) lit le NDJSON `_bulk` du client
commun (#11) sur `POST /v1/elasticsearch/_bulk` ; le `_index` de chaque
ligne d'action est la **table**, créée à la première écriture.

```
output {
  greptimedb {
    hosts => ["http://greptimedb:4000"]
    index => "logs"
    user => "${GREPTIME_USER}"
    password => "${GREPTIME_PASSWORD}"
  }
}
```

Aucune option propre : seules les options communes s'appliquent
(authentification basique `user`/`password` si le serveur l'exige).

Particularités (observées contre GreptimeDB 1.3.0) :

- **insertions seules** : `index` (défaut) et `create` ; `update` et
  `delete` sont refusées dès la compilation (le serveur les rejette en
  400) ; `_id` ignoré avec un avertissement (non stocké) ;
- **éléments sous la clé `create`** : chaque élément de `items[]` est
  rapporté sous `create`, même pour un `index` ; la sortie le relit sous
  l'action envoyée avant l'analyse commune ;
- **lot atomique** : une ligne invalide fait rejeter **tout** le lot en
  HTTP 400 sans rien écrire ; la sortie renvoie alors chaque document
  **seul** — sans doublon possible, seul le fautif est refusé (DLQ si
  configurée), avec la raison donnée par le serveur ;
- **perte silencieuse sur conflit de type** : un champ qui contredit le
  type déjà connu de la colonne n'est pas écrit alors que le serveur
  répond `201` ; rien dans la réponse ne permet de le détecter — garder
  des types stables par champ (`mutate { convert => … }`).

Santé : `health()` interroge `GET /health` (`{}` quand le serveur est
prêt).

Banc de conformité contre GreptimeDB 1.3.0 en conteneur, mode standalone
(`.github/workflows/conformite-greptimedb.yml`,
`tests/test_integration_greptimedb.py`). Fixture du banc commun (#9) en
plus.

## Sortie `siglens` (issue #20)

SigLens sépare l'ingestion (port 8081 par défaut), que vise cette
sortie, et la recherche (5122). Le NDJSON `_bulk` du client commun (#11)
est envoyé sur `POST /elastic/_bulk` ; l'index est créé à l'ingestion.

```
output {
  siglens {
    hosts => ["http://siglens:8081"]
    index => "logs"
  }
}
```

Aucune option propre : seules les options communes s'appliquent. Pas
d'authentification sur l'édition libre (`user`/`password` restent
transmis si un mandataire les exige).

Particularités (observées contre SigLens 1.0.57) :

- **actions `index` et `create` seules** : `update` et `delete` font
  rejeter tout le lot (HTTP 400) ; elles sont refusées dès la
  compilation du pipeline ;
- **pas de `_id`** : l'identifiant est accepté mais pas conservé (deux
  documents de même `_id` sont gardés, un `create` en double n'est pas
  refusé) ; un `document_id` est ignoré avec un avertissement ;
- **accusé par élément, sous la clé `index`** : `items[]` (201 indexé,
  400 `mapper_parse_exception` pour une ligne illisible), chaque élément
  rapporté sous `index` même pour un `create`, `status` d'un échec placé
  à côté de l'objet de l'action ; la sortie relit chaque élément sous
  l'action envoyée (`pystashtemplate.outputs.relecture`).

Santé : `health()` interroge `GET /elastic/` (la fiche « Elasticsearch »
de SigLens).

Banc de conformité contre SigLens 1.0.57 en conteneur
(`.github/workflows/conformite-siglens.yml`,
`tests/test_integration_siglens.py`) : documents retrouvés par l'API de
recherche (port 5122). Fixture du banc commun (#9) en plus.

## Sortie `otlp` (issue #22)

OpenTelemetry OTLP/HTTP, journaux : `POST {hosts}/v1/logs` (port 4318
par défaut), en protobuf (`application/x-protobuf`, défaut de la
spécification) ou en JSON. Un seul client couvre tous les récepteurs
OTLP (OpenTelemetry Collector, SigNoz, Uptrace, HyperDX/ClickStack,
OpenObserve, Quickwit, GreptimeDB, VictoriaLogs, Parseable…). Tampon,
réessais, DLQ et métriques sont ceux du client HTTP commun (#11).

```
output {
  otlp {
    hosts => ["http://otel-collector:4318"]
    encoding => "protobuf"
    resource => { "service.name" => "rsyslog" }
    fields => { "host" => "resource.host.name" "program" => "attributes.process.executable.name" }
  }
}
```

| Option propre | Rôle |
|---|---|
| `encoding` | `protobuf` (défaut) ou `json` |
| `resource` | Attributs de ressource fixes (défaut `service.name => "pystashtemplate"`) |
| `fields` | Correspondance champ de l'événement → cible du `LogRecord`, fusionnée avec la table par défaut |
| `scope` | Nom de la portée d'instrumentation (défaut `pystashtemplate`) |

Cibles de `fields` : `body` (défaut : `message`), `time`
(`time_unix_nano`, ISO 8601 ou nombre ; défaut : `@timestamp`),
`severity` (`severity_text`/`severity_number`, nom usuel ou code syslog
0–7), `trace_id`/`span_id` (hexadécimal), `resource.<nom>`,
`attributes.<nom>`, `drop` (non transmis). Tout autre champ devient un
attribut du journal sous son propre nom.

Particularités :

- **groupement par ressource** : les journaux d'un lot sont regroupés
  par jeu d'attributs de ressource ; `observed_time_unix_nano` est
  l'heure d'envoi ;
- **accusé global** : OTLP ne rapporte pas d'erreur par journal ; 2xx
  accepté, 400 permanent (DLQ), 429/5xx retentés ; puis
  `partial_success` : si tout le lot est refusé, il part en DLQ ; si une
  partie seulement l'est, les journaux refusés ne sont pas identifiables
  (la spécification interdit de les renvoyer) — ils sont comptés dans la
  métrique `rejected` et journalisés avec le message du serveur ;
- `compression => true` compresse le corps en gzip.

Santé : `health()` envoie une requête OTLP vide (OTLP n'a pas de point
de santé) et renvoie le statut HTTP.

Banc de conformité contre OpenTelemetry Collector contrib 0.164.0 en
conteneur, récepteur OTLP/HTTP et exportateur `file`
(`.github/workflows/conformite-otlp.yml`, `tests/test_integration_otlp.py`),
en protobuf, en JSON et compressé en gzip. Fixture du banc commun (#9) en plus.

## Sortie `parseable` (issue #25)

Parseable (port 8000 par défaut) reçoit un tableau JSON d'événements sur
`POST /api/v1/ingest` ; le jeu de données (« stream ») passe dans
l'en-tête `X-P-Stream` et il est créé s'il n'existe pas. Parseable aplatit
les champs imbriqués (`host.name` → `host_name`) et ajoute
`p_timestamp`.

```
output {
  parseable {
    hosts => ["http://parseable:8000"]
    stream => "rsyslog"
    user => "${PARSEABLE_USER}"
    password => "${PARSEABLE_PASSWORD}"
  }
}
```

| Option propre | Rôle |
|---|---|
| `stream` | Jeu de données visé, en-tête `X-P-Stream` (obligatoire) |
| `api_key` | Clé d'API, en-tête `X-API-Key` (Parseable Cloud / Enterprise) |
| `tenant_id` | Locataire, en-tête `X-P-Tenant-ID` (déploiements multi-tenant) |
| `tags` | Étiquettes ajoutées à chaque événement, en-têtes `X-P-Tag-<nom>` ; `{}` par défaut |

Particularités (observées contre Parseable 3.2.6) :

- **insertions seules** : action `index` seule, pas de `_id` (Parseable
  ignore l'identifiant et l'index) ;
- **accusé global** : 200 sans corps si le lot est accepté, un message
  texte sinon (`accuse_par_evenement=False`) : 400/404 = permanent (DLQ
  si configurée) ; réseau, 429, 5xx = transitoire ; 401/403 (clé ou
  droits refusés) = lot rendu à l'appelant ;
- **conflit de type** : pas une erreur — Parseable range la valeur dans
  une colonne suffixée (`n` numérique puis `"texte"` → colonne
  `n_utf8`) ;
- **jamais de lot vide** : Parseable 3.2.6 s'arrête net sur un tableau
  vide.

Santé : `health()` interroge `GET /api/v1/readiness` puis
`GET /api/v1/about` (`{"status": "ready", "version": …}`).

Banc de conformité contre Parseable 3.2.6 (binaire officiel en mode
`local-store`, `.github/workflows/conformite-parseable.yml`,
`tests/test_integration_parseable.py`). Fixture du banc commun (#9) en
plus.

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

### Outil qui utilise déjà `prometheus_client`

`pystashtemplate.metrics.Metrics` (issue #110) implémente
`ObservateurSortie` sur son propre registre Prometheus : il suffit de le
passer comme `observateur` des sorties. Les métriques des sorties
(`<prefixe>_output_events_total{output,outcome}`,
`<prefixe>_output_retries_total`, `<prefixe>_output_request_duration_seconds`,
`<prefixe>_output_queue_size`) rejoignent alors celles de l'outil, sous le
même préfixe (`pystashtemplate` par défaut, `rsyslogstach` pour le
connecteur rsyslog, dont les noms historiques ne changent pas).

```python
from pystashtemplate.metrics import Metrics

metrics = Metrics(prefixe="rsyslogstach")
for sortie in compiler_sorties(pipeline):
    sortie.observateur = metrics
metrics.serve(9464)
```

Voir [indexation-metriques.md](indexation-metriques.md).
