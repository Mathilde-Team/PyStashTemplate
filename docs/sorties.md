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
| `elasticsearch` | toutes | oui | oui | — | oui | oui | oui |
| `file` | `index`, `create` | non | oui | — | oui | non | non |
| `null` | toutes | oui | oui | — | non | non | non |
| `opensearch` | toutes | oui | oui | — | oui | oui | oui |
| `stdout` | `index`, `create` | non | oui | — | non | non | non |
| `victorialogs` | `index`, `create` | non | non | — | oui | oui | oui |
<!-- matrice-sorties:fin -->

La famille « API _bulk Elasticsearch » (issue #11) partage le client
`pystashtemplate.outputs.elasticsearch_bulk` : chaque produit n'est
plus qu'une sous-classe de quelques lignes (valeurs par défaut et
capacités). Les autres familles (Loki, ClickHouse, HEC, OTLP…) sont
des sous-issues du chantier #2.

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
