# Sorties agnostiques

Le nom du plugin d'un bloc `output{}` du `.conf` désigne une sortie
inscrite au registre (`pystashtemplate.outputs.REGISTRY`). La
compilation du pipeline (`compiler_sorties`) instancie chaque sortie ;
une sortie inconnue échoue dès la compilation, avant toute connexion.

## Sorties disponibles

| Plugin | Produit | Actions | `_id` | Accusé par élément |
|---|---|---|---|---|
| `opensearch` | OpenSearch | toutes | oui | oui (`items[]`) |
| `file` | fichier local (NDJSON) | `index`, `create` | non | oui |
| `stdout` | sortie standard | `index`, `create` | non | oui |
| `null` | (puits, tests) | toutes | oui | oui |

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

Les options rendues par document (`index` sprintf, `document_id`)
passent par l'API `envoyer()` du client — c'est celle qu'utilise le
connecteur (#160).

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
