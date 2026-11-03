# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie OpenSearch (issue #12) : la sous-classe de référence du client _bulk.

Tout le travail est dans le client commun (#11) : la sortie OpenSearch
n'est que quelques lignes — le nom du plugin tel qu'écrit dans le
`output{}` du `.conf`, les capacités complètes (les quatre actions,
`_id` accepté, accusé de réception par élément) et le chemin `_bulk`
natif du produit (`POST /_bulk`, sans préfixe). Le connecteur
RsysLogstach y migre son `indexer` (#160) : mêmes documents indexés,
même DLQ, mêmes métriques.

## Parité avec l'indexer historique

| Option `output{}` | Équivalent `indexer` RsysLogstach |
|---|---|
| `hosts` | `hosts` (première entrée — le failover reste à l'appelant) |
| `index` | `index_pattern` (strftime, un index par jour) |
| `user` / `password` | `auth` |
| `ssl` / `cacert` | `use_ssl` / `verify_certs` + `ca_certs` |
| `ssl_certificate` / `ssl_key` | `client_cert` / `client_key` (mTLS) |
| `dlq_path` | DLQ locale (format identique, aucune perte à la migration) |

## Gabarit de pipeline d'exemple

```
output {
  opensearch {
    hosts => ["https://opensearch:9200"]
    index  => "logs-%Y.%m.%d"
    user   => "admin"
    password => "${OPENSEARCH_PASSWORD}"
    ssl    => true
    cacert => "/etc/ssl/certs/opensearch-ca.pem"
  }
}
```
"""

from __future__ import annotations

from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput


@enregistrer
class OpenSearchOutput(ElasticsearchBulkOutput):
    """Sortie OpenSearch : le client _bulk commun, sans préfixe de chemin.

    Capacités complètes du produit : `index`, `create`, `update`,
    `delete` ; `_id` accepté ; accusé de réception par élément
    (`items[]` de l'API `_bulk`).
    """

    plugin = "opensearch"
    chemin_bulk = ""
    capacites = Capacites(
        actions=frozenset({"index", "create", "update", "delete"}),
        accepte_id=True,
        accuse_par_evenement=True,
        taille_max_requete=None,
    )
