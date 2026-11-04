# Indexation, options de connexion, métriques et DLQ

Ces modules sont rapatriés de [RsysLogstach](https://github.com/Mathilde-Team/RsysLogstach) avec la correction du découpage
[RsysLogstach#111](https://github.com/Mathilde-Team/RsysLogstach/issues/111), issue
[#110](https://github.com/Mathilde-Team/PyStashTemplate/issues/110). Ils ne dépendent pas de rsyslog : tout outil construit sur
PyStashTemplate peut s'en servir, que ce soit le connecteur rsyslog, le service autonome (#39) ou un futur …beat. Seul
`omprog` (le protocole rsyslog) reste dans RsysLogstach.

| Module | Rôle | Origine |
|---|---|---|
| `pystashtemplate.indexer` | `OpenSearchIndexer` : un index par jour, `_id` déterministe, trois issues d'envoi | RsysLogstach `indexer.py` (#33, #126, #160) |
| `pystashtemplate.opensearchconf` | Options de connexion lues dans les blocs `opensearch{}` / `elasticsearch{}` d'un `.conf` | RsysLogstach `opensearchconf.py` (#125) |
| `pystashtemplate.metrics` | `Metrics` : métriques Prometheus, endpoint `/metrics`, observateur des sorties | RsysLogstach `metrics.py` (#35) |
| `pystashtemplate.dlq` | `LocalDeadLetterQueue` : DLQ JSONL locale, rotation et purge | déjà dans la bibliothèque (chantier sorties #2) |

## Indexation OpenSearch (`indexer`)

```python
import asyncio

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.indexer import OpenSearchIndexer, SendOutcome

indexer = OpenSearchIndexer(
    hosts=[{"host": "os1", "port": 9200}],
    auth=("admin", "secret"),  # ou None
    index_pattern="logs-%Y.%m.%d",  # motif strftime, un index par jour (UTC)
    use_ssl=True,
    verify_certs=True,
    document_id_template="%{[event_id]}",
    dlq=LocalDeadLetterQueue("/var/lib/outil/dlq.jsonl"),
)
issue = asyncio.run(indexer.send({"event_id": "e1", "message": "..."}))
```

`OpenSearchIndexer` délègue l'envoi à la sortie `outputs.opensearch` (client `_bulk` commun, #11/#12), avec un document par
lot et sans retente interne. L'accusé est donc toujours réel, et c'est l'appelant qui décide du renvoi.

| `SendOutcome` | Signification | Ce que fait l'appelant |
|---|---|---|
| `INDEXED` | Document accepté | Accuser réception |
| `RETRYABLE` | Échec transitoire, ou échec permanent sans DLQ | Retenir le message et le renvoyer plus tard |
| `DEAD_LETTERED` | Échec permanent (400/404, action invalide) écrit dans la DLQ | Accuser réception, ne jamais renvoyer |

Points à connaître :

- `hosts` peut porter plusieurs nœuds, mais seul le premier est utilisé et un avertissement est journalisé. Le failover est à
  la charge de l'appelant, au moyen de `RETRYABLE`.
- `send(doc, index=..., document_id=..., action=..., pipeline=..., routing=..., doc_as_upsert=...)` : ces arguments
  surchargent l'index et l'`_id` pour un seul document. `action` prend l'une des valeurs `ACTIONS` (`index`, `create`,
  `update` ou `delete`) ; `update` et `delete` sans `_id` sont un échec permanent (`ActionInvalideError`).
- `health()` interroge `GET /` (version du cluster) ; `close()` ferme le client HTTP.
- `ca_certs`, `client_cert`, `client_key` : PEM de l'autorité, certificat client et clé (mTLS, voir plus bas).

### Identifiant de document (`_id`)

| Mécanisme | Paramètre | Cas d'usage |
|---|---|---|
| Gabarit | `document_id_template="%{[event_id]}"` | Le document porte un identifiant naturel |
| Empreinte | `document_id_fingerprint_fields=["host", "seq"]` (`[]` : le document entier) | Pas d'identifiant naturel : hash du contenu (`sha256` par défaut, `sha1`, `md5`, `murmur3`) |
| HMAC | `document_id_fingerprint_key="..."` | L'empreinte dépend d'une clé secrète (pas avec `murmur3`) |

Les deux mécanismes peuvent être configurés ensemble ; dans ce cas l'empreinte est prioritaire, et ils ne sont jamais combinés
dans un même `_id`. Si aucun n'est configuré, ou si le rendu est vide, OpenSearch génère un `_id` aléatoire.

## Options de connexion du `.conf` (`opensearchconf`)

`collect_connection_options(plugins)` lit les options de connexion de
[logstash-output-opensearch](https://github.com/opensearch-project/logstash-output-opensearch) dans les blocs
`opensearch{}` et `elasticsearch{}` (`INDEX_PLUGINS`), y compris dans les branches `if` :

```
output {
  opensearch {
    hosts => ["os1:9200"]
    ssl => true
    ssl_certificate_verification => true
    user => "admin"
    password => "secret"
    cacert => "/etc/outil/ca.pem"
  }
}
```

| Option | Effet |
|---|---|
| `hosts` | URL ou liste. Sans schéma : `https` si `ssl => true`, `http` sinon ; port 9200 par défaut (`conf_host_urls`) |
| `ssl` | Schéma des URLs qui n'en ont pas ; erreur s'il contredit une URL `http://` ou `https://` |
| `ssl_certificate_verification => false` | Certificats non vérifiés |
| `user` / `password` | Authentification basique, les deux ensemble (`conf_auth`) |
| `cacert`, `tls_certificate`, `tls_key` | Fichiers PEM (mTLS) |

Si deux blocs donnent des valeurs différentes pour la même option, la configuration est refusée : un outil n'a qu'une
connexion OpenSearch. Une valeur `${VAR}` non remplacée est aussi refusée. `apply_connection_options(args, options)` complète
un `argparse.Namespace` (attributs `opensearch_host`, `no_verify_certs`, `cacert`, `tls_certificate`, `tls_key`) sans écraser
ce que la ligne de commande a déjà fixé. Un mot de passe en clair dans le `.conf` est lisible par quiconque lit le fichier ;
mieux vaut le passer par l'environnement.

### Certificat client (mTLS)

| Logstash | `OpenSearchIndexer` | Rôle |
|---|---|---|
| `cacert` | `ca_certs` | Autorité qui a signé le certificat du cluster |
| `tls_certificate` | `client_cert` | Certificat client présenté au cluster |
| `tls_key` | `client_key` | Clé privée PEM non chiffrée |
| `keystore` / `truststore` (JKS, PKCS#12) | non pris en charge | Convertir en PEM (`openssl pkcs12 -in client.p12 -nodes`) |

Côté OpenSearch (plugin Security), il faut `plugins.security.ssl.http.clientauth_mode: OPTIONAL` (ou `REQUIRE`) et un domaine
`clientcert_auth_domain` actif ; le CN du certificat devient le nom d'utilisateur. La clé doit rester en `chmod 600`.

## Métriques Prometheus (`metrics`)

`Metrics(dlq_path=None, prefixe="pystashtemplate")` crée ses métriques sur un registre propre (jamais le registre global), si
bien que plusieurs instances ne se gênent pas. RsysLogstach passe `prefixe="rsyslogstach"` pour garder ses noms historiques.
`serve(port, addr="127.0.0.1")` démarre l'endpoint `/metrics` ; `render()` renvoie l'exposition texte.

| Métrique | Type | Alimentée par |
|---|---|---|
| `<prefixe>_messages_received_total` | compteur | `message_received(ligne)` |
| `<prefixe>_messages_total{outcome}` | compteur | `outcome(nom)` parmi `OUTCOMES` : `indexed`, `dropped`, `dead_lettered`, `retryable` |
| `<prefixe>_errors_total{cause}` | compteur | `error(cause)` parmi `ERROR_CAUSES` : `invalid_json`, `not_an_object`, `exception` |
| `<prefixe>_index_latency_seconds` | histogramme | `index_latency.observe(secondes)` après chaque envoi, réussi ou non |
| `<prefixe>_message_size_bytes` | histogramme | taille du message reçu (octets UTF-8) |
| `<prefixe>_dlq_size_bytes` | jauge | taille du fichier `dlq_path`, lue à chaque collecte |
| `<prefixe>_retry_pending` | jauge | échecs transitoires consécutifs, remis à zéro par `indexed` |
| `<prefixe>_output_events_total{output,outcome}` | compteur | observateur des sorties (#10) |
| `<prefixe>_output_retries_total{output}` | compteur | observateur des sorties |
| `<prefixe>_output_request_duration_seconds{output}` | histogramme | observateur des sorties |
| `<prefixe>_output_queue_size{output}` | jauge | observateur des sorties |

`Metrics` implémente `ObservateurSortie` (`outputs/metriques.py`) : on le passe comme `observateur` d'une sortie, et les
métriques de cette sortie rejoignent celles de l'outil. Les noms sont les mêmes que ceux de `CompteursSortie.exposition`
(préfixe `<prefixe>_output`).

## DLQ locale (`dlq`)

`LocalDeadLetterQueue(path, ...)` ajoute une ligne JSON par échec permanent : `@dlq_timestamp` (ISO-8601 UTC), `@dlq_index`, `@dlq_error` et `document`. Rotation et
purge sont désactivées par défaut :

| Paramètre | Aide de lecture | Effet |
|---|---|---|
| `max_bytes` | `parse_size("100M")` | Rotation avant qu'une écriture dépasse cette taille |
| `max_age` | `parse_duration("7d")` | Rotation quand la plus ancienne entrée dépasse cet âge |
| `backups` | — | Nombre maximal de fichiers rotatés conservés |
| `retention` | `parse_duration("30d")` | Âge maximal d'un fichier rotaté |
| `compress` | — | Fichiers rotatés en gzip (défaut) |

Les fichiers rotatés s'appellent `dlq.jsonl.20261010T120000Z.gz` (puis `-1`, `-2`… dans la même seconde) et se relisent avec
`zcat`.
