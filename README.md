# PyStashTemplate

Bibliothèque Python asynchrone qui reproduit la sémantique de Logstash :
gabarits `%{...}`, filtres (mutate, grok, kv, dissect, date, translate,
geoip, dns…), interprétation des pipelines `.conf`, enrichissement
(GeoIP/mmdb, dictionnaire, DNS), puis livraison vers vingt-cinq
destinations (OpenSearch, Elasticsearch, VictoriaLogs, Loki, Kafka…), avec
DLQ, `_id` déterministe et métriques Prometheus.

Elle a été extraite de [RsysLogstach](https://github.com/Mathilde-Team/RsysLogstach) lors du découpage
[RsysLogstach#111](https://github.com/Mathilde-Team/RsysLogstach/issues/111), décision corrigée le 04/11/2026
([#110](https://github.com/Mathilde-Team/PyStashTemplate/issues/110)) : seul `omprog`, le protocole propre à rsyslog, reste dans
RsysLogstach. Tout le reste est ici, utilisable par n'importe quel outil
(connecteur rsyslog, service autonome, …beat) qui dépend de
PyStashTemplate à version figée.

Version courante : **0.3.0** (voir [CHANGELOG.md](CHANGELOG.md)).

## Architecture

```
entrée ──► filtres JSON ──► enrichissement ──► pipeline .conf ──► sorties ──► destination
(rsyslog,  (FilterPipeline)  (dictionnaire,     (filter{},        (25 plugins,
 service)                     GeoIP, DNS)        output{})         DLQ, métriques)
```

Chaque étape est facultative et asynchrone. Le connecteur rsyslog
([RsysLogstach](https://github.com/Mathilde-Team/RsysLogstach)) et le service autonome (`pystashtemplate.service`)
ne font que fournir les documents d'entrée et accuser leur réception.

| Projet | Rôle | Utilise |
|---|---|---|
| [RsysLogstach](https://github.com/Mathilde-Team/RsysLogstach) | Connecteur rsyslog (`omprog`) | filtres, enrichissement, `.conf`, `indexer`, `dlq`, `metrics`, `opensearchconf` |
| [asnmp](https://github.com/Mathilde-Team/asnmp) | Client SNMP asynchrone (décision [#47](https://github.com/Mathilde-Team/PyStashTemplate/issues/47)) | consommé directement par les intégrations |

## Installation

Les dépendants figent la version par une étiquette git :

```toml
dependencies = [
    "pystashtemplate @ git+https://github.com/Mathilde-Team/PyStashTemplate.git@v0.3.0",
]
```

```bash
pip install "pystashtemplate @ git+https://github.com/Mathilde-Team/PyStashTemplate.git@v0.3.0"
```

Prérequis : Python 3.11 ou plus récent ; une base MaxMind GeoIP2 City
(`.mmdb`) pour le filtre `geoip` et l'enrichissement GeoIP ; `gettext`
(`apt-get install gettext`) seulement pour régénérer les traductions.

## Contenu

- `templates` : gabarits sprintf `%{...}`, chemins de champs, styles d'échappement
  ([RsysLogstach#124](https://github.com/Mathilde-Team/RsysLogstach/issues/124))
- `filters` : filtres natifs et options communes (commune/privee, reussi, ecartee...)
- `interpreter` : lecture et application des pipelines `.conf` (filter/output)
- `grok`, `fingerprint`, `multievents`, `useragent`, `xmlfilter`, `simples` : plugins
- `enrichment` : GeoIP (mmdb réels), translate, DNS (désactivable par
  `PYSTASHTEMPLATE_DNS=off`)
- `dnslookup`, `util`, `tracing`, `i18n` : utilitaires
- `dlq` : dead letter queue locale pour les échecs d'envoi permanents,
  rotation et purge (chantier « sorties » [#2](https://github.com/Mathilde-Team/PyStashTemplate/issues/2))
- `indexer` : `OpenSearchIndexer`, un index par jour, `_id` par gabarit ou
  empreinte, trois issues d'envoi (`SendOutcome`) — rapatrié de RsysLogstach ([#110](https://github.com/Mathilde-Team/PyStashTemplate/issues/110))
- `opensearchconf` : options de connexion des blocs `opensearch{}` /
  `elasticsearch{}` d'un `.conf` (hôtes, TLS, mTLS, authentification) ([#110](https://github.com/Mathilde-Team/PyStashTemplate/issues/110))
- `metrics` : métriques Prometheus et endpoint `/metrics`, préfixe
  paramétrable, observateur des sorties ([#110](https://github.com/Mathilde-Team/PyStashTemplate/issues/110)) — voir
  [docs/indexation-metriques.md](docs/indexation-metriques.md)
- `outputs` : 25 sorties agnostiques inscrites au registre (voir
  [Sorties agnostiques](#sorties-agnostiques)) et
  [docs/sorties.md](docs/sorties.md)
- `service` : service autonome asynchrone à l'écoute d'un port (syslog
  TCP/UDP, HTTP, sFlow) — voir [Service autonome](#service-autonome)
- `sflow` : décodage des datagrammes sFlow v5 (RFC 3176)

## Documentation

- [docs/pipeline-logstash.md](docs/pipeline-logstash.md) : référence de la
  chaîne de filtres JSON, de l'enrichissement et des pipelines `.conf`
  (translate, grok, useragent, xml, dns, clone/split…)
- [docs/audit-fonctions-logstash.md](docs/audit-fonctions-logstash.md) :
  état fonction par fonction (fait, partiel, pas fait)
- [docs/sorties.md](docs/sorties.md) : sorties agnostiques
- [docs/indexation-metriques.md](docs/indexation-metriques.md) : indexation
  OpenSearch, `_id`, options de connexion du `.conf`, mTLS, métriques
  Prometheus et DLQ ([#110](https://github.com/Mathilde-Team/PyStashTemplate/issues/110))
- [FEATURES.md](FEATURES.md) : fonctionnalités et limites connues
- [PATTERNS.md](PATTERNS.md) : conventions (traçabilité, graphe acyclique)
- Exemples : `filters.example.json`, `blocklist.example.txt`,
  `enrich-dict.example.json`, `pipeline.example.d/`

Rapatriés de RsysLogstach par [#57](https://github.com/Mathilde-Team/PyStashTemplate/issues/57) (découpage
[RsysLogstach#111](https://github.com/Mathilde-Team/RsysLogstach/issues/111)).

## Démarrage rapide

Toute l'API de traitement est asynchrone : `FilterPipeline.apply`
([#36](https://github.com/Mathilde-Team/PyStashTemplate/issues/36)), `PipelineInterpreter.apply` / `apply_events`
([#34](https://github.com/Mathilde-Team/PyStashTemplate/issues/34)), envois des sorties.

### Chaîne de filtres JSON

Une liste d'étapes (drop, regex, kv, dissect, mutate, geoip, translate,
date, dns), appliquées en séquence ; `apply` renvoie `False` si un `drop`
écarte le document :

```python
import asyncio

from pystashtemplate.filters import FilterPipeline
from pystashtemplate.util import load_json_file

filtres = FilterPipeline(
    [
        {"type": "drop", "source": "msg", "words_file": "/etc/outil/blocklist.txt", "mode": "contains"},
        {"type": "kv", "source": "detail", "field_split": " ", "value_split": "=", "prefix": "kv_"},
    ]
)
# ou : FilterPipeline(load_json_file("filters.json")), les fichiers cités (words_file…) devant exister
doc = {"msg": "link down", "detail": "user=admin action=down"}
garde = asyncio.run(filtres.apply(doc))  # doc modifié en place : kv_user, kv_action
```

Exemples complets : `filters.example.json` et `blocklist.example.txt` ;
référence : [docs/pipeline-logstash.md](docs/pipeline-logstash.md).

### Enrichissement

```python
from pystashtemplate.enrichment import DictEnricher, GeoIPEnricher
from pystashtemplate.util import load_json_file

par_hote = DictEnricher(load_json_file("enrich-dict.example.json"), key_field="hostname", out_field="enrichment")
geo = GeoIPEnricher("/var/lib/GeoIP/GeoLite2-City.mmdb", ip_field="fromhost-ip", out_field="geoip")
for enrichisseur in (par_hote, geo):
    enrichisseur.enrich(doc)
```

### Pipelines `.conf` Logstash

```python
from pathlib import Path

from pystashtemplate.interpreter import PipelineInterpreter

source = "\n".join(p.read_text() for p in sorted(Path("pipeline.example.d").glob("*.conf")))
interp = PipelineInterpreter(source)
for evenement, actions in asyncio.run(interp.apply_events(doc)):
    for action in actions:  # une action par bloc output{} atteint, options déjà rendues
        print(action.plugin, action.options)
```

- **Plusieurs événements par document** (`clone`, `split`,
  [RsysLogstach#141](https://github.com/Mathilde-Team/RsysLogstach/issues/141)) : `apply_events` renvoie chaque
  événement avec ses propres sorties ; `apply` ne renvoie que celles du
  document d'origine (`None` s'il a été écarté).
- **Variables `${VAR}` et `${VAR:défaut}`**
  ([RsysLogstach#122](https://github.com/Mathilde-Team/RsysLogstach/issues/122)) : substituées au chargement ; une
  variable non définie et sans défaut refuse le pipeline.
- **Style d'échappement des références de champ**
  ([RsysLogstach#124](https://github.com/Mathilde-Team/RsysLogstach/issues/124)) : `set_escape_style("none" | "percent" |
  "ampersand")` (`pystashtemplate.templates`), `none` par défaut comme
  Logstash.
- **DNS** : `PYSTASHTEMPLATE_DNS=off` (ancien nom `RSYSLOGSTACH_DNS`
  toujours lu) coupe les requêtes réseau du filtre `dns`.
- Un `.conf` peut être découpé en plusieurs fichiers lus par ordre de nom,
  comme le fait Logstash : voir `pipeline.example.d/`. État de chaque
  fonction Logstash : [docs/audit-fonctions-logstash.md](docs/audit-fonctions-logstash.md).

## Sorties agnostiques

Le même `.conf` livre à OpenSearch, Elasticsearch, VictoriaLogs… en
changeant seulement le nom du plugin du bloc `output{}` (épopée
[#3](https://github.com/Mathilde-Team/PyStashTemplate/issues/3), décision du 03/11/2026). Toutes les sorties ci-dessous sont
livrées et inscrites au registre :

| Famille | Plugins |
|---|---|
| NDJSON `_bulk` compatible Elasticsearch (client commun `elasticsearch_bulk`, [#11](https://github.com/Mathilde-Team/PyStashTemplate/issues/11)) | `elasticsearch`, `opensearch`, `victorialogs`, `quickwit`, `openobserve`, `zincsearch`, `manticore`, `siglens`, `greptimedb` |
| API d'ingestion propres | `clickhouse`, `doris`, `loki`, `parseable` |
| Protocoles de collecte | `otlp`, `splunk_hec`, `gelf`, `syslog`, `fluent_forward`, `http` |
| Courtiers de messages | `kafka`, `redis`, `rabbitmq`, `nats` |
| Locales | `file`, `stdout` (et `null`, sortie factice des tests) |

La matrice de compatibilité (actions, `_id`, accusé par élément, DLQ,
réessais, banc en conteneur) est générée dans
[docs/sorties.md](docs/sorties.md) par `scripts/matrice_sorties.py`.

Socle commun (`pystashtemplate.outputs`) :

- `Output` (`outputs/base.py`) : classe abstraite au cycle asynchrone
  `open()` / `send(batch)` / `flush()` / `close()`, utilisable en
  `async with` ; chaque sous-classe déclare son `plugin` et ses
  capacités `Capacites` (actions, `_id`, accusé par élément, taille
  maximale de requête) ([#4](https://github.com/Mathilde-Team/PyStashTemplate/issues/4)) ;
- `REGISTRY` : nom de plugin → classe, alimenté par le décorateur
  `enregistrer` ;
- `compiler_sorties(interpreter)` : instancie une sortie par bloc
  `output{}` du pipeline ; un plugin inconnu est refusé dès la
  compilation (`SortieInconnueError`), avant toute connexion ;
- lots, contre-pression et arrêt propre ([#6](https://github.com/Mathilde-Team/PyStashTemplate/issues/6)), DLQ et réessais
  génériques ([#7](https://github.com/Mathilde-Team/PyStashTemplate/issues/7)), schéma de configuration commun ([#8](https://github.com/Mathilde-Team/PyStashTemplate/issues/8)),
  bancs de conformité en conteneurs ([#9](https://github.com/Mathilde-Team/PyStashTemplate/issues/9)), métriques des sorties
  ([#10](https://github.com/Mathilde-Team/PyStashTemplate/issues/10)).

```python
from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs import compiler_sorties

interp = PipelineInterpreter('output { opensearch { hosts => ["https://os:9200"] } }')
sorties = compiler_sorties(interp)  # [OpenSearchOutput(...)]
```

### Transport HTTP commun ([#5](https://github.com/Mathilde-Team/PyStashTemplate/issues/5))

`pystashtemplate.outputs.transport.TransportHTTP` est le transport
unique des sorties HTTP (aiohttp, dépendance directe du paquet) :

- pool de connexions keep-alive (`connexions_max`, `keepalive`), délais
  configurables (`timeout_total`, `timeout_connexion`) ;
- TLS (`ca_certificats`, option `ssl_certificate_authorities`) et mTLS
  (`certificat_client`/`cle_client`), `verification_tls=False` pour
  `ssl_verification_mode => none` ;
- authentifications `AuthBasique`, `AuthBearer`, `AuthApiKey`
  (Elasticsearch) et en-têtes libres — jamais journalisées ;
- compression gzip de la requête (`compression=True`) ;
- réessais (backoff exponentiel avec jitter, plafonnés par `delai_max`)
  sur délai dépassé, erreur de connexion, `429` (avec `Retry-After`
  respecté, en secondes ou date HTTP) et `5xx` ; aucun réessai sur un
  `4xx` définitif : la réponse remonte telle quelle ; réseau épuisé →
  `TransportErreur` (échec transitoire, à rejouer).

```python
from pystashtemplate.outputs.transport import AuthBearer, TransportHTTP

async with TransportHTTP(essais_max=5, delai_initial=0.5) as transport:
    reponse = await transport.envoyer(
        "https://os:9200/_bulk",
        corps=corps_ndjson,
        authentification=AuthBearer(jeton),
        compression=True,
    )
    if reponse.statut // 100 != 2:  # DLQ, métriques…
        ...
```

## Indexation, DLQ, métriques et connexion

Modules rapatriés de RsysLogstach par [#110](https://github.com/Mathilde-Team/PyStashTemplate/issues/110), référence complète
dans [docs/indexation-metriques.md](docs/indexation-metriques.md) :

```python
from pystashtemplate.dlq import LocalDeadLetterQueue, parse_size
from pystashtemplate.indexer import OpenSearchIndexer, SendOutcome
from pystashtemplate.metrics import Metrics

metrics = Metrics(dlq_path="/var/lib/outil/dlq.jsonl")  # préfixe "pystashtemplate" par défaut
indexer = OpenSearchIndexer(
    hosts=[{"host": "os1", "port": 9200}],
    auth=None,
    index_pattern="logs-%Y.%m.%d",
    use_ssl=True,
    verify_certs=True,
    document_id_fingerprint_fields=["host", "seq"],
    dlq=LocalDeadLetterQueue("/var/lib/outil/dlq.jsonl", max_bytes=parse_size("100M")),
)
issue = await indexer.send(doc)
metrics.outcome(issue.value)  # indexed, dead_lettered ou retryable
```

| Sujet | Ce que fournit la bibliothèque |
|---|---|
| Issue d'un envoi | `SendOutcome` : `INDEXED` (accusé), `RETRYABLE` (à renvoyer par l'appelant), `DEAD_LETTERED` (écrit en DLQ, ne jamais renvoyer) |
| `_id` déterministe | gabarit (`document_id_template="%{[event_id]}"`) ou empreinte (`sha256`, `sha1`, `md5`, `murmur3`, HMAC avec une clé) ; l'empreinte est prioritaire ; sans l'un ni l'autre, `_id` aléatoire |
| DLQ | JSONL locale, rotation par taille ou par âge, gzip, purge par nombre et par âge |
| Métriques | registre Prometheus propre, endpoint `/metrics` (`serve`), préfixe paramétrable, observateur des sorties |
| Connexion | options `hosts`, `ssl`, `user`/`password`, `cacert`, `tls_certificate`/`tls_key` (mTLS) lues dans les blocs `opensearch{}` d'un `.conf` (`opensearchconf`) |
| Secrets | `opensearch_auth_from_env()` lit `OPENSEARCH_USER` / `OPENSEARCH_PASSWORD` : jamais d'identifiant en argument de ligne de commande |

Seul le premier nœud de `hosts` est utilisé : le failover revient à
l'appelant, au moyen de `RETRYABLE`.

## Service autonome

`pystashtemplate.service` fait tourner le pipeline en service asynchrone,
à l'écoute d'un ou plusieurs ports — sans rsyslog au milieu
([#39](https://github.com/Mathilde-Team/PyStashTemplate/issues/39), sFlow [#97](https://github.com/Mathilde-Team/PyStashTemplate/issues/97), décision
[RsysLogstach#111](https://github.com/Mathilde-Team/RsysLogstach/issues/111)) :

```python
from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.service import EntreeHttp, EntreeSflow, EntreeSyslogTcp, Service

service = Service(PipelineInterpreter("filter { } output { null {} }"))
await service.demarrer([EntreeSyslogTcp("0.0.0.0", 514), EntreeHttp("0.0.0.0", 8080), EntreeSflow("0.0.0.0", 6343)])
await service.arreter()  # arrêt propre : écoutes fermées, file vidée, sorties poussées
```

- **entrées** : `EntreeSyslogTcp` (cadrage RFC 6587 octet-counting ou
  LF), `EntreeSyslogUdp`, `EntreeHttp` (`POST` JSON ou NDJSON),
  `EntreeSflow` (UDP, sFlow v5 — RFC 3176) ;
- **analyse** : syslog RFC 5424/3164 en champs `message`, `host`,
  `facility`, `severity`, `appname`… ; sFlow : un document par
  échantillon de flux (MAC, IP, protocole, ports) ou de compteurs
  d'interface ;
- **contre-pression** : file bornée (`file_max`) entre réception et
  sorties — pleine, le récepteur TCP cesse de lire (la fenêtre TCP se
  referme) ; en UDP les datagrammes sont perdus ;
- **arrêt propre** : `arreter()` ferme les écoutes, vide la file,
  pousse les tampons et ferme les sorties.

## Internationalisation

Les journaux de niveau `info` et au-dessus passent par gettext (domaine
`pystashtemplate`, 29 langues héritées de RsysLogstach). Le français est la
langue source ; la langue suit l'environnement (`LANGUAGE`, `LC_ALL`,
`LC_MESSAGES`, `LANG`). Les traces `debug` restent en français.

| Fichier | Rôle |
|---|---|
| `lang/LINGUAS` | seule liste des langues |
| `lang/messages.pot` | modèle extrait des sources (`xgettext`) |
| `lang/<locale>.po` | une traduction par langue |
| `lang/locale/<locale>/LC_MESSAGES/pystashtemplate.mo` | catalogues compilés, suivis par git et installés par la roue |

Catalogues cherchés : `$PYSTASHTEMPLATE_LOCALEDIR` (ancien
`$RSYSLOGSTACH_LOCALEDIR` toujours lu), puis `lang/locale/` du dépôt,
`<prefix>/share/locale` et `/usr/share/locale`.

- **Modifier ou ajouter une chaîne** : l'entourer de `_()`
  (`from pystashtemplate.i18n import _`) en gardant les champs `{...}`,
  lancer `scripts/i18n-update.sh` et committer `lang/`. `msgmerge`
  conserve les traductions existantes ; les nouvelles chaînes arrivent
  vides, sans traduction automatique. `en_US` doit être rempli.
- **Traduire** : remplir les `msgstr` de `lang/<locale>.po` en gardant les
  champs `{...}`, retirer la marque `fuzzy` après relecture, relancer le
  script (il recompile le `.mo`).
- **Ajouter une langue** : ajouter son code à `lang/LINGUAS`, lancer le
  script (il crée `lang/<code>.po`), traduire, relancer.
- **Contrôles** : `scripts/i18n-update.sh --check` (job « i18n » de la CI),
  `scripts/i18n_report.py` (taux par langue), `tests/test_i18n.py`.

## Tests et qualité

```bash
uv sync --extra dev
uv run pytest -q --cov        # couverture lignes et branches, seuil bloquant 95 % (pyproject.toml)
uv run ruff check . && uv run ruff format --check .
scripts/i18n-update.sh --check
uv run python scripts/audit_debug_sorties.py --total   # fonctions sans debug() en entrée/sortie : 0
uv run python scripts/matrice_sorties.py --check       # matrice de docs/sorties.md à jour
```

Les bancs de conformité (`.github/workflows/conformite-<plugin>.yml`)
lancent chaque destination réelle en conteneur. Conventions (traçabilité
`@traced`/`@atraced`, graphe de modules acyclique, opérations disque hors
boucle asyncio) : [PATTERNS.md](PATTERNS.md).

## SNMP

Aucune dépendance SNMP dans la bibliothèque : le client SNMP asynchrone
est le dépôt [Mathilde-Team/asnmp](https://github.com/Mathilde-Team/asnmp),
consommé directement par les intégrations (décision [#47](https://github.com/Mathilde-Team/PyStashTemplate/issues/47)).

## Licence

GPL-3.0-or-later. Crédits et historique complet : projet
[RsysLogstach](https://github.com/Mathilde-Team/RsysLogstach) (sources
d'origine, issues [#25](https://github.com/Mathilde-Team/RsysLogstach/issues/25), [#40](https://github.com/Mathilde-Team/RsysLogstach/issues/40), [#73](https://github.com/Mathilde-Team/RsysLogstach/issues/73),
[#111](https://github.com/Mathilde-Team/RsysLogstach/issues/111)...).

La bibliothèque des motifs `grok` de base (`patterns/legacy/grok-patterns`
de [logstash-patterns-core](https://github.com/logstash-plugins/logstash-patterns-core),
Copyright 2012-2015 Elasticsearch, Apache-2.0) est recopiée sans
modification dans `src/pystashtemplate/grok.py`, avec attribution ; le texte
de la licence Apache-2.0 et la notice d'origine sont dans
[`LICENSES/`](LICENSES/) (rapatriés de RsysLogstach par [#57](https://github.com/Mathilde-Team/PyStashTemplate/issues/57)). Cette licence
est compatible avec la GPLv3.

Dépendances directes et licences, toutes compatibles avec la GPLv3 :

| Dépendance | Licence |
|---|---|
| logstash-pipeline-parser 0.0.3, loguru, pyyaml, mmh3, redis | MIT |
| geoip2, maxminddb, ua-parser, ua-parser-builtins, aiokafka, aio-pika, nats-py, msgpack | Apache-2.0 |
| aiohttp | Apache-2.0 AND MIT |
| prometheus-client | Apache-2.0 AND BSD-2-Clause |
| httpx, lxml | BSD-3-Clause |
| dnspython | ISC |
