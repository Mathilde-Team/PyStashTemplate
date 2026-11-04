# PyStashTemplate

Sémantique Logstash partagée : gabarits `%{...}`, filtres (mutate, grok,
kv, dissect, date, translate, geoip...), interprétation des `.conf`
Logstash, enrichissement (GeoIP/mmdb, DNS) — bibliothèque extraite de
RsysLogstach lors du découpage de l'issue [RsysLogstach#111](https://github.com/Mathilde-Team/RsysLogstach/issues/111)
(décision corrigée le 04/11/2026, issue
[#110](https://github.com/Mathilde-Team/PyStashTemplate/issues/110) : seul
`omprog`, propre à rsyslog, reste dans RsysLogstach ; l'indexation, la DLQ,
les métriques et les options de connexion OpenSearch sont ici, utilisables
par tout outil — connecteur rsyslog, service autonome, …beat — qui dépend
de PyStashTemplate à version figée).

Version courante : **0.3.0** (voir [CHANGELOG.md](CHANGELOG.md)).

## Installation

Les dépendants figent la version par une étiquette git :

```toml
dependencies = [
    "pystashtemplate @ git+https://github.com/Mathilde-Team/PyStashTemplate.git@v0.3.0",
]
```

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

## Utilisation

```bash
uv sync            # dépendances
uv run pytest -q --cov   # tests, couverture lignes + branches (seuil 95 %)
```

```python
import asyncio

from pystashtemplate.interpreter import PipelineInterpreter

interp = PipelineInterpreter('filter { grok { match => { "message" => "%{IP:ip}" } } }')
asyncio.run(interp.apply({"message": "1.2.3.4"}))  # API asynchrone
```

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

Domaine gettext `pystashtemplate`, catalogues dans `lang/` (29 langues,
héritées de RsysLogstach). `scripts/i18n-update.sh` régénère `.pot`/`.po`
et compile les `.mo`. La locale se force par `$LANGUAGE` ou
`$PYSTASHTEMPLATE_LOCALEDIR` (ancien `$RSYSLOGSTACH_LOCALEDIR` toujours lu).

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
