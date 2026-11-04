# PyStashTemplate

Sémantique Logstash partagée : gabarits `%{...}`, filtres (mutate, grok,
kv, dissect, date, translate, geoip...), interprétation des `.conf`
Logstash, enrichissement (GeoIP/mmdb, DNS) — bibliothèque extraite de
RsysLogstach lors du découpage de l'issue [#111](https://github.com/Mathilde-Team/RsysLogstach/issues/111)
(décision du 03/11/2026 : le connecteur rsyslog — omprog, indexer, dlq,
metrics — dépend de PyStashTemplate à version figée, comme tout autre
outil (…beat) pourrait le faire).

## Contenu

- `templates` : gabarits sprintf `%{...}`, chemins de champs, styles d'échappement (#124)
- `filters` : filtres natifs et options communes (commune/privee, reussi, ecartee...)
- `interpreter` : lecture et application des pipelines `.conf` (filter/output)
- `grok`, `fingerprint`, `multievents`, `useragent`, `xmlfilter`, `simples` : plugins
- `enrichment` : GeoIP (mmdb réels), translate, DNS (issue #22, variable `PYSTASHTEMPLATE_DNS=off`)
- `dnslookup`, `util`, `tracing`, `i18n` : utilitaires
- `dlq` : dead letter queue locale pour les échecs d'envoi permanents
  (portée de RsysLogstach, chantier « sorties » #2)
- `outputs` : sorties agnostiques (#2) — `outputs.elasticsearch_bulk`
  est le client commun de la famille « API _bulk Elasticsearch »
  (Elasticsearch, OpenSearch, VictoriaLogs, Quickwit, OpenObserve : #11) ;
  chaque produit n'est plus qu'une sous-classe de quelques lignes, dont
  `outputs.opensearch` (#12, inscrite au registre sous le plugin
  `opensearch`, banc de conformité en conteneur en CI) — voir
  [docs/sorties.md](docs/sorties.md)

## Documentation

- [docs/pipeline-logstash.md](docs/pipeline-logstash.md) : référence de la
  chaîne de filtres JSON, de l'enrichissement et des pipelines `.conf`
  (translate, grok, useragent, xml, dns, clone/split…)
- [docs/audit-fonctions-logstash.md](docs/audit-fonctions-logstash.md) :
  état fonction par fonction (fait, partiel, pas fait)
- [docs/sorties.md](docs/sorties.md) : sorties agnostiques
- [FEATURES.md](FEATURES.md) : fonctionnalités et limites connues
- [PATTERNS.md](PATTERNS.md) : conventions (traçabilité, graphe acyclique)
- Exemples : `filters.example.json`, `blocklist.example.txt`,
  `enrich-dict.example.json`, `pipeline.example.d/`

Rapatriés de RsysLogstach par #57 (découpage
[#111](https://github.com/Mathilde-Team/RsysLogstach/issues/111)).

## Utilisation

```bash
uv sync            # dépendances
uv run pytest -q --cov   # tests, couverture lignes + branches (seuil 95 %)
```

```python
import asyncio

from pystashtemplate.interpreter import PipelineInterpreter

interp = PipelineInterpreter('filter { grok { match => { "message" => "%{IP:ip}" } } }')
asyncio.run(interp.apply({"message": "1.2.3.4"}))  # asynchrone depuis #34
```

## Sorties agnostiques (#4)

Le chantier « sorties agnostiques » (décision du 03/11/2026) vise à ce
que le même `.conf` livre à OpenSearch, Elasticsearch, VictoriaLogs… en
changeant seulement le nom du plugin. Étape 1 livrée : le contrat commun.

`pystashtemplate.outputs` fournit :

- `Output` (`outputs/base.py`) : classe abstraite au cycle asynchrone
  `open()` / `send(batch)` / `flush()` / `close()`, les quatre abstraites
  (une sortie les écrit toutes), utilisable en `async with`. Chaque
  sous-classe déclare son `plugin` (nom du bloc `output{}`) et ses
  capacités `Capacites` : actions supportées (`index`, `create`,
  `update`, `delete`), `_id` de document accepté (`accepte_id`), accusé
  de réception élément par élément (`accuse_par_evenement`) ou global,
  taille maximale de requête. `verifie_action()` lève (traduit) sur une
  action non supportée.
- `REGISTRY` : nom de plugin → classe, alimenté par le décorateur
  `enregistrer` ; `null` (sortie factice qui mémorise sans rien livrer)
  y est toujours enregistrée, pour les tests.
- `compiler_sorties(plugins)` : instancie les sorties de
  `PipelineInterpreter.output_plugins` (branches `if` comprises, options
  rendues contre le document) ; un plugin inconnu est refusé à la
  compilation du pipeline par une `ValueError` traduite.
  `sorties_du_pipeline(interpreter)` enchaîne les deux.

```python
from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs import sorties_du_pipeline

interp = PipelineInterpreter("output { null {} }")
sorties = sorties_du_pipeline(interp)  # [NullOutput(...)]
```

Suit : lots et contre-pression (#6), DLQ et réessais génériques (#7),
schéma de configuration commun (#8), puis les sorties une à une
(`opensearch` #12, `elasticsearch` #13, `victorialogs` #14, `loki` #21,
`clickhouse` #23, `http` #24, `kafka` #29, `parseable` #25, `gelf` #26…).

### Transport HTTP commun (#5)

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
  `4xx` définiif : la réponse remonte telle quelle ; réseau épuisé →
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
    if reponse.statut // 100 != 2:  # DLQ, métriques… (#7)
        ...
```

## Internationalisation

Domaine gettext `pystashtemplate`, catalogues dans `lang/` (29 langues,
héritées de RsysLogstach). `scripts/i18n-update.sh` régénère `.pot`/`.po`
et compile les `.mo`. La locale se force par `$LANGUAGE` ou
`$PYSTASHTEMPLATE_LOCALEDIR` (ancien `$RSYSLOGSTACH_LOCALEDIR` toujours lu).

## Feuille de route

- Service autonome asynchrone à l'écoute d'un port (sflow notamment)
- Fonctions SNMP : un prochain dépôt livrera un client SNMP ultra rapide
  (décision #111)

## Licence

GPL-3.0-or-later. Crédits et historique complet : projet
[RsysLogstach](https://github.com/Mathilde-Team/RsysLogstach) (sources
d'origine, issues #25, #40, #73, #111...).

La bibliothèque des motifs `grok` de base (`patterns/legacy/grok-patterns`
de [logstash-patterns-core](https://github.com/logstash-plugins/logstash-patterns-core),
Copyright 2012-2015 Elasticsearch, Apache-2.0) est recopiée sans
modification dans `src/pystashtemplate/grok.py`, avec attribution ; le texte
de la licence Apache-2.0 et la notice d'origine sont dans
[`LICENSES/`](LICENSES/) (rapatriés de RsysLogstach par #57). Cette licence
est compatible avec la GPLv3.
