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
- `outputs` : contrat commun des sorties et registre des plugins `output{}` (#4)
- `dnslookup`, `util`, `tracing`, `i18n` : utilitaires

## Utilisation

```bash
uv sync            # dépendances
uv run pytest -q --cov   # tests, couverture lignes + branches (seuil 95 %)
```

```python
from pystashtemplate.interpreter import PipelineInterpreter

interp = PipelineInterpreter('filter { grok { match => { "message" => "%{IP:ip}" } } }')
interp.apply({"message": "1.2.3.4"})
```

## Sorties agnostiques (#4)

Le chantier « sorties agnostiques » (décision du 03/11/2026) vise à ce
que le même `.conf` livre à OpenSearch, Elasticsearch, VictoriaLogs… en
changeant seulement le nom du plugin. Étape 1 livrée : le contrat commun.

`pystashtemplate.outputs` fournit :

- `Output` (`outputs/base.py`) : classe abstraite au cycle asynchrone
  `open()` / `send(lot)` / `flush()` / `close()`, utilisable en
  `async with`. Chaque sous-classe déclare son `nom_plugin` (nom du bloc
  `output{}`) et ses capacités `Capacites` : actions supportées
  (`index`, `create`, `update`, `delete`), `_id` de document accepté ou
  non, accusé de réception élément par élément (`_bulk` ES/OpenSearch)
  ou global (le lot est une unité), taille maximale de requête.
- `REGISTRY` : nom de plugin → classe ; `null` (sortie factice qui
  mémorise sans rien livrer) y est toujours enregistrée, pour les tests.
  `enregistrer()` y ajoute une sortie externe.
- `compiler_sorties(plugins)` : instancie les sorties de
  `PipelineInterpreter.output_plugins` (branches `if` comprises, options
  brutes) ; un plugin inconnu est refusé à la compilation du pipeline
  par une `ValueError` traduite. `sorties_du_pipeline(interpreter)`
  enchaîne les deux.

```python
from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs import sorties_du_pipeline

interp = PipelineInterpreter("output { null {} }")
sorties = sorties_du_pipeline(interp)  # [NullOutput({})]
```

Suit : transport HTTP asynchrone partagé (#5), lots et contre-pression
(#6), DLQ et réessais génériques (#7), schéma de configuration commun
(#8), puis les sorties une à une (`opensearch` #12, `elasticsearch`
#13, `victorialogs` #14…).

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
