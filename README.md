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
