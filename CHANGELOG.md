# Changelog

Format tenu à jour d'après « Keep a Changelog » (https://keepachangelog.com/fr/1.1.0/),
le projet suit SemVer.

## [0.1.0] - 2026-11-03

### Ajouté

- Premier lot du découpage de RsysLogstach (issue #111) : la bibliothèque
  `pystashtemplate` (gabarits, filtres, interprétation des `.conf`
  Logstash, enrichissement GeoIP/translate/DNS, plugins grok, kv, dissect,
  fingerprint, useragent, xml, clone/split...) est extraite dans ce dépôt.
  RsysLogstach (le connecteur rsyslog : omprog, indexer, dlq, metrics)
  en dépendra à version figée. Catalogues gettext (29 langues) hérités,
  domaine `pystashtemplate` ; variables d'environnement
  `PYSTASHTEMPLATE_LOCALEDIR` et `PYSTASHTEMPLATE_DNS` (les anciens noms
  `RSYSLOGSTACH_*` restent lus en repli).

### Évolutions

- Agencement src/ : les modules vivent dans `src/pystashtemplate/` et
  s'importent par `from pystashtemplate.X import ...` (une bibliothèque
  installable ne peut plus exposer `filters` ou `i18n` au niveau racine
  comme le faisait RsysLogstach).

### Corrigé

- `git ls-files 'src/*.py'` → `src/pystashtemplate/*.py` (i18n-update) ;
  message d'aide mis à jour en conséquence.
