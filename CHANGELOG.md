# Changelog

Format tenu à jour d'après « Keep a Changelog » (https://keepachangelog.com/fr/1.1.0/),
le projet suit SemVer.

## [Non publié]

## [0.2.0] - 2026-11-03

### Ajouté

- Client commun « API _bulk Elasticsearch » (issue #11, chantier
  « sorties agnostiques » #2) : `pystashtemplate.outputs.elasticsearch_bulk`
  — un seul client NDJSON paramétrable par sortie (préfixe de chemin,
  actions supportées, `_id` accepté, en-têtes propres, rapport par
  élément ou statut global), sérialisation en flux, analyse `items[]`
  (4xx permanent → DLQ, 429/5xx transitoire → rendu à l'appelant),
  lotissement `batch_size` et réessais paramétrables (`reessais_max`,
  par défaut 0 pour la parité avec l'indexer de RsysLogstach).
  Registre des sorties (`outputs.enregistrer` / `sortie_par_nom`).
- `pystashtemplate.dlq` : dead letter queue locale portée de RsysLogstach
  (rotation, purge, gzip) — la DLQ fait partie du socle des sorties.
- Sortie `opensearch` (issue #12) : sous-classe de référence du client
  commun, inscrite au registre sous le plugin `opensearch` (choisie par
  le `output{}` du `.conf`) ; banc de conformité en conteneur dans la CI
  (workflow `conformite-opensearch.yml`, indexation effective,
  recherche, `_id` déterministe, rejet de mapping parti en DLQ) ;
  documentation `docs/sorties.md` avec gabarit de pipeline d'exemple.

### Évolutions

- `scripts/i18n-update.sh` : les sources suivent aussi les sous-paquets
  (`src/pystashtemplate/**/*.py`) ; `test_no_cross_imports` et
  `test_packaging` étendus en conséquence (le graphe couvre
  `outputs/`, `dlq` est un module du socle, plus du connecteur).
- `PipelineInterpreter.apply` et `apply_events` deviennent asynchrones
  (`async def`, chantier full async #1, issue #34) : sémantique
  inchangée, mais **changement d'API** — l'appelant doit désormais
  `await` (ou `asyncio.run`) le résultat. Les filtres internes restent
  synchrones pour l'instant (étapes suivantes #35–#38) ; RsysLogstach,
  épinglé à `v0.1.0`, n'est pas concerné avant sa migration (#159).
- Transport HTTP asynchrone commun des sorties (`outputs.transport`,
  issue #5) : pool aiohttp keep-alive, TLS/mTLS, authentifications,
  gzip, réessais 429/5xx avec `Retry-After`, `TransportErreur` sur
  réseau épuisé.
- Socle des sorties agnostiques (`outputs.base`, issue #4) : interface
  `Output`, capacités déclarées, registre `REGISTRY`, sortie factice
  `null`.

### Corrigé

- Sortie `_bulk` : contexte TLS explicite (autorités + paire client) —
  httpx avec `verify=<chemin>` ET `cert=` ne présentait PAS le
  certificat client (mTLS refusé par un serveur exigeant, constaté au
  banc de conformité RsysLogstach #160).

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
