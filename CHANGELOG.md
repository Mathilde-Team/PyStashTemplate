# Changelog

Format tenu à jour d'après « Keep a Changelog » (https://keepachangelog.com/fr/1.1.0/),
le projet suit SemVer.

## [Non publié]

### Corrections

- Client _bulk, produits à statut HTTP global seul
  (`accuse_par_evenement=False`) : un 5xx ou 429 aux réessais épuisés, ou
  un statut inattendu (401, 403, redirection…), était compté comme
  indexé — le lot est désormais rendu à l'appelant (#14).

### Évolutions

- Sortie `zincsearch` (issue #18) : API `_bulk` native (`/api/_bulk`),
  actions `index`/`create`/`delete` (`update` refusée : pas de mise à
  jour partielle), `_id` transmis, statut HTTP global (500 d'arrêt de
  lot retenté), `health()` sur `/version`. Banc de conformité contre
  ZincSearch 0.4.10 en conteneur.

- Sortie `manticore` (issue #17) : `/_bulk` compatible Elasticsearch,
  accusé par élément ; `create` en doublon (HTTP 409 + élément 400 avant
  29.9.0, élément 409 depuis) compté indexé et non envoyé en DLQ ; les
  `create` annulés par Manticore avant un doublon sont rendus à
  l'appelant. Banc de conformité contre Manticore 29.9.0 en conteneur.

- Sortie `quickwit` (issue #15) : sous-classe du client _bulk commun
  (`/api/v1/_elastic/_bulk`), action `create` seule (défaut, toute autre
  refusée dès la compilation), `_id` ignoré, accusé par élément (relu sous
  l'action envoyée : Quickwit répond sous la clé `index`), requête
  ≤ 10 Mio, option `refresh` (délai porté à 30 s), santé `/api/v1/version`. Banc de
  conformité contre Quickwit 0.9.3 en conteneur
  (`conformite-quickwit.yml`) et fixture du banc commun.

- Métriques des sorties (issue #10) : protocole `ObservateurSortie`
  porté par chaque sortie (nul par défaut), prévenu des documents par
  issue (`indexed`, `dead_lettered`, `retryable`, `rejected`), des
  retentes, de la latence des requêtes et de la taille du tampon ;
  branché dans le client HTTP commun et la sortie `file`.
  `CompteursSortie` : compteurs en mémoire et exposition texte
  Prometheus, sans dépendance. Vérifiées pour chaque sortie du banc
  commun de conformité. Documentées dans `docs/sorties.md` (adaptateur
  RsysLogstach à noms inchangés).

- Banc commun de conformité des sorties (issue #9) : une même suite
  (lot simple, erreur partielle, 429, coupure réseau, arrêt propre)
  rejouée sans réseau contre chaque sortie HTTP par un faux serveur
  générique (`tests/test_conformite_sorties.py`) ; une nouvelle sortie
  s'y ajoute par une seule fixture, et un test exige une fixture pour
  toute sortie HTTP du registre. Matrice de compatibilité (système ×
  fonctionnalité) générée depuis le registre dans `docs/sorties.md`
  (`scripts/matrice_sorties.py`, vérifiée à jour par les tests).

- Sortie `victorialogs` (issue #14) : modes `bulk`
  (`/insert/elasticsearch/_bulk`) et `jsonline` (`/insert/jsonline`),
  `stream_fields`, `msg_field` (`message`) et `time_field`
  (`@timestamp`) en paramètres d'ingestion, `health()` sur `/health`.
  Banc de conformité contre VictoriaLogs v1.53.0
  (`conformite-victorialogs.yml`). Documentée dans `docs/sorties.md`.

- Sortie `elasticsearch` (issue #13) : sous-classe du client _bulk
  commun, authentification par clé d'API (`api_key`, forme encodée ou
  `id:api_key`) ou jeton porteur (`token`), exclusives entre elles et
  avec `user`/`password` ; data streams (`data_stream`, action `create`
  imposée, nommage `type-dataset-namespace` comme Logstash). Banc de
  conformité contre Elasticsearch 9.5.5 en conteneur
  (`conformite-elasticsearch.yml`). Documentée dans `docs/sorties.md`.

- DLQ générique des sorties (issue #7) : chaque entrée porte le nom de la
  sortie (`@dlq_output`, option `id` sinon nom du plugin) et la raison
  (`@dlq_reason` : `permanent` ou `transitoire_epuise`), champs ajoutés au
  format historique de RsysLogstach (relu sans conversion) ; option
  `dlq_transitoires` de la famille _bulk (faux par défaut, parité) pour
  envoyer en DLQ un lot transitoire aux réessais épuisés ;
  `dlq.read_entries()` (JSONL et `.gz`) et rejeu
  `outputs.rejeu.rejouer_dlq()` vers n'importe quelle sortie, les refus
  conservés dans le fichier. Documenté dans `docs/sorties.md`.
- Schéma de configuration commun des sorties (issue #8) :
  `pystashtemplate.outputs.schema` déclare une seule fois les options
  communes à toutes les sorties (`hosts`, authentification, `ssl_*`,
  `compression`, `batch_size`, `batch_bytes`, `flush_interval`,
  `max_concurrency`, `retry_max`, `retry_backoff`, `dlq_path`) avec
  types, bornes et défauts documentés ; chaque sortie ajoute ses
  options propres (`OPTIONS_PROPRES` : `index`/`action`… pour la
  famille `_bulk`, `path`/`codec` pour `file`, `codec` pour `stdout`,
  `refresh` pour `quickwit` — migrée au schéma commun).
  Validation **à la compilation** du pipeline (via `Output.__init__`) :
  option inconnue (faute de frappe), type inattendu, valeur hors
  bornes ou hors énumération, option requise absente — échec franc,
  message traduit nommant le plugin, avant toute connexion. Noms
  d'options alignés sur les plugins Logstash correspondants ;
  documentation : tableau des options communes dans
  `docs/sorties.md`.

- Sorties locales `file` et `stdout` (issue #32, chantier sorties #2) :
  plugins `output{}` sans client — `file` écrit une ligne JSON par
  document dans le fichier rendu par le gabarit `path` (rotation par
  `%{+YYYY-MM-dd}` contre `@timestamp`, le fichier précédent fermé à la
  rotation, DLQ sur échec d'écriture via `dlq_path`) ; `stdout` écrit
  une ligne par document sur la sortie standard (flux injectable).
  Codecs partagés `outputs/codecs.py` : `json_lines` (NDJSON) et
  `rubydebug` (rendu « inspect Ruby » multi-lignes, simplifié) —
  documentés dans `docs/sorties.md`.

- Documentation et exemples de la bibliothèque rapatriés de RsysLogstach
  (issue #57, suite du découpage RsysLogstach#111) : `PATTERNS.md` et
  `FEATURES.md` (cités par les tests mais absents), référence des filtres et
  pipelines `docs/pipeline-logstash.md`, audit
  `docs/audit-fonctions-logstash.md`, exemples `filters.example.json`,
  `blocklist.example.txt`, `enrich-dict.example.json` et
  `pipeline.example.d/` (cités par le code de `filters`, `interpreter` et
  `enrichment`) ; liens depuis le README.
- `LICENSES/` (texte Apache-2.0 et notice de logstash-patterns-core) : les
  motifs grok recopiés dans `grok.py` y renvoient, le dossier était resté
  dans RsysLogstach (#57).
- `atraced` (issue #37, chantier full async #1) : équivalent asynchrone de
  `@traced` dans `tracing.py` — la sortie est journalisée quand la
  coroutine se termine réellement (autour du `await`), et non dès sa
  création. Toutes les fonctions `async def` de `src/` sont décorées
  (remplacement de `@traced`, ajout sur les non décorées) ;
  `audit_debug_sorties` reconnaît `@atraced` comme trace structurelle.
- `FilterPipeline.apply` devient asynchrone (issue #36, chantier full async #1) :
  les steps réseau sont attendus — nouveau step `dns` (mêmes options que le
  plugin `dns` du `.conf`), sans réseau pour les filtres CPU (dissect,
  regex, kv, mutate, geoip, translate, date, drop), ordre de la chaîne et
  options communes inchangés. Changement d'API : `await` requis par les
  appelants directs de `FilterPipeline.apply` (le pont `filters_json` de
  l'interpréteur l'attend déjà).
- `DnsFilter.apply` devient asynchrone (issue #35, chantier full async #1) :
  la résolution réseau passe par `dns.asyncresolver` (vraie concurrence
  réseau) ; caches, délais, essais et `PYSTASHTEMPLATE_DNS=off`
  inchangés. La chaîne `filter{}` de l'interpréteur (`_run_filter_body`,
  `_run_filter_plugin`, `_apply_dns`) attend désormais les filtres
  asynchrones ; `FilterPipeline` (JSON) reste à convertir (#36). Changement
  d'API pour les appelants directs de `DnsFilter.apply` : `await` requis.

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
- Lots et contre-pression (`outputs.lotisseur`, issue #6) : `Lotisseur`
  tamporise les documents vers une sortie (taille de lot, délai,
  réveil immédiat à l'arrêt — arrêt propre, rien de perdu).

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
