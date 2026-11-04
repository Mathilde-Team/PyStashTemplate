<!-- SPDX-License-Identifier: GPL-3.0-or-later -->
# FEATURES.md — Fonctionnalités de PyStashTemplate

Fonctionnalités et limites connues de la bibliothèque, reprises de
`FEATURES.md` de RsysLogstach lors du découpage
[#111](https://github.com/Mathilde-Team/RsysLogstach/issues/111) (rapatriées
par #57). Les numéros de session et d'issues `#1xx` antérieurs au découpage
renvoient à RsysLogstach. Référence détaillée des formats :
[docs/pipeline-logstash.md](docs/pipeline-logstash.md) ; état fonction par
fonction : [docs/audit-fonctions-logstash.md](docs/audit-fonctions-logstash.md) ;
sorties : [docs/sorties.md](docs/sorties.md).

## Fonctionnalités implémentées

### Filtres (module `filters.py`, `FilterPipeline`)

| Filtre | Options | Session |
|--------|---------|---------|
| **drop** | source, words_file, mode (contains), case_sensitive | 8 |
| **regex** | source, pattern (groupes nommés) | 8 |
| **kv** | source, field_split, value_split, prefix | 8 |
| **mutate** | convert (integer, integer_eu, float, float_eu, string, boolean ; règles de la documentation Logstash, RsysLogstach#73), remove_field, prefix | 14 |
| **mutate** | uppercase, lowercase, capitalize, split, join, merge, copy, update, coerce | 20 |
| **geoip** | source, target, tag_on_failure | 24 |
| **date** | match (ISO8601, UNIX, UNIX_MS, motif Java/Joda), target, timezone, tag_on_failure ; même implémentation que le plugin `date` des `.conf` | #34 |
| **dns** | mêmes options que le plugin `dns` du `.conf`, résolution asynchrone | #36 |

### Enrichissement (module `enrichment.py`)

- Dictionnaire statique JSON (`enrich-dict.example.json`)
- GeoIP via geoip2/maxminddb (`_build_geoip_record`, session 21)

### Pipeline Logstash (module `interpreter.py`, `PipelineInterpreter`)

- Parsing des fichiers `.conf` via `logstash-pipeline-parser`
- Exemples : `pipeline.example.d/10_input`, `25_filter`, `30_output`
- tag_on_failure pour date et geoip (sessions 19, 22, 24)
- Filtres natifs interprétés : `dissect`, `mutate`, `translate` (toutes les
  options du plugin, #119), `mmdb`, `geoip`, `date`, `drop` (avec
  `percentage`, #34), `kv` (#113), `fingerprint` (#114), `grok` (#112),
  `useragent`, `xml` et `dns` (#121), `clone` et `split` (#141, module
  `multievents.py`), plugins simples (#120, module `simples.py`), plus le
  pont `filters_json`
- `apply` / `apply_events` asynchrones (#34)

### Fingerprint (module `fingerprint.py`, session 17)

- `compute_fingerprint()` — sha256/sha1/md5, MURMUR3 et HMAC (RsysLogstach#28)
- Document entier (fields=None/[]) ou sélection ordonnée de champs
- Séparateur de contrôle `\x1f` anti-collision
- `None` si tous les champs demandés sont absents

### Traçabilité (module `tracing.py`)

- Décorateurs `@traced` et `@atraced` (#37) — entrée/sortie systématique (loguru)
- Préserve `__name__`/`__doc__`/`__wrapped__` (functools.wraps)

### Sorties (paquet `outputs`)

Voir [docs/sorties.md](docs/sorties.md) et l'épopée #3. Sorties locales
ajoutées par l'issue #32 : `file` (NDJSON avec rotation par gabarit de
chemin `%{+YYYY-MM-dd}`, DLQ sur échec d'écriture) et `stdout` (codecs
`rubydebug` par défaut et `json_lines`), codecs partagés dans
`outputs/codecs.py`.

## Limites connues

### Fingerprint (`fingerprint.py`)

- Pas de IPV4_NETWORK (portée trop spécifique)
- Pas de HMAC MURMUR3, comme le filtre Logstash

### Templates (`templates.py`)

- Fraction de seconde `S…` : précision microseconde ; au-delà de 6 lettres,
  complétée par des zéros (`SSSSSSSSS` → `123456000`) (#123)
- Lettres de date (#123) : Java Time dans `%{{…}}` (`G u y Y Q q M L w W D d E e c a h K k H m s S n N A V O X x Z z`)
  et Joda dans `%{+…}` (`G C Y x y w e E M D d a h K k H m s S Z z`) ;
  semaines ISO (lundi, semaine 1 d'au moins 4 jours) et textes en anglais,
  alors que Java Time suit la locale de la JVM ; `F` et `B` recopiées telles
  quelles
- Rendu des valeurs aligné sur Logstash 8 (`StringInterpolation`, #49) :
  booléen `true`/`false`, liste jointe par `,`, objet en JSON compact
- Références mal formées refusées (`ValueError`) comme par Logstash :
  `a[b]`, `a]`, `[]`, `[a[b]]`
- Écart assumé : un objet à l'intérieur d'une liste est rendu en JSON
  (Logstash utilise le `toString()` Java de la map)
- Rendu d'un champ `null` : chaîne vide — écart assumé avec Logstash 8, qui
  conserve le gabarit `%{champ}` littéral. **Fingerprint (#29, décision
  Mathilde)** : un champ `null` est distingué d'un champ absent — il compte
  comme présent (empreinte du contenu vide), `None` uniquement si tous les
  champs demandés sont absents

### Pipeline `.conf` (`interpreter.py`)

- Plugins natifs non interprétés (structure seule, signalée en debug) :
  `aggregate`, `ruby`… Leur équivalent passe par `filters_json` quand il existe
- Comme Logstash, un tableau à un seul élément dans une condition
  (`[level] in ["ERROR"]`) est une **référence de champ**, pas un tableau :
  écrire `[level] == "ERROR"` ou au moins deux éléments
- `mutate convert` : une chaîne non numérique (« abc ») laisse le champ
  inchangé (Logstash/Ruby donnerait 0)
- GeoIP : les tests automatisés utilisent des bases mmdb construites pour
  les tests ; aucune base MaxMind réelle n'est livrée
