<!-- SPDX-License-Identifier: GPL-3.0-or-later -->
# PATTERNS.md — Conventions et patterns de PyStashTemplate

Conventions reprises de RsysLogstach lors du découpage
[#111](https://github.com/Mathilde-Team/RsysLogstach/issues/111) (rapatriées
par #57) : elles portaient sur les modules qui vivent désormais ici. Les
numéros de session et d'issues cités renvoient à l'historique de
RsysLogstach.

## Traçabilité systématique des fonctions

**Règle :** toute fonction du paquet (`src/pystashtemplate/`, hors `tests/`)
est tracée à l'entrée ET à la sortie (retour normal ou exception).

**Implémentation :** décorateur `@traced` (`tracing.py`) pour les fonctions
synchrones, `@atraced` pour les coroutines (#37 : la sortie est journalisée
quand la coroutine se termine réellement, autour du `await`). Le décorateur
enveloppe l'appel entier — il ne peut structurellement pas rater un chemin
de sortie, contrairement à une convention appliquée à la main.
`scripts/audit_debug_sorties.py` reconnaît les deux décorateurs.

**Contenu des logs :** uniquement le `__qualname__` — jamais les arguments
ni la valeur de retour (les appelants manipulent des secrets :
identifiants des sorties, clés HMAC). Une fonction qui a besoin de
journaliser un détail précis et non sensible le fait via son propre
`logger.debug()` additionnel.

**Compatibilité :** `functools.wraps` préserve `__name__`/`__doc__`/`__wrapped__`
— compatible avec `inspect.signature`.

## Absence de dépendances circulaires entre modules

**Règle :** le graphe de dépendances entre modules du paquet reste acyclique.

**Vérification :** `tests/test_no_cross_imports.py` analyse les imports via
`ast` (pas de simple relecture) et vérifie mécaniquement l'absence de cycles.

**Exemples de décisions prises pour respecter cette règle :**

- `tracing` est intentionnellement sans AUCUNE dépendance interne — c'est le
  seul moyen pour que tous les autres modules puissent l'importer sans cycle.
- `util.add_failure_tags` a été déplacée depuis `interpreter.py` (session 24)
  vers `util.py` pour être partagée avec `filters.py::GeoipFilter` sans créer
  de dépendance `filters → interpreter` (interdite : `interpreter` dépend
  déjà de `filters`).
- `_build_geoip_record` (session 21) est dans `enrichment.py`, consommé par
  `filters.py` — pas l'inverse.
- `fingerprint.py` dépend de `templates` (résolution de chemins) mais
  `templates` ne dépend jamais de `fingerprint` — vérifié par
  `test_templates_has_no_dependency_on_fingerprint`.

## Opérations bloquantes hors boucle asyncio

**Convention :** les écritures disque (DLQ) se font via `asyncio.to_thread`,
et la résolution DNS passe par `dns.asyncresolver` (#35), pour ne jamais
bloquer la boucle asyncio de l'appelant (connecteur rsyslog, service
réseau…).
