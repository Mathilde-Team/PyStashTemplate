# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie OpenObserve (issue #16) : l'API `_bulk` compatible Elasticsearch.

OpenObserve (port 5080 par défaut) accepte le NDJSON `_bulk` du client
commun (#11) sur `POST /api/{organization}/_bulk` ; le `_index` de chaque
ligne d'action est le **stream**. Authentification basic (`user` /
`password`, via `${VAR}`), options communes du client.

Particularités prises en charge :

- **actions `index` et `create`** : OpenObserve ignore `delete` ; son
  `update` exige un `_id` que la sortie ne transmet pas (OpenObserve
  génère ses identifiants) — `_id` ignoré avec un avertissement ;
- **ordre des `items[]` non garanti** (issue amont #15716 : un stream qui
  mêle enregistrements colonnaires et de repli renvoie ses éléments dans
  un autre ordre que les actions). Attribuer le N-ième élément au N-ième
  document pourrait déclarer indexé un document refusé, et inversement.
  La sortie procède donc avec prudence (`_apparier`) :

  0. réponse sans champ ``errors`` (429, 5xx…) : règle commune (par
     position, nombre d'éléments contrôlé) ;
  1. ``errors: false`` : tout le lot est indexé, sans regarder l'ordre
     (couvre aussi le mode ``ZO_BULK_API_RESPONSE_ERRORS_ONLY``, qui
     n'envoie pas les éléments réussis) ;
  2. sinon, chaque élément en échec est apparié **par contenu** : son
     ``originalRecord`` est comparé aux documents envoyés ;
  3. un échec sans contenu reconnaissable n'est attribué **par position**
     que si la réponse a un élément par document (avertissement) ;
  4. à défaut, le lot entier est rendu à l'appelant (`RETRYABLE`) : un
     doublon possible plutôt qu'une perte.

| Option propre | Rôle |
|---|---|
| `organization` | organisation OpenObserve, dans le chemin (défaut : `default`) |

## Gabarit de pipeline d'exemple

```
output {
  openobserve {
    hosts => ["http://openobserve:5080"]
    organization => "default"
    index => "logs"
    user => "${OO_USER}"
    password => "${OO_PASSWORD}"
  }
}
```
"""

from __future__ import annotations

import json
from typing import Any, ClassVar
from urllib.parse import quote

from loguru import logger

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput
from pystashtemplate.tracing import atraced, traced

ORGANISATION_PAR_DEFAUT = "default"


def _detail(item: dict[str, Any]) -> dict[str, Any]:
    """Contenu d'un élément `{action: {...}}` (vide si l'élément est mal formé)."""
    logger.debug("_detail | entrée")
    valeur = next(iter(item.values()), {}) if isinstance(item, dict) else {}
    detail = valeur if isinstance(valeur, dict) else {}
    logger.debug("_detail | sortie")
    return detail


def _en_echec(detail: dict[str, Any]) -> bool:
    """Élément en échec : `error` présent ou statut ≥ 400."""
    logger.debug("_en_echec | entrée")
    echec = "error" in detail or int(detail.get("status", 200)) >= 400
    logger.debug("_en_echec | sortie ({})", echec)
    return echec


def _meme_contenu(document: dict[str, Any], original: Any) -> bool:
    """`original` (``originalRecord`` d'OpenObserve) est-il ce document, tel qu'envoyé en JSON ?"""
    logger.debug("_meme_contenu | entrée")
    identique = bool(json.loads(json.dumps(document, ensure_ascii=False)) == original)
    logger.debug("_meme_contenu | sortie ({})", identique)
    return identique


@enregistrer
class OpenObserveOutput(ElasticsearchBulkOutput):
    """Sortie OpenObserve : `_bulk` sous `/api/{organization}`, appariement prudent des éléments."""

    plugin = "openobserve"
    capacites = Capacites(
        actions=frozenset({"index", "create"}),
        accepte_id=False,
        accuse_par_evenement=True,
        taille_max_requete=None,
    )
    OPTIONS_CONNUES: ClassVar[frozenset[str]] = ElasticsearchBulkOutput.OPTIONS_CONNUES | frozenset({"organization"})

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        dlq: LocalDeadLetterQueue | None = None,
        transport: Any = None,
    ) -> None:
        """Place l'organisation dans le chemin `_bulk`.

        Raises:
            ValueError: organisation vide, action non supportée (`update`,
                `delete`) ou option inconnue.
        """
        if "action" in options:
            self.capacites.verifie_action(str(options["action"]))
        super().__init__(options, dlq=dlq, transport=transport)
        organisation = str(options.get("organization", ORGANISATION_PAR_DEFAUT)).strip()
        if not organisation:
            logger.warning(_("sortie openobserve : organisation vide"))
            raise ValueError(_("sortie openobserve : option « organization » vide"))
        self.organisation = organisation
        self.chemin_bulk = f"/api/{quote(organisation, safe='')}"
        logger.debug("OpenObserveOutput : chemin {}", self.chemin_bulk)

    @traced
    def _apparier(self, pendings: list[dict[str, Any]], corps: dict[str, Any]) -> list[dict[str, Any]] | None:
        """Élément de chaque document, sans se fier à l'ordre des `items[]` (amont #15716).

        Voir le docstring du module pour l'ordre des règles.
        """
        if "errors" not in corps:
            # Pas une réponse `_bulk` (429, 5xx aux réessais épuisés…) :
            # règle commune, par position avec contrôle du nombre.
            logger.debug("_apparier | réponse sans « errors » : règle commune")
            return super()._apparier(pendings, corps)
        resultats: list[dict[str, Any]] = [{p["action"]: {"status": 200}} for p in pendings]
        if corps["errors"] is False:
            logger.debug("_apparier | aucune erreur : lot indexé")
            return resultats
        items = list(corps.get("items", []))
        libres = set(range(len(pendings)))
        sans_contenu: list[tuple[int, dict[str, Any]]] = []
        for position, item in enumerate(items):
            detail = _detail(item)
            if not _en_echec(detail):
                continue
            detail = {**detail, "error": detail.get("error") or f"statut {detail.get('status')}"}
            original = detail.get("originalRecord")
            trouve = next(
                (i for i in sorted(libres) if original is not None and _meme_contenu(pendings[i]["doc"], original)),
                None,
            )
            if trouve is None:
                sans_contenu.append((position, detail))
                continue
            libres.discard(trouve)
            resultats[trouve] = {pendings[trouve]["action"]: detail}
        if not sans_contenu:
            return resultats
        if len(items) != len(pendings):
            logger.warning(
                _("réponse openobserve : {} échec(s) sans contenu reconnaissable, lot rendu à l'appelant"),
                len(sans_contenu),
            )
            return None
        logger.warning(
            _("réponse openobserve : {} échec(s) attribué(s) par position (ordre non garanti)"), len(sans_contenu)
        )
        for position, detail in sans_contenu:
            if position not in libres:
                logger.warning(_("réponse openobserve : position {} déjà attribuée, lot rendu à l'appelant"), position)
                return None
            libres.discard(position)
            resultats[position] = {pendings[position]["action"]: detail}
        return resultats

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge `GET /healthz` (sans authentification) : ``{"status": "ok"}``.

        Raises:
            httpx.HTTPStatusError: statut d'erreur.
        """
        response = await self._client_http().get("/healthz")
        response.raise_for_status()
        corps: dict[str, Any] = response.json()
        return corps
