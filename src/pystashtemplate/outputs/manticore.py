# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie Manticore Search (issue #17) : `POST /_bulk` compatible Elasticsearch.

Manticore (port HTTP 9308 par défaut) lit le NDJSON `_bulk` du client
commun (#11) et rapporte le résultat **élément par élément**
(`items[]`), comme Elasticsearch. Particularités, observées contre
Manticore 29.9.0 et décrites dans l'issue amont #4884 :

- **`create` en doublon** : avant 29.9.0, HTTP 409 pour toute la requête
  et statut d'élément 400 (`mapper_parsing_exception`, « duplicate id ») ;
  depuis 29.9.0, HTTP 200 et statut d'élément 409
  (`version_conflict_engine_exception`). Les deux formes sont reconnues
  comme un **doublon** : le document existe déjà, il est compté
  indexé (comme la sortie `elasticsearch` de Logstash, qui ne rejoue ni
  n'envoie en DLQ un conflit 409) — jamais comme une erreur de mapping
  envoyée en DLQ.
- **perte silencieuse avant un doublon** : dans une suite de `create`
  consécutifs vers la même table, un doublon annule les documents qui le
  précèdent alors qu'ils sont rapportés `201`. Ces documents sont rendus
  à l'appelant (`RETRYABLE`) pour être renvoyés : avec un `_id`, le
  renvoi est idempotent (au pire un doublon, compté indexé).
- **`_id` entier** : un `_id` non numérique est haché par Manticore en
  entier 64 bits (identifiant stable pour une même chaîne).
- **table existante** : sans le module Buddy (création automatique du
  schéma), une table absente ou une colonne inconnue est une erreur
  d'élément 400 (permanente : DLQ si configurée).

Aucune option propre : seules les options communes s'appliquent.

## Gabarit de pipeline d'exemple

```
output {
  manticore {
    hosts => ["http://manticore:9308"]
    index => "logs"
  }
}
```
"""

from __future__ import annotations

from typing import Any

import httpx
from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput, SendOutcome
from pystashtemplate.tracing import atraced, traced


@traced
def est_doublon(item: dict[str, Any], action: str) -> bool:
    """Vrai si l'élément `items[]` signale un `create` en doublon.

    Deux formes selon la version de Manticore : statut 409 (29.9.0 et
    après), ou statut 400 dont la raison est « duplicate id » (avant).
    """
    if action != "create":
        return False
    resultat = item.get(action, {})
    erreur = resultat.get("error")
    if erreur is None:
        return False
    if resultat.get("status") == 409:
        return True
    raison = erreur.get("reason", "") if isinstance(erreur, dict) else str(erreur)
    return "duplicate id" in str(raison)


@enregistrer
class ManticoreOutput(ElasticsearchBulkOutput):
    """Sortie Manticore : client _bulk commun, doublons `create` reconnus."""

    plugin = "manticore"
    chemin_bulk = ""
    capacites = Capacites(
        actions=frozenset({"index", "create", "update", "delete"}),
        accepte_id=True,
        accuse_par_evenement=True,
        taille_max_requete=None,
    )

    @atraced
    async def _analyser_reponse(self, pendings: list[dict[str, Any]], response: httpx.Response) -> list[SendOutcome]:
        """Écarte les doublons `create`, analyse le reste par le client commun.

        Un doublon est compté indexé ; les `create` qui le précèdent
        dans la même suite (même table) sont rendus à l'appelant, car
        Manticore les annule en les rapportant créés.
        """
        items = _items_de(response, len(pendings))
        doublons = set() if items is None else {i for i, p in enumerate(pendings) if est_doublon(items[i], p["action"])}
        if items is None or not doublons:
            return await super()._analyser_reponse(pendings, response)
        logger.warning(_("manticore : {} create en doublon (document déjà présent)"), len(doublons))
        autres = [i for i in range(len(pendings)) if i not in doublons]
        sous_reponse = httpx.Response(200, json={"items": [items[i] for i in autres]})
        issues = await super()._analyser_reponse([pendings[i] for i in autres], sous_reponse)
        outcomes = [SendOutcome.INDEXED] * len(pendings)
        for i, issue in zip(autres, issues, strict=True):
            outcomes[i] = issue
        perdus = _precedents_annules(pendings, doublons)
        for i in perdus:
            if outcomes[i] is SendOutcome.INDEXED:
                outcomes[i] = SendOutcome.RETRYABLE
        if perdus:
            logger.warning(_("manticore : {} create précédant un doublon rendus à l'appelant"), len(perdus))
        return outcomes


@traced
def _items_de(response: httpx.Response, attendus: int) -> list[dict[str, Any]] | None:
    """`items[]` de la réponse, ou None s'ils sont illisibles ou en nombre inattendu."""
    if response.status_code not in (200, 409):
        return None
    try:
        items = response.json().get("items")
    except (ValueError, AttributeError) as exc:
        logger.warning(_("réponse _bulk illisible erreur={error}"), error=exc)
        return None
    if not isinstance(items, list) or len(items) != attendus:
        return None
    return items


@traced
def _precedents_annules(pendings: list[dict[str, Any]], doublons: set[int]) -> set[int]:
    """Indices des `create` non doublons qui précèdent un doublon dans la même suite.

    Une suite est une séquence de `create` consécutifs vers le même
    index ; un autre type d'action ou un autre index l'interrompt.
    """
    perdus: set[int] = set()
    for k in doublons:
        j = k - 1
        while j >= 0 and pendings[j]["action"] == "create" and pendings[j]["index"] == pendings[k]["index"]:
            if j not in doublons:
                perdus.add(j)
            j -= 1
    return perdus
