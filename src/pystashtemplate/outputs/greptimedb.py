# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie GreptimeDB (issue #19) : l'API d'ingestion compatible Elasticsearch.

GreptimeDB (port HTTP 4000 par défaut) lit le NDJSON `_bulk` du client
commun (#11) sur `POST /v1/elasticsearch/_bulk` ; le `_index` de chaque
ligne d'action est la **table** (créée à la première écriture).
Authentification basic (`user` / `password`, via `${VAR}`) si le serveur
l'exige.

Particularités, observées contre GreptimeDB 1.3.0 :

- **insertions seules** : `index` (défaut) et `create` ; `update` et
  `delete` sont refusées dès la compilation (le serveur les rejette en
  400) ; `_id` ignoré avec un avertissement (non stocké par GreptimeDB) ;
- **éléments sous la clé `create`** : chaque élément de `items[]` est
  rapporté sous `create`, même pour un `index` ; la sortie le relit sous
  l'action envoyée avant l'analyse commune ;
- **lot atomique** : une ligne invalide (JSON illisible…) fait rejeter
  **tout** le lot en HTTP 400, sans rien écrire. La sortie renvoie alors
  chaque document **seul** : sans doublon possible (rien n'a été écrit),
  seul le document fautif est refusé (DLQ si configurée), avec la raison
  donnée par le serveur ;
- **perte silencieuse sur conflit de type** : un document dont un champ
  contredit le type déjà connu de la colonne (texte dans une colonne
  entière…) n'est pas écrit, alors que le serveur répond `201`. Rien dans
  la réponse ne permet de le détecter : garder des types stables par
  champ (filtres `mutate { convert => … }`).

Aucune option propre : seules les options communes s'appliquent.

## Gabarit de pipeline d'exemple

```
output {
  greptimedb {
    hosts => ["http://greptimedb:4000"]
    index => "logs"
  }
}
```
"""

from __future__ import annotations

from typing import Any

import httpx
from loguru import logger

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput, SendOutcome
from pystashtemplate.tracing import atraced, traced


@traced
def raison_du_rejet(response: httpx.Response) -> str:
    """Raison d'un rejet HTTP 400 : `items[0].<action>.error.reason`, ou le statut."""
    try:
        detail = next(iter(response.json()["items"][0].values()))
        return str(detail["error"]["reason"])
    except (ValueError, KeyError, IndexError, TypeError, AttributeError, StopIteration) as exc:
        logger.warning(_("réponse greptimedb sans raison lisible erreur={error}"), error=exc)
        return f"statut {response.status_code}"


@enregistrer
class GreptimeDbOutput(ElasticsearchBulkOutput):
    """Sortie GreptimeDB : `_bulk` sous `/v1/elasticsearch`, insertions seules, lot atomique."""

    plugin = "greptimedb"
    chemin_bulk = "/v1/elasticsearch"
    capacites = Capacites(
        actions=frozenset({"index", "create"}),
        accepte_id=False,
        accuse_par_evenement=True,
        taille_max_requete=None,
    )

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        dlq: LocalDeadLetterQueue | None = None,
        transport: Any = None,
    ) -> None:
        """Refuse dès la construction une action autre qu'une insertion.

        Raises:
            ValueError: action `update` ou `delete`, ou option inconnue.
        """
        if "action" in options:
            self.capacites.verifie_action(str(options["action"]))
        super().__init__(options, dlq=dlq, transport=transport)

    @atraced
    async def _analyser_reponse(self, pendings: list[dict[str, Any]], response: httpx.Response) -> list[SendOutcome]:
        """Isole le document fautif d'un lot rejeté, relit les éléments sous l'action envoyée.

        - 400 sur un lot de plusieurs documents : rien n'a été écrit,
          chaque document est renvoyé seul ;
        - 400 sur un document seul : refus définitif, avec la raison du
          serveur ;
        - sinon, `items[]` relus sous l'action envoyée (GreptimeDB les
          rapporte tous sous `create`), puis analyse commune.
        """
        if response.status_code == 400:
            if len(pendings) > 1:
                logger.warning(_("greptimedb : lot de {} documents rejeté, renvoi un par un"), len(pendings))
                return [(await self._issues_du_lot([pending]))[0] for pending in pendings]
            return [await self._permanent(pendings[0], raison_du_rejet(response))]
        try:
            corps = response.json()
            corps["items"] = [
                {pending["action"]: next(iter(item.values()))}
                for pending, item in zip(pendings, corps["items"], strict=True)
            ]
        except (ValueError, KeyError, TypeError, AttributeError, StopIteration) as exc:
            logger.warning(_("réponse _bulk greptimedb non normalisée erreur={error}"), error=exc)
            return await super()._analyser_reponse(pendings, response)
        relue = httpx.Response(response.status_code, json=corps, request=response.request)
        return await super()._analyser_reponse(pendings, relue)

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge `GET /health` (``{}`` quand le serveur est prêt).

        Raises:
            httpx.HTTPStatusError: statut d'erreur.
        """
        response = await self._client_http().get("/health")
        response.raise_for_status()
        corps: dict[str, Any] = response.json()
        return corps
