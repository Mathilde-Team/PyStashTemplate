# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie ZincSearch (issue #18) : l'API `_bulk` native de ZincSearch.

ZincSearch (port 4080 par défaut) lit le NDJSON `_bulk` du client commun
(#11) sur `POST /api/_bulk`, avec une compatibilité Elasticsearch
partielle (lue dans le code de ZincSearch 0.4.10,
`pkg/handlers/document/bulk.go`) :

- **actions `index`, `create`, `delete`** ; `update` est refusée dès la
  compilation : ZincSearch enregistre la ligne de données telle quelle
  comme document complet (pas de `{"doc": …}` partiel), une mise à jour
  partielle à la mode Elasticsearch écraserait le document ;
- **`_id` transmis** : un `index`/`create` avec `_id` remplace le
  document existant ;
- **statut HTTP global seul** : la réponse ne porte que
  ``{"message": "bulk data inserted", "record_count": N}``. Une erreur
  arrête le lot en **HTTP 500** (``{"error": …}``) — les documents qui
  précèdent l'erreur sont déjà indexés ; le lot est retenté
  (transitoire) puis rendu à l'appelant. Un document sans `_id` peut
  donc être indexé deux fois : fixer `document_id` évite le doublon.
  Une ligne JSON illisible est ignorée par le serveur sans erreur.

Authentification basic (`user` / `password`, via `${VAR}`) ; aucune
option propre.

## Gabarit de pipeline d'exemple

```
output {
  zincsearch {
    hosts => ["http://zincsearch:4080"]
    index => "logs"
    user => "${ZINC_USER}"
    password => "${ZINC_PASSWORD}"
  }
}
```
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput
from pystashtemplate.tracing import atraced, traced


@enregistrer
class ZincSearchOutput(ElasticsearchBulkOutput):
    """Sortie ZincSearch : `_bulk` sous `/api`, sans `update`, statut HTTP global."""

    plugin = "zincsearch"
    chemin_bulk = "/api"
    capacites = Capacites(
        actions=frozenset({"index", "create", "delete"}),
        accepte_id=True,
        accuse_par_evenement=False,
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
        """Refuse dès la construction une action non prise en charge (`update`).

        Raises:
            ValueError: action non supportée, ou option inconnue.
        """
        if "action" in options:
            self.capacites.verifie_action(str(options["action"]))
        super().__init__(options, dlq=dlq, transport=transport)
        logger.debug("ZincSearchOutput : chemin {}", self.chemin_bulk)

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge `GET /version` : la version de ZincSearch.

        Raises:
            httpx.HTTPStatusError: statut d'erreur.
        """
        response = await self._client_http().get("/version")
        response.raise_for_status()
        corps: dict[str, Any] = response.json()
        return corps
