# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie SigLens (issue #20) : l'API `_bulk` compatible Elasticsearch.

SigLens sépare deux ports : l'ingestion (8081 par défaut), que vise cette
sortie, et la recherche (5122). Le NDJSON `_bulk` du client commun (#11)
est accepté sur `POST /elastic/_bulk`, avec les limites suivantes,
observées contre SigLens 1.0.57 :

- **actions `index` et `create` seules** : `update` et `delete` font
  rejeter tout le lot (HTTP 400 « request body is required ») ; elles
  sont donc refusées dès la compilation du pipeline ;
- **pas de `_id`** : SigLens accepte l'identifiant mais ne le conserve
  pas — deux documents de même `_id` sont tous deux gardés, et un
  `create` en double n'est pas refusé. Un `document_id` est ignoré avec
  un avertissement (dégradation documentée du client commun) ;
- **accusé par élément, sous la clé `index`** : la réponse porte un
  `items[]` (201 indexé, 400 `mapper_parse_exception` pour une ligne
  illisible), chaque élément rapporté sous `index` même pour un
  `create`, et le `status` d'un élément en échec placé à côté de
  l'objet de l'action. La sortie relit chaque élément sous l'action
  envoyée, statut compris, avant l'analyse commune
  (`pystashtemplate.outputs.relecture`).

L'index est créé à l'ingestion. Pas d'authentification sur l'édition
libre de SigLens (les options `user`/`password` communes restent
transmises si un mandataire les exige).

## Gabarit de pipeline d'exemple

```
output {
  siglens {
    hosts => ["http://siglens:8081"]
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
from pystashtemplate.outputs.relecture import relire_sous_action
from pystashtemplate.tracing import atraced, traced

# Préfixe de l'API compatible Elasticsearch du port d'ingestion.
CHEMIN_ELASTIC = "/elastic"


@enregistrer
class SigLensOutput(ElasticsearchBulkOutput):
    """Sortie SigLens : `_bulk` sous `/elastic`, `index`/`create`, `_id` ignoré, accusé par élément."""

    plugin = "siglens"
    chemin_bulk = CHEMIN_ELASTIC
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
        """Refuse dès la compilation une action autre que `index` ou `create`.

        Raises:
            ValueError: action `update` ou `delete` (SigLens rejetterait
                tout le lot), option inconnue ou invalide.
        """
        action = str(options.get("action", "index"))
        if action not in self.capacites.actions:
            logger.warning(_("sortie siglens : action {action} refusée (index, create seules)"), action=action)
        self.capacites.verifie_action(action)
        super().__init__(options, dlq=dlq, transport=transport)

    @atraced
    async def _analyser_reponse(self, pendings: list[dict[str, Any]], response: httpx.Response) -> list[SendOutcome]:
        """Relit chaque élément `items[]` sous l'action envoyée (statut compris), puis analyse commune.

        SigLens rapporte tout élément sous `index`, et le `status` d'un
        élément en échec hors de l'objet de l'action : sans relecture, un
        `create` refusé serait compté indexé.
        """
        return await super()._analyser_reponse(pendings, relire_sous_action(pendings, response, self.plugin))

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge `GET /elastic/` : la fiche « Elasticsearch » de SigLens.

        Returns:
            Le corps JSON de la réponse (`version.number`…).

        Raises:
            httpx.HTTPStatusError: statut d'erreur.
        """
        response = await self._client_http().get(f"{CHEMIN_ELASTIC}/")
        response.raise_for_status()
        return dict(response.json())
