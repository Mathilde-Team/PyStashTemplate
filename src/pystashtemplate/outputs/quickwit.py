# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie Quickwit (issue #15) : l'API compatible Elasticsearch `_bulk`.

Quickwit (port 7280 par défaut) accepte le NDJSON `_bulk` du client
commun (#11) sur `POST /api/v1/_elastic/_bulk`, avec trois limites
propres au produit :

- **action `create` seule** : Quickwit ignore `update` et `delete` (et
  n'a pas d'`index` au sens Elasticsearch). L'action par défaut de la
  sortie est donc `create` ; toute autre action demandée par l'option
  `action` est refusée dès la compilation du pipeline (message traduit),
  et par `envoyer` pour un document ;
- **pas de `_id`** : Quickwit n'a pas d'identifiant de document ; un
  `document_id` est ignoré avec un avertissement (dégradation documentée
  du client commun) ;
- **pas d'accusé par élément** : la réponse ne rapporte que
  `num_docs_for_processing` ; les erreurs d'indexation ne remontent que
  dans le journal du serveur. La sortie décide sur le **statut HTTP
  global** (`accuse_par_evenement=False`) : 2xx = lot accepté ;
  400/404 = échec permanent (DLQ si configurée) ; réseau, 429, 5xx =
  transitoire.

Requête limitée à 10 Mo par Quickwit (`taille_max_requete`). L'index doit
exister (Quickwit ne le crée pas à l'ingestion).

| Option propre | Rôle |
|---|---|
| `refresh` | paramètre `refresh` de Quickwit : `true`, `wait_for`, `false` (défaut : absent, comportement du serveur) |

## Gabarit de pipeline d'exemple

```
output {
  quickwit {
    hosts => ["http://quickwit:7280"]
    index => "logs"
  }
}
```
"""

from __future__ import annotations

from typing import Any, ClassVar

from loguru import logger

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput, SendOutcome
from pystashtemplate.tracing import atraced, traced

# Valeurs du paramètre `refresh` de l'API `_bulk` de Quickwit.
REFRESH = ("true", "wait_for", "false")

# Taille maximale d'une requête `_bulk` acceptée par Quickwit (10 Mo).
TAILLE_MAX_REQUETE = 10 * 1024 * 1024


@enregistrer
class QuickwitOutput(ElasticsearchBulkOutput):
    """Sortie Quickwit : `_bulk` sous `/api/v1/_elastic`, `create` seule, statut HTTP global."""

    plugin = "quickwit"
    chemin_bulk = "/api/v1/_elastic"
    capacites = Capacites(
        actions=frozenset({"create"}),
        accepte_id=False,
        accuse_par_evenement=False,
        taille_max_requete=TAILLE_MAX_REQUETE,
    )
    OPTIONS_CONNUES: ClassVar[frozenset[str]] = ElasticsearchBulkOutput.OPTIONS_CONNUES | frozenset({"refresh"})

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        dlq: LocalDeadLetterQueue | None = None,
        transport: Any = None,
    ) -> None:
        """Impose l'action `create` et valide `refresh`.

        Raises:
            ValueError: action autre que `create`, `refresh` inconnu, ou
                option inconnue.
        """
        options = dict(options)
        action = str(options.setdefault("action", "create"))
        if action != "create":
            logger.warning(_("sortie quickwit : action {action} refusée (create seule)"), action=action)
        self.capacites.verifie_action(action)
        super().__init__(options, dlq=dlq, transport=transport)
        self._parametres: dict[str, str] = {}
        refresh = options.get("refresh")
        if refresh is not None:
            valeur = str(refresh).lower()
            if valeur not in REFRESH:
                logger.warning(_("sortie quickwit : refresh {valeur} inconnu"), valeur=valeur)
                raise ValueError(
                    _("sortie quickwit : refresh {} inconnu (attendu : {})").format(valeur, ", ".join(REFRESH))
                )
            self._parametres["refresh"] = valeur
        logger.debug("QuickwitOutput : paramètres {}", self._parametres)

    @traced
    def _parametres_requete(self) -> dict[str, str]:
        """`refresh` si configuré (copie : le client y ajoute `pipeline`)."""
        return dict(self._parametres)

    @atraced
    async def envoyer(self, doc: dict[str, Any], *, action: str = "create", **options: Any) -> SendOutcome:
        """`envoyer` du client commun, avec `create` pour action par défaut."""
        return await super().envoyer(doc, action=action, **options)

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge `GET /api/v1/version` : la version de Quickwit.

        Returns:
            Le corps JSON de la réponse (`build.version`…).

        Raises:
            httpx.HTTPStatusError: statut d'erreur.
        """
        response = await self._client_http().get("/api/v1/version")
        response.raise_for_status()
        return dict(response.json())
