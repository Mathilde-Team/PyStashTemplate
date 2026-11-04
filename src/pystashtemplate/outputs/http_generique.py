# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie HTTP générique (issue #24) : un récepteur libre, JSON ou NDJSON.

Repli pour tout récepteur HTTP — Vector (source `http_server`),
entrée `http` de Logstash, Fluent Bit, webhooks… — sans client dédié :
`POST` (ou `PUT`) vers une URL libre, options reprises de la sortie
`http` de Logstash (`url`, `http_method`, `format`, `headers`).

## Formats

| `format` | Corps de la requête |
|---|---|
| `ndjson` (défaut) | une ligne JSON par document |
| `json_batch` | un tableau JSON des documents du lot |
| `json` | **un événement par requête** (lot retaillé à 1 document) |

## Accusé de réception

Un récepteur générique n'accuse pas réception élément par élément : la
décision se prend sur le **statut HTTP global**
(`accuse_par_evenement=False`) : 2xx = lot accepté ; 400/404 (URL ou
requête refusée) = échec permanent (DLQ si configurée) ; réseau, 429,
5xx = transitoire. Un lot rejeté part en entier : aucun document n'est
isolé. Insertions seules : `index`, `_id` refusé.

## Gabarit de pipeline d'exemple

```
output {
  http {
    url => "http://vector:8080/logs"
    http_method => "post"
    format => "json_batch"
    headers => { "Authorization" => "Bearer ${TOKEN}" }
  }
}
```
"""

from __future__ import annotations

import json
from typing import Any, ClassVar
from urllib.parse import urlsplit

from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.tracing import atraced, traced

FORMATS = ("json", "json_batch", "ndjson")
METHODES = ("post", "put")


@enregistrer
class HttpOutput(ElasticsearchBulkOutput):
    """Sortie HTTP générique : POST/PUT vers une URL libre, JSON ou NDJSON, statut global."""

    plugin = "http"
    capacites = Capacites(
        actions=frozenset({"index"}),
        accepte_id=False,
        accuse_par_evenement=False,
        taille_max_requete=None,
    )
    # Options du client _bulk sans objet ici : `index` (l'URL porte la
    # destination), `action` (insertions seules), `pipeline`,
    # `routing`, `doc_as_upsert`.
    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = dict(
        {
            nom: spec
            for nom, spec in ElasticsearchBulkOutput.OPTIONS_PROPRES.items()
            if nom not in ("index", "action", "pipeline", "routing", "doc_as_upsert")
        },
        url=OptionSpec(
            "url",
            (str,),
            description="URL libre du récepteur (schéma, hôte, chemin)",
            requis=True,
        ),
        http_method=OptionSpec(
            "http_method",
            (str,),
            "post",
            valeurs=METHODES,
            description="Verbe HTTP (post par défaut)",
        ),
        format=OptionSpec(
            "format",
            (str,),
            "ndjson",
            valeurs=FORMATS,
            description="Corps : ndjson (lignes), json_batch (tableau), json (un événement par requête)",
        ),
    )

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        dlq: Any = None,
        transport: Any = None,
    ) -> None:
        """Découpe `url` (hôte pour le client, chemin pour la requête) et le format.

        `url` est **requise** et prime sur `hosts` (option commune,
        ignorée ici) : elle porte schéma, hôte et chemin du récepteur.

        Raises:
            ValueError: Option inconnue ou valeur invalide (schéma commun,
                #8) — `url` manquante, `format`/`http_method` hors
                énumération.
        """
        options = dict(options)
        # `url` porte schéma, hôte et chemin : le client commun reçoit
        # l'hôte (option `hosts` devenue interne), la requête le chemin.
        morceaux = urlsplit(str(options.get("url", "")))
        if not morceaux.scheme or not morceaux.netloc:
            raise ValueError(
                _("sortie http : url complète attendue (ex. http://recepteur:8080/logs), reçu : {}").format(
                    options.get("url")
                )
            )
        options["hosts"] = f"{morceaux.scheme}://{morceaux.netloc}"
        # La validation du schéma (super) lève si `url` manque : lu
        # seulement après, l'accès est sûr.
        super().__init__(options, dlq=dlq, transport=transport)
        self._chemin = morceaux.path or "/"
        if morceaux.query:
            self._chemin += f"?{morceaux.query}"
        self._http_method = str(options.get("http_method") or "post")
        self._format = str(options.get("format") or "ndjson")
        if self._format == "json":
            # Un événement par requête : le lot est retaillé à 1.
            self._batch_size = 1
        logger.debug(
            "HttpOutput : {} {} (format {})",
            self._http_method.upper(),
            self._chemin,
            self._format,
        )

    @traced
    def _verbe_envoi(self) -> str:
        """Verbe choisi par l'utilisateur (`post` par défaut, `put` possible)."""
        return self._http_method

    @traced
    def _chemin_envoi(self) -> str:
        """Chemin (et requête d'URL) portés par `url`."""
        return self._chemin

    @traced
    def _corps_requete(self, pendings: list[dict[str, Any]]) -> str:
        """Corps selon `format` : lignes NDJSON, tableau `json_batch`, objet `json`."""
        documents = [pending["doc"] for pending in pendings]
        if self._format == "json_batch":
            return json.dumps(documents, ensure_ascii=False)
        if self._format == "json":
            return json.dumps(documents[0], ensure_ascii=False)
        return "".join(json.dumps(doc, ensure_ascii=False) + "\n" for doc in documents)

    @atraced
    async def health(self) -> dict[str, Any]:
        """Sonde le récepteur (`GET` sur le chemin d'`url`).

        Un récepteur générique n'a pas de protocole de santé standard :
        toute réponse inférieure à 5xx prouve la joignabilité (le
        récepteur a répondu) ; réseau et 5xx lèvent.

        Returns:
            `{"status": "joignable"}` quand le récepteur répond.

        Raises:
            httpx.HTTPError: le récepteur est injoignable.
            httpx.HTTPStatusError: réponse 5xx.
        """
        response = await self._client_http().get(self._chemin_envoi())
        response.raise_for_status()
        return {"status": "joignable"}
