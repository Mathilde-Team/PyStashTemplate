# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie Parseable (issue #25) : ingestion JSON par `POST /api/v1/ingest`.

Parseable (port 8000 par défaut) reçoit un tableau JSON d'événements ;
le jeu de données (« stream ») visé passe dans l'en-tête `X-P-Stream`
et il est créé automatiquement s'il n'existe pas. Champs imbriqués
aplatis par Parseable (`host.name` → `host_name`), horodatage
d'ingestion ajouté (`p_timestamp`).

| Option | En-tête | Rôle |
|---|---|---|
| `stream` | `X-P-Stream` | jeu de données visé (obligatoire) |
| `api_key` | `X-API-Key` | clé d'API (Parseable Cloud / Enterprise) |
| `tenant_id` | `X-P-Tenant-ID` | locataire (déploiements multi-tenant) |
| `tags` | `X-P-Tag-<nom>` | étiquettes ajoutées à chaque événement |

L'authentification basique (`user`/`password`, options communes) est
celle de l'édition open source.

## Accusé de réception

Parseable n'accuse pas réception élément par élément : 200 sans corps
quand le lot est accepté, sinon un message texte pour tout le lot. La
sortie décide donc sur le **statut HTTP global**
(`accuse_par_evenement=False`) : 2xx = lot ingéré ; 400/404 = échec
permanent (DLQ si configurée) ; réseau, 429, 5xx = transitoire ; 401/403
(clé ou droits refusés) = lot rendu à l'appelant. Un conflit de type
n'est pas une erreur : Parseable range la valeur dans une colonne
suffixée (`n` numérique puis `"texte"` → colonne `n_utf8`). Parseable
ignore `_id` et l'index : actions `index` seule, pas de `_id`.

Parseable 3.2.6 s'arrête net (panique) sur un tableau vide : la sortie
n'envoie jamais de lot vide.

## Gabarit de pipeline d'exemple

```
output {
  parseable {
    hosts => ["http://parseable:8000"]
    stream => "rsyslog"
    user => "${PARSEABLE_USER}"
    password => "${PARSEABLE_PASSWORD}"
  }
}
```
"""

from __future__ import annotations

import json
import re
from typing import Any, ClassVar

import httpx
from loguru import logger

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.tracing import atraced, traced

# Nom d'étiquette utilisable dans un en-tête HTTP `X-P-Tag-<nom>`.
_NOM_ETIQUETTE = re.compile(r"^[A-Za-z0-9_-]+$")


@enregistrer
class ParseableOutput(ElasticsearchBulkOutput):
    """Sortie Parseable : tableau JSON vers `/api/v1/ingest`, statut HTTP global."""

    plugin = "parseable"
    capacites = Capacites(
        actions=frozenset({"index"}),
        accepte_id=False,
        accuse_par_evenement=False,
        taille_max_requete=10 * 1024 * 1024,
    )
    # Options du client _bulk sans objet chez Parseable : `index` (le
    # jeu de données est `stream`), `pipeline`, `routing`,
    # `doc_as_upsert` (spécifiques à l'API _bulk).
    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = dict(
        {
            nom: spec
            for nom, spec in ElasticsearchBulkOutput.OPTIONS_PROPRES.items()
            if nom not in ("index", "pipeline", "routing", "doc_as_upsert")
        },
        action=OptionSpec("action", (str,), "index", valeurs=("index",), description="Insertions seules (index)"),
        stream=OptionSpec("stream", (str,), description="Jeu de données Parseable (en-tête X-P-Stream)"),
        api_key=OptionSpec("api_key", (str,), description="Clé d'API (en-tête X-API-Key)"),
        tenant_id=OptionSpec("tenant_id", (str,), description="Locataire multi-tenant (en-tête X-P-Tenant-ID)"),
        tags=OptionSpec("tags", (dict,), {}, description="Étiquettes ajoutées aux événements (X-P-Tag-<nom>)"),
    )
    # Tableau JSON (et non NDJSON _bulk).
    en_tetes_par_defaut: ClassVar[dict[str, str]] = {"Content-Type": "application/json"}

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        dlq: LocalDeadLetterQueue | None = None,
        transport: Any = None,
    ) -> None:
        """Valide le jeu de données et prépare les en-têtes Parseable.

        Raises:
            ValueError: `stream` absent, étiquette au nom invalide, option
                inconnue ou valeur invalide (schéma commun, #8).
        """
        super().__init__(options, dlq=dlq, transport=transport)
        stream = str(options.get("stream") or "")
        if not stream:
            logger.warning(_("sortie parseable : option stream manquante"))
            raise ValueError(_("sortie parseable : stream est requis (jeu de données Parseable)"))
        self._en_tetes: dict[str, str] = {"X-P-Stream": stream}
        if options.get("api_key"):
            self._en_tetes["X-API-Key"] = str(options["api_key"])
        if options.get("tenant_id"):
            self._en_tetes["X-P-Tenant-ID"] = str(options["tenant_id"])
        for nom, valeur in dict(options.get("tags") or {}).items():
            if not _NOM_ETIQUETTE.match(str(nom)):
                logger.warning(_("sortie parseable : étiquette {} refusée"), nom)
                raise ValueError(
                    _("sortie parseable : nom d'étiquette invalide {!r} (lettres, chiffres, _ et -)").format(nom)
                )
            self._en_tetes[f"X-P-Tag-{nom}"] = str(valeur)
        logger.debug(
            "ParseableOutput : stream {}, clé d'API {}, en-têtes {}",
            stream,
            bool(options.get("api_key")),
            sorted(nom for nom in self._en_tetes if nom != "X-API-Key"),
        )

    @traced
    def _client_http(self) -> httpx.AsyncClient:
        """Client du client commun, plus les en-têtes Parseable (`X-P-Stream`…)."""
        client = super()._client_http()
        for nom, valeur in self._en_tetes.items():
            if nom not in client.headers:
                client.headers[nom] = valeur
        return client

    @traced
    def _chemin_envoi(self) -> str:
        """`/api/v1/ingest`."""
        return "/api/v1/ingest"

    @traced
    def _corps_requete(self, pendings: list[dict[str, Any]]) -> str:
        """Tableau JSON des documents du lot (jamais vide : `_flush` ne part pas sans document)."""
        return json.dumps([pending["doc"] for pending in pendings], ensure_ascii=False)

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge `GET /api/v1/readiness` puis `GET /api/v1/about` (version).

        Returns:
            `{"status": "ready", "version": ...}` quand Parseable est prêt.

        Raises:
            httpx.HTTPStatusError: statut d'erreur (non prêt, droits).
        """
        client = self._client_http()
        pret = await client.get("/api/v1/readiness")
        pret.raise_for_status()
        apropos = await client.get("/api/v1/about")
        apropos.raise_for_status()
        return {"status": "ready", "version": apropos.json().get("version")}
