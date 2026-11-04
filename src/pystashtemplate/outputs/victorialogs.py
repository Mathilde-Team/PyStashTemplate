# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie VictoriaLogs (issue #14) : ingestion par l'API _bulk ou en JSON lines.

VictoriaLogs n'est qu'un système d'ingestion (port 9428 par défaut) :

- mode `bulk` (défaut) : `POST /insert/elasticsearch/_bulk`, le NDJSON
  `_bulk` du client commun (#11) ;
- mode `jsonline` : `POST /insert/jsonline`, une ligne JSON par document,
  sans ligne d'action.

Les champs propres à VictoriaLogs passent en paramètres d'URL, comme le
recommande sa documentation pour Logstash :

| Option | Paramètre | Défaut |
|---|---|---|
| `stream_fields` | `_stream_fields` | aucun (flux par défaut `{}`) |
| `msg_field` | `_msg_field` | `message` (champ des documents Logstash) |
| `time_field` | `_time_field` | `@timestamp` |

Une valeur vide (`msg_field => ""`) retire le paramètre : VictoriaLogs
retombe alors sur ses champs `_msg` et `_time`.

## Accusé de réception

VictoriaLogs n'accuse pas réception élément par élément : il analyse le
lot ligne à ligne, s'arrête à la première erreur et la rend pour tout le
lot. La sortie décide donc sur le **statut HTTP global**
(`accuse_par_evenement=False`) : 2xx = lot indexé ; 400/404 = échec
permanent (DLQ si configurée) ; réseau, 429, 5xx = transitoire. En mode
`jsonline`, une ligne JSON invalide est ignorée par VictoriaLogs (avec un
avertissement dans son journal) sans faire échouer le lot. VictoriaLogs
ignore `_index` et `_id` : pas de `_id` (refusé, dégradation
documentée du client commun), actions `index` et `create` seules.

## Gabarit de pipeline d'exemple

```
output {
  victorialogs {
    hosts => ["http://victorialogs:9428"]
    mode => "bulk"   # ou "jsonline"
    stream_fields => ["host", "app"]
  }
}
```
"""

from __future__ import annotations

import json
from typing import Any, ClassVar

from loguru import logger

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.tracing import atraced, traced

MODES = ("bulk", "jsonline")

# Option de la sortie → (paramètre d'URL de VictoriaLogs, valeur par défaut).
_CHAMPS: dict[str, tuple[str, str]] = {
    "stream_fields": ("_stream_fields", ""),
    "msg_field": ("_msg_field", "message"),
    "time_field": ("_time_field", "@timestamp"),
}


@enregistrer
class VictoriaLogsOutput(ElasticsearchBulkOutput):
    """Sortie VictoriaLogs : `_bulk` (`/insert/elasticsearch`) ou `jsonline`, statut HTTP global."""

    plugin = "victorialogs"
    chemin_bulk = "/insert/elasticsearch"
    capacites = Capacites(
        actions=frozenset({"index", "create"}),
        accepte_id=False,
        accuse_par_evenement=False,
        taille_max_requete=None,
    )
    # Options propres, en plus de celles de la famille _bulk et des
    # communes du schéma (issue #8) : mode d'ingestion et champs de
    # VictoriaLogs.
    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = dict(
        ElasticsearchBulkOutput.OPTIONS_PROPRES,
        mode=OptionSpec(
            "mode",
            (str,),
            "bulk",
            valeurs=MODES,
            description="Ingestion _bulk ou jsonline (bulk par défaut)",
        ),
        **{
            option: OptionSpec(
                option,
                (str, list, tuple) if option == "stream_fields" else (str,),
                defaut,
                description=f"Paramètre d'URL {parametre} ({defaut} par défaut)",
            )
            for option, (parametre, defaut) in _CHAMPS.items()
        },
    )

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        dlq: LocalDeadLetterQueue | None = None,
        transport: Any = None,
    ) -> None:
        """Valide le mode et prépare les paramètres d'ingestion.

        Raises:
            ValueError: mode inconnu, option inconnue, ou `stream_fields`
                qui n'est ni une chaîne ni une liste.
        """
        super().__init__(options, dlq=dlq, transport=transport)
        # `mode` est déjà validé par le schéma commun (valeurs valides : MODES).
        self._mode = str(options.get("mode", "bulk"))
        self._parametres: dict[str, str] = {}
        for option, (parametre, defaut) in _CHAMPS.items():
            valeur = _liste_virgules(options.get(option, defaut))
            if valeur:
                self._parametres[parametre] = valeur
        logger.debug("VictoriaLogsOutput : mode {}, paramètres {}", self._mode, self._parametres)

    @traced
    def _chemin_envoi(self) -> str:
        """`/insert/elasticsearch/_bulk` en mode `bulk`, `/insert/jsonline` sinon."""
        return super()._chemin_envoi() if self._mode == "bulk" else "/insert/jsonline"

    @traced
    def _parametres_requete(self) -> dict[str, str]:
        """`_stream_fields`, `_msg_field`, `_time_field` (copie : le client y ajoute `pipeline`)."""
        return dict(self._parametres)

    @traced
    def _corps_requete(self, pendings: list[dict[str, Any]]) -> str:
        """NDJSON `_bulk` en mode `bulk` ; une ligne JSON par document en mode `jsonline`."""
        if self._mode == "bulk":
            return super()._corps_requete(pendings)
        return "".join(json.dumps(pending["doc"], ensure_ascii=False) + "\n" for pending in pendings)

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge `GET /health` (VictoriaLogs n'expose pas de version sur `/`).

        Returns:
            `{"status": <corps de la réponse>}` (`OK` quand le service va bien).

        Raises:
            httpx.HTTPStatusError: statut d'erreur.
        """
        response = await self._client_http().get("/health")
        response.raise_for_status()
        return {"status": response.text.strip()}


@traced
def _liste_virgules(valeur: Any) -> str:
    """Liste (ou chaîne) d'options → valeur séparée par des virgules.

    Raises:
        ValueError: ni chaîne, ni liste/tuple.
    """
    if isinstance(valeur, str):
        return valeur
    if isinstance(valeur, (list, tuple)):
        return ",".join(str(element) for element in valeur)
    raise ValueError(_("sortie victorialogs : liste de champs attendue, reçu {}").format(type(valeur).__name__))
