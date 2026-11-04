# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie ClickHouse (issue #23) : ingestion par l'interface HTTP.

`POST /` avec la requête dans le paramètre `query` (port 8123 par
défaut) :

```
POST /?query=INSERT%20INTO%20<base>.<table>%20FORMAT%20JSONEachRow
<document JSON par ligne>…
```

- **schéma de table colonne par colonne** : le document est envoyé tel
  quel en JSON Each Row — ClickHouse fait la correspondance champ →
  colonne (les colonnes manquantes prennent leur défaut) ; un champ
  sans colonne est écarté (`input_format_skip_unknown_fields=1`) ou
  fait échouer le lot (`champs_inconnus => "erreur"` : aucun document
  n'est isolé, le lot entier part en DLQ) ;
- **insertions asynchrones** (`async_insert`) : recommandées par
  ClickHouse pour les petits lots (le serveur tamponne et regroupe) ;
  désactivées par défaut pour une visibilité immédiate à la lecture ;
- `database` précise la base (défaut : `default`) — la table doit
  exister, ClickHouse ne la crée pas à l'ingestion ;
- l'authentification passe par les options communes (`user`,
  `password`, `ssl`, `cacert`…) ; les en-têtes HTTP additionnels
  (ex. `X-ClickHouse-Key`) passent par `headers`.

## Accusé de réception

ClickHouse n'accuse pas réception ligne par ligne : l'INSERT est
atomique, la décision se prend sur le **statut HTTP global**
(`accuse_par_evenement=False`) : 2xx = lot ingéré ; 404 (table ou base
absente), 400 (requête refusée) = échec permanent (DLQ si configurée) ;
réseau, 429, 5xx = transitoire. Un lot rejeté part en entier (aucun
document isolé) : rejouer le lot tel quel n'aboutit jamais.

## Gabarit de pipeline d'exemple

```
output {
  clickhouse {
    hosts => ["http://clickhouse:8123"]
    table => "logs"
    database => "rsyslog"
    async_insert => true
  }
}
```
"""

from __future__ import annotations

import json
from typing import Any, ClassVar

from loguru import logger

from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.tracing import atraced, traced

CHAMPS_INCONNUS = ("ignorer", "erreur")


@enregistrer
class ClickHouseOutput(ElasticsearchBulkOutput):
    """Sortie ClickHouse : INSERT JSONEachRow par HTTP, statut HTTP global."""

    plugin = "clickhouse"
    capacites = Capacites(
        actions=frozenset({"index"}),
        accepte_id=False,
        accuse_par_evenement=False,
        taille_max_requete=None,
    )
    # Options du client _bulk qui n'existent pas chez ClickHouse : `index`
    # (la base et la table portent les données), `action` (insertions
    # seules), `pipeline`, `routing`, `doc_as_upsert`.
    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = dict(
        {
            nom: spec
            for nom, spec in ElasticsearchBulkOutput.OPTIONS_PROPRES.items()
            if nom not in ("index", "action", "pipeline", "routing", "doc_as_upsert")
        },
        table=OptionSpec(
            "table",
            (str,),
            description="Table cible (doit exister)",
            requis=True,
        ),
        database=OptionSpec(
            "database",
            (str,),
            "default",
            description="Base de la table cible (default par défaut)",
        ),
        async_insert=OptionSpec(
            "async_insert",
            (bool,),
            False,
            description="Insertions asynchrones côté serveur (recommandé pour les petits lots)",
        ),
        champs_inconnus=OptionSpec(
            "champs_inconnus",
            (str,),
            "ignorer",
            valeurs=CHAMPS_INCONNUS,
            description="Champ sans colonne : ignorer (défaut) ou erreur",
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
        """Prépare la requête INSERT (table, base, réglages serveur).

        Raises:
            ValueError: Option inconnue ou valeur invalide (schéma commun,
                #8) — `table` est requise, `index`/`action`/`pipeline`/
                `routing`/`doc_as_upsert` sont refusés.
        """
        options = dict(options)
        # La validation du schéma (super) lève si `table` manque : lu
        # seulement après, l'accès est sûr.
        super().__init__(options, dlq=dlq, transport=transport)
        self._table = str(options["table"])
        self._database = str(options.get("database") or "default")
        self._async_insert = bool(options.get("async_insert", False))
        self._champs_inconnus = str(options.get("champs_inconnus") or "ignorer")
        logger.debug(
            "ClickHouseOutput : {}.{} (async_insert={}, champs_inconnus={})",
            self._database,
            self._table,
            self._async_insert,
            self._champs_inconnus,
        )

    @traced
    def _chemin_envoi(self) -> str:
        """Racine de l'interface HTTP : la requête part en paramètre `query`."""
        return "/"

    @traced
    def _parametres_requete(self) -> dict[str, str]:
        """Requête INSERT et réglages serveur, en paramètres d'URL.

        `query` porte l'INSERT (`FORMAT JSONEachRow` : une ligne JSON
        par document dans le corps), les autres paramètres sont des
        réglages serveur (`async_insert`, `input_format_skip_unknown_fields`).
        """
        parametres: dict[str, str] = {"query": f"INSERT INTO {self._database}.{self._table} FORMAT JSONEachRow"}
        if self._async_insert:
            parametres["async_insert"] = "1"
        if self._champs_inconnus == "ignorer":
            parametres["input_format_skip_unknown_fields"] = "1"
        return parametres

    @traced
    def _corps_requete(self, pendings: list[dict[str, Any]]) -> str:
        """Une ligne JSON par document (JSON Each Row), le document tel quel."""
        return "".join(json.dumps(pending["doc"], ensure_ascii=False) + "\n" for pending in pendings)

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge `GET /ping` (ClickHouse répond `Ok.`).

        Returns:
            `{"status": "Ok."}` quand le service répond.

        Raises:
            httpx.HTTPStatusError: statut d'erreur.
        """
        response = await self._client_http().get("/ping")
        response.raise_for_status()
        return {"status": response.text.strip()}
