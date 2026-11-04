# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie protocole Splunk HEC (issue #28) : `POST /services/collector/event`.

Splunk n'est pas open source : cette sortie vise le **protocole** HTTP
Event Collector, accepté aussi par des systèmes open source comme
SigLens (port d'ingestion 8081, banc de conformité) ; Splunk l'écoute
sur le port 8088. Chaque document devient une enveloppe HEC, les
enveloppes du lot sont concaténées (une par ligne) dans le corps :

```
{"event": {...document...}, "time": 1793823288.123, "host": "srv1",
 "source": "rsyslog", "sourcetype": "syslog", "index": "main",
 "fields": {"severity": "info"}}
```

| Option | Champ HEC | Rôle |
|---|---|---|
| `token` | en-tête `Authorization: Splunk <jeton>` | jeton HEC (obligatoire) |
| `sourcetype` | `sourcetype` | type de source |
| `source` | `source` | source |
| `host` | `host` | hôte fixe ; sinon le champ `host` du document (chaîne, ou `host.name`) |
| `index` | `index` | index Splunk (gabarit admis) ; absent : index par défaut du jeton |
| `time_field` | `time` | champ ISO 8601 du document (`@timestamp`) converti en secondes epoch |
| `fields` | `fields` | champs du document recopiés en champs indexés (valeurs en chaînes) |
| `channel` | en-tête `X-Splunk-Request-Channel` | canal (exigé par Splunk quand l'accusé d'indexation est actif) |

L'authentification basique commune (`user`/`password`) est refusée dès
la compilation : elle remplacerait l'en-tête `Authorization` du jeton.

## Accusé de réception

HEC n'accuse pas réception événement par événement : `200
{"text": "Success", "code": 0}` pour le lot entier, sinon un code
d'erreur pour tout le lot (`invalid-event-number` désigne seulement le
premier événement fautif). La sortie décide donc sur le **statut HTTP
global** (`accuse_par_evenement=False`) : 2xx = lot ingéré ; 400 (format
invalide, événement vide) = échec permanent du lot entier (DLQ si
configurée) ; réseau, 429, 503 (serveur occupé) = transitoire ; 401/403
(jeton absent ou refusé) = lot rendu à l'appelant. L'accusé
d'indexation différé (`/services/collector/ack`) n'est pas suivi.

## Gabarit de pipeline d'exemple

```
output {
  splunk_hec {
    hosts => ["https://siglens:8088"]
    token => "${HEC_TOKEN}"
    sourcetype => "syslog"
  }
}
```
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, ClassVar

import httpx
from loguru import logger

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.tracing import atraced, traced

# Point d'entrée des événements JSON du protocole HEC.
CHEMIN_EVENEMENTS = "/services/collector/event"
# Santé du collecteur (Splunk et SigLens).
CHEMIN_SANTE = "/services/collector/health"


@traced
def epoch_secondes(brut: Any) -> float | None:
    """Secondes epoch (à la milliseconde) d'un horodatage ISO 8601, ou `None`.

    Une valeur absente ou illisible donne `None` : le champ `time` est
    alors omis et le serveur date l'événement à sa réception.
    """
    if not isinstance(brut, str):
        return None
    try:
        moment = datetime.fromisoformat(brut.replace("Z", "+00:00"))
    except ValueError:
        logger.warning(_("sortie splunk_hec : horodatage {} illisible, heure du serveur utilisée"), brut)
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return round(moment.timestamp(), 3)


@traced
def hote_du_document(doc: dict[str, Any]) -> str | None:
    """Hôte de l'événement : `host` chaîne, ou `host.name` (ECS), sinon `None`."""
    hote = doc.get("host")
    if isinstance(hote, dict):
        hote = hote.get("name")
    return hote if isinstance(hote, str) and hote else None


@enregistrer
class SplunkHecOutput(ElasticsearchBulkOutput):
    """Sortie protocole HEC : enveloppes JSON concaténées, statut HTTP global."""

    plugin = "splunk_hec"
    capacites = Capacites(
        actions=frozenset({"index"}),
        accepte_id=False,
        accuse_par_evenement=False,
        taille_max_requete=None,
    )
    # Options du client _bulk sans objet en HEC : `pipeline`, `routing`,
    # `doc_as_upsert` (propres à l'API _bulk) ; `index` change de sens
    # (index Splunk, sans valeur par défaut).
    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = dict(
        {
            nom: spec
            for nom, spec in ElasticsearchBulkOutput.OPTIONS_PROPRES.items()
            if nom not in ("index", "pipeline", "routing", "doc_as_upsert")
        },
        action=OptionSpec("action", (str,), "index", valeurs=("index",), description="Insertions seules (index)"),
        token=OptionSpec("token", (str,), requis=True, description="Jeton HEC (en-tête Authorization: Splunk <jeton>)"),
        sourcetype=OptionSpec("sourcetype", (str,), description="Type de source HEC (sourcetype)"),
        source=OptionSpec("source", (str,), description="Source HEC (source)"),
        host=OptionSpec("host", (str,), description="Hôte HEC fixe (sinon le champ host du document)"),
        index=OptionSpec("index", (str,), description="Index Splunk (absent : index par défaut du jeton)"),
        time_field=OptionSpec(
            "time_field", (str,), "@timestamp", description="Champ ISO 8601 converti en time HEC (secondes epoch)"
        ),
        fields=OptionSpec("fields", (list,), [], description="Champs du document recopiés en champs indexés HEC"),
        channel=OptionSpec("channel", (str,), description="Canal HEC (en-tête X-Splunk-Request-Channel)"),
    )
    en_tetes_par_defaut: ClassVar[dict[str, str]] = {"Content-Type": "application/json"}

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        dlq: LocalDeadLetterQueue | None = None,
        transport: Any = None,
    ) -> None:
        """Valide le jeton et les champs indexés, prépare les en-têtes HEC.

        Raises:
            ValueError: `token` absent ou vide, `user` donné (il
                remplacerait le jeton), `fields` qui n'est pas une liste
                de noms, option inconnue ou valeur invalide (schéma
                commun, #8).
        """
        super().__init__(options, dlq=dlq, transport=transport)
        if options.get("user"):
            logger.warning(_("sortie splunk_hec : authentification basique refusée"))
            raise ValueError(_("sortie splunk_hec : user/password remplaceraient le jeton HEC, utiliser token seul"))
        jeton = str(options.get("token") or "")
        if not jeton:
            logger.warning(_("sortie splunk_hec : jeton vide"))
            raise ValueError(_("sortie splunk_hec : token est requis (jeton HEC)"))
        champs = list(options.get("fields") or [])
        if not all(isinstance(nom, str) and nom for nom in champs):
            logger.warning(_("sortie splunk_hec : fields invalide {}"), champs)
            raise ValueError(_("sortie splunk_hec : fields doit être une liste de noms de champs"))
        self._champs_indexes: list[str] = champs
        self._champ_horodatage = str(options.get("time_field") or "@timestamp")
        self._index_splunk = bool(options.get("index"))
        self._en_tetes: dict[str, str] = {"Authorization": f"Splunk {jeton}"}
        if options.get("channel"):
            self._en_tetes["X-Splunk-Request-Channel"] = str(options["channel"])
        logger.debug(
            "SplunkHecOutput : sourcetype {}, index {}, champs indexés {}, canal {}",
            options.get("sourcetype"),
            options.get("index"),
            champs,
            bool(options.get("channel")),
        )

    @traced
    def _client_http(self) -> httpx.AsyncClient:
        """Client du client commun, plus `Authorization: Splunk` (et le canal).

        Un en-tête de même nom passé dans `headers` prime.
        """
        client = super()._client_http()
        for nom, valeur in self._en_tetes.items():
            if nom not in client.headers:
                client.headers[nom] = valeur
        return client

    @traced
    def _chemin_envoi(self) -> str:
        """`/services/collector/event`."""
        return CHEMIN_EVENEMENTS

    @traced
    def _enveloppe(self, pending: dict[str, Any]) -> dict[str, Any]:
        """Enveloppe HEC d'un document : `event` plus les métadonnées présentes."""
        doc = pending["doc"]
        enveloppe: dict[str, Any] = {"event": doc}
        instant = epoch_secondes(doc.get(self._champ_horodatage))
        if instant is not None:
            enveloppe["time"] = instant
        hote = self.options.get("host") or hote_du_document(doc)
        if hote:
            enveloppe["host"] = hote
        for cle in ("source", "sourcetype"):
            if self.options.get(cle):
                enveloppe[cle] = self.options[cle]
        if self._index_splunk:
            enveloppe["index"] = pending["index"]
        champs = {nom: str(doc[nom]) for nom in self._champs_indexes if doc.get(nom) is not None}
        if champs:
            enveloppe["fields"] = champs
        return enveloppe

    @traced
    def _corps_requete(self, pendings: list[dict[str, Any]]) -> str:
        """Enveloppes HEC concaténées, une par ligne (lot jamais vide)."""
        return "\n".join(json.dumps(self._enveloppe(pending), ensure_ascii=False) for pending in pendings)

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge `GET /services/collector/health`.

        Returns:
            `{"status": "healthy", "reponse": ...}` quand le collecteur
            répond 2xx (corps JSON du serveur, ou `None` s'il n'en a pas).

        Raises:
            httpx.HTTPStatusError: statut d'erreur (collecteur indisponible).
        """
        reponse = await self._client_http().get(CHEMIN_SANTE)
        reponse.raise_for_status()
        try:
            corps = reponse.json()
        except ValueError:
            logger.debug("SplunkHecOutput.health | corps non JSON")
            corps = None
        return {"status": "healthy", "reponse": corps}
