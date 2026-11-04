# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie Loki (issue #21) : API push de Grafana Loki (modèle par labels).

`POST /loki/api/v1/push` en JSON (`Content-Type: application/json`),
le protocole accepté aussi par VictoriaLogs, GreptimeDB, OpenObserve,
SigLens et Alloy :

- chaque document devient une ligne d'un flux ; les **labels** du flux
  sont choisis dans la configuration (faible cardinalité), le document
  entier part dans la ligne de log (JSON, sans perte) ;
- la valeur d'un label est rendue contre chaque document (gabarit
  sprintf `%{host}`) ; un label rendu vide est écarté (Loki refuse les
  valeurs vides) ;
- les lignes d'un même flux sont triées par horodatage (celui du
  document `@timestamp`, l'heure d'envoi sinon), en nanosecondes ;
- multi-tenant : en-tête `X-Scope-OrgID` quand `tenant_id` est donné ;
- **cardinalité des labels** : chaque flux distinct coûte cher à Loki.
  Un avertissement est journalisé à la construction quand un label
  référence un champ connu pour varier à chaque événement (identifiants,
  adresses IP, ports, message, horodatage, URL…, voir
  `CHAMPS_FORTE_CARDINALITE`), puis une fois à l'exécution quand un label
  dépasse `SEUIL_CARDINALITE` valeurs distinctes. Ces champs ont leur
  place dans la ligne de log, pas dans les labels.

## Accusé de réception

Loki n'accuse pas réception élément par élément : `204 No Content` (ou
2xx) = lot ingéré, décision sur le **statut HTTP global**
(`accuse_par_evenement=False`) : 400 = échec permanent (DLQ si
configurée) ; réseau, 429, 5xx = transitoire, retenté puis rendu à
l'appelant. Insertions seules : `index` est la seule action acceptée,
`_id` refusé (dégradation documentée du client commun).

## Gabarit de pipeline d'exemple

```
output {
  loki {
    hosts => ["http://loki:3100"]
    labels => { "job" => "rsyslog" "host" => "%{host}" }
    tenant_id => "${LOKI_TENANT}"
  }
}
```
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any, ClassVar

import httpx
from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.templates import render_template
from pystashtemplate.tracing import atraced, traced

# Champ du document lu comme horodatage (ISO 8601) ; l'heure d'envoi
# sinon (Loki veut des nanosecondes).
CHAMP_HORODATAGE = "@timestamp"


# Champs (dernier segment du nom, en minuscules) qui varient presque à
# chaque événement : en faire un label crée un flux Loki par valeur.
CHAMPS_FORTE_CARDINALITE: frozenset[str] = frozenset(
    {
        "@timestamp",
        "timestamp",
        "message",
        "msg",
        "event.original",
        "original",
        "id",
        "_id",
        "uuid",
        "guid",
        "request_id",
        "trace_id",
        "span_id",
        "session_id",
        "transaction_id",
        "user_id",
        "pid",
        "ip",
        "src_ip",
        "dst_ip",
        "client_ip",
        "source_ip",
        "destination_ip",
        "fromhost-ip",
        "port",
        "src_port",
        "dst_port",
        "source_port",
        "destination_port",
        "url",
        "uri",
        "path",
        "query",
        "user_agent",
        "duration",
        "bytes",
        "offset",
        "sequence",
    }
)
# Au-delà de ce nombre de valeurs distinctes vues pour un label, la
# sortie avertit (une fois par label) que sa cardinalité est trop forte.
SEUIL_CARDINALITE = 100

# Références de champ d'un gabarit sprintf : `%{host}`, `%{[a][b]}`
# (les formats de date `%{+YYYY}` ne sont pas des champs).
_REFERENCE = re.compile(r"%\{([^}+][^}]*)\}")


@traced
def champs_forte_cardinalite(modele: Any) -> list[str]:
    """Champs à forte cardinalité référencés par un gabarit de label.

    Un nom est comparé, en minuscules, en entier puis par son dernier
    segment (`[client][ip]`, `client.ip` → `ip`) ; tout nom en `_id`
    compte aussi.

    Returns:
        Les références fautives, dans l'ordre du gabarit (vide pour une
        valeur fixe).
    """
    if not isinstance(modele, str):
        return []
    fautifs: list[str] = []
    for reference in _REFERENCE.findall(modele):
        nom = reference.strip().lower()
        segments = [seg for seg in re.split(r"[\[\].]+", nom) if seg]
        dernier = segments[-1] if segments else nom
        if nom in CHAMPS_FORTE_CARDINALITE or dernier in CHAMPS_FORTE_CARDINALITE or dernier.endswith("_id"):
            fautifs.append(reference)
    return fautifs


@traced
def _horodatage_ns(doc: dict[str, Any]) -> int:
    """Nanosecondes de l'horodatage du document, ou de maintenant.

    Un `@timestamp` ISO 8601 (avec `Z`) est analysé ; toute valeur
    illisible (absente, autre format) retombe sur l'heure d'envoi : la
    ligne arrive avec les autres de son lot, l'ordre du flux est
    préservé par le tri.
    """
    brut = doc.get(CHAMP_HORODATAGE)
    if isinstance(brut, str):
        try:
            moment = datetime.fromisoformat(brut.replace("Z", "+00:00"))
        except ValueError:
            logger.warning(_("@timestamp {} illisible, heure d'envoi utilisée"), brut)
            moment = None
        if moment is not None:
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=timezone.utc)
            return int(moment.timestamp() * 1_000_000_000)
    return time.time_ns()


@enregistrer
class LokiOutput(ElasticsearchBulkOutput):
    """Sortie Loki : push JSON par flux de labels, statut HTTP global."""

    plugin = "loki"
    capacites = Capacites(
        actions=frozenset({"index"}),
        accepte_id=False,
        accuse_par_evenement=False,
        taille_max_requete=None,
    )
    # Options du client _bulk qui n'existent pas chez Loki : `index`
    # (les flux portent les labels, pas d'index), `pipeline`, `routing`,
    # `doc_as_upsert` (spécifiques à l'API _bulk).
    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = dict(
        {
            nom: spec
            for nom, spec in ElasticsearchBulkOutput.OPTIONS_PROPRES.items()
            if nom not in ("index", "pipeline", "routing", "doc_as_upsert")
        },
        action=OptionSpec(
            "action",
            (str,),
            "index",
            valeurs=("index",),
            description="Insertions seules (index)",
        ),
        labels=OptionSpec(
            "labels",
            (dict,),
            {},
            description="Labels des flux Loki (gabarits sprintf, faible cardinalité)",
        ),
        tenant_id=OptionSpec(
            "tenant_id",
            (str,),
            description="Locataire multi-tenant (en-tête X-Scope-OrgID)",
        ),
    )
    # Push JSON (et non NDJSON _bulk) : en-tête de requête du produit.
    en_tetes_par_defaut: ClassVar[dict[str, str]] = {"Content-Type": "application/json"}

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        dlq: Any = None,
        transport: Any = None,
    ) -> None:
        """Prépare les labels, l'entête multi-tenant et le client HTTP commun.

        Raises:
            ValueError: Option inconnue ou valeur invalide (schéma commun,
                #8) — `index`/`pipeline`/`routing`/`doc_as_upsert` sont
                refusés ici.
        """
        options = dict(options)
        self._labels_modele: dict[str, Any] = dict(options.get("labels") or {})
        self._tenant = str(options["tenant_id"]) if options.get("tenant_id") else None
        super().__init__(options, dlq=dlq, transport=transport)
        # Valeurs distinctes vues par label (bornées à SEUIL_CARDINALITE + 1) ;
        # un label déjà signalé n'est plus suivi.
        self._valeurs_vues: dict[str, set[str]] = {str(nom): set() for nom in self._labels_modele}
        self._labels_signales: set[str] = set()
        for nom, modele in self._labels_modele.items():
            fautifs = champs_forte_cardinalite(modele)
            if fautifs:
                logger.warning(
                    _("sortie loki : label {} sur {}, champ à forte cardinalité (un flux par valeur)"),
                    nom,
                    ", ".join(fautifs),
                )
        logger.debug("LokiOutput : labels {}, tenant {}", self._labels_modele, bool(self._tenant))

    @traced
    def _client_http(self) -> httpx.AsyncClient:
        """Client du client commun, plus `X-Scope-OrgID` si un locataire est configuré."""
        client = super()._client_http()
        if self._tenant and "X-Scope-OrgID" not in client.headers:
            client.headers["X-Scope-OrgID"] = self._tenant
        return client

    @traced
    def _labels(self, doc: dict[str, Any]) -> tuple[tuple[str, str], ...]:
        """Labels du flux pour `doc`, rendus contre lui et triés par nom.

        Un label rendu vide est écarté : Loki refuse une valeur vide.
        """
        rendus: list[tuple[str, str]] = []
        for nom, modele in self._labels_modele.items():
            if isinstance(modele, str):
                valeur = render_template(modele, doc, on_missing="empty")
            else:
                valeur = "" if modele is None else str(modele)
            if valeur:
                rendus.append((str(nom), valeur))
                self._suivre_cardinalite(str(nom), valeur)
        return tuple(sorted(rendus))

    @traced
    def _suivre_cardinalite(self, nom: str, valeur: str) -> None:
        """Compte les valeurs distinctes de `nom` ; avertit une fois au-delà du seuil."""
        if nom in self._labels_signales:
            return
        vues = self._valeurs_vues.setdefault(nom, set())
        vues.add(valeur)
        if len(vues) > SEUIL_CARDINALITE:
            self._labels_signales.add(nom)
            vues.clear()
            logger.warning(
                _("sortie loki : label {} à plus de {} valeurs distinctes, cardinalité trop forte"),
                nom,
                SEUIL_CARDINALITE,
            )

    @traced
    def _corps_requete(self, pendings: list[dict[str, Any]]) -> str:
        """Corps JSON du push : un flux par jeu de labels, lignes triées par date."""
        flux: dict[tuple[tuple[str, str], ...], list[list[str]]] = {}
        for pending in pendings:
            labels = self._labels(pending["doc"])
            ligne = json.dumps(pending["doc"], ensure_ascii=False)
            flux.setdefault(labels, []).append([str(_horodatage_ns(pending["doc"])), ligne])
        streams = [
            {"stream": dict(labels), "values": sorted(valeurs, key=lambda v: int(v[0]))}
            for labels, valeurs in flux.items()
        ]
        return json.dumps({"streams": streams}, ensure_ascii=False)

    @traced
    def _chemin_envoi(self) -> str:
        """`/loki/api/v1/push`."""
        return "/loki/api/v1/push"

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge `GET /ready` (Loki : 200 quand le service accepte des écritures).

        Returns:
            `{"status": "ready"}` quand Loki répond 200.

        Raises:
            httpx.HTTPStatusError: statut d'erreur (503 tant que Loki
                n'est pas prêt).
        """
        response = await self._client_http().get("/ready")
        response.raise_for_status()
        return {"status": "ready"}
