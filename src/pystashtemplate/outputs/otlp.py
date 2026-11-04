# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie OpenTelemetry OTLP/HTTP, journaux (issue #22).

`POST {hosts}/v1/logs` (port 4318 par défaut), en protobuf
(`application/x-protobuf`, défaut de la spécification) ou en JSON
(`encoding => "json"`). Un seul client couvre tous les récepteurs OTLP :
OpenTelemetry Collector, SigNoz, Uptrace, HyperDX/ClickStack,
OpenObserve, Quickwit, GreptimeDB, VictoriaLogs, Parseable, Data
Prepper…

Sous-classe du client HTTP commun (#11) : tampon (`batch_size`),
réessais (`retry_max`, `retry_backoff`), DLQ et métriques sont ceux de
la famille `_bulk` ; seuls le corps, le chemin et l'analyse de la réponse
diffèrent.

Correspondance événement → `LogRecord` (option `fields`, fusionnée avec
la table par défaut ; clé : champ de l'événement, valeur : cible) :

| Cible | Effet |
|---|---|
| `body` | corps du journal (défaut : `message`) |
| `time` | `time_unix_nano` : date ISO 8601 ou nombre (défaut : `@timestamp`) |
| `severity` | `severity_text` et `severity_number` : nom usuel ou code syslog 0–7 (défaut : `severity`) |
| `trace_id`, `span_id` | identifiants hexadécimaux (défaut : champs de même nom) |
| `resource.<nom>` | attribut de ressource `<nom>` (les journaux sont groupés par ressource) |
| `attributes.<nom>` | attribut du journal sous le nom `<nom>` |
| `drop` | champ non transmis |

Tout autre champ devient un attribut du journal sous son propre nom ;
`observed_time_unix_nano` est l'heure d'envoi. Attributs de ressource
fixes : option `resource` (défaut `service.name => "pystashtemplate"`).

Accusé de réception **global** : OTLP ne rapporte pas d'erreur par
journal. Statut HTTP (2xx accepté ; 400 refus définitif → DLQ ;
429/5xx retentés), puis `partial_success` : si tous les journaux du lot
sont refusés, ils partent en DLQ ; si une partie seulement l'est, les
journaux refusés ne sont pas identifiables (la spécification interdit
de les renvoyer) — ils sont comptés dans la métrique `rejected` et
journalisés avec le message du serveur, le reste du lot compté indexé.

| Option propre | Rôle |
|---|---|
| `encoding` | `protobuf` (défaut) ou `json` |
| `resource` | attributs de ressource fixes |
| `fields` | table de correspondance champ → cible (ci-dessus) |
| `scope` | nom de la portée d'instrumentation (défaut : `pystashtemplate`) |
| `timeout`, `headers`, `dlq_transitoires` | comme la famille `_bulk` |

`compression => true` compresse le corps en gzip (`Content-Encoding`).

## Gabarit de pipeline d'exemple

```
output {
  otlp {
    hosts => ["http://otel-collector:4318"]
    encoding => "protobuf"
    resource => { "service.name" => "rsyslog" }
    fields => { "host" => "resource.host.name" "program" => "attributes.process.executable.name" }
  }
}
```
"""

from __future__ import annotations

import gzip
import time
from typing import Any, ClassVar

import httpx
from loguru import logger

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs import otlp_codec as codec
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput, SendOutcome
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.tracing import atraced, traced

ENCODAGES: dict[str, str] = {"protobuf": "application/x-protobuf", "json": "application/json"}

CHEMIN_JOURNAUX = "/v1/logs"

# Table de correspondance par défaut (option `fields`, fusionnée par-dessus).
CORRESPONDANCE_PAR_DEFAUT: dict[str, str] = {
    "message": "body",
    "@timestamp": "time",
    "severity": "severity",
    "trace_id": "trace_id",
    "span_id": "span_id",
}

CIBLES_SIMPLES = frozenset({"body", "time", "severity", "trace_id", "span_id", "drop"})

RESSOURCE_PAR_DEFAUT: dict[str, Any] = {"service.name": "pystashtemplate"}


@enregistrer
class OtlpOutput(ElasticsearchBulkOutput):
    """Sortie OTLP/HTTP : `/v1/logs`, protobuf ou JSON, accusé global et `partial_success`."""

    plugin = "otlp"
    capacites = Capacites(
        actions=frozenset({"index", "create"}),
        accepte_id=False,
        accuse_par_evenement=False,
        taille_max_requete=None,
    )
    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = {
        "encoding": OptionSpec(
            "encoding", (str,), "protobuf", valeurs=tuple(ENCODAGES), description="Encodage OTLP (protobuf, json)"
        ),
        "resource": OptionSpec("resource", (dict,), description="Attributs de ressource fixes"),
        "fields": OptionSpec("fields", (dict,), description="Correspondance champ → cible du LogRecord"),
        "scope": OptionSpec("scope", (str,), "pystashtemplate", description="Nom de la portée d'instrumentation"),
        "timeout": ElasticsearchBulkOutput.OPTIONS_PROPRES["timeout"],
        "headers": ElasticsearchBulkOutput.OPTIONS_PROPRES["headers"],
        "dlq_transitoires": ElasticsearchBulkOutput.OPTIONS_PROPRES["dlq_transitoires"],
    }

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        dlq: LocalDeadLetterQueue | None = None,
        transport: Any = None,
    ) -> None:
        """Valide l'encodage et la table de correspondance.

        Raises:
            ValueError: encodage inconnu, cible de `fields` inconnue, ou
                option inconnue ou invalide.
        """
        super().__init__(options, dlq=dlq, transport=transport)
        self._encodage = str(self.options.get("encoding", "protobuf"))
        self._compression = bool(self.options.get("compression", False))
        self._scope = str(self.options.get("scope", "pystashtemplate"))
        ressource = self.options.get("resource")
        self._ressource: dict[str, Any] = dict(RESSOURCE_PAR_DEFAUT if ressource is None else ressource)
        self._correspondance = dict(CORRESPONDANCE_PAR_DEFAUT)
        for champ, cible in (self.options.get("fields") or {}).items():
            cible = str(cible)
            nom = cible.partition(".")[2]
            if cible not in CIBLES_SIMPLES and not (cible.startswith(("resource.", "attributes.")) and nom):
                logger.warning(_("sortie otlp : cible {cible} inconnue pour {champ}"), cible=cible, champ=champ)
                attendu = ", ".join([*sorted(CIBLES_SIMPLES), "resource.<nom>", "attributes.<nom>"])
                raise ValueError(
                    _("sortie otlp : cible « {} » inconnue pour le champ « {} » (attendu : {})").format(
                        cible, champ, attendu
                    )
                )
            self._correspondance[str(champ)] = cible
        en_tetes = {"Content-Type": ENCODAGES[self._encodage]}
        if self._compression:
            en_tetes["Content-Encoding"] = "gzip"
        self.en_tetes_par_defaut = en_tetes  # type: ignore[misc]
        self._corps_du_lot: tuple[list[dict[str, Any]], bytes] | None = None
        logger.debug("OtlpOutput : encodage={} correspondance={}", self._encodage, self._correspondance)

    @traced
    def _chemin_envoi(self) -> str:
        """`/v1/logs` (chemin fixe de la spécification OTLP/HTTP)."""
        return CHEMIN_JOURNAUX

    @traced
    def _journal(self, doc: dict[str, Any], observe: int) -> tuple[tuple[tuple[str, str], ...], dict[str, Any]]:
        """`(clé de ressource, LogRecord JSON)` d'un document, selon la table de correspondance."""
        journal: dict[str, Any] = {"observedTimeUnixNano": str(observe)}
        ressource: dict[str, Any] = {}
        attrs: dict[str, Any] = {}
        for champ, v in doc.items():
            cible = self._correspondance.get(champ, f"attributes.{champ}")
            if cible == "drop":
                continue
            if cible == "body":
                journal["body"] = codec.valeur(v)
            elif cible == "time":
                ns = codec.horodatage_ns(v)
                if ns is None:
                    attrs[champ] = v
                else:
                    journal["timeUnixNano"] = str(ns)
            elif cible == "severity":
                numero, texte = codec.severite(v)
                journal["severityNumber"] = numero
                journal["severityText"] = texte
            elif cible in ("trace_id", "span_id"):
                hexa = codec.identifiant_hex(v, 16 if cible == "trace_id" else 8)
                if hexa is None:
                    attrs[champ] = v
                else:
                    journal["traceId" if cible == "trace_id" else "spanId"] = hexa
            elif cible.startswith("resource."):
                ressource[cible.partition(".")[2]] = v
            else:
                attrs[cible.partition(".")[2]] = v
        if attrs:
            journal["attributes"] = codec.attributs(attrs)
        cle = tuple(sorted((k, repr(v)) for k, v in ressource.items()))
        logger.debug("_journal | sortie")
        return cle, {"ressource": ressource, "journal": journal}

    @traced
    def _requete(self, pendings: list[dict[str, Any]]) -> dict[str, Any]:
        """`ExportLogsServiceRequest` (forme JSON), journaux groupés par ressource, ordre conservé."""
        observe = time.time_ns()
        groupes: dict[tuple[tuple[str, str], ...], dict[str, Any]] = {}
        for pending in pendings:
            cle, element = self._journal(pending["doc"], observe)
            groupe = groupes.setdefault(cle, {"ressource": element["ressource"], "journaux": []})
            groupe["journaux"].append(element["journal"])
        resource_logs = [
            {
                "resource": {"attributes": codec.attributs({**self._ressource, **groupe["ressource"]})},
                "scopeLogs": [{"scope": {"name": self._scope}, "logRecords": groupe["journaux"]}],
            }
            for groupe in groupes.values()
        ]
        logger.debug("_requete | {} ressource(s)", len(resource_logs))
        return {"resourceLogs": resource_logs}

    @traced
    def _corps_requete(self, pendings: list[dict[str, Any]]) -> bytes:  # type: ignore[override]
        """Corps OTLP du lot (protobuf ou JSON), compressé en gzip si demandé.

        Le corps déjà calculé pour ce même lot (contrôle de codage de
        `_issues_du_lot`) est réutilisé, réessais compris.
        """
        if self._corps_du_lot is not None and self._corps_du_lot[0] is pendings:
            logger.debug("_corps_requete | corps réutilisé")
            return self._corps_du_lot[1]
        requete = self._requete(pendings)
        corps = codec.corps_json(requete) if self._encodage == "json" else codec.corps_protobuf(requete)
        if self._compression:
            corps = gzip.compress(corps)
        self._corps_du_lot = (pendings, corps)
        logger.debug("_corps_requete | {} octet(s)", len(corps))
        return corps

    @atraced
    async def _issues_du_lot(self, pendings: list[dict[str, Any]]) -> list[SendOutcome]:
        """Lot codé puis envoyé ; un document impossible à coder (NaN en JSON) part seul en DLQ.

        Le codage JSON strict refuse les flottants non finis : le lot est
        alors recodé document par document pour n'écarter que le fautif.
        """
        try:
            self._corps_requete(pendings)
        except codec.ErreurCodageOtlp as exc:
            logger.warning(_("sortie otlp : lot impossible à coder erreur={error}"), error=exc)
            if len(pendings) == 1:
                return [await self._permanent(pendings[0], str(exc))]
            issues: list[SendOutcome] = []
            for pending in pendings:
                issues.extend(await self._issues_du_lot([pending]))
            return issues
        return await super()._issues_du_lot(pendings)

    @atraced
    async def _analyser_reponse(self, pendings: list[dict[str, Any]], response: httpx.Response) -> list[SendOutcome]:
        """Statut global (analyse commune), puis `partial_success` d'une réponse 2xx."""
        issues = await super()._analyser_reponse(pendings, response)
        if not 200 <= response.status_code < 300:
            return issues
        try:
            rejetes, message = self._succes_partiel(response)
        except ValueError as exc:
            logger.warning(_("réponse OTLP illisible, lot compté accepté erreur={error}"), error=exc)
            return issues
        if rejetes <= 0:
            return issues
        if rejetes >= len(pendings):
            logger.warning(_("réponse OTLP : lot entier refusé message={message}"), message=message)
            return [await self._permanent(pending, message or "partial_success") for pending in pendings]
        logger.warning(
            _("réponse OTLP : {rejetes} journal(aux) refusé(s) sur {total}, non identifiables message={message}"),
            rejetes=rejetes,
            total=len(pendings),
            message=message,
        )
        self.observateur.evenements(self.nom, "rejected", rejetes)
        return issues

    @traced
    def _succes_partiel(self, response: httpx.Response) -> tuple[int, str]:
        """`partial_success` de la réponse, selon son type de contenu.

        Raises:
            ValueError: réponse illisible.
        """
        if not response.content:
            return 0, ""
        if "json" in response.headers.get("content-type", ""):
            return codec.succes_partiel_json(response.json())
        return codec.succes_partiel_protobuf(response.content)

    @atraced
    async def health(self) -> dict[str, Any]:
        """Envoie une requête OTLP vide (aucun journal) : OTLP n'a pas de point de santé.

        Returns:
            `{"status": <code HTTP>}` si le récepteur l'accepte.

        Raises:
            httpx.HTTPStatusError: statut d'erreur.
        """
        corps = codec.corps_json({}) if self._encodage == "json" else b""
        if self._compression:
            corps = gzip.compress(corps)
        response = await self._client_http().post(CHEMIN_JOURNAUX, content=corps)
        response.raise_for_status()
        return {"status": response.status_code}
