# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie Apache Doris (issue #27) : chargement par Stream Load HTTP.

Doris (et StarRocks, qui en descend et garde la même interface) charge
des lignes par `PUT /api/<database>/<table>/_stream_load` sur le FE
(port HTTP 8030 par défaut). Le FE répond `307` vers un BE, qui fait le
chargement. Chaque lot de la sortie est **un** Stream Load : un tableau
JSON (`format: json`, `strip_outer_array: true`), dont les clés sont
appariées aux colonnes de la table par leur nom. La table doit exister.

Particularités :

- **redirection FE → BE avec authentification** : httpx retire
  l'en-tête `Authorization` quand une redirection change d'hôte (comme
  `curl` sans `--location-trusted`) ; la sortie suit elle-même les
  `307`/`308` (trois sauts au plus) en gardant l'authentification
  basic. `hosts` peut aussi viser un BE directement (port 8040) ;
- **label par lot** : chaque lot porte un label `<label_prefix>_<hex>`,
  calculé une fois et réutilisé à chaque retente. Si un envoi a abouti
  sans que la réponse soit reçue (coupure), la retente répond
  `Label Already Exists` avec `ExistingJobStatus: FINISHED` : le lot est
  compté comme indexé, **sans doublon**. Un lot rendu à l'appelant
  (`RETRYABLE`) puis renvoyé repart avec un nouveau label ;
- **statut dans le corps** : le serveur répond en HTTP 200 même quand le
  chargement échoue ; la sortie lit `Status` dans le JSON :
  `Success` et `Publish Timeout` (données validées, visibles sous peu)
  valent indexation ; `Label Already Exists` vaut indexation si le
  chargement d'origine est `FINISHED`, sinon le lot est rendu à
  l'appelant ; `Fail` est un refus ;
- **lot atomique** : avec `max_filter_ratio` à 0 (défaut), une seule
  ligne invalide fait échouer **tout** le lot sans rien écrire. La
  sortie renvoie alors chaque document **seul** (nouveau label chacun)
  pour isoler le fautif, refusé définitivement (DLQ si configurée) avec
  le `Message` et l'`ErrorURL` du serveur ;
- **lignes filtrées** : avec `max_filter_ratio` > 0, Doris peut écarter
  des lignes invalides et répondre `Success` ; la réponse ne dit pas
  lesquelles. Le lot est compté comme indexé et un avertissement donne
  `NumberFilteredRows` et l'`ErrorURL` ;
- **insertions seules** : ni `_id`, ni `update`, ni `delete` (les
  options `index`, `action`, `pipeline`, `routing`, `doc_as_upsert`
  sont refusées).

## Gabarit de pipeline d'exemple

```
output {
  doris {
    hosts => ["http://doris-fe:8030"]
    database => "logs"
    table => "evenements"
    user => "${DORIS_USER}"
    password => "${DORIS_PASSWORD}"
  }
}
```
"""

from __future__ import annotations

import base64
import json
import re
import uuid
from collections.abc import AsyncGenerator
from typing import Any, ClassVar

import httpx
from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput, SendOutcome
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.tracing import atraced, traced

# Doris : lettres, chiffres, `-` et `_`, 128 caractères au plus ; le
# préfixe en laisse 33 au suffixe (`_` + 32 chiffres hexadécimaux).
_MOTIF_PREFIXE = re.compile(r"^[A-Za-z0-9_-]{1,95}$")
_SAUTS_MAX = 3
_STATUTS_INDEXES = frozenset({"Success", "Publish Timeout"})


@traced
def raison_du_refus(corps: dict[str, Any]) -> str:
    """Raison d'un `Fail` : `Message`, suivi de l'`ErrorURL` quand le serveur en donne une."""
    raison = str(corps.get("Message") or corps.get("msg") or "Stream Load : Fail")
    if corps.get("ErrorURL"):
        raison = f"{raison} ({corps['ErrorURL']})"
    return raison


class AuthStreamLoad(httpx.Auth):
    """Authentification basic qui suit les redirections FE → BE (`307`/`308`).

    httpx retire `Authorization` d'une requête redirigée vers un autre
    hôte ; ce flux d'authentification refait la requête (même verbe,
    même corps, mêmes en-têtes) vers l'adresse donnée par `Location`,
    authentification comprise, sur `_SAUTS_MAX` sauts au plus.
    """

    requires_request_body = True

    @traced
    def __init__(self, user: str | None, password: str | None) -> None:
        """Garde les identifiants (aucun : redirections suivies sans authentification)."""
        self._en_tete: str | None = None
        if user:
            jeton = base64.b64encode(f"{user}:{password or ''}".encode()).decode("ascii")
            self._en_tete = f"Basic {jeton}"

    def _signer(self, request: httpx.Request) -> httpx.Request:
        """Ajoute l'en-tête `Authorization` quand des identifiants sont configurés."""
        logger.debug("AuthStreamLoad._signer | {} {}", request.method, request.url.copy_with(query=None))
        if self._en_tete is not None:
            request.headers["Authorization"] = self._en_tete
        logger.debug("AuthStreamLoad._signer | sortie (authentifiée={})", self._en_tete is not None)
        return request

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        """Envoie la requête signée, puis chaque redirection signée à son tour."""
        logger.debug("AuthStreamLoad.async_auth_flow | entrée")
        response = yield self._signer(request)
        for _saut in range(_SAUTS_MAX):
            if response.status_code not in (307, 308) or "location" not in response.headers:
                logger.debug("AuthStreamLoad.async_auth_flow | sortie (statut {})", response.status_code)
                return
            cible = request.url.join(response.headers["location"])
            if cible.scheme not in ("http", "https"):
                logger.warning(_("doris : redirection vers {cible} non suivie"), cible=cible)
                return
            await response.aread()
            logger.debug("AuthStreamLoad | redirection vers {}", cible.copy_with(query=None))
            en_tetes = {nom: valeur for nom, valeur in request.headers.items() if nom.lower() != "host"}
            request = httpx.Request(request.method, cible, headers=en_tetes, content=request.content)
            response = yield self._signer(request)
        logger.debug("AuthStreamLoad.async_auth_flow | sortie ({} sauts atteints)", _SAUTS_MAX)


@enregistrer
class DorisOutput(ElasticsearchBulkOutput):
    """Sortie Apache Doris / StarRocks : Stream Load HTTP, un label par lot, lot atomique."""

    plugin = "doris"
    capacites = Capacites(
        actions=frozenset({"index"}),
        accepte_id=False,
        accuse_par_evenement=False,
        taille_max_requete=None,
    )
    en_tetes_par_defaut: ClassVar[dict[str, str]] = {"Content-Type": "application/json"}
    # Options du client _bulk sans objet pour un Stream Load : la base et
    # la table portent les données, insertions seules.
    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = dict(
        {
            nom: spec
            for nom, spec in ElasticsearchBulkOutput.OPTIONS_PROPRES.items()
            if nom not in ("index", "action", "pipeline", "routing", "doc_as_upsert")
        },
        database=OptionSpec("database", (str,), description="Base de la table cible", requis=True),
        table=OptionSpec("table", (str,), description="Table cible (doit exister)", requis=True),
        label_prefix=OptionSpec(
            "label_prefix",
            (str,),
            "pystashtemplate",
            description="Préfixe du label de chaque lot (lettres, chiffres, - et _)",
        ),
        max_filter_ratio=OptionSpec(
            "max_filter_ratio",
            (int, float),
            0,
            minimum=0,
            description="Part de lignes invalides tolérée par lot (0 : lot refusé dès une ligne)",
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
        """Prépare le Stream Load (base, table, préfixe de label, tolérance).

        Raises:
            ValueError: Option inconnue ou valeur invalide (schéma commun,
                #8) — `database` et `table` sont requises ; préfixe de
                label hors `[A-Za-z0-9_-]` ou trop long ; `max_filter_ratio`
                au-delà de 1.
        """
        options = dict(options)
        super().__init__(options, dlq=dlq, transport=transport)
        self._database = str(options["database"])
        self._table = str(options["table"])
        self._label_prefix = str(options.get("label_prefix") or "pystashtemplate")
        if not _MOTIF_PREFIXE.match(self._label_prefix):
            raise ValueError(
                _("doris : label_prefix {prefixe!r} invalide (lettres, chiffres, - et _, 95 au plus)").format(
                    prefixe=self._label_prefix
                )
            )
        self._max_filter_ratio = float(options.get("max_filter_ratio", 0))
        if self._max_filter_ratio > 1:
            raise ValueError(_("doris : max_filter_ratio {ratio} hors de [0, 1]").format(ratio=self._max_filter_ratio))
        logger.debug(
            "DorisOutput : {}.{} (label_prefix={}, max_filter_ratio={})",
            self._database,
            self._table,
            self._label_prefix,
            self._max_filter_ratio,
        )

    @traced
    def _client_http(self) -> httpx.AsyncClient:
        """Client du client commun, avec l'authentification qui suit FE → BE."""
        client = super()._client_http()
        if not isinstance(client.auth, AuthStreamLoad):
            client.auth = AuthStreamLoad(
                str(self.options["user"]) if self.options.get("user") else None,
                str(self.options.get("password") or ""),
            )
        return client

    @traced
    def _verbe_envoi(self) -> str:
        """`put`, comme `curl -T` de la documentation Stream Load."""
        return "put"

    @traced
    def _chemin_envoi(self) -> str:
        """`/api/<database>/<table>/_stream_load`."""
        return f"/api/{self._database}/{self._table}/_stream_load"

    @traced
    def _nouveau_label(self) -> str:
        """Label unique d'un lot : `<label_prefix>_<32 chiffres hexadécimaux>`."""
        return f"{self._label_prefix}_{uuid.uuid4().hex}"

    @traced
    def _entetes_requete(self, pendings: list[dict[str, Any]]) -> dict[str, str]:
        """En-têtes Stream Load du lot : label (stable d'une retente à l'autre), format, tolérance."""
        return {
            # Le FE refuse un Stream Load sans cet en-tête.
            "Expect": "100-continue",
            "label": self._nouveau_label(),
            "format": "json",
            "strip_outer_array": "true",
            "max_filter_ratio": f"{self._max_filter_ratio:g}",
        }

    @traced
    def _corps_requete(self, pendings: list[dict[str, Any]]) -> str:
        """Tableau JSON des documents du lot (`strip_outer_array`)."""
        return json.dumps([pending["doc"] for pending in pendings], ensure_ascii=False)

    @atraced
    async def _analyser_reponse(self, pendings: list[dict[str, Any]], response: httpx.Response) -> list[SendOutcome]:
        """Lit `Status` dans le corps : la réponse HTTP vaut 200 même en cas d'échec.

        - statut HTTP autre que 2xx : analyse commune (refus définitif
          sur 400/404, lot rendu à l'appelant sinon) ;
        - corps illisible ou sans `Status` : lot rendu à l'appelant ;
        - `Success`, `Publish Timeout` : indexé (avertissement si des
          lignes ont été filtrées) ;
        - `Label Already Exists` : indexé si le chargement d'origine est
          `FINISHED`, rendu à l'appelant sinon ;
        - `Fail` : renvoi un par un d'un lot de plusieurs documents,
          refus définitif d'un document seul.
        """
        if not 200 <= response.status_code < 300:
            return await super()._analyser_reponse(pendings, response)
        try:
            corps = response.json()
            statut = str(corps["Status"])
        except (ValueError, KeyError, TypeError) as exc:
            logger.warning(_("réponse Stream Load illisible erreur={error}"), error=exc)
            return [SendOutcome.RETRYABLE] * len(pendings)
        if statut in _STATUTS_INDEXES:
            filtrees = corps.get("NumberFilteredRows") or 0
            if filtrees:
                logger.warning(
                    _("doris : {nombre} ligne(s) écartée(s) par le serveur, détail : {url}"),
                    nombre=filtrees,
                    url=corps.get("ErrorURL"),
                )
            return [SendOutcome.INDEXED] * len(pendings)
        if statut == "Label Already Exists":
            existant = corps.get("ExistingJobStatus")
            if existant == "FINISHED":
                logger.info(_("doris : lot déjà chargé (label {label}), compté comme indexé"), label=corps.get("Label"))
                return [SendOutcome.INDEXED] * len(pendings)
            logger.warning(
                _("doris : label {label} déjà pris (état {etat}), lot rendu à l'appelant"),
                label=corps.get("Label"),
                etat=existant,
            )
            return [SendOutcome.RETRYABLE] * len(pendings)
        if statut == "Fail":
            if len(pendings) > 1:
                logger.warning(_("doris : lot de {} documents refusé, renvoi un par un"), len(pendings))
                return [(await self._issues_du_lot([pending]))[0] for pending in pendings]
            return [await self._permanent(pendings[0], raison_du_refus(corps))]
        logger.warning(_("doris : statut Stream Load inattendu {statut}, lot rendu à l'appelant"), statut=statut)
        return [SendOutcome.RETRYABLE] * len(pendings)

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge `GET /api/health` du FE (nombre de BE vivants).

        Returns:
            Le corps JSON du FE (`{"code": 0, "data": {...}}` chez Doris).

        Raises:
            httpx.HTTPStatusError: statut d'erreur.
        """
        response = await self._client_http().get("/api/health")
        response.raise_for_status()
        corps: dict[str, Any] = response.json()
        return corps
