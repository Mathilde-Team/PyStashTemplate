# SPDX-License-Identifier: GPL-3.0-or-later
"""Client commun de la famille « API _bulk Elasticsearch » (issue #11).

Un seul client NDJSON paramétré par sortie : tous les produits compatibles
de l'API `_bulk` (Elasticsearch, OpenSearch, VictoriaLogs, Quickwit,
OpenObserve — #2) ne sont plus que des sous-classes de quelques lignes
(valeurs par défaut et capacités) :

- `chemin_bulk` : préfixe de chemin avant `/_bulk` (`/insert/elasticsearch`
  pour VictoriaLogs, `/api/v1/_elastic` pour Quickwit, `/api/{org}` pour
  OpenObserve) ;
- `capacites` : actions supportées (Quickwit n'accepte que `create`),
  `_id` accepté ou non, accusé de réception par élément (`items[]`) ou
  statut HTTP global (Quickwit ne rapporte pas les erreurs par élément) —
  dégradation documentée ;
- `en_tetes` : en-têtes HTTP propres (authentification OpenObserve…).

Deux portes d'entrée, même tampon :

- `envoyer(doc, ...)` — API par document, utilisée par le connecteur
  RsysLogstach (l'`indexer` y migre en #160, sans changement de
  comportement) : rend un `SendOutcome` par document ;
- `send(batch)` — contrat `Output` du socle (#4) : les options du bloc
  `output{}`, déjà rendues par l'interpréteur contre chaque document,
  servent le lot entier (cas courant : `index` fixe, pas de `_id` par
  document — les options par document passent par `envoyer`).

Comportement (parité avec l'`indexer` de RsysLogstach) :

- un échec **permanent** (4xx : 400 mapping, 404…) mène le document en
  DLQ si elle est configurée (`SendOutcome.DEAD_LETTERED`), sinon
  `RETRYABLE` (comportement historique) ;
- un échec **transitoire** (connexion, délai dépassé, 429, 5xx) rend le
  document à l'appelant (`RETRYABLE`) : il le retient et le renvoie,
  exactement comme rsyslog le fait pour l'indexer historique. Le client
  ne re-tamponne pas un échec transitoire (un double envoi serait
  possible si l'appelant renvoyait aussi) ;
- `retry_max` (0 par défaut, parité exacte) : après un échec
  transitoire, retente le lot jusqu'à `retry_max` fois, délai
  `retry_backoff` entre chaque (exponentiel : délai × 2^n) ;
- `dlq_transitoires` (faux par défaut, parité exacte) : avec une DLQ,
  un échec transitoire du lot entier dont les réessais sont épuisés
  (réseau, 429, 5xx global) mène les documents en DLQ
  (`DEAD_LETTERED`, raison `transitoire_epuise`) au lieu de les rendre
  à l'appelant — pour un appelant qui ne retient rien (contrat `send`).
  Un échec transitoire d'un seul élément (`items[]`), jamais retenté
  individuellement, reste `RETRYABLE` ;
- chaque entrée de DLQ porte le nom de la sortie (`@dlq_output` : option
  `id`, sinon nom du plugin) et la raison (`@dlq_reason`) ;
- `batch_size` (1 par défaut) : les documents sont tamponnés et envoyés
  en un seul `_bulk` quand le tampon est plein — `envoyer` renvoie alors
  `SendOutcome.EN_ATTENTE` pour un document seulement tamponné (résultat
  non encore connu), le résultat réel arrivant au `flush()` suivant.

Ne dépend que de `dlq`, `i18n` et `tracing` (feuilles du graphe) ;
utilise `httpx` pour le client HTTP asynchrone. Les méthodes asynchrones
journalisent entrée/sortie à la main (`tracing.traced` est synchrone —
décorateur asynchrone : #37).
"""

from __future__ import annotations

import asyncio
import enum
import json
import ssl
import time
from datetime import datetime, timezone
from typing import Any, ClassVar

import httpx
from loguru import logger

from pystashtemplate.dlq import REASON_PERMANENT, REASON_TRANSIENT_EXHAUSTED, LocalDeadLetterQueue
from pystashtemplate.i18n import _
from pystashtemplate.outputs.base import ACTIONS, Document, Output
from pystashtemplate.outputs.schema import OptionSpec
from pystashtemplate.tracing import atraced, traced

# Statuts HTTP traités comme des échecs définitifs de TOUT le lot : un
# 400/404 sur la requête _bulk elle-même (payload refusé, index absent)
# ne devient jamais valide en rejouant le lot tel quel — même
# classification que `_PERMANENT_ERRORS` de l'indexer historique
# (RequestError/NotFoundError d'opensearch-py).
_STATUTS_PERMANENTS = frozenset({400, 404})

# Statuts HTTP transitoires de la requête entière : retentés (jusqu'à
# `retry_max`), puis rendus à l'appelant comme un échec transitoire.
_STATUTS_TRANSITOIRES = frozenset({408, 425, 429})


@traced
def _statut_transitoire(statut: int) -> bool:
    """Vrai pour un statut HTTP transitoire : 408, 425, 429 ou 5xx."""
    return statut >= 500 or statut in _STATUTS_TRANSITOIRES


class ActionInvalideError(ValueError):
    """Action inconnue ou non supportée par la sortie, ou `_id` exigé absent."""


class SendOutcome(enum.Enum):
    """Résultat d'un envoi, mêmes trois issues que l'indexer de RsysLogstach (#160).

    - INDEXED : le document a été envoyé avec succès.
    - RETRYABLE : échec transitoire — le document est rendu à l'appelant,
      qui le retient et le renvoie (comportement historique de l'indexer).
    - DEAD_LETTERED : échec permanent, écrit dans la DLQ locale si elle
      est configurée — décision de routage définitive : ne JAMAIS
      retenter. Sans DLQ configurée, ce cas n'est jamais produit : on
      retombe sur RETRYABLE (comportement historique).
    - EN_ATTENTE : document tamponné (`batch_size` non atteint) — résultat
      réel connu au `flush()` qui l'enverra ; n'apparaît jamais avec
      `batch_size = 1` (défaut, parité avec l'indexer historique).
    """

    INDEXED = "indexed"
    RETRYABLE = "retryable"
    DEAD_LETTERED = "dead_lettered"
    EN_ATTENTE = "en_attente"


class ElasticsearchBulkOutput(Output):
    """Envoi de documents par l'API `_bulk` (NDJSON), paramétrable par produit.

    Chaque document tamponné devient une paire de lignes NDJSON (ligne
    d'action puis document), sérialisées en flux — `batch_size = 1` par
    défaut pour la parité avec l'indexer historique de RsysLogstach.
    L'`index` peut être un gabarit sprintf `%{+YYYY.MM.dd}` : déjà rendu
    par l'interpréteur du `output{}`, il arrive ici en nom complet ; la
    sortie fournit aussi le motif strftime (`index`, ex.
    ``logs-%Y.%m.%d``) rendu à chaque envoi quand aucun `index` n'est
    passé pour le document. Sous-classe du contrat `Output` (#4) : le
    nom de plugin (`plugin`) et l'inscription au registre sont à la
    charge de chaque produit (ex. `opensearch` : #12).
    """

    # Préfixe de chemin avant `/_bulk` (surchargeable par sous-classe).
    chemin_bulk: ClassVar[str] = ""
    # En-têtes HTTP propres au produit (surchargeables par sous-classe).
    en_tetes_par_defaut: ClassVar[dict[str, str]] = {}

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        dlq: LocalDeadLetterQueue | None = None,
    ) -> None:
        """Prépare le client HTTP et le tampon NDJSON depuis les options du bloc.

        Args:
            options: Options du bloc `output{}` : connexion (`hosts`,
                `user`, `password`, `ssl`, `cacert`, `ssl_certificate`,
                `ssl_key`, `timeout`), envoi (`index`, `batch_size`,
                `retry_max`, `retry_backoff`, `action`, `pipeline`,
                `routing`, `doc_as_upsert`) et DLQ (`dlq_path`). Une
                `dlq` toute construite (tests) prime sur `dlq_path`.
            transport: Transport httpx de remplacement (couture de test —
                un faux serveur via `httpx.MockTransport`).

        Raises:
            ValueError: `batch_size` ou `retry_max` négatif, option
                inconnue ou mal formée.
        """
        super().__init__(options)
        self._hosts = self._hosts_de(options.get("hosts"))
        self._index_pattern = options.get("index") or "logs-%Y.%m.%d"
        batch_size = int(options.get("batch_size", 1))
        retry_max = int(options.get("retry_max", 0))
        self._batch_size = batch_size
        self._retry_max = retry_max
        self._retry_backoff = float(options.get("retry_backoff", 1.0))
        self._dlq_transitoires = bool(options.get("dlq_transitoires", False))
        self._dlq = dlq
        if self._dlq is None and options.get("dlq_path"):
            self._dlq = LocalDeadLetterQueue(str(options["dlq_path"]))
        self._transport = transport
        self._pending: list[dict[str, Any]] = []
        self._client: httpx.AsyncClient | None = None

    # Options propres à cette famille de sorties, en plus des communes du
    # schéma (issue #8) : la base valide à l'instanciation qu'une option
    # inconnue (faute de frappe) ou une valeur invalide échoue franchement.
    OPTIONS_PROPRES: ClassVar[dict[str, OptionSpec]] = {
        "index": OptionSpec("index", (str,), "logs-%Y.%m.%d", description="Gabarit du nom d'index"),
        "action": OptionSpec(
            "action",
            (str,),
            "index",
            valeurs=("index", "create", "update", "delete"),
            description="Action _bulk (index par défaut)",
        ),
        "pipeline": OptionSpec("pipeline", (str,), description="Pipeline d'ingestion côté serveur"),
        "routing": OptionSpec("routing", (str,), description="Routage _bulk"),
        "doc_as_upsert": OptionSpec(
            "doc_as_upsert", (bool,), False, description="Update crée le document s'il est absent"
        ),
        "timeout": OptionSpec(
            "timeout", (int, float), 30.0, minimum=0, description="Secondes avant abandon d'une requête"
        ),
        "headers": OptionSpec("headers", (dict,), description="En-têtes HTTP additionnels"),
        "dlq_transitoires": OptionSpec(
            "dlq_transitoires",
            (bool,),
            False,
            description="Les échecs transitoires épuisés partent aussi en DLQ (#7)",
        ),
    }

    @traced
    def _hosts_de(self, hosts: Any) -> str:
        """Première URL des `hosts` (chaîne seule ou liste).

        Raises:
            ValueError: `hosts` absent ou liste vide.
        """
        if isinstance(hosts, str):
            return hosts
        if isinstance(hosts, (list, tuple)) and hosts:
            return str(hosts[0])
        raise ValueError(_("hosts est requis (ex. https://opensearch:9200)"))

    @traced
    def _client_http(self) -> httpx.AsyncClient:
        """Client HTTP paresseux (créé au premier envoi, jamais avant `open`)."""
        if self._client is None:
            # Contexte TLS explicite dès que la configuration sort du
            # booléen simple : httpx avec `verify=<chemin>` ET `cert=`
            # ne présente PAS le certificat client (poignée de main
            # refusée par un serveur mTLS, constaté au banc #160) — un
            # `SSLContext` construit à la main (autorités + paire client)
            # fonctionne. Même approche que `TransportHTTP._contexte_ssl`.
            verify: bool | ssl.SSLContext = bool(self.options.get("ssl", True))
            if self.options.get("cacert"):
                verify = ssl.create_default_context(cafile=str(self.options["cacert"]))
            cert = (
                (str(self.options["ssl_certificate"]), str(self.options["ssl_key"]))
                if self.options.get("ssl_certificate")
                else None
            )
            self._client = httpx.AsyncClient(
                base_url=self._hosts,
                auth=(str(self.options["user"]), str(self.options["password"])) if self.options.get("user") else None,
                verify=verify,
                cert=cert,
                timeout=float(self.options.get("timeout", 3.0)),
                headers={
                    "Content-Type": "application/x-ndjson",
                    **self.en_tetes_par_defaut,
                    **self._en_tetes_d_options(),
                },
                transport=self._transport,
            )
        return self._client

    @traced
    def _en_tetes_d_options(self) -> dict[str, str]:
        """En-têtes propres passés dans les options (chaque valeur devient une chaîne)."""
        en_tetes = self.options.get("headers")
        return {str(k): str(v) for k, v in en_tetes.items()} if isinstance(en_tetes, dict) else {}

    @traced
    def _current_index(self) -> str:
        """Calcule le nom de l'index du jour à partir du motif configuré."""
        return datetime.now(timezone.utc).strftime(self._index_pattern)

    @traced
    def _ligne_action(self, pending: dict[str, Any]) -> str:
        """Ligne d'action NDJSON du document (première ligne de la paire).

        L'action a déjà été validée par `envoyer` (inconnue, non
        supportée par la sortie, `update`/`delete` sans `_id`) : ici, il
        ne reste qu'à la mettre en forme.
        """
        action = pending["action"]
        meta: dict[str, Any] = {"_index": pending["index"]}
        document_id = pending["document_id"]
        if document_id and self.capacites.accepte_id:
            meta["_id"] = document_id
        elif document_id and not self.capacites.accepte_id:
            logger.warning(_("sortie : _id refusé par le produit, ignoré"))
        if pending["routing"]:
            meta["routing"] = pending["routing"]
        return json.dumps({action: meta}, ensure_ascii=False)

    @traced
    def _chemin_envoi(self) -> str:
        """Chemin de la requête d'envoi : `<chemin_bulk>/_bulk` (surchargeable)."""
        return f"{self.chemin_bulk}/_bulk"

    @traced
    def _parametres_requete(self) -> dict[str, str]:
        """Paramètres d'URL propres au produit, ajoutés à chaque envoi (aucun par défaut)."""
        return {}

    @traced
    def _corps_requete(self, pendings: list[dict[str, Any]]) -> str:
        """Corps de la requête d'envoi : le NDJSON `_bulk` (surchargeable)."""
        return self._corps_ndjson(pendings)

    @traced
    def _corps_ndjson(self, pendings: list[dict[str, Any]]) -> str:
        """Corps NDJSON du lot : ligne d'action + document (ou rien pour `delete`).

        Construit en flux (une ligne à la fois) : compatible avec un
        envoi par morceaux si un produit l'exige.
        """
        lignes: list[str] = []
        for pending in pendings:
            lignes.append(self._ligne_action(pending))
            if pending["action"] == "delete":
                continue
            if pending["action"] == "update":
                corps: dict[str, Any] = {"doc": pending["doc"]}
                if pending["doc_as_upsert"]:
                    corps["doc_as_upsert"] = True
                lignes.append(json.dumps(corps, ensure_ascii=False))
            else:
                lignes.append(json.dumps(pending["doc"], ensure_ascii=False))
        return "\n".join(lignes) + "\n"

    @atraced
    async def envoyer(
        self,
        doc: dict[str, Any],
        *,
        index: str | None = None,
        document_id: str | None = None,
        action: str = "index",
        pipeline: str | None = None,
        routing: str | None = None,
        doc_as_upsert: bool = False,
    ) -> SendOutcome:
        """Tamponne un document et envoie le lot quand il est plein.

        Args:
            doc: Document prêt à envoyer.
            index: Nom d'index pour CE document (déjà rendu par
                l'interpréteur si gabarit sprintf) ; `None` retombe sur le
                motif quotidien `index`.
            document_id: `_id` pour CE document ; ignoré si le produit
                n'accepte pas de `_id` (dégradation documentée).
            action: `index` (défaut), `create`, `update` ou `delete`.
            pipeline: Pipeline d'ingestion (`index`, `create`) — un seul
                par lot : des pipelines différents dans un même lot
                échouent à la configuration.
            routing: Valeur de routage (`routing`).
            doc_as_upsert: Pour `update` : crée le document s'il manque.

        Returns:
            `SendOutcome` de CE document : réel si le lot est parti avec
            lui, `EN_ATTENTE` s'il est seulement tamponné.

        Raises:
            ActionInvalideError: action inconnue/non supportée, ou
                `update`/`delete` sans `document_id`.
            ValueError: pipelines différents dans un même lot.
        """
        logger.debug("ElasticsearchBulkOutput.envoyer | entrée")
        resolved_index = index or self._current_index()
        if action in ("update", "delete") and not (document_id and self.capacites.accepte_id):
            raise ActionInvalideError(f"l'action {action} exige document_id")
        if action not in ACTIONS:
            raise ActionInvalideError(f"action inconnue : {action!r}")
        if action not in self.capacites.actions:
            raise ActionInvalideError(f"action non supportée par la sortie : {action!r}")
        if pipeline:
            for pending in self._pending:
                if pending["pipeline"] not in (None, pipeline):
                    raise ValueError("des pipelines différents ne peuvent pas partager un lot")
        pending = {
            "doc": doc,
            "index": resolved_index,
            "document_id": document_id,
            "action": action,
            "pipeline": pipeline,
            "routing": routing,
            "doc_as_upsert": doc_as_upsert,
        }
        self._pending.append(pending)
        self.observateur.file(self.nom, len(self._pending))
        if len(self._pending) < self._batch_size:
            logger.debug("ElasticsearchBulkOutput.envoyer | document tamponné ({})", len(self._pending))
            logger.debug("ElasticsearchBulkOutput.envoyer | sortie")
            return SendOutcome.EN_ATTENTE
        outcomes = await self._flush()
        logger.debug("ElasticsearchBulkOutput.envoyer | sortie")
        return outcomes[-1]

    @atraced
    async def send(self, batch: list[Document]) -> None:
        """Envoie un lot de documents (contrat `Output` #4).

        Chaque document part avec les options du bloc `output{}` (déjà
        rendues par l'interpréteur) : `index`, `action`, `pipeline`,
        `routing`, `doc_as_upsert`. Le résultat de chaque document est
        journalisé ; les échecs suivent la même politique que `envoyer`
        (permanent → DLQ, transitoire rendu à l'appelant).
        """
        logger.debug("ElasticsearchBulkOutput.send | entrée ({} document(s))", len(batch))
        for doc in batch:
            outcome = await self.envoyer(
                dict(doc),
                action=str(self.options.get("action", "index")),
                pipeline=self.options.get("pipeline"),
                routing=self.options.get("routing"),
                doc_as_upsert=bool(self.options.get("doc_as_upsert", False)),
            )
            logger.debug("ElasticsearchBulkOutput.send | {} → {}", outcome.value, doc)
        logger.debug("ElasticsearchBulkOutput.send | sortie")

    @atraced
    async def flush(self) -> None:
        """Pousse les documents en attente (contrat `Output` : arrêt propre).

        Un lot vide ne fait aucune requête. Après un échec transitoire
        épuisé (`retry_max`), les documents sont retirés du tampon et
        rendus à l'appelant (`RETRYABLE`) — qui les retient et les
        renvoie — pour éviter un double envoi s'il renvoie aussi.
        """
        logger.debug("ElasticsearchBulkOutput.flush | entrée")
        await self._flush()
        logger.debug("ElasticsearchBulkOutput.flush | sortie")

    @atraced
    async def _flush(self) -> list[SendOutcome]:
        """Envoie le tampon en un `_bulk` et renvoie le résultat de chaque document.

        Un lot vide ne fait aucune requête (`[]`). Après un échec
        transitoire épuisé (`retry_max`), les documents sont retirés du
        tampon et rendus à l'appelant (`RETRYABLE`) — qui les retient et
        les renvoie — pour éviter un double envoi s'il renvoie aussi.
        """
        if not self._pending:
            return []
        pendings = self._pending
        self._pending = []
        logger.debug("_flush | {} document(s)", len(pendings))
        self.observateur.file(self.nom, 0)
        outcomes = await self._issues_du_lot(pendings)
        for outcome in (SendOutcome.INDEXED, SendOutcome.DEAD_LETTERED, SendOutcome.RETRYABLE):
            nombre = outcomes.count(outcome)
            if nombre:
                self.observateur.evenements(self.nom, outcome.value, nombre)
        logger.debug("_flush | sortie")
        return outcomes

    @atraced
    async def _issues_du_lot(self, pendings: list[dict[str, Any]]) -> list[SendOutcome]:
        """Envoie un lot (retentes comprises) et renvoie l'issue de chaque document."""
        response = await self._envoyer_lot(pendings)
        if response is None:
            # Échec transitoire épuisé : rendu à l'appelant (docstring),
            # ou en DLQ si `dlq_transitoires` (#7).
            return await self._transitoires_epuises(pendings, "réseau : réessais épuisés")
        if self._dlq_transitoires and _statut_transitoire(response.status_code):
            return await self._transitoires_epuises(pendings, f"statut {response.status_code} : réessais épuisés")
        return await self._analyser_reponse(pendings, response)

    @atraced
    async def _transitoires_epuises(self, pendings: list[dict[str, Any]], erreur: str) -> list[SendOutcome]:
        """Lot en échec transitoire, réessais épuisés : RETRYABLE, ou DLQ si `dlq_transitoires`."""
        if self._dlq is None or not self._dlq_transitoires:
            return [SendOutcome.RETRYABLE] * len(pendings)
        logger.warning(_("lot en échec transitoire mis en DLQ documents={nombre}"), nombre=len(pendings))
        for pending in pendings:
            await self._dlq.write(
                pending["doc"],
                index=pending["index"],
                error=erreur,
                output=self.nom,
                reason=REASON_TRANSIENT_EXHAUSTED,
            )
        return [SendOutcome.DEAD_LETTERED] * len(pendings)

    @atraced
    async def _envoyer_lot(self, pendings: list[dict[str, Any]]) -> httpx.Response | None:
        """POSTe le NDJSON, avec retentes sur échec transitoire.

        Returns:
            La réponse HTTP, ou None après `retry_max` retentes épuisées
            sur un échec transitoire (connexion, 429, 5xx…).
        """
        params: dict[str, str] = self._parametres_requete()
        pipelines = {p["pipeline"] for p in pendings if p["pipeline"]}
        if len(pipelines) == 1:
            params["pipeline"] = next(iter(pipelines))
        essai = 0
        while True:
            debut = time.perf_counter()
            try:
                response = await self._client_http().post(
                    self._chemin_envoi(),
                    content=self._corps_requete(pendings),
                    params=params or None,
                )
            except httpx.HTTPError as exc:
                self.observateur.latence(self.nom, time.perf_counter() - debut)
                logger.warning(_("envoi _bulk échoué (réseau) erreur={error}"), error=exc)
                if essai >= self._retry_max:
                    return None
                essai += 1
                self.observateur.reessai(self.nom)
                await asyncio.sleep(self._retry_backoff * 2 ** (essai - 1))
                continue
            self.observateur.latence(self.nom, time.perf_counter() - debut)
            if response.status_code in _STATUTS_PERMANENTS:
                logger.warning(_("envoi _bulk refusé définitivement statut={statut}"), statut=response.status_code)
                return response
            if _statut_transitoire(response.status_code):
                logger.warning(_("envoi _bulk échoué statut={statut}"), statut=response.status_code)
                if essai >= self._retry_max:
                    return response
                essai += 1
                self.observateur.reessai(self.nom)
                await asyncio.sleep(self._retry_backoff * 2 ** (essai - 1))
                continue
            return response

    @atraced
    async def _analyser_reponse(self, pendings: list[dict[str, Any]], response: httpx.Response) -> list[SendOutcome]:
        """Analyse la réponse `_bulk` et renvoie le résultat de chaque document.

        Par élément (`items[]`) quand le produit le rapporte ; sinon
        (dégradation documentée) le statut HTTP global décide pour tout
        le lot.
        """
        if response.status_code in _STATUTS_PERMANENTS:
            return [await self._permanent(pending, f"statut {response.status_code}") for pending in pendings]
        if not self.capacites.accuse_par_evenement:
            # Statut global seul : seul un 2xx vaut indexation. Un 5xx ou
            # 429 aux réessais épuisés, ou un statut inattendu (401, 403…),
            # rend le lot à l'appelant — jamais compté comme indexé (#14).
            if not 200 <= response.status_code < 300:
                logger.warning(
                    _("envoi _bulk : statut global {statut}, lot rendu à l'appelant"), statut=response.status_code
                )
                return [SendOutcome.RETRYABLE] * len(pendings)
            logger.debug("_analyser_reponse | statut global seul")
            return [SendOutcome.INDEXED] * len(pendings)
        try:
            corps = response.json()
            items = corps.get("items", [])
        except (ValueError, AttributeError) as exc:
            logger.warning(_("réponse _bulk illisible erreur={error}"), error=exc)
            return [SendOutcome.RETRYABLE] * len(pendings)
        if len(items) != len(pendings):
            logger.warning(_("réponse _bulk : {} éléments attendus, {} reçus"), len(pendings), len(items))
            return [SendOutcome.RETRYABLE] * len(pendings)
        outcomes: list[SendOutcome] = []
        for pending, item in zip(pendings, items, strict=True):
            resultat = item.get(pending["action"], {})
            erreur = resultat.get("error")
            if erreur is None:
                outcomes.append(SendOutcome.INDEXED)
                continue
            statut = resultat.get("status", 400)
            if _statut_transitoire(statut):
                logger.warning(_("envoi _bulk élément échoué statut={statut}"), statut=statut)
                outcomes.append(SendOutcome.RETRYABLE)
                continue
            outcomes.append(await self._permanent(pending, str(erreur)))
        return outcomes

    @atraced
    async def _permanent(self, pending: dict[str, Any], erreur: str) -> SendOutcome:
        """Échec définitif d'un document : DLQ si configurée, sinon RETRYABLE."""
        logger.warning(_("envoi _bulk échoué définitivement erreur={error}"), error=erreur)
        self.observateur.evenements(self.nom, "rejected")
        if self._dlq is None:
            return SendOutcome.RETRYABLE
        await self._dlq.write(
            pending["doc"], index=pending["index"], error=erreur, output=self.nom, reason=REASON_PERMANENT
        )
        return SendOutcome.DEAD_LETTERED

    @atraced
    async def open(self) -> None:
        """Ouvre la sortie : le client HTTP est paresseux, rien à ouvrir ici (contrat `Output`)."""
        logger.debug("ElasticsearchBulkOutput.open | entrée")
        self._client_http()  # crée le client (et valide TLS/auth) sans requête
        logger.debug("ElasticsearchBulkOutput.open | sortie")

    @atraced
    async def close(self) -> None:
        """Ferme proprement le client HTTP (idempotent)."""
        logger.debug("ElasticsearchBulkOutput.close | entrée")
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        logger.debug("ElasticsearchBulkOutput.close | sortie")

    @atraced
    async def health(self) -> dict[str, Any]:
        """Interroge le serveur (GET /) : sa version, ou lève l'erreur.

        Utilisé par le contrôle de santé périodique d'un connecteur
        (RsysLogstach #37) : `GET {hosts}` renvoie la version du
        produit, comme `client.info()` de l'indexer historique.
        """
        response = await self._client_http().get("/")
        response.raise_for_status()
        return response.json()
