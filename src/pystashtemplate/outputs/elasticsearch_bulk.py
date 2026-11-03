# SPDX-License-Identifier: GPL-3.0-or-later
"""Client commun de la famille « API _bulk Elasticsearch » (issue #11).

Un seul client NDJSON paramétré par sortie : tous les produits compatibles
de l'API `_bulk` (Elasticsearch, OpenSearch, VictoriaLogs, Quickwit,
OpenObserve — #2) ne sont plus que des sous-classes de quelques lignes
(valeurs par défaut et capacités) :

- `chemin_bulk` : préfixe de chemin avant `/_bulk` (`/insert/elasticsearch`
  pour VictoriaLogs, `/api/v1/_elastic` pour Quickwit, `/api/{org}` pour
  OpenObserve) ;
- `actions` : actions supportées (Quickwit n'accepte que `create`) ;
- `id_accepte` : `_id` accepté ou non (Quickwit le refuse) ;
- `rapport_par_element` : réponse analysée élément par élément
  (`items[]`) ou seulement statut HTTP global (Quickwit ne rapporte pas
  les erreurs par élément) — dégradation documentée ;
- `en_tetes` : en-têtes HTTP propres (authentification OpenObserve…).

Comportement (parité avec l'`indexer` de RsysLogstach, que #160 migre
dessus sans changement) :

- un échec **permanent** (4xx : 400 mapping, 404…) mène le document en
  DLQ si elle est configurée (`SendOutcome.DEAD_LETTERED`), sinon
  `RETRYABLE` (comportement historique) ;
- un échec **transitoire** (connexion, délai dépassé, 429, 5xx) rend le
  document à l'appelant (`RETRYABLE`) : il le retient et le renvoie,
  exactement comme rsyslog le fait pour l'indexer historique. Le client
  ne re-tampone pas un échec transitoire (un double envoi serait
  possible si l'appelant renvoyait aussi) ;
- `reessais_max` (0 par défaut, parité exacte) : après un échec
  transitoire, retente le lot jusqu'à `reessais_max` fois, délai
  `reessai_delai` entre chaque (exponentiel : délai × 2^n) ;
- `batch_size` (1 par défaut) : les documents sont tamponnés et envoyés
  en un seul `_bulk` quand le tampon est plein — `send` renvoie alors
  `SendOutcome.EN_ATTENTE` pour un document seulement tamponné (résultat
  non encore connu), le résultat réel arrivant au `flush()` suivant.

Ne dépend que de `tracing`, `i18n` et `dlq` (feuilles du graphe) ;
utilise `httpx` pour le client HTTP asynchrone.
"""

from __future__ import annotations

import asyncio
import enum
import json
from datetime import datetime, timezone
from typing import Any

import httpx
from loguru import logger

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.i18n import _
from pystashtemplate.tracing import traced

# Actions `bulk` du plugin de sortie Logstash (RsysLogstach #126).
ACTIONS = ("index", "create", "update", "delete")

# Statuts HTTP traités comme des échecs définitifs de TOUT le lot : un
# 400/404 sur la requête _bulk elle-même (payload refusé, index absent)
# ne devient jamais valide en rejouant le lot tel quel — même
# classification que `_PERMANENT_ERRORS` de l'indexer historique
# (RequestError/NotFoundError d'opensearch-py).
_STATUTS_PERMANENTS = frozenset({400, 404})

# Statuts HTTP transitoires de la requête entière : retentés (jusqu'à
# `reessais_max`), puis rendus à l'appelant comme un échec transitoire.
_STATUTS_TRANSITOIRES = frozenset({408, 425, 429})


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


class ElasticsearchBulkOutput:
    """Envoi de documents par l'API `_bulk` (NDJSON), paramétrable par produit.

    Chaque document tamponné devient une paire de lignes NDJSON (ligne
    d'action puis document), sérialisées en flux — `batch_size = 1` par
    défaut pour la parité avec l'indexer historique de RsysLogstach.
    L'`index` peut être un gabarit sprintf `%{+YYYY.MM.dd}` : déjà rendu
    par l'interpréteur du `output{}`, il arrive ici en nom complet ; la
    sortie autonome fournit aussi `index_pattern` (strftime, ex.
    ``logs-%Y.%m.%d``) rendu à chaque envoi quand aucun `index` n'est
    passé pour le document.
    """

    # Capacités par défaut de la famille (surchargeables par sous-classe).
    chemin_bulk: str = ""
    actions: tuple[str, ...] = ACTIONS
    id_accepte: bool = True
    rapport_par_element: bool = True

    @traced
    def __init__(
        self,
        *,
        base_url: str,
        index_pattern: str = "logs-%Y.%m.%d",
        auth: tuple[str, str] | None = None,
        en_tetes: dict[str, str] | None = None,
        verify: bool = True,
        ca_certs: str | None = None,
        client_cert: str | None = None,
        client_key: str | None = None,
        timeout: float = 3.0,
        batch_size: int = 1,
        reessais_max: int = 0,
        reessai_delai: float = 1.0,
        dlq: LocalDeadLetterQueue | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Prépare le client HTTP et le tampon NDJSON.

        Args:
            base_url: URL de base, ex. ``https://opensearch:9200`` (le
                chemin `_bulk` complet est `{chemin_bulk}/_bulk`).
            index_pattern: Motif strftime du nom d'index (défaut, un
                index par jour) quand `send` ne reçoit pas d'`index`.
            auth: Couple (utilisateur, mot de passe), ou None.
            en_tetes: En-têtes HTTP propres au produit (fusionnés après
                les en-têtes standards).
            verify: Vérifier les certificats TLS.
            ca_certs: Autorité de signature (PEM) du serveur.
            client_cert: Certificat client PEM (mTLS).
            client_key: Clé privée PEM de `client_cert`.
            timeout: Délai d'attente du client HTTP, en secondes.
            batch_size: Documents tamponnés avant envoi d'un `_bulk`.
            reessais_max: Retentes d'un lot après échec transitoire (0 =
                jamais, parité avec l'indexer historique).
            reessai_delai: Délai de base entre retentes (exponentiel).
            dlq: Dead letter queue locale optionnelle pour les échecs
                permanents.
            transport: Transport httpx de remplacement (couture de test —
                un faux serveur via `httpx.MockTransport`).

        Raises:
            ValueError: `batch_size` ou `reessais_max` négatif.
        """
        if batch_size < 1 or reessais_max < 0:
            raise ValueError("batch_size (>= 1) et reessais_max (>= 0) doivent être positifs")
        self._index_pattern = index_pattern
        self._dlq = dlq
        self._batch_size = batch_size
        self._reessais_max = reessais_max
        self._reessai_delai = reessai_delai
        self._en_tetes = dict(en_tetes) if en_tetes else {}
        self._pending: list[dict[str, Any]] = []
        # Failover multi-nœuds (RsysLogstach #37) : le transport
        # opensearch-py retentait le nœud suivant ; httpx, lui, ne gère
        # qu'une URL — la re-tamponisation d'un échec transitoire étant
        # rendue à l'appelant (voir la docstring du module), le failover
        # revient à l'appelant (rsyslog) qui renvoie le document.
        # httpx accepte dans `verify` un booléen OU le chemin d'un fichier
        # d'autorités (CA bundle) : `ca_certs` remplace le magasin système.
        self._client = httpx.AsyncClient(
            base_url=base_url,
            auth=auth,
            verify=ca_certs or verify,
            cert=(client_cert, client_key) if client_cert else None,
            timeout=timeout,
            headers={"Content-Type": "application/x-ndjson", **self._en_tetes},
            transport=transport,
        )

    @traced
    async def health(self) -> dict[str, Any]:
        """Interroge le serveur (GET /) : sa version, ou lève l'erreur.

        Utilisé par le contrôle de santé périodique d'un connecteur
        (RsysLogstach #37) : `GET {base_url}` renvoie la version du
        produit, comme `client.info()` de l'indexer historique.
        """
        response = await self._client.get("/")
        response.raise_for_status()
        return response.json()

    @traced
    def _current_index(self) -> str:
        """Calcule le nom de l'index du jour à partir du motif configuré."""
        return datetime.now(timezone.utc).strftime(self._index_pattern)

    @traced
    def _ligne_action(self, pending: dict[str, Any]) -> str:
        """Ligne d'action NDJSON du document (première ligne de la paire).

        L'action a déjà été validée par `send` (inconnue, non supportée
        par la sortie, `update`/`delete` sans `_id`) : ici, il ne reste
        qu'à la mettre en forme.
        """
        action = pending["action"]
        meta: dict[str, Any] = {"_index": pending["index"]}
        document_id = pending["document_id"]
        if document_id and self.id_accepte:
            meta["_id"] = document_id
        elif document_id and not self.id_accepte:
            logger.warning(_("sortie : _id refusé par le produit, ignoré"))
        if pending["routing"]:
            meta["routing"] = pending["routing"]
        return json.dumps({action: meta}, ensure_ascii=False)

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

    @traced
    async def send(
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
                motif quotidien `index_pattern`.
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
        resolved_index = index or self._current_index()
        if action in ("update", "delete") and not (document_id and self.id_accepte):
            raise ActionInvalideError(f"l'action {action} exige document_id")
        if action not in ACTIONS:
            raise ActionInvalideError(f"action inconnue : {action!r}")
        if action not in self.actions:
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
        if len(self._pending) < self._batch_size:
            logger.debug("send | document tamponné ({})", len(self._pending))
            return SendOutcome.EN_ATTENTE
        outcomes = await self.flush()
        return outcomes[-1]

    @traced
    async def flush(self) -> list[SendOutcome]:
        """Envoie le tampon en un `_bulk` et renvoie le résultat de chaque document.

        Un lot vide ne fait aucune requête (`[]`). Après un échec
        transitoire épuisé (`reessais_max`), les documents sont retirés du
        tampon et rendus à l'appelant (`RETRYABLE`) — qui les retient et
        les renvoie — pour éviter un double envoi s'il renvoie aussi.
        """
        if not self._pending:
            return []
        pendings = self._pending
        self._pending = []
        logger.debug("flush | {} document(s)", len(pendings))
        response = await self._envoyer_lot(pendings)
        if response is None:
            # Échec transitoire épuisé : rendu à l'appelant (docstring).
            return [SendOutcome.RETRYABLE] * len(pendings)
        return await self._analyser_reponse(pendings, response)

    @traced
    async def _envoyer_lot(self, pendings: list[dict[str, Any]]) -> httpx.Response | None:
        """POSTe le NDJSON, avec retentes sur échec transitoire.

        Returns:
            La réponse HTTP, ou None après `reessais_max` retentes
            épuisées sur un échec transitoire (connexion, 429, 5xx…).
        """
        params: dict[str, str] = {}
        pipelines = {p["pipeline"] for p in pendings if p["pipeline"]}
        if len(pipelines) == 1:
            params["pipeline"] = next(iter(pipelines))
        essai = 0
        while True:
            try:
                response = await self._client.post(
                    f"{self.chemin_bulk}/_bulk",
                    content=self._corps_ndjson(pendings),
                    params=params or None,
                )
            except httpx.HTTPError as exc:
                logger.warning(_("envoi _bulk échoué (réseau) erreur={error}"), error=exc)
                if essai >= self._reessais_max:
                    return None
                essai += 1
                await asyncio.sleep(self._reessai_delai * 2 ** (essai - 1))
                continue
            if response.status_code in _STATUTS_PERMANENTS:
                logger.warning(_("envoi _bulk refusé définitivement statut={statut}"), statut=response.status_code)
                return response
            if response.status_code >= 500 or response.status_code in _STATUTS_TRANSITOIRES:
                logger.warning(_("envoi _bulk échoué statut={statut}"), statut=response.status_code)
                if essai >= self._reessais_max:
                    return response
                essai += 1
                await asyncio.sleep(self._reessai_delai * 2 ** (essai - 1))
                continue
            return response

    @traced
    async def _analyser_reponse(self, pendings: list[dict[str, Any]], response: httpx.Response) -> list[SendOutcome]:
        """Analyse la réponse `_bulk` et renvoie le résultat de chaque document.

        Par élément (`items[]`) quand le produit le rapporte ; sinon
        (dégradation documentée) le statut HTTP global décide pour tout
        le lot.
        """
        if response.status_code in _STATUTS_PERMANENTS:
            return [await self._permanent(pending, f"statut {response.status_code}") for pending in pendings]
        if not self.rapport_par_element:
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
            if statut in _STATUTS_TRANSITOIRES or statut >= 500:
                logger.warning(_("envoi _bulk élément échoué statut={statut}"), statut=statut)
                outcomes.append(SendOutcome.RETRYABLE)
                continue
            outcomes.append(await self._permanent(pending, str(erreur)))
        return outcomes

    @traced
    async def _permanent(self, pending: dict[str, Any], erreur: str) -> SendOutcome:
        """Échec définitif d'un document : DLQ si configurée, sinon RETRYABLE."""
        logger.warning(_("envoi _bulk échoué définitivement erreur={error}"), error=erreur)
        if self._dlq is None:
            return SendOutcome.RETRYABLE
        await self._dlq.write(pending["doc"], index=pending["index"], error=erreur)
        return SendOutcome.DEAD_LETTERED

    @traced
    async def close(self) -> None:
        """Ferme proprement le client HTTP."""
        await self._client.aclose()
