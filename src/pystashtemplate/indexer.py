# SPDX-License-Identifier: GPL-3.0-or-later
"""Indexation OpenSearch, un index par jour, avec document_id optionnel façon gabarit Logstash.

Rapatrié de RsysLogstach (`indexer.py`) lors de la correction du
découpage (Mathilde-Team/RsysLogstach#111, issue #110) : le contrat
d'indexation (`SendOutcome`, DLQ, fingerprint, gabarit d'index et de
`_id`) sert à tout outil bâti sur PyStashTemplate, pas seulement au
connecteur rsyslog. Dépend de `tracing`, `templates`, `fingerprint`,
`dlq` et de la sortie `OpenSearchOutput` (issue #12, client `_bulk`
commun de l'issue #11) ; jamais de `filters` ni d'`enrichment`.

`OpenSearchIndexer` traduit les options d'indexation en options de
sortie, délègue l'envoi et mappe les résultats en trois issues
(`SendOutcome`). L'option `hosts` peut porter plusieurs nœuds mais la
sortie n'en utilise qu'un (première entrée) : le failover revient à
l'appelant (`SendOutcome.RETRYABLE`, par exemple rsyslog qui retient le
message). `pipeline` est transmis pour toutes les actions, y compris
`update` et `delete`.
"""

from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Any

from loguru import logger

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.fingerprint import compute_fingerprint
from pystashtemplate.i18n import _
from pystashtemplate.outputs.elasticsearch_bulk import (
    ActionInvalideError as _ActionSortieInvalide,
)
from pystashtemplate.outputs.elasticsearch_bulk import SendOutcome as _SortieOutcome
from pystashtemplate.outputs.opensearch import OpenSearchOutput
from pystashtemplate.templates import CompiledTemplate, compile_template, render
from pystashtemplate.tracing import traced

OPENSEARCH_TIMEOUT = 3.0  # secondes, timeout socket du client OpenSearch

# Issue #126 : actions `bulk` du plugin de sortie Logstash.
ACTIONS = ("index", "create", "update", "delete")


class ActionInvalideError(ValueError):
    """Action inconnue, ou `update`/`delete` sans `_id` : échec permanent (#126)."""


class SendOutcome(enum.Enum):
    """Résultat d'`OpenSearchIndexer.send`, distingue trois issues.

    - INDEXED : le document a été indexé avec succès.
    - RETRYABLE : échec transitoire : rendre le message à l'appelant
      pour qu'il le retienne et le renvoie plus tard.
    - DEAD_LETTERED : échec permanent, écrit dans la DLQ locale si
      `dlq` est configuré (voir `dlq.py`) — décision de routage
      définitive, comme un `DropFilter` (`filters.py`) : ne JAMAIS
      retenter, mais confirmer l'accusé à l'appelant pour ne pas boucler
      indéfiniment. Si `dlq` n'est PAS configuré, ce cas n'est jamais
      produit : `send` retombe alors sur RETRYABLE.
    """

    INDEXED = "indexed"
    RETRYABLE = "retryable"
    DEAD_LETTERED = "dead_lettered"


class OpenSearchIndexer:
    """Indexation asynchrone des documents dans OpenSearch, un index par jour.

    document_id : si `document_id_template` est fourni, un gabarit façon
    Logstash (`templates.py`, ex. "%{[event_id]}" ou
    "%{host}-%{+YYYY.MM.dd}") est rendu contre chaque document pour
    produire un `_id` déterministe — même usage que `document_id =>
    "%{event_id}"` dans un output elasticsearch/opensearch de Logstash
    (voir le document de référence, section 5, "Identifiant de document
    dynamique"). Sans ce gabarit, OpenSearch génère un `_id` aléatoire
    (comportement par défaut). Un
    gabarit qui rend une chaîne vide (tous ses champs référencés absents)
    revient au comportement par défaut pour CE document plutôt que
    d'indexer avec un `_id` vide.

    dlq : si fourni, un échec d'indexation identifié comme
    PERMANENT (statut 400/404, action invalide — voir la sortie
    PyStashTemplate) est écrit dans cette dead letter queue locale plutôt
    que rendu indéfiniment à l'appelant — voir `dlq.py` et `SendOutcome`.
    Sans `dlq` (défaut), comportement historique inchangé : tout échec,
    permanent ou non, retombe sur `SendOutcome.RETRYABLE`.

    document_id_fingerprint_fields (`fingerprint.py`) : si
    fourni (même une liste vide), un `_id` déterministe est dérivé d'un
    HASH du contenu (voir `fingerprint.compute_fingerprint`) plutôt que du
    gabarit `document_id_template` — pour les documents sans champ identifiant naturel. `None`
    (défaut) désactive ce mécanisme et laisse `document_id_template` seul
    décider, comportement historique inchangé. Les deux options ne sont
    PAS combinées : si `document_id_fingerprint_fields` est fourni, il a
    priorité sur `document_id_template` pour CE document (voir
    `_document_id`) — cohérent avec le fait qu'un fingerprint EST déjà un
    identifiant déterministe, inutile de le combiner à un gabarit.
    """

    @traced
    def __init__(
        self,
        hosts: list[dict[str, str | int]],
        auth: tuple[str, str] | None,
        index_pattern: str,
        use_ssl: bool,
        verify_certs: bool,
        document_id_template: str | None = None,
        dlq: LocalDeadLetterQueue | None = None,
        document_id_fingerprint_fields: list[str] | None = None,
        document_id_fingerprint_method: str = "sha256",
        document_id_fingerprint_key: str | None = None,
        ca_certs: str | None = None,
        client_cert: str | None = None,
        client_key: str | None = None,
    ) -> None:
        """Initialise la sortie OpenSearch de PyStashTemplate.

        Args:
            hosts: Liste de nœuds, ex. [{"host": "127.0.0.1", "port": 9200}]
                — seule la PREMIÈRE entrée est utilisée (failover à
                l'appelant, voir la docstring du module).
            auth: Couple (utilisateur, mot de passe), ou None si non authentifié.
            index_pattern: Motif strftime du nom d'index quotidien (ex.
                "switch-logs-%Y.%m.%d").
            use_ssl: Utiliser HTTPS pour se connecter au nœud.
            verify_certs: Vérifier les certificats TLS (à désactiver
                seulement pour un cluster interne à certificat auto-signé).
            document_id_template: Gabarit Logstash optionnel (voir la
                docstring de la classe) pour un `_id` déterministe.
            dlq: Dead letter queue locale optionnelle (voir la
                docstring de la classe) pour les échecs permanents.
            document_id_fingerprint_fields: Liste de chemins de champs
                (voir la docstring de la classe) pour un `_id`
                dérivé d'un hash du contenu, au lieu de
                `document_id_template`. `None` (défaut) désactive ce
                mécanisme ; une liste VIDE empreint le document entier
                (voir `fingerprint.compute_fingerprint`).
            document_id_fingerprint_method: Algorithme de hachage
                (`sha256` par défaut, voir `fingerprint.py`).
            ca_certs: Fichier PEM (ou dossier) de l'autorité qui a signé le
                certificat du cluster ; `cacert` du plugin
                logstash-output-opensearch (issue #100). `None` : magasin
                système / certifi.
            client_cert: Certificat client PEM présenté au cluster (mTLS) ;
                `tls_certificate` du plugin Logstash (issue #100).
            client_key: Clé privée PEM non chiffrée de `client_cert` ;
                `tls_key` du plugin Logstash (issue #100).
        """
        self._index_pattern = index_pattern
        self._document_id_template: CompiledTemplate | None = (
            compile_template(document_id_template) if document_id_template else None
        )
        self._document_id_fingerprint_fields = document_id_fingerprint_fields
        self._document_id_fingerprint_method = document_id_fingerprint_method
        self._document_id_fingerprint_key = document_id_fingerprint_key
        self._dlq = dlq
        if len(hosts) > 1:
            logger.warning(
                _("Plusieurs nœuds configurés ({hosts}), seul le premier est utilisé"),
                hosts=len(hosts),
            )
        noeud = hosts[0]
        scheme = "https" if use_ssl else "http"
        options: dict[str, Any] = {
            "hosts": f"{scheme}://{noeud['host']}:{noeud['port']}",
            "index": index_pattern,
            "ssl": verify_certs,
            "timeout": OPENSEARCH_TIMEOUT,
            # Un document par lot : l'accusé de réception est TOUJOURS
            # réel (jamais EN_ATTENTE), comme l'ancien client qui
            # envoyait chaque document individuellement.
            "batch_size": 1,
            # retry_max=0 : parité avec l'ancien indexer, qui ne retentait
            # PAS lui-même (le transport opensearch-py retentait au pire
            # une fois par nœud) — le retour à l'appelant (RETRYABLE) reste
            # le mécanisme de réessai.
            "retry_max": 0,
        }
        if auth is not None:
            options["user"], options["password"] = auth
        if ca_certs is not None:
            options["cacert"] = ca_certs
        if client_cert is not None:
            options["ssl_certificate"] = client_cert
        if client_key is not None:
            options["ssl_key"] = client_key
        self.sortie = OpenSearchOutput(options, dlq=dlq)

    @traced
    async def health(self) -> dict[str, Any]:
        """Interroge le cluster (GET /) : sa version, ou lève l'erreur.

        Utilisé par le health check périodique de l'appelant (RsysLogstach, issue #37) —
        délégue à `OpenSearchOutput.health`.
        """
        return await self.sortie.health()

    @traced
    def _current_index(self) -> str:
        """Calcule le nom de l'index du jour à partir du motif configuré."""
        return datetime.now(timezone.utc).strftime(self._index_pattern)

    @traced
    def _document_id(self, doc: dict) -> str | None:
        """Calcule l'`_id` statique de `doc` : fingerprint si configuré, sinon gabarit, sinon None.

        Priorité (explicitée par l'issue #33) :
        `document_id_fingerprint_fields` d'abord (si non `None`, même une
        liste vide), sinon `document_id_template`. Configurer les deux à
        la fois est AUTORISÉ (le fingerprint est alors prioritaire — un
        gabarit peut servir de repli en changeant de configuration, sans
        rien retirer) ; les deux ne sont jamais combinés dans un MÊME
        `_id`. Ni l'un ni l'autre configuré, ou empreinte/gabarit rendu
        vide pour CE document : `None` (retombe sur un `_id` aléatoire).
        """
        if self._document_id_fingerprint_fields is not None:
            return compute_fingerprint(
                doc,
                self._document_id_fingerprint_fields,
                self._document_id_fingerprint_method,
                key=self._document_id_fingerprint_key,
            )
        if self._document_id_template is None:
            return None
        rendered = render(self._document_id_template, doc, on_missing="empty")
        return rendered or None

    @traced
    async def _client(self):
        """Client httpx paresseux de la sortie (couture : template d'index).

        Le client est partagé avec `send` (il est recréé paresseusement
        après une fermeture) — l'appelant qui le ferme doit savoir que
        le prochain envoi le rouvrira.
        """
        return self.sortie._client_http()

    @traced
    async def send(
        self,
        doc: dict,
        *,
        index: str | None = None,
        document_id: str | None = None,
        action: str = "index",
        pipeline: str | None = None,
        routing: str | None = None,
        doc_as_upsert: bool = False,
    ) -> SendOutcome:
        """Indexe un document dans OpenSearch (via `OpenSearchOutput.envoyer`).

        Args:
            doc: Document déjà enrichi, prêt à indexer.
            index: Nom d'index à utiliser pour CE document, au lieu du
                motif quotidien statique (`index_pattern`) — surcharge
                ponctuelle produite par exemple par un output
                `elasticsearch`/`opensearch` d'un pipeline `.conf`
                interprété (`interpreter.PipelineInterpreter`). `None`
                (par défaut) conserve le comportement historique.
            document_id: `_id` à utiliser pour CE document, au lieu du
                gabarit statique (`document_id_template`) — même logique
                de surcharge ponctuelle. Une chaîne vide est traitée comme
                `None` (retombe sur `document_id_template`/`_id`
                aléatoire), même convention que `_document_id` pour un
                gabarit qui rend une chaîne vide.

            action: `index` (défaut), `create`, `update` ou `delete` (#126),
                comme l'option `action` du plugin de sortie Logstash.
            pipeline: Pipeline d'ingestion OpenSearch (`index`, `create`).
            routing: Valeur de routage (`_routing`).
            doc_as_upsert: Pour `update` : crée le document s'il manque.

        Returns:
            `SendOutcome` (voir sa docstring) : `INDEXED` si
            l'indexation a réussi, `DEAD_LETTERED` si l'échec est permanent
            ET qu'une `dlq` est configurée, `RETRYABLE` sinon (échec
            transitoire, ou permanent sans `dlq` configurée — comportement
            historique).
        """
        resolved_index = index or self._current_index()
        resolved_id = document_id or self._document_id(doc)
        try:
            outcome = await self.sortie.envoyer(
                doc,
                index=resolved_index,
                document_id=resolved_id,
                action=action,
                pipeline=pipeline,
                routing=routing,
                doc_as_upsert=doc_as_upsert,
            )
        except _ActionSortieInvalide as exc:
            # La sortie valide l'action de façon synchrone (elle ne peut
            # pas tamponner un document sans `_id` pour `update`/
            # `delete`) : même chemin permanent que l'ancien `_executer`.
            logger.warning(_("Indexation OpenSearch échouée définitivement erreur={error}"), error=exc)
            if self._dlq is not None:
                await self._dlq.write(doc, index=resolved_index, error=str(exc))
                return SendOutcome.DEAD_LETTERED
            return SendOutcome.RETRYABLE
        if outcome is _SortieOutcome.DEAD_LETTERED:
            return SendOutcome.DEAD_LETTERED
        if outcome is _SortieOutcome.INDEXED:
            return SendOutcome.INDEXED
        return SendOutcome.RETRYABLE

    @traced
    async def close(self) -> None:
        """Ferme proprement la sortie (et son client HTTP)."""
        await self.sortie.close()
