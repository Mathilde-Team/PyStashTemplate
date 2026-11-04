# SPDX-License-Identifier: GPL-3.0-or-later
"""Métriques Prometheus d'un connecteur ou d'un service (issue #110).

Rapatrié de RsysLogstach (`metrics.py`, issue #35 du connecteur) lors de
la correction du découpage (Mathilde-Team/RsysLogstach#111) : tout outil
bâti sur PyStashTemplate — connecteur rsyslog, service autonome (#39),
…beat — expose ainsi les mêmes métriques. Sur un registre PROPRE à
chaque `Metrics` (jamais le registre global de `prometheus_client`, pour
que plusieurs instances, ou les tests, ne se marchent pas dessus), avec
le préfixe `prefixe` (`pystashtemplate` par défaut ; RsysLogstach passe
`rsyslogstach` pour garder ses noms historiques) :

- compteurs : `<prefixe>_messages_received_total`,
  `<prefixe>_messages_total{outcome=...}` (issue d'un message :
  `indexed`, `dropped`, `dead_lettered`, `retryable`) et
  `<prefixe>_errors_total{cause=...}` (`invalid_json`, `not_an_object`,
  `exception`) ;
- histogrammes : `<prefixe>_index_latency_seconds` (durée d'un envoi)
  et `<prefixe>_message_size_bytes` (message reçu, en octets UTF-8) ;
- jauges : `<prefixe>_dlq_size_bytes` (taille du fichier DLQ courant,
  lue à chaque collecte) et `<prefixe>_retry_pending` (échecs
  transitoires consécutifs depuis le dernier envoi réussi) ;
- métriques des sorties (#10) : `Metrics` implémente
  `ObservateurSortie` (`outputs/metriques.py`) — à brancher comme
  `observateur` d'une sortie — et expose
  `<prefixe>_output_events_total{output,outcome}`,
  `<prefixe>_output_retries_total{output}`,
  `<prefixe>_output_request_duration_seconds{output}` et
  `<prefixe>_output_queue_size{output}` — mêmes noms que
  `CompteursSortie.exposition` (préfixe `<prefixe>_output`).

L'endpoint HTTP `/metrics` n'est démarré que sur demande (`serve`).
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

from loguru import logger
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, start_http_server

from pystashtemplate.i18n import _
from pystashtemplate.tracing import traced

OUTCOMES = ("indexed", "dropped", "dead_lettered", "retryable")
ERROR_CAUSES = ("invalid_json", "not_an_object", "exception")

# Bornes en secondes : une indexation locale prend quelques ms, un cluster
# lent ou un failover peut dépasser la seconde.
_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
# Bornes en octets : un message syslog tient d'ordinaire sous 1 Kio, la
# limite de lecture de stdin est de 16 Mio (`omprog._stdin_reader`).
_SIZE_BUCKETS = (128, 256, 512, 1024, 2048, 4096, 8192, 16384, 65536, 262144, 1048576)


class Metrics:
    """Ensemble des métriques d'un pont, sur son propre registre Prometheus."""

    @traced
    def __init__(self, *, dlq_path: str | None = None, prefixe: str = "pystashtemplate") -> None:
        """Crée les métriques sur un registre propre.

        Args:
            dlq_path: Fichier DLQ dont la taille alimente la jauge (0 sans DLQ).
            prefixe: Préfixe des noms de métriques (`pystashtemplate` par
                défaut ; RsysLogstach passe `rsyslogstach`).
        """
        self.registry = CollectorRegistry()
        self._dlq_path = dlq_path
        self.prefixe = prefixe
        self.received = Counter(f"{prefixe}_messages_received", "Messages reçus", registry=self.registry)
        self.outcomes = Counter(
            f"{prefixe}_messages", "Messages traités, par issue", ["outcome"], registry=self.registry
        )
        self.errors = Counter(
            f"{prefixe}_errors", "Erreurs de traitement, par cause", ["cause"], registry=self.registry
        )
        for outcome in OUTCOMES:
            self.outcomes.labels(outcome=outcome)
        for cause in ERROR_CAUSES:
            self.errors.labels(cause=cause)
        self.index_latency = Histogram(
            f"{prefixe}_index_latency_seconds",
            "Durée d'un envoi à la destination",
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.message_size = Histogram(
            f"{prefixe}_message_size_bytes",
            "Taille des messages reçus",
            buckets=_SIZE_BUCKETS,
            registry=self.registry,
        )
        self.dlq_size = Gauge(f"{prefixe}_dlq_size_bytes", "Taille du fichier DLQ courant", registry=self.registry)
        self.dlq_size.set_function(self._dlq_size_bytes)
        self.retry_pending = Gauge(
            f"{prefixe}_retry_pending",
            "Échecs transitoires consécutifs depuis le dernier envoi réussi",
            registry=self.registry,
        )
        # Métriques des sorties (#10) : Metrics est un ObservateurSortie.
        self.output_events = Counter(
            f"{prefixe}_output_events",
            "Documents par sortie et par issue",
            ["output", "outcome"],
            registry=self.registry,
        )
        self.output_retries = Counter(
            f"{prefixe}_output_retries", "Retentes des sorties", ["output"], registry=self.registry
        )
        self.output_latency = Histogram(
            f"{prefixe}_output_request_duration_seconds",
            "Durée d'une requête d'une sortie vers sa cible",
            ["output"],
            buckets=_LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.output_queue = Gauge(
            f"{prefixe}_output_queue_size",
            "Documents en attente dans le tampon d'une sortie",
            ["output"],
            registry=self.registry,
        )

    @traced
    def _dlq_size_bytes(self) -> float:
        """Taille du fichier DLQ courant ; 0 sans DLQ ou tant que le fichier n'existe pas."""
        if not self._dlq_path:
            return 0.0
        try:
            return float(os.path.getsize(self._dlq_path))
        except OSError as exc:
            logger.debug("taille de la DLQ illisible, 0 retenu : {}", exc)
            return 0.0

    @traced
    def message_received(self, line: str) -> None:
        """Compte une ligne reçue et observe sa taille en octets."""
        self.received.inc()
        self.message_size.observe(len(line.encode("utf-8")))

    @traced
    def outcome(self, name: str) -> None:
        """Compte l'issue d'un message (`OUTCOMES`) et tient la jauge des renvois à jour."""
        self.outcomes.labels(outcome=name).inc()
        if name == "retryable":
            self.retry_pending.inc()
        elif name == "indexed":
            self.retry_pending.set(0)

    @traced
    def error(self, cause: str) -> None:
        """Compte une erreur de traitement (`ERROR_CAUSES`)."""
        self.errors.labels(cause=cause).inc()

    # -- ObservateurSortie (outputs/metriques.py, #10) ---------------------

    @traced
    def evenements(self, sortie: str, issue: str, nombre: int = 1) -> None:
        """`nombre` documents de la sortie `sortie` ont eu l'issue `issue`."""
        self.output_events.labels(output=sortie, outcome=issue).inc(nombre)

    @traced
    def reessai(self, sortie: str) -> None:
        """La sortie retente une requête après un échec transitoire."""
        self.output_retries.labels(output=sortie).inc()

    @traced
    def latence(self, sortie: str, secondes: float) -> None:
        """Durée d'une requête de la sortie vers sa cible."""
        self.output_latency.labels(output=sortie).observe(secondes)

    @traced
    def file(self, sortie: str, taille: int) -> None:
        """Documents en attente dans le tampon de la sortie."""
        self.output_queue.labels(output=sortie).set(taille)

    @traced
    def render(self) -> bytes:
        """Exposition texte Prometheus du registre (contenu de `/metrics`)."""
        return generate_latest(self.registry)

    @traced
    def serve(
        self, port: int, addr: str = "127.0.0.1", *, server_factory: Callable[..., Any] = start_http_server
    ) -> Any:
        """Démarre l'endpoint HTTP `/metrics` (thread de `prometheus_client`) et le renvoie.

        `server_factory` est injectable pour les tests. Écoute sur la boucle
        locale par défaut : exposer les métriques au réseau est un choix
        explicite (`addr=\"0.0.0.0\"`).
        """
        logger.info(_("Métriques Prometheus sur http://{addr}:{port}/metrics"), addr=addr, port=port)
        return server_factory(port, addr=addr, registry=self.registry)
