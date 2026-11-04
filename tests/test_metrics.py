# SPDX-License-Identifier: GPL-3.0-or-later
"""Métriques Prometheus (`pystashtemplate.metrics`, issue #110).

Rapatriées de RsysLogstach (issue #35 du connecteur) : registre propre à
chaque instance, préfixe paramétrable, et `Metrics` branché comme
observateur des sorties (#10).
"""

from __future__ import annotations

import urllib.request

from prometheus_client.parser import text_string_to_metric_families

from pystashtemplate.metrics import ERROR_CAUSES, OUTCOMES, Metrics
from pystashtemplate.outputs.metriques import ObservateurSortie


def _valeurs(metrics: Metrics) -> dict[tuple[str, tuple], float]:
    """Échantillons exposés, indexés par (nom, étiquettes triées)."""
    valeurs = {}
    for famille in text_string_to_metric_families(metrics.render().decode("utf-8")):
        for echantillon in famille.samples:
            valeurs[(echantillon.name, tuple(sorted(echantillon.labels.items())))] = echantillon.value
    return valeurs


def _issue(metrics: Metrics, outcome: str) -> float:
    return _valeurs(metrics)[("pystashtemplate_messages_total", (("outcome", outcome),))]


def _erreur(metrics: Metrics, cause: str) -> float:
    return _valeurs(metrics)[("pystashtemplate_errors_total", (("cause", cause),))]


def test_series_presentes_a_zero_des_le_depart():
    metrics = Metrics()
    valeurs = _valeurs(metrics)
    assert valeurs[("pystashtemplate_messages_received_total", ())] == 0
    for outcome in OUTCOMES:
        assert _issue(metrics, outcome) == 0
    for cause in ERROR_CAUSES:
        assert _erreur(metrics, cause) == 0
    assert valeurs[("pystashtemplate_dlq_size_bytes", ())] == 0
    assert valeurs[("pystashtemplate_retry_pending", ())] == 0
    assert valeurs[("pystashtemplate_index_latency_seconds_count", ())] == 0


def test_registres_independants():
    a, b = Metrics(), Metrics()
    a.message_received("x")
    assert _valeurs(a)[("pystashtemplate_messages_received_total", ())] == 1
    assert _valeurs(b)[("pystashtemplate_messages_received_total", ())] == 0


def test_taille_des_messages_en_octets_utf8():
    metrics = Metrics()
    metrics.message_received("é" * 100)  # 200 octets
    valeurs = _valeurs(metrics)
    assert valeurs[("pystashtemplate_message_size_bytes_sum", ())] == 200
    assert valeurs[("pystashtemplate_message_size_bytes_bucket", (("le", "128.0"),))] == 0
    assert valeurs[("pystashtemplate_message_size_bytes_bucket", (("le", "256.0"),))] == 1


def test_renvois_en_attente_remis_a_zero_par_une_indexation():
    metrics = Metrics()
    metrics.outcome("retryable")
    metrics.outcome("retryable")
    metrics.outcome("dropped")
    assert _valeurs(metrics)[("pystashtemplate_retry_pending", ())] == 2
    metrics.outcome("indexed")
    assert _valeurs(metrics)[("pystashtemplate_retry_pending", ())] == 0
    assert _issue(metrics, "retryable") == 2


def test_taille_de_la_dlq(tmp_path):
    dlq = tmp_path / "dlq.jsonl"
    metrics = Metrics(dlq_path=str(dlq))
    assert _valeurs(metrics)[("pystashtemplate_dlq_size_bytes", ())] == 0  # fichier pas encore créé
    dlq.write_text("0123456789", encoding="utf-8")
    assert _valeurs(metrics)[("pystashtemplate_dlq_size_bytes", ())] == 10


def test_endpoint_http_reel():
    metrics = Metrics()
    metrics.error("exception")
    server, thread = metrics.serve(0)
    try:
        port = server.server_port
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as reponse:
            texte = reponse.read().decode("utf-8")
    finally:
        server.shutdown()
        thread.join(timeout=5)
    assert 'pystashtemplate_errors_total{cause="exception"} 1.0' in texte


def test_serve_transmet_adresse_et_registre():
    appels = []
    metrics = Metrics()
    assert metrics.serve(9464, "0.0.0.0", server_factory=lambda *a, **kw: appels.append((a, kw)) or "srv") == "srv"
    assert appels == [((9464,), {"addr": "0.0.0.0", "registry": metrics.registry})]


def test_prefixe_parametrable():
    metrics = Metrics(prefixe="rsyslogstach")
    metrics.message_received("x")
    valeurs = _valeurs(metrics)
    assert valeurs[("rsyslogstach_messages_received_total", ())] == 1
    assert not any(nom.startswith("pystashtemplate_") for nom, _ in valeurs)


def test_observateur_des_sorties():
    metrics = Metrics()
    assert isinstance(metrics, ObservateurSortie)
    metrics.evenements("opensearch", "indexed", 3)
    metrics.evenements("opensearch", "rejected")
    metrics.reessai("opensearch")
    metrics.latence("opensearch", 0.2)
    metrics.file("opensearch", 7)
    valeurs = _valeurs(metrics)
    sortie = ("output", "opensearch")
    assert valeurs[("pystashtemplate_output_events_total", (("outcome", "indexed"), sortie))] == 3
    assert valeurs[("pystashtemplate_output_events_total", (("outcome", "rejected"), sortie))] == 1
    assert valeurs[("pystashtemplate_output_retries_total", (sortie,))] == 1
    assert valeurs[("pystashtemplate_output_request_duration_seconds_count", (sortie,))] == 1
    assert valeurs[("pystashtemplate_output_request_duration_seconds_sum", (sortie,))] == 0.2
    assert valeurs[("pystashtemplate_output_queue_size", (sortie,))] == 7


def test_erreur_comptee():
    metrics = Metrics()
    metrics.error("invalid_json")
    assert _erreur(metrics, "invalid_json") == 1
