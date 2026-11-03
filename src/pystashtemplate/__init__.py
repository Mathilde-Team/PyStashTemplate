# SPDX-License-Identifier: GPL-3.0-or-later
"""PyStashTemplate : sémantique Logstash partagée (découpage #111).

Bibliothèque extraite de RsysLogstach (issue #111, décision du 03/11/2026) :
gabarits sprintf `%{...}`, filtres, interprétation des `.conf` Logstash,
enrichissement (GeoIP/mmdb, translate, DNS), plugins natifs (grok, kv,
dissect, fingerprint, useragent, xml, clone/split...). RsysLogstach (le
connecteur rsyslog : omprog, indexer, dlq, metrics) en dépend ; d'autres
outils (…beat) pourront faire de même. Prévu aussi comme service autonome
asynchrone à l'écoute d'un port (sflow, snmp — voir la feuille de route).
"""

from __future__ import annotations
