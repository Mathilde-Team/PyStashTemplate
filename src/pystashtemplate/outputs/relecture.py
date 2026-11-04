# SPDX-License-Identifier: GPL-3.0-or-later
"""Relecture des éléments `items[]` sous l'action envoyée.

Plusieurs produits compatibles `_bulk` rapportent chaque élément sous une
clé fixe, quelle que soit l'action envoyée : `index` pour Quickwit et
SigLens, `create` pour GreptimeDB. Le client commun lit l'élément sous
l'action du document : sans relecture, il n'y trouverait rien et
compterait indexé un document refusé.

SigLens place en outre le `status` d'un élément en échec à côté de
l'objet de l'action (`{"index": {"error": …}, "status": 400}`) : il est
replacé dans l'élément.
"""

from __future__ import annotations

from typing import Any

import httpx
from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.tracing import traced


@traced
def relire_sous_action(pendings: list[dict[str, Any]], response: httpx.Response, produit: str) -> httpx.Response:
    """Réponse dont chaque élément est rangé sous l'action du document correspondant.

    Un élément par document est attendu, dans l'ordre. Si la réponse ne
    s'y prête pas (corps illisible, sans `items`, nombre inattendu…), elle
    est rendue telle quelle : le client commun applique alors ses propres
    règles (lot rendu à l'appelant).
    """
    try:
        corps = response.json()
        relus: list[dict[str, Any]] = []
        for pending, item in zip(pendings, corps["items"], strict=True):
            detail = dict(next(iter(item.values())))
            if "status" not in detail and "status" in item:
                detail["status"] = item["status"]
            relus.append({pending["action"]: detail})
    except (ValueError, KeyError, TypeError, AttributeError, StopIteration) as exc:
        logger.warning(_("réponse _bulk {produit} non relue erreur={error}"), produit=produit, error=exc)
        return response
    corps["items"] = relus
    return httpx.Response(response.status_code, json=corps, request=response.request)
