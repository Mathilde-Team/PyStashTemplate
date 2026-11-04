# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la relecture des éléments `items[]` sous l'action envoyée (Quickwit, SigLens)."""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from pystashtemplate.outputs.relecture import relire_sous_action

REQUETE = httpx.Request("POST", "http://faux/_bulk")


def reponse(corps: Any, statut: int = 200) -> httpx.Response:
    return httpx.Response(statut, json=corps, request=REQUETE)


def test_elements_ranges_sous_l_action_de_chaque_document() -> None:
    pendings = [{"action": "create"}, {"action": "index"}]
    corps = {"errors": False, "items": [{"index": {"status": 201}}, {"index": {"status": 201}}]}
    relue = relire_sous_action(pendings, reponse(corps), "essai")
    assert relue.json()["items"] == [{"create": {"status": 201}}, {"index": {"status": 201}}]
    assert relue.json()["errors"] is False
    assert relue.status_code == 200
    assert relue.request is REQUETE


def test_statut_hors_element_replace_dans_l_element() -> None:
    corps = {"errors": True, "items": [{"index": {"error": {"type": "x"}}, "status": 400}]}
    relue = relire_sous_action([{"action": "create"}], reponse(corps), "essai")
    assert relue.json()["items"] == [{"create": {"error": {"type": "x"}, "status": 400}}]


def test_statut_de_l_element_prioritaire() -> None:
    corps = {"items": [{"index": {"status": 201}, "status": 400}]}
    relue = relire_sous_action([{"action": "index"}], reponse(corps), "essai")
    assert relue.json()["items"] == [{"index": {"status": 201}}]


@pytest.mark.parametrize(
    "corps",
    [
        {"took": 1},
        {"items": []},
        {"items": [{}]},
        {"items": ["texte"]},
        {"items": [{"index": "texte"}]},
        [],
    ],
)
def test_reponse_non_relue_rendue_telle_quelle(corps: Any) -> None:
    origine = reponse(corps)
    assert relire_sous_action([{"action": "create"}], origine, "essai") is origine


def test_corps_illisible_rendu_tel_quel() -> None:
    origine = httpx.Response(200, content=b"pas du json", request=REQUETE)
    assert relire_sous_action([{"action": "create"}], origine, "essai") is origine
