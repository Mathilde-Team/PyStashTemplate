# SPDX-License-Identifier: GPL-3.0-or-later
"""Issue #115 : filtre `json` natif dans `--pipeline-conf`."""

from __future__ import annotations

import asyncio

from pystashtemplate.interpreter import PipelineInterpreter


def _run(coro):
    # Sans pytest-asyncio (convention du dépôt, cf. RsysLogstach) : asyncio.run direct (#34).
    return asyncio.run(coro)


def _conf(doc: dict, corps: str) -> dict:
    _run(PipelineInterpreter(f"filter {{ json {{ {corps} }} }}").apply(doc))
    return doc


def test_fusion_a_la_racine() -> None:
    doc = _conf({"message": '{"a": 1, "b": {"c": "x"}}'}, 'source => "message" add_tag => ["ok"]')
    assert doc == {"message": '{"a": 1, "b": {"c": "x"}}', "a": 1, "b": {"c": "x"}, "tags": ["ok"]}


def test_target_imbrique_et_non_objet() -> None:
    assert _conf({"m": "[1, 2]"}, 'source => "m" target => "[p][v]"')["p"] == {"v": [1, 2]}
    assert _conf({"m": '"x"'}, 'source => "m" target => "t"')["t"] == "x"


def test_source_imbriquee() -> None:
    assert _conf({"a": {"b": '{"k": true}'}}, 'source => "[a][b]"')["k"] is True


def test_json_invalide_tag_par_defaut() -> None:
    doc = _conf({"message": "{pas json"}, 'source => "message" add_tag => ["ok"]')
    assert doc["tags"] == ["_jsonparsefailure"]


def test_tag_on_failure_personnalise_et_non_texte() -> None:
    doc = _conf({"message": 3}, 'source => "message" tag_on_failure => ["x", "y"]')
    assert doc["tags"] == ["x", "y"]


def test_skip_on_invalid_json() -> None:
    doc = _conf({"message": "{pas json"}, 'source => "message" skip_on_invalid_json => true')
    assert doc == {"message": "{pas json"}


def test_non_objet_sans_target_echoue() -> None:
    doc = _conf({"message": "[1]"}, 'source => "message"')
    assert doc == {"message": "[1]", "tags": ["_jsonparsefailure"]}


def test_source_absente_ou_nulle_sans_effet() -> None:
    assert _conf({"autre": 1}, 'source => "message" add_tag => ["ok"]') == {"autre": 1}
    assert _conf({"message": None}, 'source => "message"') == {"message": None}


def test_option_source_manquante() -> None:
    assert _conf({"message": "{}"}, 'add_tag => ["ok"]') == {"message": "{}"}
