# SPDX-License-Identifier: GPL-3.0-or-later
"""Issue #117 : `convert_datatype` et `tag_on_failure` du filtre `dissect` natif."""

from __future__ import annotations

import pytest

from pystashtemplate.interpreter import PipelineInterpreter


def _conf(doc: dict, corps: str) -> dict:
    PipelineInterpreter(f"filter {{ dissect {{ {corps} }} }}").apply(doc)
    return doc


MAPPING = 'mapping => { "message" => "%{n} %{f} %{[x][y]}" }'


def test_convert_datatype_int_float_et_chemin() -> None:
    doc = _conf(
        {"message": "3 2.5 7"}, MAPPING + ' convert_datatype => { "n" => "int" "f" => "float" "[x][y]" => "int" }'
    )
    assert (doc["n"], doc["f"], doc["x"]) == (3, 2.5, {"y": 7})


@pytest.mark.parametrize(
    "conversion",
    ['"n" => "float" "f" => "int"', '"n" => "bool"', '"absent" => "int"'],
    ids=["impossible", "type-inconnu", "champ-absent"],
)
def test_convert_datatype_sans_effet(conversion: str) -> None:
    doc = _conf({"message": "a 2.5 7"}, MAPPING + f" convert_datatype => {{ {conversion} }}")
    assert (doc["n"], doc["f"]) == ("a", "2.5")


def test_convert_datatype_seul_et_options_communes() -> None:
    doc = _conf({"n": "4"}, 'convert_datatype => { "n" => "int" } add_tag => ["ok"]')
    assert doc == {"n": 4, "tags": ["ok"]}


def test_sans_mapping_ni_conversion_pas_d_options_communes() -> None:
    assert _conf({"message": "a"}, 'add_tag => ["ok"]') == {"message": "a"}


@pytest.mark.parametrize("doc", [{"autre": 1}, {"message": ""}, {"message": "sans espace"}])
def test_echec_tag_par_defaut_sans_conversion(doc: dict) -> None:
    _conf(doc, 'mapping => { "message" => "%{a} %{b} %{c}" } convert_datatype => { "a" => "int" } add_tag => ["ok"]')
    assert doc["tags"] == ["_dissectfailure"]


def test_tag_on_failure_personnalise_et_mapping_partiel() -> None:
    doc = _conf(
        {"message": "1 2", "autre": "x"},
        'mapping => { "message" => "%{a} %{b}" "autre" => "%{c}-%{d}" } tag_on_failure => ["e1", "e2"]',
    )
    assert doc == {"message": "1 2", "autre": "x", "a": "1", "b": "2", "tags": ["e1", "e2"]}


def test_source_imbriquee() -> None:
    doc = _conf({"m": {"t": "1 2"}}, 'mapping => { "[m][t]" => "%{a} %{b}" }')
    assert (doc["a"], doc["b"]) == ("1", "2")
