# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de `multievents.py` et de `apply_events` (issue #141) : clone et split."""

from __future__ import annotations


import pytest

from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.multievents import SPLIT_FAILURE_TAG, CloneFilter, SplitFilter, _ruby_split


def _events(conf: str, doc: dict) -> list[tuple[dict, list]]:
    return PipelineInterpreter(conf).apply_events(doc)


def _docs(conf: str, doc: dict) -> list[dict]:
    return [event for event, _ in _events(conf, doc)]


# --- clone ----------------------------------------------------------------


def test_clone_ecs_ajoute_le_tag_et_garde_l_original():
    doc = {"message": "m", "tags": ["a"]}
    docs = _docs('filter { clone { clones => ["c1", "c2"] } }', doc)
    assert docs[0] is doc
    assert docs == [
        {"message": "m", "tags": ["a"]},
        {"message": "m", "tags": ["a", "c1"]},
        {"message": "m", "tags": ["a", "c2"]},
    ]


def test_clone_ecs_disabled_ecrit_type():
    docs = _docs('filter { clone { clones => "copie" ecs_compatibility => disabled } }', {"type": "x"})
    assert docs == [{"type": "x"}, {"type": "copie"}]


def test_clone_tags_chaine_et_copie_profonde():
    doc = {"tags": "t", "a": {"b": 1}}
    clone = CloneFilter({"clones": ["c"]}).apply(doc)[0]
    clone["a"]["b"] = 2
    assert clone["tags"] == ["t", "c"]
    assert doc == {"tags": "t", "a": {"b": 1}}


def test_clone_options_communes_sur_les_copies_seulement():
    docs = _docs('filter { clone { clones => ["c"] add_field => { "copie" => "oui" } } }', {"m": 1})
    assert docs == [{"m": 1}, {"m": 1, "tags": ["c"], "copie": "oui"}]


def test_clone_puis_conditions_sur_la_copie():
    conf = 'filter { clone { clones => ["c"] } if "c" in [tags] { mutate { add_field => { "x" => "1" } } } }'
    assert _docs(conf, {"m": 1}) == [{"m": 1}, {"m": 1, "tags": ["c"], "x": "1"}]


def test_clone_drop_n_ecarte_que_la_copie():
    conf = 'filter { clone { clones => ["c"] } if "c" in [tags] { drop {} } }'
    assert _docs(conf, {"m": 1}) == [{"m": 1}]


def test_clone_drop_de_l_original_garde_la_copie():
    conf = 'filter { clone { clones => ["c"] } if "c" not in [tags] { drop {} } }'
    pi = PipelineInterpreter(conf)
    doc = {"m": 1}
    assert [e for e, _ in pi.apply_events(doc)] == [{"m": 1, "tags": ["c"]}]
    assert pi.apply({"m": 1}) is None


@pytest.mark.parametrize("options", [{}, {"clones": [1]}, {"clones": {"a": "b"}}])
def test_clone_options_invalides(options):
    with pytest.raises(ValueError):
        CloneFilter(options)


def test_clone_invalide_bloc_ignore():
    pi = PipelineInterpreter("filter { clone { } clone { } }")
    assert [e for e, _ in pi.apply_events({"m": 1})] == [{"m": 1}]


def test_clone_liste_vide():
    assert _docs("filter { clone { clones => [] } }", {"m": 1}) == [{"m": 1}]


# --- split ----------------------------------------------------------------


def test_split_message_par_defaut():
    doc = {"message": "a\nb\n\nc"}
    docs = _docs("filter { split { } }", doc)
    assert docs == [{"message": "a"}, {"message": "b"}, {"message": "c"}]
    assert all(d is not doc for d in docs)


def test_split_terminator_target_et_options_communes():
    conf = 'filter { split { field => "[l]" terminator => "," target => "[v]" add_tag => ["s"] } }'
    assert _docs(conf, {"l": "x,y"}) == [{"l": "x,y", "v": "x", "tags": ["s"]}, {"l": "x,y", "v": "y", "tags": ["s"]}]


def test_split_liste_sans_target_remplace_le_champ():
    assert _docs('filter { split { field => "l" } }', {"l": [1, None, "", {"a": 2}]}) == [{"l": 1}, {"l": {"a": 2}}]


def test_split_liste_avec_target_garde_le_champ():
    assert _docs('filter { split { field => "l" target => "e" } }', {"l": ["a"]}) == [{"l": ["a"], "e": "a"}]


def test_split_un_seul_morceau_inchange_sans_option_commune():
    doc = {"message": "seul"}
    assert _docs('filter { split { add_tag => ["s"] } }', doc) == [{"message": "seul"}]


def test_split_chaine_vide_ou_liste_vide_ecarte_l_evenement():
    assert _events("filter { split { } }", {"message": ""}) == []
    assert _events('filter { split { field => "l" } }', {"l": []}) == []


@pytest.mark.parametrize(
    "doc", [{}, {"message": 3}, {"message": 3, "tags": "t"}, {"message": None, "tags": [SPLIT_FAILURE_TAG]}]
)
def test_split_champ_non_decoupable_tague(doc):
    (event,) = _docs("filter { split { } }", dict(doc))
    assert event["tags"][-1] == SPLIT_FAILURE_TAG
    assert event["tags"].count(SPLIT_FAILURE_TAG) == 1


def test_split_puis_filtres_suivants_sur_chaque_morceau():
    conf = (
        'filter { split { terminator => " " } mutate { add_field => { "n" => "%{message}!" } } } '
        'output { opensearch { index => "i-%{message}" } }'
    )
    events = _events(conf, {"message": " a  b "})
    assert [e for e, _ in events] == [{"message": "a", "n": "a!"}, {"message": "b", "n": "b!"}]
    assert [a[0].options["index"] for _, a in events] == ["i-a", "i-b"]


def test_split_dans_une_branche():
    conf = (
        'filter { if [k] == "1" { split { } mutate { add_tag => ["in"] } } '
        'else { mutate { add_tag => ["out"] } } mutate { add_tag => ["fin"] } }'
    )
    assert _docs(conf, {"k": "1", "message": "a\nb"}) == [
        {"k": "1", "message": "a", "tags": ["in", "fin"]},
        {"k": "1", "message": "b", "tags": ["in", "fin"]},
    ]
    assert _docs(conf, {"k": "2", "message": "a\nb"}) == [{"k": "2", "message": "a\nb", "tags": ["out", "fin"]}]


def test_branche_apres_ecartement_total():
    conf = 'filter { drop { } if [a] { mutate { add_tag => ["x"] } } else if [b] { } else { } mutate { } }'
    assert _events(conf, {"a": 1}) == []


def test_plusieurs_sections_filter_avant_output():
    conf = 'output { opensearch { index => "%{message}" } } filter { split { terminator => "," } }'
    assert [a[0].options["index"] for _, a in _events(conf, {"message": "a,b"})] == ["a", "b"]


@pytest.mark.parametrize("options", [{"field": 1}, {"terminator": ["x"]}, {"target": 2}, {"field": ""}])
def test_split_options_invalides(options):
    with pytest.raises(ValueError):
        SplitFilter(options)


def test_split_invalide_bloc_ignore():
    assert _docs('filter { split { field => "" } }', {"message": "a\nb"}) == [{"message": "a\nb"}]


@pytest.mark.parametrize(
    ("text", "terminator", "expected"),
    [
        ("", ",", []),
        ("a,b,", ",", ["a", "b", ""]),
        ("  a b ", " ", ["a", "b", ""]),
        ("   ", " ", []),
        ("ab", "", ["a", "b", ""]),
    ],
)
def test_ruby_split(text, terminator, expected):
    assert _ruby_split(text, terminator) == expected


def test_apply_renvoie_none_si_split_remplace_l_original():
    assert PipelineInterpreter("filter { split { } }").apply({"message": "a\nb"}) is None



