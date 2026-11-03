# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests des conditions, branches et options de `src/interpreter.py` (issue #73).

Sémantique des conditions Logstash (comparaisons avec coercition chaîne ↔
nombre, regex, `in`/`not in`, négations, `and`/`or`/`xor`/`nand` de gauche
à droite), chaînes `if`/`else if`/`else` dans `filter{}` comme dans
`output{}`, normalisation des options répétées et rendu des gabarits dans
les options de sortie. Chaque cas est écrit en vrai pipeline `.conf`, tel
que le parse `logstash_pipeline_parser`.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pystashtemplate.interpreter import PipelineInterpreter, _coerce_literal, _evaluate_expression


def _vrai(condition: str, doc: dict) -> bool:
    """La condition `if <condition>` retient-elle la branche pour `doc` ?"""
    actions = PipelineInterpreter(f"output {{ if {condition} {{ oui {{ }} }} }}").apply(doc)
    return bool(actions)


DOC = {
    "level": "ERROR",
    "severity": 3,
    "code": "7",
    "ratio": 0.5,
    "actif": False,
    "vide": "",
    "zero": 0,
    "nul": None,
    "tags": ["_grokparsefailure", "reseau"],
    "host": {"ip": "127.0.0.1", "name": "sw1.lan"},
    "message": "lien eth0 up",
}


class TestComparaisons:
    @pytest.mark.parametrize(
        ("condition", "attendu"),
        [
            ('[level] == "ERROR"', True),
            ('[level] != "ERROR"', False),
            ("[severity] == 3", True),
            ('[severity] == "3"', True),  # chaîne convertie en nombre
            ("[code] == 7", True),  # idem, dans l'autre sens
            ("[code] > 5", True),
            ("[severity] < 5", True),
            ("[severity] <= 3", True),
            ("[severity] >= 4", False),
            ("[ratio] >= 0.5", True),
            ('[ratio] == "0.5"', True),
            ('[severity] == "trois"', False),  # pas un nombre : opérandes inchangés
            ('"trois" == [severity]', False),
            ("[level] > 5", False),  # str > int : non comparable, faux
            ('[host][ip] == "127.0.0.1"', True),  # littéral IP ramené en chaîne
            ("[absent] == 1", False),
            ("[absent] != 1", True),  # un absent n'est égal à rien
            ("[absent] < 1", False),
            ("1 != [absent]", True),
        ],
    )
    def test_comparaison(self, condition: str, attendu: bool) -> None:
        assert _vrai(condition, DOC) is attendu


class TestRegexEtIn:
    @pytest.mark.parametrize(
        ("condition", "attendu"),
        [
            ("[message] =~ /eth\\d+ up$/", True),
            ("[message] !~ /down/", True),
            ("[severity] =~ /^3$/", True),  # valeur non textuelle comparée en texte
            ("[absent] =~ /.*/", False),
            ("[absent] !~ /.*/", True),
            ('"_grokparsefailure" in [tags]', True),
            ('"absent" in [tags]', False),
            ('"_grokparsefailure" not in [tags]', False),
            ('"eth0" in [message]', True),  # sous-chaîne
            ('[level] in ["ERROR", "WARN"]', True),
            ('[level] not in ["INFO", "DEBUG"]', True),
            ("[severity] in [1, 2, 3]", True),
            ('"x" in [severity]', False),  # ni chaîne ni tableau
            ("[absent] in [tags]", False),
            ('"x" in [absent]', False),
            ('[absent] not in ["a", "b"]', True),
        ],
    )
    def test_regex_et_appartenance(self, condition: str, attendu: bool) -> None:
        assert _vrai(condition, DOC) is attendu

    def test_tableau_a_un_element_est_une_reference_de_champ(self) -> None:
        """Fidèle à Logstash : `["ERROR"]` est la référence du champ `"ERROR"`, pas un tableau.

        Documentation Elastic (« Field References Deep Dive ») : « a quoted
        value (such as `["foo"]`) is considered a field reference and isn't
        treated as a single element array ». Le parseur produit d'ailleurs la
        chaîne `'["ERROR"]'`, pas une liste.
        """
        assert _vrai('[level] in ["ERROR"]', DOC) is False
        assert _vrai('[level] in ["ERROR"]', {**DOC, '"ERROR"': ["ERROR"]}) is True


class TestVeriteEtNegation:
    @pytest.mark.parametrize(
        ("condition", "attendu"),
        [
            ("[level]", True),
            ("[vide]", True),  # chaîne vide : champ présent, donc vrai
            ("[zero]", True),
            ("[actif]", False),
            ("[nul]", False),
            ("[absent]", False),
            ("![absent]", True),
            ("![level]", False),
            ('!([level] == "ERROR")', False),
            ('!([level] == "INFO" or [severity] == 9)', True),
            ('"texte"', True),  # littéral nu
        ],
    )
    def test_verite_logstash(self, condition: str, attendu: bool) -> None:
        assert _vrai(condition, DOC) is attendu

    def test_formes_d_ast_directes(self) -> None:
        """Formes que la grammaire actuelle ne produit pas, gérées par repli défensif."""
        assert _evaluate_expression(True, DOC) is True
        assert _evaluate_expression(None, DOC) is False
        assert _evaluate_expression([["[level]", "==", "ERROR"]], DOC) is True
        assert _evaluate_expression(["[level]", "[severity]"], DOC) is True  # repli : liste non vide
        assert _evaluate_expression(["!"], DOC) is False  # négation d'une condition vide (vraie)


class TestCombinaisons:
    @pytest.mark.parametrize(
        ("condition", "attendu"),
        [
            ('[level] == "ERROR" and [severity] == 3', True),
            ('[level] == "ERROR" and [absent]', False),
            ("[absent] or [severity] == 3", True),
            ("[absent] or [nul]", False),
            ("[level] xor [absent]", True),
            ("[level] xor [severity]", False),
            ("[level] nand [severity]", False),
            ("[level] nand [absent]", True),
            # Strictement de gauche à droite : (vrai or faux) and faux = faux.
            ("[level] or [absent] and [nul]", False),
            ('([level] == "ERROR" xor [severity] == 9) nand [absent]', True),
            ('[level] == "ERROR" and ([absent] or [severity] > 1)', True),
        ],
    )
    def test_operateurs_booleens(self, condition: str, attendu: bool) -> None:
        assert _vrai(condition, DOC) is attendu


class TestChainesDeBranches:
    CONF = """
    filter {
        if [n] == 1 { mutate { add_tag => ["un"] } }
        else if [n] == 2 { mutate { add_tag => ["deux"] } }
        else if [n] == 3 { mutate { add_tag => ["trois"] } }
        else { mutate { add_tag => ["autre"] } }
        mutate { add_tag => ["apres"] }
    }
    output {
        if [n] == 1 { un { } }
        else if [n] == 2 { deux { } }
        else if [n] == 3 { trois { } }
        fin { }
    }
    """

    @pytest.mark.parametrize(
        ("n", "tags", "sorties"),
        [
            (1, ["un", "apres"], ["un", "fin"]),
            (2, ["deux", "apres"], ["deux", "fin"]),
            (3, ["trois", "apres"], ["trois", "fin"]),
            (4, ["autre", "apres"], ["fin"]),  # aucune branche de sortie, pas de else
        ],
    )
    def test_une_seule_branche_puis_la_suite(self, n: int, tags: list, sorties: list) -> None:
        doc = {"n": n}
        actions = PipelineInterpreter(self.CONF).apply(doc)
        assert doc["tags"] == tags
        assert [a.plugin for a in actions] == sorties

    def test_filtre_sans_branche_retenue(self) -> None:
        doc = {"n": 9}
        PipelineInterpreter('filter { if [n] == 1 { mutate { add_tag => ["x"] } } }').apply(doc)
        assert doc == {"n": 9}

    def test_branches_imbriquees(self) -> None:
        conf = """
        output {
            if [a] {
                if [b] { ab { } } else { a_seul { } }
            } else if [b] { b_seul { } }
        }
        """
        interp = PipelineInterpreter(conf)
        assert [x.plugin for x in interp.apply({"a": 1, "b": 1})] == ["ab"]
        assert [x.plugin for x in interp.apply({"a": 1})] == ["a_seul"]
        assert [x.plugin for x in interp.apply({"b": 1})] == ["b_seul"]
        assert interp.apply({}) == []

    def test_sections_input_ignorees(self) -> None:
        interp = PipelineInterpreter("input { stdin { } } output { stdout { } }")
        assert [x.plugin for x in interp.apply({})] == ["stdout"]


class TestOptions:
    def test_options_repetees_fusionnees(self) -> None:
        """Pipeline réel : plusieurs add_field/add_tag successifs dans un même mutate."""
        conf = """filter { mutate {
            add_field => { "a" => "1" }
            add_field => { "b" => "2" }
            add_tag => ["x"]
            add_tag => ["y"]
        } }"""
        doc: dict = {}
        PipelineInterpreter(conf).apply(doc)
        assert (doc["a"], doc["b"], doc["tags"]) == ("1", "2", ["x", "y"])

    def test_option_scalaire_repetee_derniere_gagne(self) -> None:
        conf = 'output { s { index => "a" index => "b" } }'
        assert PipelineInterpreter(conf).apply({})[0].options == {"index": "b"}

    def test_options_de_sortie_rendues_recursivement(self) -> None:
        conf = """output { opensearch {
            hosts => ["https://%{[host][name]}:9200", "https://secours:9200"]
            index => "logs-%{+YYYY.MM.dd}"
            retry => 3
            headers => { "X-Source" => "%{[host][name]}" "X-Fixe" => "ok" }
        } }"""
        maintenant = datetime(2026, 10, 22, 8, 30, tzinfo=timezone.utc)
        options = PipelineInterpreter(conf).apply(DOC, now=maintenant)[0].options
        assert options == {
            "hosts": ["https://sw1.lan:9200", "https://secours:9200"],
            "index": "logs-2026.10.22",
            "retry": 3,
            "headers": {"X-Source": "sw1.lan", "X-Fixe": "ok"},
        }

    def test_on_missing_empty_pour_les_sorties(self) -> None:
        conf = 'output { s { index => "logs-%{[absent]}" } }'
        interp = PipelineInterpreter(conf)
        assert interp.apply({})[0].options["index"] == "logs-%{[absent]}"
        assert interp.apply({}, on_missing="empty")[0].options["index"] == "logs-"

    def test_plugin_filter_non_interprete_ignore(self) -> None:
        doc = {"message": "a=1"}
        assert PipelineInterpreter('filter { ruby { code => "x" } }').apply(doc) == []
        assert doc == {"message": "a=1"}

    def test_coerce_literal(self) -> None:
        from ipaddress import IPv6Address
        from pathlib import PurePosixPath

        assert _coerce_literal(IPv6Address("::1")) == "::1"
        assert _coerce_literal(PurePosixPath("/etc/a.yml")) == "/etc/a.yml"
        assert _coerce_literal(3) == 3


class TestDissectCas:
    @pytest.mark.parametrize(
        "conf",
        [
            pytest.param('filter { dissect { mapping => "%{a}" } }', id="mapping-pas-un-hash"),
            pytest.param('filter { dissect { mapping => { "message" => 3 } } }', id="patron-non-textuel"),
            pytest.param('filter { dissect { mapping => { "message" => "sans champ" } } }', id="patron-invalide"),
        ],
    )
    def test_ignore_sans_modifier(self, conf: str) -> None:
        doc = {"message": "a b"}
        PipelineInterpreter(conf).apply(doc)
        assert doc == {"message": "a b"}

    def test_sans_correspondance_tag_echec(self) -> None:
        doc = {"message": "a b"}
        PipelineInterpreter('filter { dissect { mapping => { "message" => "%{a} x %{b}" } } }').apply(doc)
        assert doc == {"message": "a b", "tags": ["_dissectfailure"]}  # #117

    def test_patron_compile_une_seule_fois(self) -> None:
        interp = PipelineInterpreter('filter { dissect { mapping => { "message" => "%{a} %{b}" } } }')
        for message in ("1 2", "3 4"):
            doc = {"message": message}
            interp.apply(doc)
        assert (doc["a"], doc["b"]) == ("3", "4")
        assert list(interp._dissect_cache) == ["%{a} %{b}"]
