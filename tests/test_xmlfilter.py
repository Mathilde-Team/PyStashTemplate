# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de `src/xmlfilter.py` et du plugin `xml {}` natif des `.conf` (issue #121)."""

from __future__ import annotations

import asyncio

import pytest
from lxml import etree

from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.xmlfilter import XmlFilter, _paires, _texte_ruby, _vide, options_analyse


def _run(coro):
    # Sans pytest-asyncio (convention du dépôt, cf. RsysLogstach) : asyncio.run direct (#34).
    return asyncio.run(coro)


def _xml(options: dict, valeur: object) -> tuple[bool, dict]:
    """Applique `XmlFilter(options)` à `{"m": valeur}` ; renvoie (réussi, doc sans `m`)."""
    doc = {"m": valeur}
    reussi = XmlFilter({"source": "m", **options}).apply(doc)
    doc.pop("m", None)
    return reussi, doc


class TestStoreXml:
    def test_xmlsimple_par_defaut(self) -> None:
        ok, doc = _xml({"target": "d"}, '<r a="1"><b>x</b><b>y</b><c/><e> </e><f k="v"/></r>')
        assert ok
        # Racine avec attribut : les enfants vides sont gardés (comme XmlSimple).
        assert doc["d"] == {"a": "1", "b": ["x", "y"], "c": [None], "e": [{}], "f": [{"k": "v"}]}

    def test_enfants_vides_supprimes(self) -> None:
        assert _xml({"target": "d"}, "<r><c/><e> </e><b>1</b></r>")[1]["d"] == {"b": ["1"]}

    def test_racine_vide(self) -> None:
        assert _xml({"target": "d"}, "<r/>")[1] == {"d": None}
        assert _xml({"target": "d", "suppress_empty": "false"}, "<r><c/></r>")[1] == {"d": {"c": [{}]}}

    def test_texte_seul(self) -> None:
        assert _xml({"target": "d"}, "<r> txt </r>")[1] == {"d": " txt "}
        assert _xml({"target": "d", "force_content": True}, "<r>txt</r>")[1] == {"d": {"content": "txt"}}
        assert _xml({"target": "d"}, '<r a="1"> </r>')[1] == {"d": {"a": "1", "content": " "}}

    def test_contenu_mixte_et_espaces_de_noms(self) -> None:
        ok, doc = _xml(
            {"target": "[x][d]", "force_array": False},
            '<r xmlns:p="u" p:z="2" xml:lang="fr">t1<b>x</b>t2 &amp;<!-- c --></r>',
        )
        assert doc["x"]["d"] == {"xmlns:p": "u", "p:z": "2", "xml:lang": "fr", "b": "x", "content": ["t1", "t2 &amp;"]}
        ok, doc = _xml({"target": "d"}, "<r>avant<b>x</b>\n</r>")
        assert doc["d"] == {"b": ["x"], "content": ["avant", "\n"]}

    def test_espaces_de_noms_herites_et_noms_locaux(self) -> None:
        ok, doc = _xml({"target": "d"}, '<r xmlns="urn:a" xmlns:p="urn:p"><p:b p:k="1" xmlns:q="urn:q">x</p:b></r>')
        assert doc["d"] == {
            "xmlns": "urn:a",
            "xmlns:p": "urn:p",
            "b": [{"xmlns:q": "urn:q", "p:k": "1", "content": "x"}],
        }

    def test_attribut_espace_de_noms_sans_prefixe_connu(self) -> None:
        racine = etree.fromstring("<r/>")
        racine.set("{urn:z}k", "1")
        ok, doc = _xml({"target": "d"}, etree.tostring(racine, encoding=str))
        assert ok
        assert list(doc["d"].values()) == ["urn:z", "1"]

    def test_tableau_anonyme(self) -> None:
        assert _xml({"target": "d"}, "<r><anon>1</anon><anon>2</anon></r>")[1] == {"d": ["1", "2"]}
        assert _xml({"target": "d"}, "<r><anon>1</anon><x>2</x></r>")[1] == {"d": {"anon": ["1"], "x": ["2"]}}

    def test_tableau_sans_force_array(self) -> None:
        ok, doc = _xml({"target": "d", "force_array": False}, "<r><b>1</b><b>2</b><b>3</b></r>")
        assert doc["d"] == {"b": ["1", "2", "3"]}

    def test_analyse_stricte(self) -> None:
        assert _xml({"target": "d"}, "<r><b>x</b>") == (False, {"tags": ["_xmlparsefailure"]})
        assert _xml({"target": "d"}, "pas du xml") == (False, {"tags": ["_xmlparsefailure"]})

    def test_entite_externe_non_resolue(self) -> None:
        ok, doc = _xml({"target": "d"}, '<!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]><r>&x;</r>')
        assert ok
        assert doc == {"d": None}

    def test_declaration_d_encodage(self) -> None:
        assert _xml({"target": "d"}, '<?xml version="1.0" encoding="ISO-8859-1"?><r>é</r>')[1] == {"d": "é"}


class TestValeurs:
    @pytest.mark.parametrize("valeur", [None, False, "", "   "])
    def test_sans_effet(self, valeur: object) -> None:
        assert _xml({"target": "d"}, valeur) == (False, {})

    def test_absent(self) -> None:
        doc: dict = {}
        assert not XmlFilter({"source": "m", "target": "d"}).apply(doc)
        assert doc == {}

    def test_tableau_un_element(self) -> None:
        assert _xml({"target": "d"}, ["<r>1</r>"]) == (True, {"d": "1"})

    @pytest.mark.parametrize("valeur", [[], ["<r/>", "<r/>"], 42, {"a": 1}])
    def test_echecs(self, valeur: object) -> None:
        assert _xml({"target": "d"}, valeur) == (False, {"tags": ["_xmlparsefailure"]})


class TestXpath:
    def test_conversions(self) -> None:
        xpath = {
            "/r/b/text()": "x",
            "count(//b)": "n",
            "/r/b": "el",
            "/r/@a": "a",
            "string(/r/b)": "s",
            "string(/r/z)": "vide",
            "boolean(/r/b)": "vrai",
            "boolean(/r/z)": "faux",
            "/r/z": "rien",
            "/r/comment()": "c",
        }
        ok, doc = _xml({"store_xml": False, "xpath": xpath}, '<r a="1"><b>x&lt;</b><b>y</b><!--c--></r>')
        assert ok
        assert doc == {
            "x": ["x&lt;", "y"],
            "n": ["2.0"],
            "el": ["<b>x&lt;</b>", "<b>y</b>"],
            "a": ["1"],
            "s": ["x<"],
            "vide": [""],
            "vrai": ["true"],
            "c": ["<!--c-->"],
        }

    def test_aucun_resultat(self) -> None:
        assert _xml({"store_xml": False, "xpath": {"/r/z": "z"}}, "<r/>") == (False, {})

    def test_force_array_faux(self) -> None:
        ok, doc = _xml(
            {"store_xml": False, "force_array": False, "xpath": {"/r/b/text()": "[a][b]"}}, "<r><b>1</b></r>"
        )
        assert doc == {"a": {"b": "1"}}

    def test_avec_store_xml(self) -> None:
        ok, doc = _xml({"target": "d", "xpath": ["/r/b/text()", "x"]}, "<r><b>1</b></r>")
        assert doc == {"x": ["1"], "d": {"b": ["1"]}}

    def test_espaces_de_noms_racine_et_option(self) -> None:
        conf = '<r xmlns="urn:a" xmlns:p="urn:p"><q>1</q><p:t>2</p:t></r>'
        assert _xml({"store_xml": False, "xpath": {"//xmlns:q/text()": "q", "//p:t/text()": "t"}}, conf)[1] == {
            "q": ["1"],
            "t": ["2"],
        }
        options = {"store_xml": False, "namespaces": {"z": "urn:a"}, "xpath": {"//z:q/text()": "q"}}
        assert _xml(options, conf)[1] == {"q": ["1"]}

    def test_remove_namespaces(self) -> None:
        options = {"store_xml": False, "remove_namespaces": True, "xpath": {"//q/@k": "k", "//t/text()": "t"}}
        conf = '<r xmlns="urn:a" xmlns:p="urn:p"><q p:k="v" j="w">1</q><!--c--><p:t>2</p:t></r>'
        assert _xml(options, conf)[1] == {"k": ["v"], "t": ["2"]}

    def test_tolerant_par_defaut(self) -> None:
        assert _xml({"store_xml": False, "xpath": {"//b/text()": "b"}}, "<r><b>x</b>") == (True, {"b": ["x"]})
        assert _xml({"store_xml": False, "xpath": {"//b": "b"}}, "pas du xml") == (False, {})

    def test_strict_et_erreurs(self) -> None:
        options = {"store_xml": False, "parse_options": "strict", "xpath": {"//b": "b"}}
        assert _xml(options, "<r><b>x</b>") == (False, {"tags": ["_xmlparsefailure"]})
        assert _xml({"store_xml": False, "xpath": {"//b[": "b"}}, "<r/>") == (False, {"tags": ["_xmlparsefailure"]})
        assert _xml({"store_xml": False, "xpath": {"//p:b": "b"}}, "<r/>") == (False, {"tags": ["_xmlparsefailure"]})

    def test_sans_xpath_ni_store(self) -> None:
        assert _xml({"store_xml": False}, "<r/>") == (False, {})


class TestConversionRuby:
    @pytest.mark.parametrize(
        ("valeur", "attendu"),
        [
            (None, None),
            (False, None),
            (True, "true"),
            (3.0, "3.0"),
            (0.5, "0.5"),
            (float("nan"), "NaN"),
            (float("inf"), "Infinity"),
            (float("-inf"), "-Infinity"),
            ("a<b", "a<b"),
        ],
    )
    def test_texte_ruby(self, valeur: object, attendu: str | None) -> None:
        assert _texte_ruby(valeur) == attendu

    def test_vide(self) -> None:
        assert _vide(None) and _vide({}) and _vide(" \n") and not _vide([]) and not _vide("x") and not _vide({"a": 1})


class TestOptions:
    @pytest.mark.parametrize(
        ("texte", "attendu"),
        [
            (None, {"recover": True}),
            ("strict", {}),
            ("no_error|no_warning", {}),
            ("recover, nonet ,", {"recover": True}),
            (
                "huge,nsclean,compact,noblanks",
                {"huge_tree": True, "ns_clean": True, "compact": True, "remove_blank_text": True},
            ),
            ("default_xml", {"recover": True}),
        ],
    )
    def test_parse_options(self, texte: str | None, attendu: dict) -> None:
        assert options_analyse(texte) == attendu

    @pytest.mark.parametrize("texte", ["noent", "dtd_load", "xinclude", "strict|DTDVALID", "inconnu"])
    def test_parse_options_refusees(self, texte: str) -> None:
        with pytest.raises(ValueError, match="parse_options"):
            options_analyse(texte)

    @pytest.mark.parametrize(
        "options",
        [
            {},
            {"source": ""},
            {"source": "m"},
            {"source": "m", "target": ""},
            {"source": "m", "store_xml": "peut-être", "target": "d"},
            {"source": "m", "store_xml": False, "xpath": "x"},
            {"source": "m", "store_xml": False, "xpath": ["a"]},
            {"source": "m", "store_xml": False, "xpath": {"a": 1}},
            {"source": "m", "store_xml": False, "namespaces": 3},
            {"source": "m", "store_xml": False, "parse_options": "noent"},
        ],
    )
    def test_invalides(self, options: dict) -> None:
        with pytest.raises(ValueError, match="xml"):
            XmlFilter(options)

    def test_paires(self) -> None:
        assert _paires(["a", "b", "c", "d"], "xpath") == [("a", "b"), ("c", "d")]


class TestInterpreteur:
    def test_conf(self) -> None:
        conf = 'filter { xml { source => "m" target => "d" xpath => ["/r/b/text()", "[x][y]"] add_tag => ["ok"] } }'
        doc = {"m": "<r><b>1</b></r>"}
        _run(PipelineInterpreter(conf).apply(doc))
        assert doc == {"m": "<r><b>1</b></r>", "x": {"y": ["1"]}, "d": {"b": ["1"]}, "tags": ["ok"]}

    def test_echec_sans_options_communes(self) -> None:
        doc = {"m": "<r>"}
        _run(PipelineInterpreter('filter { xml { source => "m" target => "d" add_tag => ["ok"] } }').apply(doc))
        assert doc["tags"] == ["_xmlparsefailure"]

    def test_bloc_invalide_ignore_et_cache(self) -> None:
        interp = PipelineInterpreter('filter { xml { source => "m" add_tag => ["ok"] } }')
        for _ in range(2):
            doc = {"m": "<r/>"}
            _run(interp.apply(doc))
            assert doc == {"m": "<r/>"}
        assert len(interp._xml_filters) == 1
