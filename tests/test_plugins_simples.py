# SPDX-License-Identifier: GPL-3.0-or-later
"""Issue #120 : plugins `filter{}` simples (`simples.py`) dans `--pipeline-conf`."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

import pytest

from pystashtemplate.interpreter import PipelineInterpreter


def _run(coro):
    # Sans pytest-asyncio (convention du dépôt, cf. RsysLogstach) : asyncio.run direct (#34).
    return asyncio.run(coro)


def _f(plugin: str, doc: dict, corps: str = "") -> dict:
    _run(PipelineInterpreter(f"filter {{ {plugin} {{ {corps} }} }}").apply(doc))
    return doc


class TestCsv:
    def test_colonnes_nommees_et_generees(self) -> None:
        doc = _f("csv", {"message": 'a,"b,c",3,'}, 'columns => ["x", "y"] add_tag => ["ok"]')
        assert doc == {"message": 'a,"b,c",3,', "x": "a", "y": "b,c", "column3": "3", "column4": "", "tags": ["ok"]}

    def test_options(self) -> None:
        doc = _f(
            "csv",
            {"l": "1;'2.5';oui;;z"},
            'source => "l" separator => ";" quote_char => "\'" target => "[c]" columns => ["i", "f", "b", "v"]'
            ' convert => { "i" => "integer" "f" => "float" "b" => "boolean" } skip_empty_columns => true'
            " autogenerate_column_names => false",
        )
        assert doc["c"] == {"i": 1, "f": 2.5, "b": "oui"}

    @pytest.mark.parametrize(("texte", "attendu"), [("true", True), ("N", False), ("x", "x")])
    def test_boolean(self, texte: str, attendu: object) -> None:
        assert _f("csv", {"message": texte}, 'columns => ["b"] convert => { "b" => "boolean" }')["b"] == attendu

    def test_type_inconnu_et_conversion_impossible(self) -> None:
        doc = _f("csv", {"message": "a,b"}, 'columns => ["x", "y"] convert => { "x" => "date" "y" => "integer" }')
        assert (doc["x"], doc["y"]) == ("a", "b")

    def test_skip_header_et_ligne_vide(self) -> None:
        assert _f("csv", {"message": "x,y"}, 'columns => ["x", "y"] skip_header => true add_tag => ["ok"]') == {
            "message": "x,y"
        }
        assert _f("csv", {"message": ""}, 'add_tag => ["ok"]') == {"message": "", "tags": ["ok"]}

    def test_echecs(self) -> None:
        assert _f("csv", {"message": 'a,"b'}, "")["tags"] == ["_csvparsefailure"]
        assert _f("csv", {"message": 3}, 'tag_on_failure => ["e"]')["tags"] == ["e"]
        assert _f("csv", {"autre": 1}, 'add_tag => ["ok"]') == {"autre": 1}


class TestCidr:
    def test_correspondance_et_options_communes(self, tmp_path: Path) -> None:
        fichier = tmp_path / "reseaux.txt"
        fichier.write_text("# commentaire\n\n192.168.0.0/16\nnimporte\n", encoding="utf-8")
        conf = (
            f'filter {{ cidr {{ address => ["%{{src}}", "%{{dst}}"] network => ["10.0.0.0/8", "2001:db8::/32"]'
            f' network_path => "{fichier}" add_tag => ["prive"] }} }}'
        )
        interp = PipelineInterpreter(conf)
        for doc, tags in (
            ({"src": "pas-ip", "dst": "192.168.1.1"}, ["prive"]),
            ({"src": "2001:db8::1"}, ["prive"]),
            ({"src": "8.8.8.8", "dst": "10.1.1.1"}, ["prive"]),
            ({"src": "8.8.8.8"}, None),
        ):
            _run(interp.apply(doc))
            assert doc.get("tags") == tags
        assert len(interp._cidr_cache) == 1

    def test_fichier_absent(self, tmp_path: Path) -> None:
        doc = _f(
            "cidr", {"ip": "10.0.0.1"}, f'address => "%{{ip}}" network_path => "{tmp_path / "x"}" add_tag => ["t"]'
        )
        assert "tags" not in doc


class TestPrune:
    def test_noms(self) -> None:
        doc = {"a": 1, "b": 2, "%{x}": 3, "bb": 4}
        assert _f("prune", doc, 'whitelist_names => ["^b"]') == {"b": 2, "bb": 4}
        assert _f("prune", {"a": 1, "%{x}": 3}) == {"a": 1}
        assert _f("prune", {"a": 1, "b": 2}, 'blacklist_names => "a"') == {"b": 2}

    def test_valeurs(self) -> None:
        doc = {"m": ["GET", "POST", "PUT"], "s": "x", "t": "garde", "u": "ok", "v": ["a"]}
        conf = (
            'whitelist_values => { "m" => "^(GET|PUT)$" "s" => "y" "v" => "z" }'
            ' blacklist_values => { "m" => "PUT" "t" => "rien" "u" => "ok" }'
        )
        assert _f("prune", doc, conf + ' add_tag => ["p"]') == {"m": ["GET"], "t": "garde", "tags": ["p"]}


class TestUrldecode:
    def test_champ_et_tableau(self) -> None:
        assert _f("urldecode", {"message": "a%20b%C3%A9"})["message"] == "a bé"
        doc = _f("urldecode", {"u": ["%41", 3]}, 'field => "u"')
        assert doc["u"] == ["A", 3]

    def test_all_fields_et_absent(self) -> None:
        assert _f("urldecode", {"a": "%41", "b": 1}, "all_fields => true") == {"a": "A", "b": 1}
        assert _f("urldecode", {"x": 1}, 'add_tag => ["t"]') == {"x": 1, "tags": ["t"]}

    def test_echec(self) -> None:
        assert _f("urldecode", {"message": "%FF"})["tags"] == ["_urldecodefailure"]


class TestUuid:
    def test_cible_et_overwrite(self) -> None:
        uuid.UUID(_f("uuid", {}, 'target => "[a][id]"')["a"]["id"])
        assert _f("uuid", {"id": "x"}, 'target => "id"')["id"] == "x"
        assert _f("uuid", {"id": "x"}, 'target => "id" overwrite => true')["id"] != "x"

    def test_sans_target(self) -> None:
        assert _f("uuid", {}, 'add_tag => ["t"]') == {}


class TestSyslogPri:
    def test_defaut_13(self) -> None:
        assert _f("syslog_pri", {}) == {
            "syslog_facility_code": 1,
            "syslog_severity_code": 5,
            "syslog_facility": "user-level",
            "syslog_severity": "notice",
        }

    @pytest.mark.parametrize(("pri", "fac", "sev"), [(["165"], "local4", "notice"), (0, "kernel", "emergency")])
    def test_valeurs(self, pri: object, fac: str, sev: str) -> None:
        doc = _f("syslog_pri", {"syslog_pri": pri})
        assert (doc["syslog_facility"], doc["syslog_severity"]) == (fac, sev)

    def test_options_et_cas_limites(self) -> None:
        doc = _f("syslog_pri", {"p": "abc"}, 'syslog_pri_field_name => "p" use_labels => false')
        assert (doc["syslog_facility_code"], "syslog_facility" in doc) == (0, False)
        doc = _f("syslog_pri", {"syslog_pri": []}, 'facility_labels => ["f0"] severity_labels => ["s0"]')
        assert ("syslog_facility" in doc, "syslog_severity" in doc) == (False, False)
        assert _f("syslog_pri", {"syslog_pri": 191})["syslog_facility"] == "local7"


class TestTruncate:
    def test_tous_les_champs_utf8(self) -> None:
        assert _f("truncate", {"a": "éééé", "b": ["abcdef"], "n": 5}, "length_bytes => 3") == {
            "a": "é",
            "b": ["abc"],
            "n": 5,
        }

    def test_fields(self) -> None:
        doc = _f("truncate", {"a": "abcd", "b": "abcd"}, 'length_bytes => 2 fields => ["a", "absent"]')
        assert doc == {"a": "ab", "b": "abcd"}

    @pytest.mark.parametrize("valeur", ["0", "true", '"3"'])
    def test_length_bytes_invalide(self, valeur: str) -> None:
        assert _f("truncate", {"a": "abcd"}, f'length_bytes => {valeur} add_tag => ["t"]') == {"a": "abcd"}


def test_cidr_sans_network_path() -> None:
    assert _f("cidr", {"ip": "10.0.0.1"}, 'address => "%{ip}" network => "10.0.0.0/8" add_tag => ["t"]')["tags"] == [
        "t"
    ]
