# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de templates.py (module reconstruit, issue #40)."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from pystashtemplate.templates import _MISSING, parse_field_path, render_template, resolve_field

DOC = {
    "hostname": "sw01",
    "host": {"ip": "10.0.0.1", "tags": ["core", "lab"]},
    "count": 3,
    "up": True,
    "nothing": None,
}


class TestParseFieldPath:
    @pytest.mark.parametrize(
        ("ref", "path"),
        [("hostname", ["hostname"]), ("[host][ip]", ["host", "ip"]), ("[a]", ["a"]), (" x ", ["x"])],
    )
    def test_valid(self, ref, path):
        assert parse_field_path(ref) == path

    # Issue #49 : « a[b] », « [] », « a] » et « [a[b]] » sont refusés, comme par Logstash.
    @pytest.mark.parametrize("ref", ["", "  ", "[a][b", "[a]x[b]", "a[b]", "[]", "a]", "[a][]", "[a[b]]"])
    def test_invalid(self, ref):
        with pytest.raises(ValueError):
            parse_field_path(ref)


def _field(ref):
    return resolve_field(DOC, parse_field_path(ref))


class TestResolveField:
    def test_nested_and_list_index(self):
        assert _field("[host][ip]") == "10.0.0.1"
        assert _field("[host][tags][1]") == "lab"
        assert _field("[host][tags][-1]") == "lab"

    def test_missing_returns_sentinel(self):
        for ref in ("absent", "[host][absent]", "[host][tags][5]", "[hostname][x]"):
            assert _field(ref) is _MISSING

    def test_null_is_present(self):
        assert _field("nothing") is None


class TestRenderTemplate:
    def test_simple_and_nested(self):
        assert render_template("%{hostname}-%{[host][ip]}", DOC) == "sw01-10.0.0.1"

    def test_value_formatting(self):
        """Issue #49 : rendu identique à StringInterpolation de Logstash 8."""
        assert render_template("%{count}|%{up}|%{nothing}", DOC) == "3|true|"
        assert render_template("%{[host][tags]}", DOC) == "core,lab"
        assert render_template("%{host}", DOC) == '{"ip":"10.0.0.1","tags":["core","lab"]}'

    def test_value_formatting_edge_cases(self):
        doc = {"off": False, "mix": [1, None, True, ["a", "b"]], "vide": [], "e": {"n": "é"}, "pi": 1.5}
        assert render_template("%{off}", doc) == "false"
        assert render_template("%{mix}", doc) == "1,,true,a,b"
        assert render_template("x%{vide}x", doc) == "xx"
        assert render_template("%{e}", doc) == '{"n":"é"}'
        assert render_template("%{pi}", doc) == "1.5"

    def test_malformed_reference_raises(self):
        with pytest.raises(ValueError, match="mal formé"):
            render_template("%{a[b]}", DOC)

    def test_without_reference(self):
        assert render_template("texte fixe", DOC) == "texte fixe"
        assert render_template("", DOC) == ""

    def test_missing_keep_is_default(self):
        assert render_template("a-%{absent}-b", DOC) == "a-%{absent}-b"

    def test_missing_empty(self):
        assert render_template("a-%{absent}-b", DOC, on_missing="empty") == "a--b"

    def test_unknown_on_missing(self):
        with pytest.raises(ValueError, match="on_missing"):
            render_template("%{hostname}", DOC, on_missing="ignore")

    def test_null_renders_like_missing_empty(self):
        """Limite documentée (#29) : null et absent donnent tous deux ''."""
        assert render_template("%{nothing}", DOC, on_missing="empty") == render_template(
            "%{absent}", DOC, on_missing="empty"
        )


def test_templates_has_no_dependency_on_fingerprint():
    """PATTERNS.md : templates est une feuille, il ne dépend que de tracing."""
    tree = ast.parse((Path(__file__).resolve().parents[1] / "src" / "templates.py").read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
        elif isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
    local = {p.stem for p in (Path(__file__).resolve().parents[1] / "src").glob("*.py")}
    assert imported & local == {"tracing"}
