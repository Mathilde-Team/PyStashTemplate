# SPDX-License-Identifier: GPL-3.0-or-later
"""Issue #128 : les chaînes `.conf` gardent leurs antislashs, comme Logstash.

Logstash (réglage par défaut `config.support_escapes: false`) lit le texte
entre guillemets tel quel : `"a\\.b"` vaut `a\\.b`, `"\\d+"` vaut `\\d+`.
`logstash-pipeline-parser==0.0.3` retirait l'antislash devant tout
caractère, ce qui cassait les regex de `gsub`, des conditions `=~ "..."`
et des motifs `grok` (#112).
"""

from __future__ import annotations

import asyncio

import pytest
from logstash_pipeline_parser import Pipeline
from logstash_pipeline_parser import tree as parser_tree

import pystashtemplate.interpreter as interpreter
from pystashtemplate.interpreter import PipelineInterpreter, _normalize_attrs


def _run(coro):
    # Sans pytest-asyncio (convention du dépôt, cf. RsysLogstach) : asyncio.run direct (#34).
    return asyncio.run(coro)


def _options(conf: str) -> dict:
    """Options normalisées du premier plugin du premier bloc `filter{}`."""
    ast = Pipeline(conf).parse()
    return _normalize_attrs(ast[0][1][0][1])


class TestChainesBrutes:
    @pytest.mark.parametrize(
        ("chaine", "attendu"),
        [
            (r'"a\.b"', r"a\.b"),
            (r'"\d+"', r"\d+"),
            (r'"\[%{POSINT:pid}\]"', r"\[%{POSINT:pid}\]"),
            (r'"dit \"oui\""', r"dit \"oui\""),
            (r"'it\'s'", r"it\'s"),
            (r'"tab\t"', r"tab\t"),
            ('"sans antislash"', "sans antislash"),
            ('""', ""),
        ],
    )
    def test_texte_brut(self, chaine: str, attendu: str) -> None:
        assert _options(f"filter {{ x {{ v => {chaine} }} }}") == {"v": attendu}

    def test_cles_et_listes(self) -> None:
        options = _options(r'filter { x { h => { "a\.b" => ["\d", "\w"] } } }')
        assert options == {"h": {r"a\.b": [r"\d", r"\w"]}}

    def test_regex_litterale_inchangee(self) -> None:
        ast = Pipeline(r"output { if [m] =~ /eth\d+/ { o { } } }").parse()
        assert ast[0][1][0][1][0] == ["[m]", "=~", r"eth\d+"]

    def test_configuration_idempotente(self) -> None:
        actions = list(parser_tree.PEG.double_quoted_string.parseAction)
        interpreter._conserver_antislashs()
        assert list(parser_tree.PEG.double_quoted_string.parseAction) == actions
        assert _options(r'filter { x { v => "\." } }') == {"v": r"\."}


class TestEffetsSurLesFiltres:
    def test_gsub_point_litteral(self) -> None:
        doc = {"m": "a.b axb"}
        _run(PipelineInterpreter(r'filter { mutate { gsub => ["m", "a\.b", "X"] } }').apply(doc))
        assert doc["m"] == "X axb"

    def test_gsub_classe_de_caracteres(self) -> None:
        doc = {"m": "port 8080"}
        _run(PipelineInterpreter(r'filter { mutate { gsub => ["m", "\d+", "N"] } }').apply(doc))
        assert doc["m"] == "port N"

    @pytest.mark.parametrize(
        ("condition", "attendu"),
        [
            (r'[m] =~ "eth\d+ up"', True),
            (r'[m] =~ "eth\. up"', False),
            (r'[m] !~ "\s{2}"', True),
        ],
    )
    def test_condition_regex_en_chaine(self, condition: str, attendu: bool) -> None:
        actions = _run(PipelineInterpreter(f"output {{ if {condition} {{ o {{ }} }} }}").apply({"m": "eth0 up"}))
        assert bool(actions) is attendu
