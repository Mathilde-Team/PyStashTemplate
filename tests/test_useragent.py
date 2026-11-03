# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de `src/useragent.py` et du plugin `useragent {}` natif des `.conf` (issue #121)."""

from __future__ import annotations

import asyncio

import pytest

from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.useragent import UserAgentFilter, _ajuster_version, _entier_ruby, _version_os, _version_ua


def _run(coro):
    # Sans pytest-asyncio (convention du dépôt, cf. RsysLogstach) : asyncio.run direct (#34).
    return asyncio.run(coro)


CHROME = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36"
)
IPHONE = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/14.0.3 Mobile/15E148 Safari/604.1"
)


def _ua(options_conf: str, doc: dict) -> dict:
    """Applique `filter { useragent { <options_conf> } }` à `doc` et le renvoie."""
    _run(PipelineInterpreter(f"filter {{ useragent {{ {options_conf} }} }}").apply(doc))
    return doc


class TestEcs:
    def test_chrome_windows(self) -> None:
        doc = _ua('source => "agent"', {"agent": CHROME})
        assert doc["user_agent"] == {
            "name": "Chrome",
            "version": "89.0.4389.90",
            "device": {"name": "Other"},
            "os": {"name": "Windows", "version": "10", "full": "Windows 10"},
        }
        meta = doc["@metadata"]["filter"]["user_agent"]
        assert meta["version"] == {"major": "89", "minor": "0", "patch": "4389"}
        assert meta["os"]["version"] == {"major": "10"}
        assert doc["agent"] == CHROME

    def test_iphone(self) -> None:
        doc = _ua('source => "[a][ua]" target => "[web][ua]"', {"a": {"ua": IPHONE}})
        ua = doc["web"]["ua"]
        assert ua["name"] == "Mobile Safari"
        assert ua["version"] == "14.0.3"
        assert ua["device"] == {"name": "iPhone"}
        assert ua["os"] == {"name": "iOS", "version": "14.4", "full": "iOS 14.4"}

    def test_inconnu(self) -> None:
        doc = _ua('source => "agent" add_tag => ["ua"]', {"agent": "xyzzy"})
        assert doc["user_agent"] == {
            "name": "Other",
            "device": {"name": "Other"},
            "os": {"name": "Other", "full": "Other"},
        }
        assert "@metadata" not in doc
        assert doc["tags"] == ["ua"]

    def test_tableau_premier_element(self) -> None:
        doc = _ua('source => "agent"', {"agent": ["curl/7.68.0", "autre"]})
        assert doc["user_agent"]["name"] == "curl"
        assert doc["user_agent"]["version"] == "7.68.0"

    @pytest.mark.parametrize("valeur", [None, "", []])
    def test_vide_sans_effet(self, valeur: object) -> None:
        doc = _ua('source => "agent" add_tag => ["ua"]', {"agent": valeur})
        assert doc == {"agent": valeur}

    def test_absent_sans_effet(self) -> None:
        assert _ua('source => "agent" add_tag => ["ua"]', {}) == {}

    def test_valeur_non_textuelle(self) -> None:
        doc = _ua('source => "agent"', {"agent": 42})
        assert doc["user_agent"]["name"] == "Other"

    def test_target_egale_source(self) -> None:
        doc = _ua('source => "agent" target => "agent"', {"agent": "curl/7.68.0"})
        assert doc["agent"]["name"] == "curl"

    def test_target_egale_source_imbriquee(self) -> None:
        doc = _ua('source => "[a][b]" target => "[a][b]"', {"a": {"b": "curl/7.68.0"}})
        assert doc["a"]["b"]["name"] == "curl"

    def test_target_egale_source_parent_non_objet(self) -> None:
        # Source dans une liste : rien à retirer, la cible est écrite à côté.
        filtre = UserAgentFilter({"source": "[a][0]", "target": "[a][0]"})
        doc = {"a": ["curl/1.2.3"]}
        assert filtre.apply(doc)
        assert doc["a"] == ["curl/1.2.3"]

    def test_prefix_ignore(self) -> None:
        doc = _ua('source => "agent" prefix => "ua_"', {"agent": "curl/7.68.0"})
        assert doc["user_agent"]["name"] == "curl"


class TestLegacy:
    def test_racine_et_prefixe(self) -> None:
        doc = _ua('source => "agent" ecs_compatibility => disabled prefix => "ua_"', {"agent": CHROME})
        assert doc["ua_name"] == "Chrome"
        assert doc["ua_device"] == "Other"
        assert (doc["ua_major"], doc["ua_minor"], doc["ua_patch"]) == ("89", "0", "4389")
        assert doc["ua_version"] == "89.0.4389.90"
        assert doc["ua_os"] == doc["ua_os_name"] == "Windows"
        assert doc["ua_os_full"] == "Windows 10"
        assert doc["ua_os_version"] == doc["ua_os_major"] == "10"

    def test_target(self) -> None:
        doc = _ua('source => "agent" ecs_compatibility => "disabled" target => "ua"', {"agent": IPHONE})
        assert doc["ua"]["os_minor"] == "4"
        assert doc["ua"]["device"] == "iPhone"


class TestVersions:
    @pytest.mark.parametrize(
        ("texte", "attendu"), [("10", True), ("-1", True), ("010", False), ("Vista", False), ("", False), ("1 ", False)]
    )
    def test_entier_ruby(self, texte: str, attendu: bool) -> None:
        assert _entier_ruby(texte) is attendu

    @pytest.mark.parametrize(
        ("parties", "attendu"),
        [
            ((None, "1", None, None), None),
            (("10", None, None, None), "10"),
            (("10", "15", None, None), "10.15"),
            (("10", "15", "7", None), "10.15.7"),
            (("10", "15", "7", "1"), "10.15.7.1"),
            (("Vista", "SP2", None, None), "Vista SP2"),
        ],
    )
    def test_version_os(self, parties: tuple, attendu: str | None) -> None:
        assert _version_os(*parties) == attendu

    @pytest.mark.parametrize(
        ("source", "parties", "attendu"),
        [
            ("x", (None, None, None, None), None),
            ("x", ("1", None, None, None), "1"),
            ("x", ("1", "2", None, None), "1.2"),
            ("x", ("1", "2", "3", "4"), "1.2.3.4"),
            ("App/1.2.3.45 suite", ("1", "2", "3", None), "1.2.3.45"),
            ("App/1.2.3.045 suite", ("1", "2", "3", None), "1.2.3"),
            ("App/1.2.3.45", ("1", "2", "3", None), "1.2.3"),
            ("App/1.2.3 suite", ("1", "2", "3", None), "1.2.3"),
            ("App/9", ("1", "2", "3", None), "1.2.3"),
        ],
    )
    def test_version_ua(self, source: str, parties: tuple, attendu: str | None) -> None:
        assert _version_ua(source, *parties) == attendu

    def test_ajuster_version_absente(self) -> None:
        assert _ajuster_version("abc", "1.2.3") == "1.2.3"


class TestRegexes:
    def test_fichier_personnalise(self, tmp_path) -> None:
        regexes = tmp_path / "regexes.yaml"
        regexes.write_text(
            "user_agent_parsers:\n"
            "  - regex: '(MonAgent)/(\\d+)\\.(\\d+)'\n"
            "os_parsers:\n"
            "  - regex: '(MonOS) (\\w+)'\n"
            "device_parsers:\n"
            "  - regex: 'Boitier'\n"
            "    device_replacement: 'Boitier'\n",
            encoding="utf-8",
        )
        doc = _ua(
            f'source => "agent" regexes => "{regexes}" lru_cache_size => 0',
            {"agent": "MonAgent/3.1 MonOS Vista Boitier"},
        )
        assert doc["user_agent"]["name"] == "MonAgent"
        assert doc["user_agent"]["version"] == "3.1"
        assert doc["user_agent"]["os"] == {"name": "MonOS", "version": "Vista", "full": "MonOS Vista"}
        assert doc["user_agent"]["device"] == {"name": "Boitier"}

    def test_fichier_illisible_bloc_ignore(self, tmp_path) -> None:
        options = f'source => "agent" regexes => "{tmp_path / "absent.yaml"}" add_tag => ["ua"]'
        interp = PipelineInterpreter(f"filter {{ useragent {{ {options} }} }}")
        for _ in range(2):
            doc = {"agent": CHROME}
            _run(interp.apply(doc))
            assert doc == {"agent": CHROME}


class TestOptions:
    @pytest.mark.parametrize(
        "options",
        [
            {},
            {"source": ""},
            {"source": 3},
            {"source": "a", "target": 3},
            {"source": "a", "prefix": 3},
            {"source": "a", "lru_cache_size": "beaucoup"},
            {"source": "a", "regexes": 3},
        ],
    )
    def test_invalides(self, options: dict) -> None:
        with pytest.raises(ValueError, match="useragent"):
            UserAgentFilter(options)

    def test_cache_interpreteur(self) -> None:
        interp = PipelineInterpreter('filter { useragent { source => "agent" } }')
        for ua in ("curl/7.68.0", "curl/8.0.1"):
            doc = {"agent": ua}
            _run(interp.apply(doc))
        assert doc["user_agent"]["version"] == "8.0.1"
        assert len(interp._useragent_filters) == 1
