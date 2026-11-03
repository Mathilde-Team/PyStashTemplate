# SPDX-License-Identifier: GPL-3.0-or-later
"""Issue #116 : options communes (`add_field`, `add_tag`, `remove_field`, `remove_tag`) sur tous les filtres.

Comme `filter_matched` de Logstash : appliquées après un filtre réussi
seulement, dans cet ordre ; jamais après un échec (`tag_on_failure`).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import IP_INCONNUE, IP_STRASBOURG

from pystashtemplate.filters import apply_common_options
from pystashtemplate.interpreter import PipelineInterpreter

COMMUNES = (
    'add_field => { "[ok][src]" => "%{host}" } add_tag => ["vu"] remove_field => ["brut"] remove_tag => ["ancien"]'
)


def _appliquer(conf: str, doc: dict) -> dict:
    interp = PipelineInterpreter(conf)
    try:
        interp.apply(doc)
    finally:
        interp.close()
    return doc


def _doc(**champs) -> dict:
    return {"host": "sw1", "brut": "x", "tags": ["ancien"], **champs}


def _verifie_appliquees(doc: dict) -> None:
    assert doc["ok"] == {"src": "sw1"}
    assert doc["tags"] == ["vu"]
    assert "brut" not in doc


def _verifie_ignorees(doc: dict) -> None:
    assert "ok" not in doc
    assert "vu" not in doc["tags"]
    assert doc["brut"] == "x"


class TestApresSucces:
    def test_dissect(self) -> None:
        conf = f'filter {{ dissect {{ mapping => {{ "message" => "%{{a}} %{{b}}" }} {COMMUNES} }} }}'
        doc = _appliquer(conf, _doc(message="un deux"))
        assert doc["a"] == "un"
        _verifie_appliquees(doc)

    def test_date(self) -> None:
        conf = f'filter {{ date {{ match => ["quand", "ISO8601"] {COMMUNES} }} }}'
        doc = _appliquer(conf, _doc(quand="2026-11-03T10:00:00Z"))
        assert doc["@timestamp"].startswith("2026-11-03T10:00:00")
        _verifie_appliquees(doc)

    def test_translate(self, tmp_path: Path) -> None:
        dico = tmp_path / "d.yml"
        dico.write_text('"1": un\n', encoding="utf-8")
        conf = f'filter {{ translate {{ source => "n" target => "t" dictionary_path => "{dico}" {COMMUNES} }} }}'
        doc = _appliquer(conf, _doc(n="1"))
        assert doc["t"] == "un"
        _verifie_appliquees(doc)

    def test_geoip(self, base_geoip_city: str) -> None:
        conf = f'filter {{ geoip {{ source => "ip" database => "{base_geoip_city}" {COMMUNES} }} }}'
        doc = _appliquer(conf, _doc(ip=IP_STRASBOURG))
        assert doc["geoip"]["city_name"] == "Strasbourg"
        _verifie_appliquees(doc)

    def test_mmdb(self, base_mmdb_perso: str) -> None:
        conf = f'filter {{ mmdb {{ source => "ip" database => "{base_mmdb_perso}" target => "geo" {COMMUNES} }} }}'
        doc = _appliquer(conf, _doc(ip=IP_STRASBOURG))
        assert "geo" in doc
        _verifie_appliquees(doc)


class TestApresEchec:
    def test_dissect_sans_correspondance(self) -> None:
        conf = f'filter {{ dissect {{ mapping => {{ "message" => "%{{a}} - %{{b}}" }} {COMMUNES} }} }}'
        _verifie_ignorees(_appliquer(conf, _doc(message="sans tiret")))

    def test_dissect_mapping_absent(self) -> None:
        _verifie_ignorees(_appliquer(f"filter {{ dissect {{ {COMMUNES} }} }}", _doc()))

    def test_date_invalide(self) -> None:
        conf = f'filter {{ date {{ match => ["quand", "ISO8601"] {COMMUNES} }} }}'
        doc = _appliquer(conf, _doc(quand="pas une date"))
        _verifie_ignorees(doc)
        assert "_dateparsefailure" in doc["tags"]

    def test_translate_sans_correspondance(self, tmp_path: Path) -> None:
        dico = tmp_path / "d.yml"
        dico.write_text('"1": un\n', encoding="utf-8")
        conf = f'filter {{ translate {{ source => "n" target => "t" dictionary_path => "{dico}" {COMMUNES} }} }}'
        _verifie_ignorees(_appliquer(conf, _doc(n="2")))

    def test_geoip_inconnue(self, base_geoip_city: str) -> None:
        conf = f'filter {{ geoip {{ source => "ip" database => "{base_geoip_city}" {COMMUNES} }} }}'
        _verifie_ignorees(_appliquer(conf, _doc(ip=IP_INCONNUE)))

    def test_plugin_non_interprete(self) -> None:
        _verifie_ignorees(_appliquer(f"filter {{ csv {{ {COMMUNES} }} }}", _doc()))


class TestFonction:
    def test_mal_typees_ignorees(self) -> None:
        doc = {"tags": "pas une liste", "a": 1}
        apply_common_options(doc, {"add_field": "x", "remove_field": "a", "remove_tag": ["t"]})
        assert doc == {"tags": "pas une liste", "a": 1}

    @pytest.mark.parametrize("tags", [None, ["vu"]])
    def test_add_tag_sans_doublon(self, tags: list | None) -> None:
        doc = {} if tags is None else {"tags": list(tags)}
        apply_common_options(doc, {"add_tag": ["vu", "vu"]})
        assert doc["tags"] == ["vu"]
