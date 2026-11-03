# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests d'`src/enrichment.py` (issue #25) : DictEnricher, TranslateDictionary
et la construction des enregistrements GeoIP (via un double de réponse geoip2)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import yaml

from pystashtemplate.enrichment import DictEnricher, TranslateDictionary, _build_geoip_record


def _fake_geoip2_response(
    *,
    city: str | None = "Strasbourg",
    country_code: str | None = "FR",
    country_name: str | None = "France",
    latitude: float | None = 48.583,
    longitude: float | None = 7.745,
) -> SimpleNamespace:
    """Double minimal d'une réponse `geoip2.models.City` (attributs utilisés)."""
    return SimpleNamespace(
        city=SimpleNamespace(name=city),
        continent=SimpleNamespace(code="EU"),
        country=SimpleNamespace(iso_code=country_code, name=country_name),
        postal=SimpleNamespace(code="67000"),
        subdivisions=SimpleNamespace(most_specific=SimpleNamespace(name="Grand Est")),
        location=SimpleNamespace(time_zone="Europe/Paris", latitude=latitude, longitude=longitude),
    )


class TestDictEnricher:
    def test_cle_connue_fusionne_les_champs(self) -> None:
        enricher = DictEnricher(
            mapping={"srv1": {"site": "strasbourg", "role": "web"}},
            key_field="hostname",
            out_field="host_meta",
        )
        doc = {"hostname": "srv1"}
        enricher.enrich(doc)
        assert doc["host_meta"] == {"site": "strasbourg", "role": "web"}

    def test_cle_absente_sans_effet(self) -> None:
        enricher = DictEnricher(mapping={"srv1": {"site": "x"}}, key_field="hostname", out_field="m")
        doc = {"hostname": "srv2"}
        enricher.enrich(doc)
        assert doc == {"hostname": "srv2"}

    def test_champ_cle_absent_sans_effet(self) -> None:
        enricher = DictEnricher(mapping={"srv1": {"site": "x"}}, key_field="hostname", out_field="m")
        doc = {"autre": 1}
        enricher.enrich(doc)
        assert doc == {"autre": 1}


class TestTranslateDictionary:
    def test_lookup_hit(self, tmp_path) -> None:
        f = tmp_path / "ifName.yml"
        f.write_text("eth0: Uplink\neth1: Lan\n", encoding="utf-8")
        d = TranslateDictionary(str(f))
        assert d.lookup("eth0") == "Uplink"
        assert d.lookup("eth1") == "Lan"

    def test_lookup_miss_renvoie_none(self, tmp_path) -> None:
        f = tmp_path / "d.yml"
        f.write_text("eth0: Uplink\n", encoding="utf-8")
        d = TranslateDictionary(str(f))
        assert d.lookup("eth9") is None

    def test_fichier_vide_dictionnaire_vide(self, tmp_path) -> None:
        f = tmp_path / "vide.yml"
        f.write_text("", encoding="utf-8")
        d = TranslateDictionary(str(f))
        assert d.lookup("eth0") is None

    def test_racine_non_mapping_dictionnaire_vide(self, tmp_path) -> None:
        # Une simple liste YAML à la racine → traité comme dict vide.
        f = tmp_path / "liste.yml"
        f.write_text("- a\n- b\n", encoding="utf-8")
        d = TranslateDictionary(str(f))
        assert d.lookup("a") is None

    def test_fichier_introuvable_leve(self, tmp_path) -> None:
        with pytest.raises(OSError):
            TranslateDictionary(str(tmp_path / "absent.yml"))

    def test_yaml_malforme_leve(self, tmp_path) -> None:
        f = tmp_path / "mauvais.yml"
        f.write_text("a: [1, 2\n", encoding="utf-8")
        with pytest.raises(yaml.YAMLError):
            TranslateDictionary(str(f))

    def test_cle_non_hachable_renvoie_none(self, tmp_path) -> None:
        # Une clé liste/dict ne fait pas échouer le document.
        f = tmp_path / "d.yml"
        f.write_text("eth0: Uplink\n", encoding="utf-8")
        d = TranslateDictionary(str(f))
        assert d.lookup(["eth0"]) is None


class TestBuildGeoipRecord:
    def test_champs_alignes_sur_le_filtre_officiel(self) -> None:
        geo = _build_geoip_record("1.2.3.4", _fake_geoip2_response())
        assert geo["ip"] == "1.2.3.4"
        assert geo["city_name"] == "Strasbourg"
        assert geo["continent_code"] == "EU"
        assert geo["country_code2"] == "FR"
        assert geo["country_name"] == "France"
        assert geo["postal_code"] == "67000"
        assert geo["region_name"] == "Grand Est"
        assert geo["timezone"] == "Europe/Paris"
        assert geo["latitude"] == 48.583
        assert geo["longitude"] == 7.745
        # Convention GeoJSON [lon, lat], compatible geo_point OpenSearch.
        assert geo["location"] == [7.745, 48.583]

    def test_latitude_absente_pas_de_location(self) -> None:
        geo = _build_geoip_record("1.2.3.4", _fake_geoip2_response(latitude=None))
        assert "latitude" not in geo
        assert "longitude" not in geo
        assert "location" not in geo

    def test_valeurs_none_omises(self) -> None:
        geo = _build_geoip_record("1.2.3.4", _fake_geoip2_response(city=None))
        assert "city_name" not in geo
