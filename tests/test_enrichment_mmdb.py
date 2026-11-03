# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests d'`src/enrichment.py` sur de vraies bases MaxMind DB (issue #73).

Les bases sont générées par `conftest.py` (`mmdb-writer`) : `GeoIPEnricher`,
`GeoIPLookup` et `GenericMmdbLookup` les ouvrent comme une base GeoLite2
réelle, sans double de test. Couvre le chemin heureux resté jusqu'ici hors
des tests automatisés (voir FEATURES.md, « Limites connues »), les IP
absentes, invalides ou privées, et une base aux champs partiels.
"""

from __future__ import annotations

import maxminddb
import pytest
from conftest import ENREGISTREMENT_PERSO, IP_CONTINENT_SEUL, IP_INCONNUE, IP_SANS_DETAIL, IP_STRASBOURG

from pystashtemplate.enrichment import GenericMmdbLookup, GeoIPEnricher, GeoIPLookup

GEO_STRASBOURG = {
    "ip": IP_STRASBOURG,
    "city_name": "Strasbourg",
    "continent_code": "EU",
    "country_code2": "FR",
    "country_name": "France",
    "postal_code": "67000",
    "region_name": "Grand Est",
    "timezone": "Europe/Paris",
    "latitude": 48.58,
    "longitude": 7.75,
    "location": [7.75, 48.58],
}


class TestGeoIPEnricher:
    def test_ip_trouvee_enregistrement_complet(self, base_geoip_city: str) -> None:
        enricher = GeoIPEnricher(base_geoip_city, ip_field="client_ip", out_field="geoip")
        try:
            doc = {"client_ip": IP_STRASBOURG}
            enricher.enrich(doc)
        finally:
            enricher.close()
        assert doc == {"client_ip": IP_STRASBOURG, "geoip": GEO_STRASBOURG}

    @pytest.mark.parametrize(
        ("ip", "attendu"),
        [
            pytest.param(IP_SANS_DETAIL, {"country_code2": "DE"}, id="pays-seul"),
            pytest.param(IP_CONTINENT_SEUL, {"continent_code": "EU"}, id="continent-seul"),
        ],
    )
    def test_champs_partiels_omis(self, base_geoip_city: str, ip: str, attendu: dict) -> None:
        """Base aux champs partiels : aucune clé à None, pas de location."""
        enricher = GeoIPEnricher(base_geoip_city, ip_field="ip", out_field="geo")
        try:
            doc = {"ip": ip}
            enricher.enrich(doc)
        finally:
            enricher.close()
        assert doc["geo"] == {"ip": ip, **attendu}

    @pytest.mark.parametrize(
        "doc",
        [
            pytest.param({"ip": IP_INCONNUE}, id="absente-de-la-base"),
            pytest.param({"ip": "10.0.0.1"}, id="privee"),
            pytest.param({"ip": "pas-une-ip"}, id="invalide"),
            pytest.param({"ip": ""}, id="vide"),
            pytest.param({"autre": 1}, id="champ-absent"),
        ],
    )
    def test_sans_resultat_document_inchange(self, base_geoip_city: str, doc: dict) -> None:
        avant = dict(doc)
        enricher = GeoIPEnricher(base_geoip_city, ip_field="ip", out_field="geo")
        try:
            enricher.enrich(doc)
        finally:
            enricher.close()
        assert doc == avant

    def test_base_introuvable_leve(self, tmp_path) -> None:
        with pytest.raises(OSError):
            GeoIPEnricher(str(tmp_path / "absente.mmdb"), ip_field="ip", out_field="geo")


class TestGeoIPLookup:
    def test_ip_trouvee(self, base_geoip_city: str) -> None:
        lookup = GeoIPLookup(base_geoip_city)
        try:
            assert lookup.lookup(IP_STRASBOURG) == GEO_STRASBOURG
        finally:
            lookup.close()

    @pytest.mark.parametrize("ip", [IP_INCONNUE, "pas-une-ip", ""])
    def test_sans_resultat_none(self, base_geoip_city: str, ip: str) -> None:
        lookup = GeoIPLookup(base_geoip_city)
        try:
            assert lookup.lookup(ip) is None
        finally:
            lookup.close()

    def test_fichier_non_mmdb_leve(self, tmp_path) -> None:
        faux = tmp_path / "faux.mmdb"
        faux.write_bytes(b"pas une base MaxMind")
        with pytest.raises(maxminddb.InvalidDatabaseError):
            GeoIPLookup(str(faux))


class TestGenericMmdbLookup:
    def test_enregistrement_brut_sans_schema(self, base_mmdb_perso: str) -> None:
        lookup = GenericMmdbLookup(base_mmdb_perso)
        try:
            assert lookup.lookup(IP_STRASBOURG) == ENREGISTREMENT_PERSO
        finally:
            lookup.close()

    def test_ip_absente_none(self, base_mmdb_perso: str) -> None:
        lookup = GenericMmdbLookup(base_mmdb_perso)
        try:
            assert lookup.lookup(IP_INCONNUE) is None
        finally:
            lookup.close()

    @pytest.mark.parametrize("ip", ["pas-une-ip", 42])
    def test_ip_invalide_none(self, base_mmdb_perso: str, ip: object) -> None:
        lookup = GenericMmdbLookup(base_mmdb_perso)
        try:
            assert lookup.lookup(ip) is None
        finally:
            lookup.close()

    def test_base_introuvable_leve(self, tmp_path) -> None:
        with pytest.raises(OSError):
            GenericMmdbLookup(str(tmp_path / "absente.mmdb"))
