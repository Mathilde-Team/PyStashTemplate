# SPDX-License-Identifier: GPL-3.0-or-later
"""Ajoute `src/` au sys.path pour que les tests importent le paquet
`pystashtemplate` sans installation préalable.

Hérité du conftest de RsysLogstach (découpage #111) : fixtures mmdb,
langue fixée en français, DNS réseau coupé.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Les messages sont traduits selon l'environnement (gettext) : les tests
# vérifient les messages d'origine (français), quelle que soit la machine.
os.environ["LANGUAGE"] = "fr_FR"
os.environ.pop("PYSTASHTEMPLATE_LOCALEDIR", None)
os.environ.pop("RSYSLOGSTACH_LOCALEDIR", None)
# Aucun test n'interroge un vrai serveur DNS (fichiers hosts seuls).
os.environ["PYSTASHTEMPLATE_DNS"] = "off"
os.environ["RSYSLOGSTACH_DNS"] = "off"

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


# ----------------------------------------------------------------------
# Bases MaxMind DB réelles, générées pour les tests
# ----------------------------------------------------------------------

import pytest  # noqa: E402

IP_STRASBOURG = "81.0.1.2"  # réseau 81.0.0.0/16 : enregistrement City complet
IP_SANS_DETAIL = "82.0.1.2"  # réseau 82.0.0.0/16 : pays seulement
IP_CONTINENT_SEUL = "83.0.1.2"  # réseau 83.0.0.0/16 : continent seulement
IP_INCONNUE = "8.8.8.8"  # absente des bases de test

ENREGISTREMENT_CITY = {
    "city": {"names": {"en": "Strasbourg"}},
    "continent": {"code": "EU"},
    "country": {"iso_code": "FR", "names": {"en": "France"}},
    "location": {"latitude": 48.58, "longitude": 7.75, "time_zone": "Europe/Paris"},
    "postal": {"code": "67000"},
    "subdivisions": [{"names": {"en": "Grand Est"}}],
}
ENREGISTREMENT_PERSO = {"tz": "Europe/Paris", "WWW": "https://exemple.invalid", "longitude": 7.75}


def _ecrire_base(chemin: Path, database_type: str, reseaux: dict[str, dict]) -> Path:
    from mmdb_writer import MMDBWriter
    from netaddr import IPSet

    writer = MMDBWriter(ip_version=6, database_type=database_type, ipv4_compatible=True)
    for reseau, enregistrement in reseaux.items():
        writer.insert_network(IPSet([reseau]), enregistrement)
    writer.to_db_file(str(chemin))
    return chemin


@pytest.fixture(scope="session")
def base_geoip_city(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Base de type GeoLite2-City : une IP complète, une avec le pays seul, une avec le continent seul."""
    chemin = tmp_path_factory.mktemp("mmdb") / "GeoLite2-City-test.mmdb"
    return str(
        _ecrire_base(
            chemin,
            "GeoLite2-City",
            {
                "81.0.0.0/16": ENREGISTREMENT_CITY,
                "82.0.0.0/16": {"country": {"iso_code": "DE"}},
                "83.0.0.0/16": {"continent": {"code": "EU"}},
            },
        )
    )


@pytest.fixture(scope="session")
def base_mmdb_perso(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Base personnalisée (schéma libre, comme la base XXXX.mmdb du pipeline réel)."""
    chemin = tmp_path_factory.mktemp("mmdb") / "perso.mmdb"
    return str(_ecrire_base(chemin, "Perso", {"81.0.0.0/16": ENREGISTREMENT_PERSO}))
