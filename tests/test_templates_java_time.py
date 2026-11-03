# SPDX-License-Identifier: GPL-3.0-or-later
"""Lettres Java Time et Joda des gabarits de date `%{{…}}` et `%{+…}` (issue #123).

Date de référence : jeudi 22 octobre 2026, 08:05:09.123456 UTC (semaine
ISO 43, 295e jour de l'année, 4e semaine d'octobre).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from pystashtemplate.templates import _semaine_du_mois, render_template

NOW = datetime(2026, 10, 22, 8, 5, 9, 123456, tzinfo=timezone.utc)
SOIR = datetime(2026, 10, 22, 20, 5, tzinfo=timezone.utc)
MINUIT = datetime(2026, 10, 22, 0, 30, tzinfo=timezone.utc)


def _java(motif: str, moment: datetime = NOW) -> str:
    return render_template(f"%{{{{{motif}}}}}", {"@timestamp": moment.isoformat()}, now=NOW)


def _joda(motif: str, moment: datetime = NOW) -> str:
    return render_template(f"%{{+{motif}}}", {"@timestamp": moment.isoformat()}, now=NOW)


@pytest.mark.parametrize(
    ("motif", "attendu"),
    [
        ("G", "AD"),
        ("GGGG", "Anno Domini"),
        ("GGGGG", "A"),
        ("u", "2026"),
        ("uu", "26"),
        ("Y", "2026"),
        ("YY", "26"),
        ("Q", "4"),
        ("QQ", "04"),
        ("QQQ", "Q4"),
        ("QQQQ", "4th quarter"),
        ("QQQQQ", "4"),
        ("qqq", "Q4"),
        ("MMM", "Oct"),
        ("MMMM", "October"),
        ("MMMMM", "O"),
        ("LLL", "Oct"),
        ("L", "10"),
        ("w", "43"),
        ("W", "4"),
        ("D", "295"),
        ("DDD", "295"),
        ("E", "Thu"),
        ("EEEE", "Thursday"),
        ("EEEEE", "T"),
        ("e", "4"),
        ("ee", "04"),
        ("eee", "Thu"),
        ("c", "4"),
        ("cccc", "Thursday"),
        ("a", "AM"),
        ("h", "8"),
        ("hh", "08"),
        ("K", "8"),
        ("k", "8"),
        ("n", "123456000"),
        ("A", "29109123"),
        ("N", "29109123456000"),
        ("VV", "Z"),
        ("O", "GMT"),
        ("OOOO", "GMT"),
        ("EEE, dd MMM yyyy HH:mm:ss", "Thu, 22 Oct 2026 08:05:09"),
    ],
)
def test_lettres_java_time(motif: str, attendu: str) -> None:
    assert _java(motif) == attendu


@pytest.mark.parametrize(
    ("motif", "moment", "attendu"),
    [
        ("a h K k", SOIR, "PM 8 8 20"),
        ("a h K k", MINUIT, "AM 12 0 24"),
        ("O", datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=2))), "GMT+2"),
        ("O", datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=5, minutes=30))), "GMT+5:30"),
        ("OOOO", datetime(2026, 1, 1, tzinfo=timezone(-timedelta(hours=3))), "GMT-03:00"),
        ("VV", datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=2))), "+02:00"),
        ("Y-'W'w-e", datetime(2027, 1, 1, tzinfo=timezone.utc), "2026-W53-5"),
    ],
)
def test_heures_et_fuseaux(motif: str, moment: datetime, attendu: str) -> None:
    assert _java(motif, moment) == attendu


def test_identifiant_iana() -> None:
    # Un datetime aware avec ZoneInfo garde son identifiant.
    moment = datetime(2026, 10, 22, 10, tzinfo=ZoneInfo("Europe/Paris"))
    assert render_template("%{{VV}}", {"@timestamp": moment}, now=NOW) == "Europe/Paris"


@pytest.mark.parametrize(
    ("motif", "attendu"),
    [
        ("Y", "2026"),  # Joda : année de l'ère
        ("x", "2026"),  # Joda : année de la semaine ISO
        ("xx", "26"),
        ("C", "20"),
        ("e", "4"),
        ("EEE", "Thu"),
        ("EEEEE", "Thursday"),  # Joda : 4 lettres ou plus → texte complet
        ("MMMMM", "October"),
        ("GGGG", "AD"),
        ("ww", "43"),
        ("u", "u"),  # pas une lettre Joda : recopiée
        ("Q", "Q"),
        ("W", "W"),
        ("VV", "VV"),
    ],
)
def test_lettres_joda(motif: str, attendu: str) -> None:
    assert _joda(motif) == attendu


def test_annee_de_semaine_joda() -> None:
    assert _joda("x-ww", datetime(2027, 1, 1, tzinfo=timezone.utc)) == "2026-53"
    assert _joda("Y", datetime(2027, 1, 1, tzinfo=timezone.utc)) == "2027"


@pytest.mark.parametrize(
    ("jour", "attendu"),
    [
        (datetime(2027, 1, 1), 0),  # vendredi 1er : semaine de 3 jours, semaine 0
        (datetime(2027, 1, 4), 1),
        (datetime(2026, 6, 1), 1),  # lundi 1er
        (datetime(2026, 6, 8), 2),
        (datetime(2026, 10, 1), 1),  # jeudi 1er : 4 jours, semaine 1
    ],
)
def test_semaine_du_mois(jour: datetime, attendu: int) -> None:
    assert _semaine_du_mois(jour) == attendu


def test_lettres_inconnues_recopiees() -> None:
    assert _java("F B") == "F B"
    assert _java("V") == "V"
    assert _java("OO") == "OO"
