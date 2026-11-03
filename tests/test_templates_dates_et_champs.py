# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de `src/templates.py` : gabarits de date et écriture de champs (issue #73).

Gabarits `%{+FORMAT}` (Joda), `%{{FORMAT}}` (Java Time) et `%{{TIME_NOW}}`,
horodatage tiré de `@timestamp` (chaîne ISO, `Z`, naïf, `datetime`, invalide)
ou de `now`, chaque lettre de motif gérée, littéraux entre quotes, gabarits
mal refermés recopiés tels quels ; puis `assign_field` et
`remove_field_path` sur des chemins imbriqués, listes et scalaires.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from pystashtemplate.templates import assign_field, compile_template, remove_field_path, render, render_template

PARIS_ETE = timezone(timedelta(hours=2))
INDE = timezone(timedelta(hours=5, minutes=30))
NOW = datetime(2026, 10, 22, 8, 5, 9, 123456, tzinfo=timezone.utc)


class TestGabaritsDeDate:
    def test_ancienne_syntaxe_depuis_timestamp(self) -> None:
        doc = {"@timestamp": "2026-01-02T03:04:05.678Z"}
        assert render_template("logs-%{+YYYY.MM.dd}", doc, now=NOW) == "logs-2026.01.02"

    def test_epoch_secondes_depuis_timestamp(self) -> None:
        """`%{+%s}` : secondes depuis l'epoch, comme Logstash (issue #102)."""
        doc = {"@timestamp": "2026-11-03T10:00:00.900Z"}
        assert render_template("id-%{+%s}", doc, now=NOW) == "id-1793700000"

    def test_epoch_secondes_repli_sur_now(self) -> None:
        """Sans `@timestamp`, `%{+%s}` prend l'heure de traitement."""
        assert render_template("%{+%s}", {}, now=NOW) == str(int(NOW.timestamp()))

    def test_nouvelle_syntaxe_et_time_now(self) -> None:
        doc = {"@timestamp": "2026-01-02T03:04:05+00:00"}
        assert render_template("%{{yyyy-MM-dd'T'HH:mm:ss}}", doc, now=NOW) == "2026-01-02T03:04:05"
        assert render_template("%{{TIME_NOW}}", doc, now=NOW) == "2026-10-22T08:05:09.123Z"

    def test_time_now_par_defaut_heure_courante(self) -> None:
        avant = datetime.now(timezone.utc).replace(microsecond=0)
        rendu = render_template("%{{TIME_NOW}}", {})
        assert datetime.fromisoformat(rendu.replace("Z", "+00:00")) >= avant - timedelta(seconds=1)

    @pytest.mark.parametrize(
        ("timestamp", "attendu"),
        [
            pytest.param("2026-01-02T03:04:05", "2026-01-02 03", id="iso-naif-utc"),
            pytest.param("2026-01-02T03:04:05+02:00", "2026-01-02 03", id="iso-avec-decalage"),
            pytest.param(datetime(2026, 1, 2, 3, 4), "2026-01-02 03", id="datetime-naif"),
            pytest.param(datetime(2026, 1, 2, 3, 4, tzinfo=PARIS_ETE), "2026-01-02 03", id="datetime-aware"),
            pytest.param("pas une date", "2026-10-22 08", id="invalide-repli-now"),
            pytest.param(1767322800, "2026-10-22 08", id="nombre-repli-now"),
            pytest.param("", "2026-10-22 08", id="vide-repli-now"),
        ],
    )
    def test_source_de_l_horodatage(self, timestamp, attendu: str) -> None:
        assert render_template("%{+yyyy-MM-dd HH}", {"@timestamp": timestamp}, now=NOW) == attendu

    def test_evenement_non_dict_repli_now(self) -> None:
        assert render(compile_template("%{+yyyy}"), ["pas", "un", "dict"], now=NOW) == "2026"

    @pytest.mark.parametrize(
        ("motif", "attendu"),
        [
            ("yy", "26"),
            ("y", "2026"),  # année complète : seul `yy` est réduit (Joda et Java Time)
            ("yyy", "2026"),
            ("yyyy", "2026"),
            ("yyyyy", "02026"),
            ("M", "10"),
            ("MM", "10"),
            ("d", "22"),
            ("H", "8"),
            ("HH", "08"),
            ("mm", "05"),
            ("s", "9"),
            ("ss", "09"),
            ("S", "1"),
            ("SSS", "123"),
            ("SSSSSS", "123456"),  # microsecondes (#123)
            ("SSSSSSSSS", "123456000"),  # complétées par des zéros
            ("X", "Z"),
            ("Z", "+0000"),
            ("ZZZZ", "+00:00"),
            ("EEE", "Thu"),  # #123
            ("F", "F"),  # lettre non gérée : recopiée (limite documentée)
            ("HH'h'mm", "08h05"),
            ("'T'", "T"),
            ("''", "'"),
            ("'o''clock'", "o'clock"),
            ("HH 'non refermé", "08 non refermé"),
            ("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "2026-10-22T08:05:09.123Z"),
        ],
    )
    def test_lettres_et_litteraux_utc(self, motif: str, attendu: str) -> None:
        assert render_template(f"%{{{{{motif}}}}}", {}, now=NOW) == attendu

    @pytest.mark.parametrize(
        ("fuseau", "motif", "attendu"),
        [
            (PARIS_ETE, "X", "+02"),
            (PARIS_ETE, "XX", "+0200"),
            (PARIS_ETE, "XXX", "+02:00"),
            (INDE, "XX", "+0530"),
            (timezone(-timedelta(hours=3)), "XXX", "-03:00"),
            (timezone(-timedelta(hours=3)), "Z", "-0300"),
            (INDE, "zzzz", "+05:30"),
        ],
    )
    def test_decalages(self, fuseau: timezone, motif: str, attendu: str) -> None:
        doc = {"@timestamp": datetime(2026, 10, 22, 10, 0, tzinfo=fuseau)}
        assert render_template(f"%{{+{motif}}}", doc, now=NOW) == attendu

    @pytest.mark.parametrize(
        ("gabarit", "attendu"),
        [
            ("a-%{hostname", "a-%{hostname"),
            ("a-%{{yyyy", "a-%{{yyyy"),
            ("%{h}-%{", "x-%{"),
            ("100%", "100%"),
            ("%%{h}", "%x"),
        ],
    )
    def test_gabarits_mal_refermes_recopies(self, gabarit: str, attendu: str) -> None:
        assert render_template(gabarit, {"h": "x"}, now=NOW) == attendu


def test_indice_non_numerique_dans_une_liste_absent() -> None:
    from pystashtemplate.templates import _MISSING, resolve_field

    assert resolve_field({"l": [1, 2]}, ["l", "x"]) is _MISSING


class TestAssignField:
    def test_cree_les_dictionnaires_intermediaires(self) -> None:
        doc: dict = {"a": "scalaire"}
        assign_field(doc, ["a", "b", "c"], 1)  # un scalaire intermédiaire est remplacé
        assign_field(doc, ["x", "y"], 2)
        assert doc == {"a": {"b": {"c": 1}}, "x": {"y": 2}}

    def test_traverse_et_ecrit_dans_une_liste(self) -> None:
        doc = {"l": [{"v": 0}, [0, 0]]}
        assign_field(doc, ["l", "0", "v"], 1)
        assign_field(doc, ["l", "-1", "1"], 2)
        assign_field(doc, ["l", "1", "0"], 3)
        assert doc == {"l": [{"v": 1}, [3, 2]]}

    @pytest.mark.parametrize(
        "chemin",
        [
            pytest.param(["l", "5", "v"], id="indice-intermediaire-hors-limites"),
            pytest.param(["l", "x", "v"], id="indice-intermediaire-non-numerique"),
            pytest.param(["l", "1", "9"], id="indice-final-hors-limites"),
            pytest.param(["l", "1", "x"], id="indice-final-non-numerique"),
            pytest.param(["l", "1", "0", "v"], id="dernier-parent-scalaire"),
            pytest.param(["l", "1", "0", "v", "w"], id="intermediaire-scalaire"),
            pytest.param([], id="chemin-vide"),
        ],
    )
    def test_ecriture_abandonnee_sans_modifier(self, chemin: list) -> None:
        doc = {"l": [{"s": "t"}, [0]]}
        assign_field(doc, chemin, 1)
        assert doc == {"l": [{"s": "t"}, [0]]}


class TestRemoveFieldPath:
    def test_retire_imbrique_et_premier_niveau(self) -> None:
        doc = {"a": {"b": 1, "c": 2}, "d": 3}
        remove_field_path(doc, ["a", "b"])
        remove_field_path(doc, ["d"])
        assert doc == {"a": {"c": 2}}

    @pytest.mark.parametrize(
        "chemin",
        [
            pytest.param([], id="chemin-vide"),
            pytest.param(["absent", "x"], id="parent-absent"),
            pytest.param(["a", "absent"], id="cle-absente"),
            pytest.param(["l", "0"], id="parent-liste-ignore"),
            pytest.param(["s", "x"], id="parent-scalaire"),
        ],
    )
    def test_sans_effet(self, chemin: list) -> None:
        doc = {"a": {"b": 1}, "l": [1, 2], "s": "texte"}
        remove_field_path(doc, chemin)
        assert doc == {"a": {"b": 1}, "l": [1, 2], "s": "texte"}
