# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests du filtre `date` partagé par `--filters` et `--pipeline-conf` (issue #34).

- step JSON `"date"` de `FilterPipeline` : motifs ISO8601, UNIX, UNIX_MS,
  Java/Joda, `timezone`, `target` imbriqué, `tag_on_failure` ;
- configuration invalide refusée dès la compilation ;
- `DateFilter` utilisé directement, et même résultat que le plugin `date`
  d'un `.conf` (une seule implémentation pour les deux mécanismes).
"""

from __future__ import annotations

from datetime import timedelta
from zoneinfo import ZoneInfo

import pytest

from pystashtemplate.filters import DateFilter, FilterPipeline, parse_date_value, translate_date_pattern
from pystashtemplate.interpreter import PipelineInterpreter


def _pipeline(**options):
    return FilterPipeline([{"type": "date", "_comment": "test", **options}])


class TestStepDateFilters:
    """Step `"date"` du format JSON de `--filters`."""

    def test_iso8601_vers_timestamp(self):
        doc = {"horodatage": "2026-10-24T08:15:00+02:00"}
        assert _pipeline(match=["horodatage", "ISO8601"]).apply(doc) is True
        assert doc["@timestamp"] == "2026-10-24T06:15:00+00:00"

    def test_iso8601_suffixe_z(self):
        doc = {"t": "2026-10-24T06:15:00Z"}
        _pipeline(match=["t", "ISO8601"]).apply(doc)
        assert doc["@timestamp"] == "2026-10-24T06:15:00+00:00"

    @pytest.mark.parametrize(
        ("motif", "valeur"),
        [("UNIX", "1792822500"), ("UNIX_MS", "1792822500000")],
    )
    def test_epoch(self, motif, valeur):
        doc = {"t": valeur}
        _pipeline(match=["t", motif]).apply(doc)
        assert doc["@timestamp"] == "2026-10-24T06:15:00+00:00"

    def test_motif_joda_avec_fuseau_iana(self):
        doc = {"t": "Oct 24 2026 08:15:00"}
        _pipeline(match=["t", "MMM dd yyyy HH:mm:ss"], timezone="Europe/Paris").apply(doc)
        assert doc["@timestamp"] == "2026-10-24T06:15:00+00:00"

    def test_fuseau_inconnu_retombe_sur_utc(self):
        doc = {"t": "2026-10-24 06:15:00"}
        _pipeline(match=["t", "yyyy-MM-dd HH:mm:ss"], timezone="Pas/Un_Fuseau").apply(doc)
        assert doc["@timestamp"] == "2026-10-24T06:15:00+00:00"

    def test_sss_et_zone_textuelle_dans_le_step(self):
        # #118 : SSS + « CET » (+01:00) → 05:15:00.123 UTC.
        doc = {"t": "2026-10-24 06:15:00.123 CET"}
        _pipeline(match=["t", "yyyy-MM-dd HH:mm:ss.SSS z"]).apply(doc)
        assert doc["@timestamp"] == "2026-10-24T05:15:00.123000+00:00"

    def test_locale_dans_le_step(self):
        # #118 : option `locale` du plugin officiel, noms français.
        doc = {"t": "lun. 24 oct. 2026 06:15:00"}
        _pipeline(match=["t", "EEE dd MMM yyyy HH:mm:ss"], locale="fr").apply(doc)
        assert doc["@timestamp"] == "2026-10-24T06:15:00+00:00"

    def test_premier_motif_qui_correspond(self):
        doc = {"t": "24/10/2026 06:15"}
        _pipeline(match=["t", "ISO8601", 42, "dd/MM/yyyy HH:mm"]).apply(doc)
        assert doc["@timestamp"] == "2026-10-24T06:15:00+00:00"
        assert "tags" not in doc

    def test_target_imbrique(self):
        doc = {"evt": {"t": "2026-10-24T06:15:00Z"}}
        _pipeline(match=["[evt][t]", "ISO8601"], target="[evt][date]").apply(doc)
        assert doc["evt"]["date"] == "2026-10-24T06:15:00+00:00"
        assert "@timestamp" not in doc

    def test_echec_tag_par_defaut(self):
        doc = {"t": "pas une date"}
        assert _pipeline(match=["t", "ISO8601"]).apply(doc) is True  # date n'écarte jamais le document
        assert doc["tags"] == ["_dateparsefailure"]
        assert "@timestamp" not in doc

    def test_champ_absent_tag_personnalise(self):
        doc = {"tags": ["deja"]}
        _pipeline(match=["t", "ISO8601"], tag_on_failure=["_date_ko"]).apply(doc)
        assert doc["tags"] == ["deja", "_date_ko"]

    def test_champ_none(self):
        doc = {"t": None}
        _pipeline(match=["t", "ISO8601"]).apply(doc)
        assert doc["tags"] == ["_dateparsefailure"]

    def test_suite_de_la_chaine_executee(self):
        steps = [
            {"type": "dissect", "source": "message", "pattern": "%{quand} %{reste}"},
            {"type": "date", "match": ["quand", "ISO8601"]},
            {"type": "mutate", "remove_field": ["quand"]},
        ]
        doc = {"message": "2026-10-24T06:15:00Z bonjour"}
        assert FilterPipeline(steps).apply(doc) is True
        assert doc["@timestamp"] == "2026-10-24T06:15:00+00:00"
        assert "quand" not in doc
        assert doc["reste"] == "bonjour"

    @pytest.mark.parametrize(
        "options",
        [{}, {"match": "t"}, {"match": ["t"]}, {"match": [1, "ISO8601"]}],
    )
    def test_match_invalide_refuse_a_la_compilation(self, options):
        with pytest.raises(ValueError, match="filtre date"):
            _pipeline(**options)


class TestDateFilterDirect:
    """`DateFilter` sans `FilterPipeline`, comme l'utilise `--pipeline-conf`."""

    def test_match_invalide_ignore(self):
        doc = {"t": "2026-10-24T06:15:00Z"}
        assert DateFilter({"match": ["t"]}).apply(doc) is False
        assert doc == {"t": "2026-10-24T06:15:00Z"}

    def test_succes_renvoie_vrai(self):
        assert DateFilter({"match": ["t", "UNIX"]}).apply({"t": 0}) is True

    def test_meme_resultat_que_le_plugin_conf(self):
        conf = 'filter { date { match => ["t", "dd/MM/yyyy HH:mm"] timezone => "Europe/Paris" } }'
        doc_conf = {"t": "24/10/2026 08:15"}
        PipelineInterpreter(conf).apply(doc_conf)  # modifie le document en place
        doc_json = {"t": "24/10/2026 08:15"}
        _pipeline(match=["t", "dd/MM/yyyy HH:mm"], timezone="Europe/Paris").apply(doc_json)
        assert doc_conf["@timestamp"] == doc_json["@timestamp"] == "2026-10-24T06:15:00+00:00"


class TestFonctionsPartagees:
    """Fonctions désormais publiques de `filters` (déplacées d'`interpreter` en #34)."""

    def test_translate_date_pattern(self):
        assert translate_date_pattern("yyyy-MM-dd'T'HH:mm:ssXXX") == "%Y-%m-%dT%H:%M:%S%z"
        # #118 : SSS (millisecondes) et noms de fuseaux textuels traduits.
        assert translate_date_pattern("HH:mm:ss.SSS") == "%H:%M:%S.%f"
        assert translate_date_pattern("yyyy zzz") == "%Y %Z"
        assert translate_date_pattern("yyyy ZZZ") == "%Y %Z"
        # Lettres non prises en charge (#123) : motif refusé.
        assert translate_date_pattern("D") is None

    def test_parse_date_value_motif_inconnu(self):
        assert parse_date_value("123", "D", None) is None

    def test_parse_date_value_sss_millisecondes(self):
        # "123" avec SSS : 123 millisecondes (pas 123 microsecondes).
        dt = parse_date_value("12:34:56.123", "HH:mm:ss.SSS", None)
        assert dt is not None
        assert (dt.hour, dt.minute, dt.second, dt.microsecond) == (12, 34, 56, 123000)

    def test_parse_date_value_zone_textuelle_abreviation(self):
        # « CET » : +01:00, prime sur `timezone` comme un fuseau explicite.
        dt = parse_date_value("2020-01-02 CET", "yyyy-MM-dd z", None)
        assert dt is not None
        assert dt.utcoffset() == timedelta(hours=1)
        assert dt.year == 2020

    def test_parse_date_value_zone_textuelle_id_iana(self):
        dt = parse_date_value("2020-01-02 Europe/Paris", "yyyy-MM-dd zzz", None)
        assert dt is not None
        assert dt.tzinfo == ZoneInfo("Europe/Paris")

    def test_parse_date_value_zone_textuelle_absente_echec(self):
        # Le motif attend un nom de fuseau, le texte n'en a pas : échec.
        assert parse_date_value("2020-01-02", "yyyy-MM-dd z", None) is None

    def test_parse_date_value_locale_fr(self):
        # Noms français traduits en anglais avant strptime. Le nom de jour
        # est lu puis écarté : la date elle-même gagne (comme Joda).
        dt = parse_date_value("lun. 2 janv. 2020 12:34:56", "EEE dd MMM yyyy HH:mm:ss", None, locale="fr")
        assert dt is not None
        assert (dt.year, dt.month, dt.day) == (2020, 1, 2)

    def test_parse_date_value_locale_fr_noms_complets(self):
        dt = parse_date_value("lundi 2 mars 2021", "EEEE dd MMMM yyyy", None, locale="fr")
        assert dt is not None
        assert (dt.year, dt.month, dt.day) == (2021, 3, 2)

    def test_parse_date_value_locale_inconnue_ignoree(self):
        # Locale sans table : noms anglais attendus, « janv. » ne matche pas.
        assert parse_date_value("lun. 2 janv. 2020", "EEE dd MMM yyyy", None, locale="xx") is None
