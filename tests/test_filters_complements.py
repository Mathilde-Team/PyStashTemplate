# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests complémentaires de `src/filters.py` (issue #73).

- `convert_value` : règles de la documentation officielle du filtre `mutate`
  (option `convert`), cas par cas ;
- options `mutate` restantes (`coerce`, `rename`, `update`, `gsub`, casse,
  `strip`, `split`, `join`, `merge`, `copy`, `tag_on_failure`) ;
- steps JSON `geoip` et `translate` sur une vraie base `.mmdb` et un vrai
  YAML, avec leurs erreurs de configuration ;
- `FilterPipeline` : `regex`, `target`, `drop` mal configuré, `close()`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import IP_INCONNUE, IP_STRASBOURG

from pystashtemplate.filters import (
    DropFilter,
    FilterPipeline,
    GeoipFilter,
    TranslateFilter,
    apply_mutate,
    convert_value,
)


class TestConvertValueDocumentationLogstash:
    """https://www.elastic.co/docs/reference/logstash/plugins/plugins-filters-mutate (option convert)."""

    @pytest.mark.parametrize(
        ("valeur", "type_cible", "attendu"),
        [
            # integer
            ("1,000", "integer", 1000),
            ("1000.5", "integer", 1000),
            (" 42 ", "integer", 42),
            (3.99, "integer", 3),
            (-2.7, "integer", -2),
            (True, "integer", 1),
            (False, "integer", 0),
            ("-1,234.9", "int", -1234),
            # integer_eu
            ("1.000", "integer_eu", 1000),
            ("1.000,9", "integer_eu", 1000),
            # float
            (3, "float", 3.0),
            ("1,000.5", "float", 1000.5),
            (True, "float", 1.0),
            (False, "float", 0.0),
            # float_eu
            ("1.000,5", "float_eu", 1000.5),
            ("3,14", "float_eu", 3.14),
            # string
            (42, "string", "42"),
            (True, "string", "true"),
            (False, "str", "false"),
            # boolean
            (0, "boolean", False),
            (1, "boolean", True),
            (0.0, "boolean", False),
            (1.0, "boolean", True),
            (True, "bool", True),
            *[(texte, "boolean", True) for texte in ("true", "t", "yes", "y", "1", "1.0", " TRUE ")],
            *[(texte, "boolean", False) for texte in ("false", "f", "no", "n", "0", "0.0", "", "No")],
        ],
    )
    def test_regle_documentee(self, valeur, type_cible: str, attendu) -> None:
        resultat = convert_value(valeur, type_cible, "champ")
        assert resultat == attendu
        assert type(resultat) is type(attendu)

    @pytest.mark.parametrize(
        ("valeur", "type_cible"),
        [
            ("peut-être", "boolean"),  # « all other values pass straight through »
            (5, "boolean"),
            ("abc", "integer"),  # écart assumé : pas de « abc ».to_i == 0
            ("abc", "float_eu"),
            ("nan", "float"),
            ("inf", "integer"),
            (None, "integer"),
            ("1", "inconnu"),
        ],
    )
    def test_valeur_inchangee(self, valeur, type_cible: str) -> None:
        assert convert_value(valeur, type_cible, "champ") is valeur

    def test_tableau_chaque_element_hash_sans_action(self) -> None:
        assert convert_value(["1", "2,000", "x"], "integer", "c") == [1, 2000, "x"]
        hash_ = {"a": "1"}
        assert convert_value(hash_, "integer", "c") is hash_

    def test_via_apply_mutate_et_chemin_imbrique(self) -> None:
        doc = {"net": {"port": "8,080", "ok": "yes"}, "taille": "1.024,5"}
        apply_mutate(
            doc,
            {"convert": {"[net][port]": "integer", "[net][ok]": "boolean", "taille": "float_eu", "absent": "integer"}},
        )
        assert doc == {"net": {"port": 8080, "ok": True}, "taille": 1024.5}


class TestOptionsMutate:
    def test_coerce_seulement_si_null(self) -> None:
        doc = {"a": None, "b": "x"}
        apply_mutate(doc, {"coerce": {"a": "défaut", "b": "jamais", "c": "absent"}})
        assert doc == {"a": "défaut", "b": "x"}

    def test_rename_imbrique_et_absent(self) -> None:
        doc = {"src": {"ip": "1.2.3.4"}}
        apply_mutate(doc, {"rename": {"[src][ip]": "[source][address]", "absent": "x"}})
        assert doc == {"src": {}, "source": {"address": "1.2.3.4"}}

    def test_update_seulement_si_present(self) -> None:
        doc = {"a": 1}
        apply_mutate(doc, {"update": {"a": "%{b}", "c": "jamais"}}, on_missing="empty")
        assert doc == {"a": ""}

    def test_gsub_triplets_motif_invalide_et_non_texte(self) -> None:
        doc = {"m": "a-b_c", "n": 5, "o": "x"}
        apply_mutate(doc, {"gsub": ["m", "[-_]", ".", "n", "5", "6", "o", "(", "y", "m", "a"]})
        assert doc == {"m": "a.b.c", "n": 5, "o": "x"}

    def test_casse_strip_split_join(self) -> None:
        doc = {"u": "abc", "c": "hELLO", "l": "ABC", "s": "  x  ", "sp": "a,b", "j": [1, "b"], "n": 3}
        apply_mutate(
            doc,
            {
                "uppercase": ["u", "n"],
                "capitalize": ["c"],
                "lowercase": ["l"],
                "strip": ["s", "n"],
                "split": {"sp": ",", "n": ","},
                "join": {"j": "-", "n": "-"},
            },
        )
        assert doc == {"u": "ABC", "c": "Hello", "l": "abc", "s": "x", "sp": ["a", "b"], "j": "1-b", "n": 3}

    def test_replace_split_join_absents_remove_tag(self) -> None:
        doc = {"a": "ancien", "h": "sw1", "tags": ["x", "y", "x"]}
        apply_mutate(
            doc,
            {
                "replace": {"a": "%{h}-neuf", "[nouveau][champ]": "créé"},
                "split": {"absent": ","},
                "join": {"absent": "-"},
                "remove_tag": ["x"],
            },
        )
        assert doc == {"a": "sw1-neuf", "h": "sw1", "tags": ["y"], "nouveau": {"champ": "créé"}}

    @pytest.mark.parametrize(
        ("dest", "source", "attendu"),
        [
            pytest.param({"a": 1}, {"b": 2}, {"a": 1, "b": 2}, id="hash-hash"),
            pytest.param(["a"], "b", ["a", "b"], id="tableau-chaine"),
            pytest.param("a", "b", ["a", "b"], id="chaine-chaine"),
            pytest.param("a", ["b", "c"], ["a", "b", "c"], id="chaine-tableau"),
            pytest.param(["a"], {"b": 2}, ["a"], id="tableau-hash-ignore"),
            pytest.param({"a": 1}, "b", {"a": 1}, id="hash-chaine-ignore"),
        ],
    )
    def test_merge_matrice_documentee(self, dest, source, attendu) -> None:
        doc = {"d": dest, "s": source}
        apply_mutate(doc, {"merge": {"d": "s"}})
        assert doc["d"] == attendu
        assert doc["s"] == source

    def test_merge_destination_ou_source_absente(self) -> None:
        doc = {"s": ["x"]}
        apply_mutate(doc, {"merge": {"d": "s", "s": "absent"}})
        assert doc == {"s": ["x"], "d": ["x"]}

    def test_copy_profonde(self) -> None:
        doc = {"a": {"l": [1]}}
        apply_mutate(doc, {"copy": {"a": "[b][c]", "absent": "x"}})
        doc["a"]["l"].append(2)
        assert doc == {"a": {"l": [1, 2]}, "b": {"c": {"l": [1]}}}

    @pytest.mark.parametrize(
        ("tag_on_failure", "attendu"),
        [(None, "_mutate_error"), ("mon_echec", "mon_echec"), (["liste"], "_mutate_error")],
    )
    def test_exception_tag_on_failure_et_operations_restantes_ignorees(self, tag_on_failure, attendu) -> None:
        options: dict = {"rename": {"a[": "b"}, "add_field": {"jamais": "1"}}
        if tag_on_failure is not None:
            options["tag_on_failure"] = tag_on_failure
        doc = {"tags": [attendu]} if attendu == "mon_echec" else {}
        apply_mutate(doc, options)
        assert doc == {"tags": [attendu]}


# ───────────────────────────── steps geoip / translate ─────────────────────────────


class TestStepGeoip:
    def test_pipeline_json_geolocalise_et_ferme(self, base_geoip_city: str) -> None:
        pipeline = FilterPipeline(
            [{"type": "geoip", "source": "[client][ip]", "database": base_geoip_city, "fields": ["city_name"]}]
        )
        doc = {"client": {"ip": IP_STRASBOURG}}
        assert pipeline.apply(doc) is True
        pipeline.close()
        assert doc["geoip"] == {"city_name": "Strasbourg"}

    @pytest.mark.parametrize(
        "doc",
        [pytest.param({"ip": IP_INCONNUE}, id="sans-correspondance"), pytest.param({}, id="ip-absente")],
    )
    def test_echec_tag(self, base_geoip_city: str, doc: dict) -> None:
        filtre = GeoipFilter({"source": "ip", "database": base_geoip_city, "tag_on_failure": ["geo_ko"]})
        filtre.apply(doc)
        filtre.close()
        assert doc["tags"] == ["geo_ko"] and "geoip" not in doc

    @pytest.mark.parametrize(
        "options",
        [
            pytest.param({"database": "x.mmdb"}, id="source-absente"),
            pytest.param({"source": "ip"}, id="database-absente"),
            pytest.param({"source": "ip", "database": "x.mmdb", "target": 3}, id="target-non-textuel"),
        ],
    )
    def test_step_mal_forme(self, options: dict) -> None:
        with pytest.raises(ValueError, match="step geoip mal formé"):
            GeoipFilter(options)

    def test_base_illisible(self, tmp_path: Path) -> None:
        faux = tmp_path / "faux.mmdb"
        faux.write_bytes(b"pas une base")
        with pytest.raises(ValueError, match="base geoip illisible"):
            FilterPipeline([{"type": "geoip", "source": "ip", "database": str(faux)}])


class TestStepTranslate:
    @pytest.fixture
    def dico(self, tmp_path: Path) -> str:
        chemin = tmp_path / "ifName.yml"
        chemin.write_text('"1": Uplink\n', encoding="utf-8")
        return str(chemin)

    @pytest.mark.parametrize(
        ("doc", "options", "attendu"),
        [
            pytest.param({"i": "1"}, {}, "Uplink", id="trouve"),
            pytest.param({"i": "9"}, {"fallback": "index: %{i}"}, "index: 9", id="fallback-gabarit"),
            pytest.param({"i": "9"}, {"fallback": 0}, 0, id="fallback-non-textuel"),
            pytest.param({"i": None}, {"fallback": "%{i}"}, "", id="source-nulle-fallback-vide"),
        ],
    )
    def test_dans_un_pipeline_json(self, dico: str, doc: dict, options: dict, attendu) -> None:
        step = {"type": "translate", "dictionary_path": dico, "source": "i", "target": "[if][nom]", **options}
        FilterPipeline([step]).apply(doc, on_missing="empty")
        assert doc["if"]["nom"] == attendu

    def test_sans_correspondance_ni_fallback(self, dico: str) -> None:
        doc = {"i": "9"}
        TranslateFilter({"dictionary_path": dico, "source": "i", "target": "nom"}).apply(doc)
        assert doc == {"i": "9"}

    def test_step_mal_forme(self) -> None:
        with pytest.raises(ValueError, match="step translate mal formé"):
            TranslateFilter({"target": "nom"})  # sans source (un dictionnaire vide en ligne est permis, #119)

    @pytest.mark.parametrize("contenu", [None, "a: [1, 2\n"], ids=["absent", "yaml-malforme"])
    def test_dictionnaire_illisible(self, tmp_path: Path, contenu: str | None) -> None:
        chemin = tmp_path / "d.yml"
        if contenu is not None:
            chemin.write_text(contenu, encoding="utf-8")
        with pytest.raises(ValueError, match="dictionnaire translate illisible"):
            TranslateFilter({"dictionary_path": str(chemin), "source": "i", "target": "nom"})


class TestFilterPipelineComplements:
    def test_regex_vers_target_et_sans_correspondance(self) -> None:
        pipeline = FilterPipeline(
            [
                {"type": "regex", "source": "msg", "pattern": r"^(?P<mod>[A-Z]+)/(?P<sev>\d)", "target": "parsed"},
                {"type": "regex", "source": "msg", "pattern": r"^jamais$"},
                {"type": "kv", "source": "absent"},
            ]
        )
        doc = {"msg": "IFNET/4: lien up", "parsed": {"deja": 1}}
        assert pipeline.apply(doc) is True
        assert doc["parsed"] == {"deja": 1, "mod": "IFNET", "sev": "4"}

    def test_drop_sans_mots(self) -> None:
        with pytest.raises(ValueError, match="ni 'words'"):
            FilterPipeline([{"type": "drop", "source": "msg"}])

    def test_drop_mode_inconnu(self) -> None:
        with pytest.raises(ValueError, match="mode de drop inconnu"):
            DropFilter(["x"], mode="regex")

    def test_drop_egalite_sensible_a_la_casse(self) -> None:
        filtre = DropFilter(["Keepalive"], mode="equals", case_sensitive=True)
        assert filtre.matches("Keepalive") is True
        assert filtre.matches("keepalive") is False
        assert filtre.matches("Keepalive 1") is False

    def test_close_sans_geoip(self) -> None:
        FilterPipeline([{"type": "mutate", "add_tag": ["x"]}]).close()
