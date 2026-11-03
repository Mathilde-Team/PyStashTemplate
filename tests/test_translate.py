# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests des options `translate` alignées sur logstash-filter-translate (issue #119)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from pystashtemplate.enrichment import TranslateDictionary
from pystashtemplate.filters import FilterPipeline, TranslateFilter, _option_booleenne, _texte_ruby
from pystashtemplate.interpreter import PipelineInterpreter


def _conf(options_conf: str, doc: dict) -> dict:
    """Applique `filter { translate { <options_conf> } }` à `doc` et le renvoie."""
    PipelineInterpreter(f"filter {{ translate {{ {options_conf} }} }}").apply(doc)
    return doc


def _fichier(tmp_path: Path, nom: str, contenu: str) -> str:
    chemin = tmp_path / nom
    chemin.write_text(contenu, encoding="utf-8")
    return str(chemin)


class _Horloge:
    """Horloge monotone pilotée par le test."""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


class TestUtilitaires:
    @pytest.mark.parametrize(
        ("valeur", "attendu"),
        [(None, ""), (True, "true"), (False, "false"), (200, "200"), (1.5, "1.5"), ({"a": 1}, '{"a":1}'), ("é", "é")],
    )
    def test_texte_ruby(self, valeur, attendu: str) -> None:
        assert _texte_ruby(valeur) == attendu

    @pytest.mark.parametrize(
        ("valeur", "attendu"), [(True, True), ("false", False), (" TRUE ", True), ("peut-être", None), (None, None)]
    )
    def test_option_booleenne(self, valeur, attendu) -> None:
        assert _option_booleenne(valeur, None) is attendu


class TestDictionnaireFichier:
    def test_json(self, tmp_path: Path) -> None:
        d = TranslateDictionary(_fichier(tmp_path, "d.json", '\ufeff{"200": "OK", "404": "absent"}'))
        assert d.mapping == {"200": "OK", "404": "absent"}

    @pytest.mark.parametrize("contenu", ["", "  \n", "[1, 2]"])
    def test_json_vide_ou_non_objet(self, tmp_path: Path, contenu: str) -> None:
        assert TranslateDictionary(_fichier(tmp_path, "d.json", contenu)).mapping == {}

    def test_json_malforme(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            TranslateDictionary(_fichier(tmp_path, "d.json", "{"))

    def test_csv(self, tmp_path: Path) -> None:
        d = TranslateDictionary(_fichier(tmp_path, "d.CSV", '200,OK\n\n"4,04","a, b"\nseul\n'))
        assert d.mapping == {"200": "OK", "4,04": "a, b", "seul": None}

    def test_extension_inconnue_lue_en_yaml(self, tmp_path: Path) -> None:
        assert TranslateDictionary(_fichier(tmp_path, "d.dict", "a: b\n")).mapping == {"a": "b"}

    def test_recharger_merge_et_replace(self, tmp_path: Path) -> None:
        chemin = _fichier(tmp_path, "d.yml", "a: 1\nb: 2\n")
        d = TranslateDictionary(chemin)
        assert d.recharger() is False
        Path(chemin).write_text("a: 10\nc: 3\n", encoding="utf-8")
        os.utime(chemin, (1, 1))
        assert d.recharger("merge") is True
        assert (d.mapping, d.version) == ({"a": 10, "b": 2, "c": 3}, 1)
        os.utime(chemin, (2, 2))
        assert d.recharger("replace") is True
        assert d.mapping == {"a": 10, "c": 3}

    @pytest.mark.parametrize("panne", ["disparu", "malforme"])
    def test_recharger_en_echec_garde_l_ancien(self, tmp_path: Path, panne: str) -> None:
        chemin = _fichier(tmp_path, "d.yml", "a: 1\n")
        d = TranslateDictionary(chemin)
        if panne == "disparu":
            os.remove(chemin)
        else:
            Path(chemin).write_text("a: [1\n", encoding="utf-8")
            os.utime(chemin, (5, 5))
        assert d.recharger() is False
        assert (d.mapping, d.version) == ({"a": 1}, 0)


class TestOptions:
    @pytest.mark.parametrize(
        ("options", "message"),
        [
            ({}, "'source'"),
            ({"source": ""}, "chaîne non vide"),
            ({"source": "a", "field": "b"}, "exclusifs"),
            ({"source": "a", "target": "t", "destination": "d"}, "exclusifs"),
            ({"source": "a", "override": False}, "en place"),
            ({"source": "a", "refresh_behaviour": "tout"}, "refresh_behaviour"),
            ({"source": "a", "refresh_interval": "jamais"}, "refresh_interval"),
            ({"source": "a", "dictionary": ["impair"]}, "paires"),
            ({"source": "a", "dictionary": "texte"}, "paires"),
            ({"source": "a", "dictionary_path": 3}, "chaîne"),
            ({"source": "a", "dictionary": {"x": "y"}, "dictionary_path": "d.yml"}, "exclusifs"),
            ({"source": "[a"}, "crochet"),
        ],
    )
    def test_refus(self, options: dict, message: str) -> None:
        with pytest.raises(ValueError, match=message):
            TranslateFilter(options)

    def test_dictionnaire_illisible(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="illisible"):
            TranslateFilter(
                {"source": "a", "dictionary_path": _fichier(tmp_path, "d.csv", "a,b\n")}, dictionary_factory=_csv_casse
            )

    def test_anciens_noms_field_destination(self) -> None:
        doc = {"code": "200"}
        assert TranslateFilter({"field": "code", "destination": "texte", "dictionary": {"200": "OK"}}).apply(doc)
        assert doc["texte"] == "OK"

    def test_cible_par_defaut_en_place(self) -> None:
        doc = {"code": "200"}
        TranslateFilter({"source": "code", "dictionary": {"200": "OK"}}).apply(doc)
        assert doc == {"code": "OK"}

    def test_cible_par_defaut_ecs_desactive(self) -> None:
        doc = {"code": "200"}
        TranslateFilter({"source": "code", "dictionary": {"200": "OK"}, "ecs_compatibility": "disabled"}).apply(doc)
        assert doc == {"code": "200", "translation": "OK"}

    def test_dictionnaire_en_tableau_de_paires(self) -> None:
        doc = {"c": "101"}
        TranslateFilter({"source": "c", "target": "t", "dictionary": ["100", "Continue", "101", "Switching"]}).apply(
            doc
        )
        assert doc["t"] == "Switching"


def _csv_casse(chemin: str) -> TranslateDictionary:
    import csv

    raise csv.Error(f"csv cassé : {chemin}")


class TestRecherche:
    def test_cles_comparees_en_texte(self) -> None:
        filtre = TranslateFilter({"source": "c", "target": "t", "dictionary": {200: "OK", True: "vrai"}})
        doc = {"c": 200}
        assert filtre.apply(doc) and doc["t"] == "OK"
        doc = {"c": "true"}
        assert filtre.apply(doc) and doc["t"] == "vrai"

    def test_tableau_premier_element_et_tableau_vide(self) -> None:
        filtre = TranslateFilter({"source": "c", "target": "t", "dictionary": {"a": 1, "": "vide"}})
        doc = {"c": ["a", "b"]}
        filtre.apply(doc)
        assert doc["t"] == 1
        doc = {"c": []}
        filtre.apply(doc)
        assert doc["t"] == "vide"

    def test_valeur_copiee(self) -> None:
        filtre = TranslateFilter({"source": "c", "target": "t", "dictionary": {"a": {"x": [1]}}})
        premier, second = {"c": "a"}, {"c": "a"}
        filtre.apply(premier)
        premier["t"]["x"].append(2)
        filtre.apply(second)
        assert second["t"] == {"x": [1]}

    def test_regex(self) -> None:
        filtre = TranslateFilter(
            {"source": "m", "target": "t", "regex": True, "dictionary": {"(": "invalide", "^err": "erreur", "e": "e"}}
        )
        doc = {"m": "error disk"}
        assert filtre.apply(doc) and doc["t"] == "erreur"
        doc = {"m": "info"}
        assert filtre.apply(doc) is False and "t" not in doc

    def test_exact_faux_remplace_dans_la_valeur(self) -> None:
        filtre = TranslateFilter({"source": "m", "target": "t", "exact": "false", "dictionary": {"foo": "bar", "1": 2}})
        doc = {"m": "foofing 1"}
        assert filtre.apply(doc) and doc["t"] == "barfing 2"
        doc = {"m": "rien"}
        assert filtre.apply(doc) is False

    def test_exact_faux_dictionnaire_vide(self) -> None:
        assert TranslateFilter({"source": "m", "target": "t", "exact": False}).apply({"m": "x"}) is False

    def test_regex_ignore_si_exact_faux(self) -> None:
        doc = {"m": "a.c abc"}
        TranslateFilter(
            {"source": "m", "target": "t", "exact": False, "regex": True, "dictionary": {"a.c": "X"}}
        ).apply(doc)
        assert doc["t"] == "X abc"


class TestOverride:
    def test_cible_existante_non_reecrite(self) -> None:
        doc = {"c": "a", "t": "garde"}
        assert TranslateFilter({"source": "c", "target": "t", "dictionary": {"a": 1}}).apply(doc) is False
        assert doc["t"] == "garde"

    def test_override_vrai(self) -> None:
        doc = {"c": "a", "t": "garde"}
        assert TranslateFilter({"source": "c", "target": "t", "override": "true", "dictionary": {"a": 1}}).apply(doc)
        assert doc["t"] == 1

    def test_en_place_sans_correspondance_compte_comme_reussi(self) -> None:
        doc = {"c": "z"}
        assert TranslateFilter({"source": "c", "dictionary": {"a": 1}}).apply(doc) is True
        assert doc == {"c": "z"}

    def test_source_absente(self) -> None:
        assert TranslateFilter({"source": "c", "dictionary": {"a": 1}, "fallback": "x"}).apply({}) is False


class TestIterateOn:
    def test_tableau_de_valeurs(self) -> None:
        options = {"source": "codes", "iterate_on": "codes", "target": "noms", "dictionary": {"1": "un", "2": "deux"}}
        doc = {"codes": [1, "2", 3]}
        assert TranslateFilter(options).apply(doc)
        assert doc["noms"] == ["un", "deux", None]

    def test_tableau_de_valeurs_repli_et_scalaire(self) -> None:
        options = {
            "source": "c",
            "iterate_on": "c",
            "target": "n",
            "dictionary": {"1": "un"},
            "fallback": "?%{x}",
        }
        doc = {"c": "9", "x": "!"}
        assert TranslateFilter(options).apply(doc) and doc["n"] == ["?!"]
        doc = {"c": None}
        assert TranslateFilter(options).apply(doc) is False and doc["n"] == []

    def test_tableau_de_valeurs_cible_occupee_ou_source_absente(self) -> None:
        options = {"source": "c", "iterate_on": "c", "target": "n", "dictionary": {"1": "un"}}
        assert TranslateFilter(options).apply({"c": ["1"], "n": 0}) is False
        assert TranslateFilter(options).apply({}) is False

    def test_tableau_d_objets(self) -> None:
        options = {"iterate_on": "[ifs]", "source": "[id]", "target": "[nom]", "dictionary": {"1": "uplink"}}
        doc = {"ifs": [{"id": 1}, {"id": 2}, {"autre": 3}, {"id": None}, "texte", {"id": 1, "nom": "x"}]}
        assert TranslateFilter(options).apply(doc)
        assert doc["ifs"] == [
            {"id": 1, "nom": "uplink"},
            {"id": 2},
            {"autre": 3},
            {"id": None},
            "texte",
            {"id": 1, "nom": "uplink"},
        ]

    def test_tableau_d_objets_repli_objet_seul_et_absent(self) -> None:
        options = {"iterate_on": "ifs", "source": "id", "target": "nom", "dictionary": {}, "fallback": "inconnu %{h}"}
        doc = {"h": "sw1", "ifs": {"id": 7}}
        assert TranslateFilter(options).apply(doc)
        assert doc["ifs"] == {"id": 7, "nom": "inconnu sw1"}
        assert TranslateFilter(options).apply({"ifs": None}) is False
        assert TranslateFilter(options).apply({}) is False


class TestRafraichissement:
    def test_relu_apres_intervalle(self, tmp_path: Path) -> None:
        chemin = _fichier(tmp_path, "d.json", '{"a": "1"}')
        horloge = _Horloge()
        filtre = TranslateFilter(
            {
                "source": "c",
                "target": "t",
                "dictionary_path": chemin,
                "refresh_interval": 10,
                "refresh_behaviour": "replace",
            },
            clock=horloge,
        )
        Path(chemin).write_text('{"b": "2"}', encoding="utf-8")
        os.utime(chemin, (1, 1))
        horloge.t = 5
        doc = {"c": "a"}
        filtre.apply(doc)
        assert doc["t"] == "1"
        horloge.t = 10
        doc = {"c": "a"}
        assert filtre.apply(doc) is False
        doc = {"c": "b"}
        filtre.apply(doc)
        assert doc["t"] == "2"
        horloge.t = 30
        filtre.apply({"c": "b"})

    def test_intervalle_nul_desactive(self, tmp_path: Path) -> None:
        chemin = _fichier(tmp_path, "d.yml", "a: 1\n")
        horloge = _Horloge()
        filtre = TranslateFilter(
            {"source": "c", "target": "t", "dictionary_path": chemin, "refresh_interval": 0}, clock=horloge
        )
        Path(chemin).write_text("a: 2\n", encoding="utf-8")
        os.utime(chemin, (1, 1))
        horloge.t = 1000
        doc = {"c": "a"}
        filtre.apply(doc)
        assert doc["t"] == 1


class TestDansUnConf:
    def test_dictionnaire_en_ligne_regex_et_options_communes(self) -> None:
        doc = _conf(
            'source => "m" target => "niveau" regex => true dictionary => { "^E" => "erreur" "^W" => "alerte" } '
            'add_tag => ["traduit"]',
            {"m": "Error disque"},
        )
        assert (doc["niveau"], doc["tags"]) == ("erreur", ["traduit"])

    def test_override_dans_un_conf(self, tmp_path: Path) -> None:
        chemin = _fichier(tmp_path, "d.csv", "eth0,Uplink\n")
        conf = f'source => "i" target => "n" dictionary_path => "{chemin}"'
        assert _conf(conf, {"i": "eth0", "n": "x"})["n"] == "x"
        assert _conf(conf + " override => true", {"i": "eth0", "n": "x"})["n"] == "Uplink"

    def test_bloc_incoherent_ignore_et_mis_en_cache(self) -> None:
        interp = PipelineInterpreter('filter { translate { source => "a" field => "b" } }')
        for _ in range(2):
            doc = {"a": "1"}
            interp.apply(doc)
            assert doc == {"a": "1"}
        assert list(interp._translate_filters.values()) == [None]

    def test_dictionnaire_partage_par_chemin(self, tmp_path: Path) -> None:
        chemin = _fichier(tmp_path, "d.yml", "a: 1\n")
        interp = PipelineInterpreter(
            f'filter {{ translate {{ source => "c" target => "t1" dictionary_path => "{chemin}" }} '
            f'translate {{ source => "c" target => "t2" dictionary_path => "{chemin}" }} }}'
        )
        doc = {"c": "a"}
        interp.apply(doc)
        assert (doc["t1"], doc["t2"]) == (1, 1)
        assert len(interp._translate_dictionaries) == 1 and len(interp._translate_filters) == 2

    def test_step_filters_json(self) -> None:
        doc = {"c": "a"}
        FilterPipeline([{"type": "translate", "source": "c", "dictionary": {"a": "A"}}]).apply(doc)
        assert doc == {"c": "A"}
