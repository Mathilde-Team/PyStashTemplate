# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests des plugins `filter{}` enrichis de `src/interpreter.py` (issue #73).

`translate`, `mmdb`, `geoip`, `filters_json` et `date`, écrits en vrai
pipeline `.conf` et appliqués par `PipelineInterpreter` : fichiers YAML et
JSON réels dans `tmp_path`, bases MaxMind réelles générées par
`conftest.py` (aucun double pour le chemin heureux), plus les erreurs de
configuration (plugin mal formé, fichier illisible) qui doivent être ignorées
sans faire échouer le document.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import ENREGISTREMENT_PERSO, IP_INCONNUE, IP_STRASBOURG

from pystashtemplate.interpreter import PipelineInterpreter


def _appliquer(conf: str, doc: dict, **kwargs) -> tuple[dict, list | None]:
    interp = PipelineInterpreter(conf)
    try:
        resultat = interp.apply(doc, **kwargs)
    finally:
        interp.close()
    return doc, resultat


# ───────────────────────────── translate ─────────────────────────────


@pytest.fixture
def dico_ifname(tmp_path: Path) -> str:
    chemin = tmp_path / "ifName.yml"
    chemin.write_text('"1": Uplink\n"2": Lan\n', encoding="utf-8")
    return str(chemin)


class TestTranslate:
    def test_cle_trouvee_ecrite_a_target(self, dico_ifname: str) -> None:
        conf = f'''filter {{ translate {{
            dictionary_path => "{dico_ifname}"
            source => "[ifTable][index]"
            target => "[ifTable][name]"
        }} }}'''
        doc, _ = _appliquer(conf, {"ifTable": {"index": "1"}})
        assert doc == {"ifTable": {"index": "1", "name": "Uplink"}}

    def test_fallback_gabarit_rendu(self, dico_ifname: str) -> None:
        """Comme le pipeline réel : `fallback => "index: %{[ifTable][index]}"`."""
        conf = f'''filter {{ translate {{
            dictionary_path => "{dico_ifname}"
            source => "[ifTable][index]"
            target => "[ifTable][name]"
            fallback => "index: %{{[ifTable][index]}}"
        }} }}'''
        doc, _ = _appliquer(conf, {"ifTable": {"index": "9"}})
        assert doc["ifTable"]["name"] == "index: 9"

    def test_fallback_non_textuel_ecrit_tel_quel(self, dico_ifname: str) -> None:
        conf = f'''filter {{ translate {{
            dictionary_path => "{dico_ifname}" source => "idx" target => "nom" fallback => 0
        }} }}'''
        doc, _ = _appliquer(conf, {"idx": "9"})
        assert doc["nom"] == 0

    @pytest.mark.parametrize(
        "doc",
        [pytest.param({"idx": "9"}, id="cle-inconnue"), pytest.param({"autre": 1}, id="source-absente")],
    )
    def test_sans_correspondance_ni_fallback_inchange(self, dico_ifname: str, doc: dict) -> None:
        avant = dict(doc)
        conf = f'filter {{ translate {{ dictionary_path => "{dico_ifname}" source => "idx" target => "nom" }} }}'
        assert _appliquer(conf, doc)[0] == avant

    def test_dictionnaire_charge_une_seule_fois(self, dico_ifname: str) -> None:
        conf = f'filter {{ translate {{ dictionary_path => "{dico_ifname}" source => "idx" target => "nom" }} }}'
        interp = PipelineInterpreter(conf)
        chargements: list[str] = []
        fabrique = interp._translate_dictionary_factory

        def compter(chemin: str):
            chargements.append(chemin)
            return fabrique(chemin)

        interp._translate_dictionary_factory = compter
        for idx in ("1", "2"):
            doc = {"idx": idx}
            interp.apply(doc)
        assert doc["nom"] == "Lan"
        assert chargements == [dico_ifname]

    def test_plugin_mal_forme_ignore(self) -> None:
        doc, actions = _appliquer('filter { translate { source => "idx" target => "nom" } }', {"idx": "1"})
        assert (doc, actions) == ({"idx": "1"}, [])

    @pytest.mark.parametrize("contenu", [None, "a: [1, 2\n"], ids=["absent", "yaml-malforme"])
    def test_dictionnaire_illisible_ignore(self, tmp_path: Path, contenu: str | None) -> None:
        chemin = tmp_path / "d.yml"
        if contenu is not None:
            chemin.write_text(contenu, encoding="utf-8")
        conf = f'filter {{ translate {{ dictionary_path => "{chemin}" source => "idx" target => "nom" }} }}'
        assert _appliquer(conf, {"idx": "1"})[0] == {"idx": "1"}


# ───────────────────────────── mmdb ─────────────────────────────


class TestMmdb:
    def test_enregistrement_fusionne_sous_target(self, base_mmdb_perso: str) -> None:
        """Fusion champ par champ : [host][ip] posé avant est conservé (pipeline réel)."""
        conf = f'filter {{ mmdb {{ source => "[host][ip]" database => "{base_mmdb_perso}" target => "host" }} }}'
        doc, _ = _appliquer(conf, {"host": {"ip": IP_STRASBOURG, "domain": "sw1.lan"}})
        assert doc == {"host": {"ip": IP_STRASBOURG, "domain": "sw1.lan", **ENREGISTREMENT_PERSO}}

    @pytest.mark.parametrize(
        "doc",
        [
            pytest.param({"ip": IP_INCONNUE}, id="ip-absente-de-la-base"),
            pytest.param({"ip": 42}, id="ip-non-textuelle"),
            pytest.param({"autre": 1}, id="source-absente"),
        ],
    )
    def test_sans_resultat_inchange(self, base_mmdb_perso: str, doc: dict) -> None:
        avant = json.loads(json.dumps(doc))
        conf = f'filter {{ mmdb {{ source => "ip" database => "{base_mmdb_perso}" target => "geo" }} }}'
        assert _appliquer(conf, doc)[0] == avant

    def test_plugin_mal_forme_ignore(self) -> None:
        conf = 'filter { mmdb { source => "ip" target => "geo" } }'
        assert _appliquer(conf, {"ip": IP_STRASBOURG})[0] == {"ip": IP_STRASBOURG}

    @pytest.mark.parametrize("contenu", [None, b"pas une base"], ids=["absente", "invalide"])
    def test_base_illisible_ignoree(self, tmp_path: Path, contenu: bytes | None) -> None:
        chemin = tmp_path / "x.mmdb"
        if contenu is not None:
            chemin.write_bytes(contenu)
        conf = f'filter {{ mmdb {{ source => "ip" database => "{chemin}" target => "geo" }} }}'
        assert _appliquer(conf, {"ip": IP_STRASBOURG})[0] == {"ip": IP_STRASBOURG}

    def test_base_ouverte_une_fois_et_fermee(self, base_mmdb_perso: str) -> None:
        conf = f'filter {{ mmdb {{ source => "ip" database => "{base_mmdb_perso}" target => "geo" }} }}'
        interp = PipelineInterpreter(conf)
        for _ in range(2):
            interp.apply({"ip": IP_STRASBOURG})
        assert list(interp._mmdb_lookups) == [base_mmdb_perso]
        interp.close()
        assert interp._mmdb_lookups == {}


# ───────────────────────────── geoip ─────────────────────────────


class TestGeoip:
    def test_target_par_defaut_geoip(self, base_geoip_city: str) -> None:
        conf = f'filter {{ geoip {{ source => "client" database => "{base_geoip_city}" }} }}'
        doc, _ = _appliquer(conf, {"client": IP_STRASBOURG})
        assert doc["geoip"]["city_name"] == "Strasbourg"
        assert doc["geoip"]["location"] == [7.75, 48.58]

    def test_fields_restreint_dans_l_ordre_de_l_enregistrement(self, base_geoip_city: str) -> None:
        conf = f'''filter {{ geoip {{
            source => "client" database => "{base_geoip_city}" target => "[host][geo]"
            fields => ["timezone", "city_name", "postal_code_inexistant"]
        }} }}'''
        doc, _ = _appliquer(conf, {"client": IP_STRASBOURG})
        assert list(doc["host"]["geo"].items()) == [("city_name", "Strasbourg"), ("timezone", "Europe/Paris")]

    def test_fields_vide_aucune_restriction(self, base_geoip_city: str) -> None:
        conf = f'filter {{ geoip {{ source => "c" database => "{base_geoip_city}" fields => [] }} }}'
        doc, _ = _appliquer(conf, {"c": IP_STRASBOURG})
        assert len(doc["geoip"]) == 11

    @pytest.mark.parametrize(
        "doc",
        [pytest.param({"c": IP_INCONNUE}, id="sans-correspondance"), pytest.param({"autre": 1}, id="ip-absente")],
    )
    def test_echec_tag_par_defaut(self, base_geoip_city: str, doc: dict) -> None:
        conf = f'filter {{ geoip {{ source => "c" database => "{base_geoip_city}" }} }}'
        doc, _ = _appliquer(conf, doc)
        assert doc["tags"] == ["_geoip_lookup_failure"]
        assert "geoip" not in doc

    def test_echec_tag_personnalise_sans_doublon(self, base_geoip_city: str) -> None:
        conf = f'''filter {{ geoip {{
            source => "c" database => "{base_geoip_city}" tag_on_failure => ["geo_ko"]
        }} }}'''
        doc, _ = _appliquer(conf, {"c": IP_INCONNUE, "tags": ["geo_ko"]})
        assert doc["tags"] == ["geo_ko"]

    def test_mal_forme_ou_base_illisible_sans_tag(self, tmp_path: Path) -> None:
        """Erreurs de configuration : ignorées SANS tag (distinctes d'un échec par document)."""
        for conf in (
            'filter { geoip { source => "c" } }',
            f'filter {{ geoip {{ source => "c" database => "{tmp_path / "absente.mmdb"}" }} }}',
        ):
            assert _appliquer(conf, {"c": IP_STRASBOURG})[0] == {"c": IP_STRASBOURG}

    def test_base_ouverte_une_fois_et_fermee(self, base_geoip_city: str) -> None:
        conf = f'filter {{ geoip {{ source => "c" database => "{base_geoip_city}" }} }}'
        interp = PipelineInterpreter(conf)
        for _ in range(2):
            interp.apply({"c": IP_STRASBOURG})
        assert list(interp._geoip_lookups) == [base_geoip_city]
        interp.close()
        assert interp._geoip_lookups == {}


# ───────────────────────────── filters_json ─────────────────────────────


def _filtres(tmp_path: Path, steps: list | str, nom: str = "filtres.json") -> str:
    chemin = tmp_path / nom
    chemin.write_text(steps if isinstance(steps, str) else json.dumps(steps), encoding="utf-8")
    return str(chemin)


class TestFiltersJson:
    def test_chaine_appliquee_en_place(self, tmp_path: Path) -> None:
        chemin = _filtres(tmp_path, [{"type": "kv", "source": "detail", "prefix": "kv_"}])
        conf = f'filter {{ filters_json {{ path => "{chemin}" }} }} output {{ stdout {{ }} }}'
        doc, actions = _appliquer(conf, {"detail": "user=admin action=login"})
        assert doc["kv_user"] == "admin" and doc["kv_action"] == "login"
        assert actions is not None and [a.plugin for a in actions] == ["stdout"]

    def test_drop_ecarte_tout_le_document(self, tmp_path: Path) -> None:
        """Drop dans une branche imbriquée : apply() renvoie None, l'output n'est pas évalué."""
        chemin = _filtres(tmp_path, [{"type": "drop", "source": "msg", "words": ["keepalive"]}])
        conf = f"""
        filter {{ if [msg] {{ if [msg] =~ /keep/ {{ filters_json {{ path => "{chemin}" }} }} }} }}
        output {{ stdout {{ }} }}
        """
        assert _appliquer(conf, {"msg": "keepalive port 1"})[1] is None
        assert _appliquer(conf, {"msg": "lien up"})[1] != []

    def test_on_missing_propre_au_fichier(self, tmp_path: Path) -> None:
        chemin = _filtres(tmp_path, [{"type": "mutate", "add_field": {"x": "%{absent}"}}])
        conf = f'filter {{ filters_json {{ path => "{chemin}" on_missing => "empty" }} }}'
        assert _appliquer(conf, {}, on_missing="keep")[0] == {"x": ""}

    def test_pipeline_compile_une_seule_fois(self, tmp_path: Path) -> None:
        chemin = _filtres(tmp_path, [{"type": "mutate", "add_tag": ["vu"]}])
        interp = PipelineInterpreter(f'filter {{ filters_json {{ path => "{chemin}" }} }}')
        interp.apply({})
        compile_ = interp._filter_pipelines[chemin]
        Path(chemin).write_text("pas du JSON", encoding="utf-8")  # plus relu
        doc: dict = {}
        interp.apply(doc)
        assert interp._filter_pipelines[chemin] is compile_
        assert doc["tags"] == ["vu"]

    @pytest.mark.parametrize(
        "contenu",
        [
            pytest.param(None, id="fichier-absent"),
            pytest.param("{pas du json", id="json-invalide"),
            pytest.param([{"type": "inconnu", "source": "a"}], id="type-inconnu"),
            pytest.param([{"type": "dissect"}], id="cle-requise-manquante"),
        ],
    )
    def test_fichier_invalide_ignore(self, tmp_path: Path, contenu) -> None:
        chemin = str(tmp_path / "absent.json") if contenu is None else _filtres(tmp_path, contenu)
        conf = f'filter {{ filters_json {{ path => "{chemin}" }} }} output {{ stdout {{ }} }}'
        doc, actions = _appliquer(conf, {"a": "1"})
        assert doc == {"a": "1"}
        assert actions is not None and len(actions) == 1

    def test_sans_path_ignore(self) -> None:
        assert _appliquer("filter { filters_json { } }", {"a": 1}) == ({"a": 1}, [])

    def test_close_ferme_les_bases_des_fichiers_filters_json(self, tmp_path: Path, base_geoip_city: str) -> None:
        """Un step geoip d'un filters_json ouvre sa base : close() doit la fermer aussi."""
        chemin = _filtres(tmp_path, [{"type": "geoip", "source": "c", "database": base_geoip_city}])
        interp = PipelineInterpreter(f'filter {{ filters_json {{ path => "{chemin}" }} }}')
        doc = {"c": IP_STRASBOURG}
        interp.apply(doc)
        assert doc["geoip"]["city_name"] == "Strasbourg"
        fermes: list[str] = []
        pipeline = interp._filter_pipelines[chemin]
        fermer = pipeline.close

        def suivre() -> None:
            fermes.append(chemin)
            fermer()

        pipeline.close = suivre
        interp.close()
        assert fermes == [chemin]
        assert interp._filter_pipelines == {}


# ───────────────────────────── date ─────────────────────────────


class TestDate:
    @pytest.mark.parametrize(
        ("valeur", "motifs", "attendu"),
        [
            pytest.param("2026-10-22T08:30:00Z", '"ISO8601"', "2026-10-22T08:30:00+00:00", id="iso8601-z"),
            pytest.param("2026-10-22T10:30:00+02:00", '"ISO8601"', "2026-10-22T08:30:00+00:00", id="iso8601-decalage"),
            pytest.param("1792657800", '"UNIX"', "2026-10-22T08:30:00+00:00", id="unix"),
            pytest.param(1792657800500, '"UNIX_MS"', "2026-10-22T08:30:00.500000+00:00", id="unix-ms"),
            pytest.param("22/Oct/2026:10:30:00 +0200", '"dd/MMM/yyyy:HH:mm:ss Z"', "2026-10-22T08:30:00+00:00",
                         id="joda-avec-fuseau"),
            pytest.param("2026-10-22 08:30:00", '"yyyy-MM-dd HH:mm:ss"', "2026-10-22T08:30:00+00:00",
                         id="joda-sans-fuseau-utc"),
            pytest.param("2026-10-22T08:30", "\"yyyy-MM-dd'T'HH:mm\"", "2026-10-22T08:30:00+00:00",
                         id="litteral-entre-quotes"),
            pytest.param("Thursday 22 October 26 08:30 AM", '"EEEE dd MMMM yy hh:mm a"',
                         "2026-10-22T08:30:00+00:00", id="noms-longs-12h"),
            pytest.param("Thu 22 10 2026 o'clock", "\"EEE dd MM yyyy 'o''clock'\"", "2026-10-22T00:00:00+00:00",
                         id="quote-litterale"),
            pytest.param("2026%10%22 'h'", "\"yyyy%MM'%'dd '''h'''\"", "2026-10-22T00:00:00+00:00",
                         id="pourcent-et-quotes-isolees"),
        ],
    )  # fmt: skip
    def test_motifs_reconnus(self, valeur, motifs: str, attendu: str) -> None:
        conf = f'filter {{ date {{ match => ["ts", {motifs}] }} }}'
        doc, _ = _appliquer(conf, {"ts": valeur})
        assert doc["@timestamp"] == attendu

    def test_premier_motif_qui_correspond_target_et_timezone(self) -> None:
        conf = """filter { date {
            match => ["[log][ts]", "pas_un_jeton_Q", "'non refermé", "UNIX", "yyyy-MM-dd HH:mm:ss"]
            timezone => "Europe/Paris"
            target => "[event][created]"
        } }"""
        doc, _ = _appliquer(conf, {"log": {"ts": "2026-10-22 10:30:00"}})
        assert doc["event"]["created"] == "2026-10-22T08:30:00+00:00"
        assert "tags" not in doc

    def test_timezone_inconnu_retombe_sur_utc(self) -> None:
        conf = 'filter { date { match => ["ts", "yyyy-MM-dd HH:mm:ss"] timezone => "Europe/Atlantide" } }'
        doc, _ = _appliquer(conf, {"ts": "2026-10-22 08:30:00"})
        assert doc["@timestamp"] == "2026-10-22T08:30:00+00:00"

    @pytest.mark.parametrize(
        ("doc", "conf_options"),
        [
            pytest.param({"ts": "pas une date"}, '"ISO8601", "UNIX"', id="aucun-motif"),
            pytest.param({"ts": "1e400"}, '"UNIX"', id="hors-limites"),
            pytest.param({}, '"ISO8601"', id="champ-absent"),
            pytest.param({"ts": None}, '"ISO8601"', id="champ-null"),
        ],
    )
    def test_echec_tag_dateparsefailure(self, doc: dict, conf_options: str) -> None:
        conf = f'filter {{ date {{ match => ["ts", {conf_options}] }} }}'
        doc, _ = _appliquer(conf, doc)
        assert doc["tags"] == ["_dateparsefailure"]
        assert "@timestamp" not in doc

    def test_echec_tag_personnalise(self) -> None:
        conf = 'filter { date { match => ["ts", "ISO8601"] tag_on_failure => ["date_ko", "a_revoir"] } }'
        assert _appliquer(conf, {"ts": "x"})[0]["tags"] == ["date_ko", "a_revoir"]

    @pytest.mark.parametrize(
        "match",
        ['["ts"]', '"ts"', '[1, "ISO8601"]'],
        ids=["un-seul-element", "pas-une-liste", "champ-non-textuel"],
    )
    def test_match_invalide_ignore(self, match: str) -> None:
        conf = f"filter {{ date {{ match => {match} }} }}"
        assert _appliquer(conf, {"ts": "2026-10-22T08:30:00Z"})[0] == {"ts": "2026-10-22T08:30:00Z"}

    def test_motif_non_textuel_saute(self) -> None:
        conf = 'filter { date { match => ["ts", 42, "ISO8601"] } }'
        assert _appliquer(conf, {"ts": "2026-10-22T08:30:00Z"})[0]["@timestamp"] == "2026-10-22T08:30:00+00:00"


class TestDropNatif:
    """`drop {}` natif d'un `.conf` (#34) : même effet qu'un step `"drop"` de `filters_json`."""

    def test_drop_sous_condition_ecarte_le_document(self) -> None:
        conf = 'filter { if [x] == "bruit" { drop { } } }'
        doc, actions = _appliquer(conf, {"x": "bruit"})
        assert actions is None

    def test_condition_fausse_conserve_le_document(self) -> None:
        conf = 'filter { if [x] == "bruit" { drop { } } mutate { add_field => { "vu" => "oui" } } }'
        doc, actions = _appliquer(conf, {"x": "utile"})
        assert actions == []
        assert doc["vu"] == "oui"

    def test_drop_interrompt_les_etapes_suivantes_et_output(self) -> None:
        conf = 'filter { drop { } mutate { add_field => { "vu" => "oui" } } } output { stdout { } }'
        doc, actions = _appliquer(conf, {"x": 1})
        assert actions is None
        assert "vu" not in doc

    @pytest.mark.parametrize(("tirage", "attendu_ecarte"), [(0.10, True), (0.49, True), (0.50, False), (0.99, False)])
    def test_percentage_ecarte_une_part_aleatoire(self, monkeypatch, tirage: float, attendu_ecarte: bool) -> None:
        monkeypatch.setattr("interpreter.random.random", lambda: tirage)
        _, actions = _appliquer("filter { drop { percentage => 50 } }", {"x": 1})
        assert (actions is None) is attendu_ecarte

    @pytest.mark.parametrize("valeur", ['"100"', "100", '"pas un nombre"'])
    def test_percentage_100_ou_invalide_ecarte_toujours(self, monkeypatch, valeur: str) -> None:
        monkeypatch.setattr("interpreter.random.random", lambda: 0.999)
        _, actions = _appliquer(f"filter {{ drop {{ percentage => {valeur} }} }}", {"x": 1})
        assert actions is None
