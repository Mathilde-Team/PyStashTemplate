# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de `src/grok.py` et du plugin `grok {}` natif des `.conf` (issue #112)."""

from __future__ import annotations

import asyncio
import warnings

import pytest

from pystashtemplate.grok import (
    BASE_PATTERNS,
    Bibliotheque,
    GrokFilter,
    convertir,
    lire_dossiers_de_motifs,
    traduire_oniguruma,
)
from pystashtemplate.interpreter import PipelineInterpreter


def _run(coro):
    # Sans pytest-asyncio (convention du dépôt, cf. RsysLogstach) : asyncio.run direct (#34).
    return asyncio.run(coro)


LIGNE_SYSLOG = "Nov  3 12:00:01 sw1 sshd[123]: Accepted password for bob"


def _grok(options_conf: str, doc: dict) -> dict:
    """Applique `filter { grok { <options_conf> } }` à `doc` et le renvoie."""
    _run(PipelineInterpreter(f"filter {{ grok {{ {options_conf} }} }}").apply(doc))
    return doc


class TestTraductionOniguruma:
    @pytest.mark.parametrize(
        ("source", "attendu"),
        [
            ("(/[[[:alnum:]]_%-]*)+", "(/[a-zA-Z0-9_%-]*)+"),
            ("[[:alnum:]]", "[a-zA-Z0-9]"),
            ("[[:digit:]_]", "[0-9_]"),
            ("[[:space:][:upper:]]", r"[\sA-Z]"),
            (r"fin\z", r"fin\Z"),
            ("(?<=a)b", "(?<=a)b"),
        ],
    )
    def test_traduction(self, source: str, attendu: str) -> None:
        assert traduire_oniguruma(source) == attendu


class TestConversion:
    @pytest.mark.parametrize(
        ("valeur", "type_", "attendu"),
        [
            ("42", "int", 42),
            ("12.5", "int", 12),
            (" -3x", "int", -3),
            ("abc", "int", 0),
            ("1.5e3", "float", 1500.0),
            (".5", "float", 0.5),
            ("x", "float", 0.0),
            ("42", None, "42"),
            ("42", "string", "42"),
        ],
    )
    def test_comme_ruby(self, valeur: str, type_: str | None, attendu: object) -> None:
        resultat = convertir(valeur, type_)
        assert resultat == attendu and type(resultat) is type(attendu)


class TestDossiersDeMotifs:
    def test_lecture(self, tmp_path) -> None:
        (tmp_path / "a").write_text("# commentaire\n\nPORT_SW \\d{1,5}\n  VLAN vlan%{INT}\nmal-forme\n")
        (tmp_path / "b.txt").write_text("PORT_SW [0-9]+\n")
        (tmp_path / "sous-dossier").mkdir()
        (tmp_path / "illisible").write_bytes(b"\xff\xfe\x00")
        motifs = lire_dossiers_de_motifs([str(tmp_path), str(tmp_path / "absent")])
        assert motifs == {"PORT_SW": "[0-9]+", "VLAN": "vlan%{INT}"}

    def test_glob(self, tmp_path) -> None:
        (tmp_path / "a.grok").write_text("A x\n")
        (tmp_path / "b.txt").write_text("B y\n")
        assert lire_dossiers_de_motifs([str(tmp_path)], "*.grok") == {"A": "x"}


class TestBibliotheque:
    def test_tous_les_motifs_de_base_compilent_sans_avertissement(self) -> None:
        bibliotheque = Bibliotheque()
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            for nom in BASE_PATTERNS:
                bibliotheque.compiler(f"%{{{nom}}}")
        assert len(BASE_PATTERNS) == 70

    @pytest.mark.parametrize(
        ("motif", "texte", "attendu"),
        [
            ("%{IP:ip}", "de 10.0.0.1 vers", "10.0.0.1"),
            ("%{IPV6:ip}", "fe80::1 lien", "fe80::1"),
            ("%{NUMBER:n}", "taille -12.5 Mo", "-12.5"),
            ("%{TIMESTAMP_ISO8601:t}", "le 2026-11-03T10:00:00Z", "2026-11-03T10:00:00Z"),
            ("%{QUOTEDSTRING:q}", 'dit "bonjour \\"toi\\"" fin', '"bonjour \\"toi\\""'),
            ("%{MAC:m}", "port 00:1a:2b:3c:4d:5e", "00:1a:2b:3c:4d:5e"),
            ("%{UNIXPATH:p}", "cd /var/log/x.log", "/var/log/x.log"),
            ("%{LOGLEVEL:l}", "[WARN] disque", "WARN"),
            ("%{HTTPDATE:d}", "[03/Nov/2026:10:00:00 +0100]", "03/Nov/2026:10:00:00 +0100"),
        ],
    )
    def test_motifs_usuels(self, motif: str, texte: str, attendu: str) -> None:
        compile_ = Bibliotheque().compiler(motif)
        assert compile_.regex.search(texte).group("g0") == attendu

    def test_captures_imbriquees_de_syslogbase(self) -> None:
        compile_ = Bibliotheque().compiler("%{SYSLOGBASE}")
        assert [c.champ for c in compile_.captures.values()] == [
            "timestamp",
            "facility",
            "priority",
            "logsource",
            "program",
            "pid",
        ]

    def test_groupe_nomme_oniguruma_et_type(self) -> None:
        compile_ = Bibliotheque().compiler(r"(?<pid>\d+) %{WORD:[a][b]} %{INT:n:int} %{WORD:x:inconnu}")
        captures = list(compile_.captures.values())
        assert [(c.champ, c.chemin, c.type) for c in captures] == [
            ("pid", ("pid",), None),
            ("[a][b]", ("a", "b"), None),
            ("n", ("n",), "int"),
            ("x", ("x",), "inconnu"),
        ]

    def test_deux_points_hors_type(self) -> None:
        compile_ = Bibliotheque().compiler("%{WORD:a:1}")
        assert list(compile_.captures.values())[0].champ == "a:1"

    @pytest.mark.parametrize(
        ("supplements", "motif", "message"),
        [
            ({}, "%{INCONNU}", "inconnu"),
            ({"A": "%{B}", "B": "%{A}"}, "%{A}", "récursif"),
            ({}, "(", "invalide"),
            ({}, "%{WORD:[a}", "crochet"),
        ],
    )
    def test_erreurs(self, supplements: dict, motif: str, message: str) -> None:
        with pytest.raises(ValueError, match=message):
            Bibliotheque(supplements).compiler(motif)


class TestPluginGrok:
    def test_syslogbase(self) -> None:
        doc = _grok('match => { "message" => "%{SYSLOGBASE} %{GREEDYDATA:texte}" }', {"message": LIGNE_SYSLOG})
        assert doc == {
            "message": LIGNE_SYSLOG,
            "timestamp": "Nov  3 12:00:01",
            "logsource": "sw1",
            "program": "sshd",
            "pid": "123",
            "texte": "Accepted password for bob",
        }

    def test_antislashs_du_conf_et_conversions(self) -> None:
        doc = _grok(
            r'match => { "message" => "\[%{POSINT:pid:int}\] %{NUMBER:charge:float} %{IP:[client][ip]}" }',
            {"message": "[42] 0.75 192.168.1.10"},
        )
        assert doc["pid"] == 42 and doc["charge"] == 0.75
        assert doc["client"] == {"ip": "192.168.1.10"}

    def test_break_on_match_par_defaut(self) -> None:
        doc = _grok('match => { "message" => ["%{WORD:a}", "%{INT:b}"] }', {"message": "mot 12"})
        assert doc["a"] == "mot" and "b" not in doc

    def test_break_on_match_faux(self) -> None:
        doc = _grok(
            'match => { "message" => ["%{WORD:a}", "%{INT:b}"] "autre" => "%{WORD:c}" } break_on_match => false',
            {"message": "mot 12", "autre": "x"},
        )
        assert (doc["a"], doc["b"], doc["c"]) == ("mot", "12", "x")

    def test_break_on_match_en_texte(self) -> None:
        doc = _grok('match => { "message" => ["%{WORD:a}", "%{INT:b}"] } break_on_match => "false"', {"message": "m 1"})
        assert doc["b"] == "1"

    def test_arret_entre_champs(self) -> None:
        doc = _grok('match => { "m1" => "%{WORD:a}" "m2" => "%{WORD:b}" }', {"m1": "x", "m2": "y"})
        assert "b" not in doc

    def test_champ_suivant_si_le_premier_echoue(self) -> None:
        doc = _grok('match => { "m1" => "%{INT:a}" "m2" => "%{WORD:b}" }', {"m1": "x", "m2": "y"})
        assert doc["b"] == "y" and "tags" not in doc

    def test_overwrite(self) -> None:
        doc = _grok(
            'match => { "message" => "%{WORD:niveau}: %{GREEDYDATA:message}" } overwrite => ["message"]',
            {"message": "ERREUR: disque plein"},
        )
        assert doc["message"] == "disque plein"

    def test_sans_overwrite_texte_existant_en_tableau(self) -> None:
        doc = _grok('match => { "message" => "%{WORD:message}" }', {"message": "un deux"})
        assert doc["message"] == ["un deux", "un"]

    def test_sans_overwrite_ajout_a_un_tableau(self) -> None:
        doc = _grok('match => { "message" => "%{WORD:tags}" }', {"message": "x", "tags": ["a"]})
        assert doc["tags"] == ["a", "x"]

    def test_sans_overwrite_valeur_non_textuelle_inchangee(self) -> None:
        doc = _grok('match => { "message" => "%{INT:n:int}" }', {"message": "5", "n": 1})
        assert doc["n"] == 1

    def test_champ_existant_nul_remplace(self) -> None:
        doc = _grok('match => { "message" => "%{WORD:a}" }', {"message": "x", "a": None})
        assert doc["a"] == "x"

    @pytest.mark.parametrize(
        ("doc", "options", "tags"),
        [
            ({"message": "!!"}, "", ["_grokparsefailure"]),
            ({}, "", ["_grokparsefailure"]),
            ({"message": None}, "", ["_grokparsefailure"]),
            ({"message": "!!", "tags": ["x"]}, 'tag_on_failure => "_echec"', ["x", "_echec"]),
        ],
    )
    def test_tag_on_failure(self, doc: dict, options: str, tags: list) -> None:
        assert _grok(f'match => {{ "message" => "%{{INT:n}}" }} {options}', doc)["tags"] == tags

    def test_tag_on_failure_vide(self) -> None:
        doc = _grok('match => { "message" => "%{INT:n}" } tag_on_failure => []', {"message": "!!"})
        assert "tags" not in doc

    def test_valeurs_en_tableau_et_booleen(self) -> None:
        doc = _grok('match => { "v" => "%{WORD:w}" } break_on_match => false', {"v": ["un", "deux"]})
        assert doc["w"] == ["un", "deux"]
        assert _grok('match => { "v" => "%{WORD:w}" }', {"v": True})["w"] == "true"

    def test_valeur_numerique(self) -> None:
        assert _grok('match => { "v" => "%{INT:n:int}" }', {"v": 8080})["n"] == 8080

    def test_keep_empty_captures(self) -> None:
        options = 'match => { "m" => "a(?<vide>x*)b" }'
        assert "vide" not in _grok(options, {"m": "ab"})
        assert _grok(options + " keep_empty_captures => true", {"m": "ab"})["vide"] == ""

    def test_target(self) -> None:
        doc = _grok('match => { "m" => "%{WORD:a}" } target => "[analyse]"', {"m": "x"})
        assert doc["analyse"] == {"a": "x"}

    def test_patterns_dir_et_pattern_definitions(self, tmp_path) -> None:
        (tmp_path / "reseau").write_text("INTERFACE (?:eth|ge)%{INT}\nVLAN vlan%{INT}\n")
        doc = _grok(
            f'match => {{ "m" => "%{{INTERFACE:if}} %{{VLAN:vlan}}" }} patterns_dir => ["{tmp_path}"] '
            'pattern_definitions => { "VLAN" => "VLAN%{INT}" }',
            {"m": "eth0 VLAN12"},
        )
        assert doc["if"] == "eth0" and doc["vlan"] == "VLAN12"

    def test_forme_tableau_historique(self) -> None:
        assert _grok('match => ["message", "%{INT:n}"]', {"message": "7"})["n"] == "7"

    def test_motif_invalide_ignore_les_autres_actifs(self) -> None:
        doc = _grok('match => { "m" => ["%{INCONNU:x}", "%{WORD:w}"] }', {"m": "ok"})
        assert doc["w"] == "ok"

    @pytest.mark.parametrize(
        "options",
        [
            'match => { "m" => "%{INCONNU:x}" }',
            'match => ["m"]',
            'match => "texte"',
            'match => { "m" => [1, 2] }',
            'match => { "[m" => "%{WORD:w}" }',
            "",
        ],
    )
    def test_filtre_inactif_sans_tag(self, options: str) -> None:
        assert _grok(options, {"m": "ok"}) == {"m": "ok"}

    def test_compilation_unique_par_jeu_d_options(self) -> None:
        interp = PipelineInterpreter('filter { grok { match => { "m" => "%{WORD:w}" } } }')
        _run(interp.apply({"m": "a"}))
        _run(interp.apply({"m": "b"}))
        assert len(interp._grok_cache) == 1

    def test_dans_une_condition(self) -> None:
        interp = PipelineInterpreter(
            'filter { if [type] == "syslog" { grok { match => { "message" => "%{SYSLOGBASE}" } } } }'
        )
        doc = {"type": "syslog", "message": LIGNE_SYSLOG}
        _run(interp.apply(doc))
        assert doc["program"] == "sshd"


class TestGrokFilterDirect:
    def test_inactif_renvoie_faux(self) -> None:
        filtre = GrokFilter({})
        assert filtre.actif is False and filtre.apply({"m": "x"}) is False

    def test_renvoie_vrai_sur_correspondance(self) -> None:
        assert GrokFilter({"match": {"m": "%{WORD:w}"}}).apply({"m": "x"}) is True


class TestOptionsCommunes:
    def test_appliquees_apres_correspondance_seulement(self) -> None:
        options = (
            'match => { "m" => "%{INT:n}" } add_tag => ["grok_ok"] '
            'add_field => { "vu" => "%{n}" } remove_field => ["m"]'
        )
        assert _grok(options, {"m": "7"}) == {"n": "7", "tags": ["grok_ok"], "vu": "7"}
        assert _grok(options, {"m": "x"}) == {"m": "x", "tags": ["_grokparsefailure"]}
