# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de `src/interpreter.py` (issue #25) : le pipeline `.conf` Logstash,
via `PipelineInterpreter` — parse une fois, `apply()` par document.

Couvre les briques `filter{}` réellement interprétées (dissect, mutate),
les branches `if`/`else if`/`else`, le rendu sprintf des options
`output{}` et la sémantique Logstash de vérité (voir test_filters.py pour
les filtres unitaires, test_dlq.py/test_omprog.py pour les voisins)."""

from __future__ import annotations

import asyncio

import pytest
from logstash_pipeline_parser.tree import ParseError

from pystashtemplate.interpreter import PipelineInterpreter, substituer_variables_environnement


def _run(coro):
    # Sans pytest-asyncio (convention du dépôt, cf. RsysLogstach) : asyncio.run direct (#34).
    return asyncio.run(coro)


class TestParse:
    def test_pipeline_malforme_leve(self) -> None:
        with pytest.raises(ParseError):
            PipelineInterpreter("filter { dissect { mapping => { }")

    def test_pipeline_minimal(self) -> None:
        interp = PipelineInterpreter("filter { }")
        assert _run(interp.apply({"a": 1})) == []


class TestHasOutput:
    def test_sans_output(self) -> None:
        assert PipelineInterpreter("filter { }").has_output is False

    def test_avec_output(self) -> None:
        interp = PipelineInterpreter("output { stdout { } }")
        assert interp.has_output is True


class TestFilterDissect:
    def test_mapping_dissect(self) -> None:
        conf = """
        filter {
            dissect {
                mapping => { "message" => "%{src} %{level} %{msg}" }
            }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {"message": "app1 INFO connexion ok"}
        _run(interp.apply(doc))
        assert doc == {
            "message": "app1 INFO connexion ok",
            "src": "app1",
            "level": "INFO",
            "msg": "connexion ok",
        }

    def test_source_non_textuelle_ignorer(self) -> None:
        conf = 'filter { dissect { mapping => { "message" => "%{a} %{b}" } } }'
        interp = PipelineInterpreter(conf)
        doc = {"message": 123}
        _run(interp.apply(doc))
        assert doc == {"message": 123, "tags": ["_dissectfailure"]}  # #117, comme Logstash


class TestFilterKV:
    def test_bloc_natif_basique(self) -> None:
        conf = """
        filter {
            kv { source => "message" }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {"message": "user=admin action=login"}
        _run(interp.apply(doc))
        assert doc == {
            "message": "user=admin action=login",
            "user": "admin",
            "action": "login",
        }

    def test_options_completes(self) -> None:
        conf = """
        filter {
            kv {
                source => "message"
                field_split => ";"
                value_split => ":"
                prefix => "kv_"
                exclude_keys => ["secret"]
                trim_key => "'"
                trim_value => "\\""
                allow_duplicate_values => false
            }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {"message": "'user':\"admin\";tag:a;tag:b;tag:b;secret:x"}
        _run(interp.apply(doc))
        # secret exclu, tag répété dédupliqué (et préfixé), quotes retirées.
        assert doc == {
            "message": "'user':\"admin\";tag:a;tag:b;tag:b;secret:x",
            "kv_user": "admin",
            "kv_tag": ["a", "b"],
        }

    def test_include_keys(self) -> None:
        conf = """
        filter {
            kv {
                source => "message"
                include_keys => ["user"]
            }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {"message": "user=admin action=login"}
        _run(interp.apply(doc))
        assert doc == {
            "message": "user=admin action=login",
            "user": "admin",
        }

    def test_options_communes_apres_succes(self) -> None:
        # #116 : add_tag appliqué après un kv réussi (filter_matched).
        conf = """
        filter {
            kv {
                source => "message"
                add_tag => ["decoupe"]
            }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {"message": "a=1", "tags": ["existant"]}
        _run(interp.apply(doc))
        assert doc == {"message": "a=1", "a": "1", "tags": ["existant", "decoupe"]}

    def test_options_communes_ignorees_si_aucune_paire(self) -> None:
        conf = """
        filter {
            kv {
                source => "message"
                add_tag => ["decoupe"]
            }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {"message": "sans separateur"}
        _run(interp.apply(doc))
        assert doc == {"message": "sans separateur"}

    def test_target_fusionne_dans_hash_existant(self) -> None:
        conf = """
        filter {
            kv { source => "message" target => "pairs" }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {"message": "a=1 b=2", "pairs": {"existing": "yes"}}
        _run(interp.apply(doc))
        assert doc == {
            "message": "a=1 b=2",
            "pairs": {"existing": "yes", "a": "1", "b": "2"},
        }

    def test_target_cree_le_hash(self) -> None:
        conf = 'filter { kv { source => "message" target => "p" } }'
        interp = PipelineInterpreter(conf)
        doc = {"message": "a=1"}
        _run(interp.apply(doc))
        assert doc == {"message": "a=1", "p": {"a": "1"}}

    def test_target_non_hash_remplace(self) -> None:
        conf = 'filter { kv { source => "message" target => "p" } }'
        interp = PipelineInterpreter(conf)
        doc = {"message": "a=1", "p": "ancien"}
        _run(interp.apply(doc))
        assert doc == {"message": "a=1", "p": {"a": "1"}}

    def test_cle_repetee_liste(self) -> None:
        conf = 'filter { kv { source => "message" } }'
        interp = PipelineInterpreter(conf)
        doc = {"message": "tag=a tag=b"}
        _run(interp.apply(doc))
        assert doc == {"message": "tag=a tag=b", "tag": ["a", "b"]}

    def test_source_absente_ignorer(self) -> None:
        conf = 'filter { kv { source => "message" } }'
        interp = PipelineInterpreter(conf)
        doc = {"autre": "x"}
        _run(interp.apply(doc))
        assert doc == {"autre": "x"}

    def test_source_non_textuelle_ignorer(self) -> None:
        conf = 'filter { kv { source => "message" } }'
        interp = PipelineInterpreter(conf)
        doc = {"message": 123}
        _run(interp.apply(doc))
        assert doc == {"message": 123}


class TestVariablesEnvironnement:
    """`${VAR}` / `${VAR:défaut}` au chargement du `.conf` (#122)."""

    def test_substitution_dans_une_valeur(self, monkeypatch) -> None:
        monkeypatch.setenv("RSYSLOGSTACH_TEST_ENV", "valeur")
        conf = """
        filter {
            mutate { add_field => { "champ" => "${RSYSLOGSTACH_TEST_ENV}" } }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {}
        _run(interp.apply(doc))
        assert doc == {"champ": "valeur"}

    def test_valeur_par_defaut_si_variable_absente(self, monkeypatch) -> None:
        monkeypatch.delenv("RSYSLOGSTACH_TEST_ENV", raising=False)
        conf = 'filter { mutate { add_field => { "champ" => "${RSYSLOGSTACH_TEST_ENV:défaut}" } } }'
        doc = {}
        _run(PipelineInterpreter(conf).apply(doc))
        assert doc == {"champ": "défaut"}

    def test_variable_definie_prime_sur_le_defaut(self, monkeypatch) -> None:
        monkeypatch.setenv("RSYSLOGSTACH_TEST_ENV", "valeur")
        conf = 'filter { mutate { add_field => { "champ" => "${RSYSLOGSTACH_TEST_ENV:défaut}" } } }'
        doc = {}
        _run(PipelineInterpreter(conf).apply(doc))
        assert doc == {"champ": "valeur"}

    def test_variable_absente_sans_defaut_refuse(self, monkeypatch) -> None:
        monkeypatch.delenv("RSYSLOGSTACH_TEST_ENV", raising=False)
        conf = 'filter { mutate { add_field => { "champ" => "${RSYSLOGSTACH_TEST_ENV}" } } }'
        with pytest.raises(ValueError, match="RSYSLOGSTACH_TEST_ENV"):
            PipelineInterpreter(conf)

    def test_variables_manquantes_toutes_nommees(self, monkeypatch) -> None:
        monkeypatch.delenv("RSYSLOGSTACH_TEST_A", raising=False)
        monkeypatch.delenv("RSYSLOGSTACH_TEST_B", raising=False)
        conf = 'filter { mutate { add_field => { "a" => "${RSYSLOGSTACH_TEST_A}" "b" => "${RSYSLOGSTACH_TEST_B}" } } }'
        with pytest.raises(ValueError, match="RSYSLOGSTACH_TEST_A, RSYSLOGSTACH_TEST_B"):
            PipelineInterpreter(conf)

    def test_gabarit_sprintf_non_concerne(self) -> None:
        # `%{champ}` est rendu par événement : aucun `${}` dans le .conf,
        # la substitution ne doit pas y toucher.
        conf = 'filter { mutate { add_field => { "x" => "%{champ}" } } }'
        doc = {"champ": "valeur"}
        _run(PipelineInterpreter(conf).apply(doc))
        assert doc == {"champ": "valeur", "x": "valeur"}

    def test_nom_invalide_laisse_telle_quelle(self) -> None:
        conf = 'filter { mutate { add_field => { "champ" => "${1invalide}" } } }'
        doc = {}
        _run(PipelineInterpreter(conf).apply(doc))
        assert doc == {"champ": "${1invalide}"}

    def test_substitution_dans_un_tableau(self, monkeypatch) -> None:
        monkeypatch.setenv("RSYSLOGSTACH_TEST_HOST", "h1.exemple")
        conf = 'filter { mutate { add_field => { "hotes" => ["${RSYSLOGSTACH_TEST_HOST}", "fixe"] } } }'
        doc = {}
        _run(PipelineInterpreter(conf).apply(doc))
        assert doc == {"hotes": ["h1.exemple", "fixe"]}

    def test_fonction_directe_sans_variable(self) -> None:
        assert substituer_variables_environnement("sans variable") == "sans variable"
        assert substituer_variables_environnement("") == ""


class TestFilterMutate:
    def test_add_field_avec_gabarit(self) -> None:
        conf = """
        filter {
            mutate {
                add_field => { "[syslog][name]" => "%{hostname}-log" }
            }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {"hostname": "srv1"}
        _run(interp.apply(doc))
        # Chemin imbriqué : doc["syslog"]["name"], pas une clé plate.
        assert doc["syslog"]["name"] == "srv1-log"

    def test_convert_integer(self) -> None:
        conf = 'filter { mutate { convert => { "port" => "integer" } } }'
        interp = PipelineInterpreter(conf)
        doc = {"port": "22"}
        _run(interp.apply(doc))
        assert doc["port"] == 22


class TestBranches:
    def test_if_vrai(self) -> None:
        conf = """
        filter {
            if [level] {
                mutate { add_field => { "graded" => "yes" } }
            }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {"level": "INFO"}
        _run(interp.apply(doc))
        assert doc["graded"] == "yes"

    def test_if_faux_chaine_absente(self) -> None:
        conf = """
        filter {
            if [level] {
                mutate { add_field => { "graded" => "yes" } }
            }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {"other": 1}
        _run(interp.apply(doc))
        assert "graded" not in doc

    def test_verite_logstash_chaine_vide_est_vraie(self) -> None:
        # Comportement réel de Logstash : présence de champ, pas "valeur vide".
        conf = """
        filter {
            if [level] {
                mutate { add_field => { "graded" => "yes" } }
            }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {"level": ""}
        _run(interp.apply(doc))
        assert doc["graded"] == "yes"

    def test_else(self) -> None:
        conf = """
        filter {
            if [level] == "ERROR" {
                mutate { add_field => { "gravite" => "haute" } }
            } else {
                mutate { add_field => { "gravite" => "basse" } }
            }
        }
        """
        interp = PipelineInterpreter(conf)
        doc = {"level": "INFO"}
        _run(interp.apply(doc))
        assert doc["gravite"] == "basse"


class TestOutput:
    def test_options_rendues_sans_gabarit(self) -> None:
        conf = 'output { opensearch { index => "logs-2026" } }'
        interp = PipelineInterpreter(conf)
        actions = _run(interp.apply({"a": 1}))
        assert len(actions) == 1
        assert actions[0].plugin == "opensearch"
        assert actions[0].options["index"] == "logs-2026"

    def test_gabarit_sprintf_rendu(self) -> None:
        conf = 'output { opensearch { index => "logs-%{+YYYY.MM.dd}" } }'
        interp = PipelineInterpreter(conf)
        actions = _run(interp.apply({"a": 1}))
        # Le gabarit %{+YYYY.MM.dd} doit être rendu en date, pas laissé tel quel.
        index = actions[0].options["index"]
        assert index != "logs-%{+YYYY.MM.dd}"
        assert index.startswith("logs-2026.") or index.startswith("logs-20")

    def test_output_conditionnel(self) -> None:
        conf = """
        output {
            if [level] == "ERROR" {
                opensearch { index => "errors" }
            }
        }
        """
        interp = PipelineInterpreter(conf)
        assert _run(interp.apply({"level": "ERROR"})) != []
        assert _run(interp.apply({"level": "INFO"})) == []

    def test_output_ignorer_si_faux(self) -> None:
        conf = 'output { if ![host] { opensearch { index => "nohost" } } }'
        interp = PipelineInterpreter(conf)
        assert _run(interp.apply({"host": "srv1"})) == []


class TestApplyRenvoieToujoursLesActions:
    def test_document_non_filtre_sans_output(self) -> None:
        # Sans output{} : [] — l'appelant retombe sur sa config statique.
        interp = PipelineInterpreter("filter { }")
        assert _run(interp.apply({"a": 1})) == []
