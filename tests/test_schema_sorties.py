# SPDX-License-Identifier: GPL-3.0-or-later
"""Schéma de configuration commun des sorties (issue #8).

Options communes à toutes les sorties (`hosts`, authentification, `ssl_*`,
`compression`, `batch_size`, `batch_bytes`, `flush_interval`,
`max_concurrency`, `retry_max`, `retry_backoff`, `dlq_path`) : types,
bornes, valeurs par défaut et options propres déclarées par chaque sortie,
validées à la compilation du pipeline — option inconnue ou valeur
invalide = erreur traduite nommant le plugin.
"""

from __future__ import annotations

import pytest

from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs import REGISTRY, compiler_sorties
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput
from pystashtemplate.outputs.fichier import FileOutput
from pystashtemplate.outputs.schema import OPTIONS_COMMUNES, OptionSpec, defauts_communs, valider_options


class TestOptionsCommunes:
    def test_les_seize_communes_sont_declarees(self) -> None:
        assert set(OPTIONS_COMMUNES) == {
            "hosts",
            "user",
            "password",
            "ssl",
            "cacert",
            "ssl_certificate",
            "ssl_key",
            "compression",
            "batch_size",
            "batch_bytes",
            "flush_interval",
            "max_concurrency",
            "retry_max",
            "retry_backoff",
            "dlq_path",
            "id",
        }

    def test_defauts_documentes(self) -> None:
        # Ce sont les valeurs du tableau de docs/sorties.md.
        assert defauts_communs() == {
            "ssl": True,
            "compression": False,
            "batch_size": 1,
            "batch_bytes": 10_485_760,
            "flush_interval": 1.0,
            "max_concurrency": 4,
            "retry_max": 0,
            "retry_backoff": 1.0,
        }

    def test_chaque_option_a_une_description(self) -> None:
        for spec in OPTIONS_COMMUNES.values():
            assert spec.description, spec.nom


class TestValiderOptions:
    def test_options_communes_acceptees(self) -> None:
        valider_options(
            "null",
            {"hosts": "https://a:9200", "user": "u", "batch_size": 5, "ssl": False},
        )

    def test_option_inconnue_refusee(self) -> None:
        with pytest.raises(ValueError, match="option « hosts_typo » inconnue pour la sortie « null »"):
            valider_options("null", {"hosts_typo": "https://a:9200"})

    def test_erreur_nomme_les_options_valides(self) -> None:
        with pytest.raises(ValueError, match="batch_size"):
            valider_options("null", {"hosts_typo": 1})

    def test_type_inattendu_refuse(self) -> None:
        with pytest.raises(ValueError, match="type int attendu \\(reçu : str\\)"):
            valider_options("null", {"batch_size": "beaucoup"})

    def test_hosts_accepte_chaine_et_liste(self) -> None:
        valider_options("null", {"hosts": "https://a:9200"})
        valider_options("null", {"hosts": ["https://a:9200", "https://b:9200"]})

    def test_borne_minimale_batch_size(self) -> None:
        with pytest.raises(ValueError, match="au moins 1 \\(reçu : 0\\)"):
            valider_options("null", {"batch_size": 0})

    def test_borne_minimale_retry_max(self) -> None:
        with pytest.raises(ValueError, match="au moins 0 \\(reçu : -1\\)"):
            valider_options("null", {"retry_max": -1})

    def test_borne_minimale_flush_interval(self) -> None:
        # 0 est valide (poussée immédiate), -1 non.
        valider_options("null", {"flush_interval": 0})
        with pytest.raises(ValueError, match="au moins 0"):
            valider_options("null", {"flush_interval": -0.5})

    def test_option_requise_absente(self) -> None:
        with pytest.raises(ValueError, match="sortie « file » : l'option « path » est requise"):
            valider_options("file", {}, {"path": OptionSpec("path", (str,), requis=True)})

    def test_enum_refusee(self) -> None:
        spec = OptionSpec("codec", (str,), valeurs=("json_lines", "rubydebug"))
        valider_options("stdout", {"codec": "json_lines"}, {"codec": spec})
        with pytest.raises(ValueError, match="valeurs valides : json_lines, rubydebug \\(reçu : yaml\\)"):
            valider_options("stdout", {"codec": "yaml"}, {"codec": spec})


class TestSortiesConcretes:
    def test_fichier_options_propres(self) -> None:
        FileOutput({"path": "/tmp/x.ndjson", "codec": "rubydebug", "dlq_path": "/tmp/dlq"})

    def test_elasticsearch_bulk_options_propres(self) -> None:
        sortie = ElasticsearchBulkOutput(
            {"hosts": "https://a:9200", "index": "logs", "action": "create", "headers": {}}
        )
        assert sortie._index_pattern == "logs"

    def test_elasticsearch_bulk_action_invalide(self) -> None:
        with pytest.raises(ValueError, match="valeurs valides : index, create, update, delete"):
            ElasticsearchBulkOutput({"hosts": "https://a:9200", "action": "upsert"})

    def test_validation_a_la_compilation(self) -> None:
        # Le schéma s'applique au pipeline entier : une option inconnue
        # dans un .conf échoue à la compilation, avant toute connexion.
        interp = PipelineInterpreter('output { null { hosts_typo => "x" } }')
        with pytest.raises(ValueError, match="inconnue pour la sortie « null »"):
            compiler_sorties(interp)
        interp.close()

    def test_pipeline_complet_bascule_de_sortie(self) -> None:
        # Critère d'acceptation #8 : un même pipeline change de
        # destination en changeant le seul nom du plugin et l'URL —
        # les options communes passent telles quelles.
        conf = (
            'output { opensearch { hosts => "https://os:9200" user => "u"'
            ' password => "p" batch_size => 500 retry_max => 3 } }'
        )
        interp = PipelineInterpreter(conf)
        (sortie,) = compiler_sorties(interp)
        assert type(sortie) is REGISTRY["opensearch"]
        assert sortie.options["batch_size"] == 500
        interp.close()
