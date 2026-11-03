# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests des filtres de `src/filters.py` (issue #25) : dissect, regex, kv,
drop, mutate et la chaîne `FilterPipeline`."""

from __future__ import annotations

import pytest

from pystashtemplate.filters import (
    Dissector,
    DropFilter,
    FilterPipeline,
    KeyValueFilter,
    RegexFilter,
    apply_mutate,
    load_word_list,
)


class TestDissect:
    def test_apply_basique(self) -> None:
        d = Dissector("%{date} %{time} %{hostname} %{message}")
        res = d.apply("2026-10-20 10:15:30 srv1 connexion établie")
        assert res == {
            "date": "2026-10-20",
            "time": "10:15:30",
            "hostname": "srv1",
            "message": "connexion établie",
        }

    def test_delimiteur_introuvable_renvoie_none(self) -> None:
        # Structure inattendue : le délimiteur ":" du patron est absent.
        d = Dissector("%{ip}:%{port}")
        assert d.apply("1.2.3.4 443") is None

    def test_prefix_litteral_absent_renvoie_none(self) -> None:
        # Le patron commence par "<" : une source sans ce préfixe ne matche pas.
        d = Dissector("<%{pri}>%{msg}")
        assert d.apply("pas un syslog") is None

    def test_patron_sans_champ_leve(self) -> None:
        with pytest.raises(ValueError):
            Dissector("aucun champ ici")

    def test_patron_vide_leve(self) -> None:
        with pytest.raises(ValueError):
            Dissector("")

    def test_modificateur_plus_concatene(self) -> None:
        # %{+name} (Logstash) : concatène au champ du même nom déjà extrait.
        d = Dissector("%{a} %{+a}")
        assert d.apply("x y") == {"a": "xy"}

    def test_modificateur_interrogation_extrait_sans_conserver(self) -> None:
        # %{?name} : la valeur est consommée mais absente du résultat.
        d = Dissector("%{a} %{?b} %{c}")
        assert d.apply("x y z") == {"a": "x", "c": "z"}

    def test_marqueur_vide_extrait_sans_conserver(self) -> None:
        # %{} : segment ignoré, jamais présent dans le résultat.
        d = Dissector("%{a} %{} %{c}")
        assert d.apply("x y z") == {"a": "x", "c": "z"}

    def test_fleche_ignore_les_repetitions_du_delimiteur(self) -> None:
        # %{a->} : "123   abc" (espaces multiples) → pas de segment vide.
        d = Dissector("%{a->} %{b}")
        assert d.apply("123   abc") == {"a": "123", "b": "abc"}

    def test_dernier_champ_jusqu_a_la_fin(self) -> None:
        # Sans délimiteur après le dernier champ, il prend tout le reste.
        d = Dissector("%{h} %{msg}")
        assert d.apply("srv1 message avec  espaces") == {
            "h": "srv1",
            "msg": "message avec  espaces",
        }


class TestRegexFilter:
    def test_apply_avec_groupes_nommes(self) -> None:
        pat = r"%%\d+(?P<module>[A-Z_]+)/(?P<severity_code>\d+)/(?P<event_name>[A-Z_]+): ?(?P<detail>.*)"
        f = RegexFilter(pat)
        res = f.apply("%%10IFNET/3/LINK_UPDOWN: Interface up")
        assert res == {
            "module": "IFNET",
            "severity_code": "3",
            "event_name": "LINK_UPDOWN",
            "detail": "Interface up",
        }

    def test_aucune_correspondance_renvoie_none(self) -> None:
        f = RegexFilter(r"^(?P<a>x+)$")
        assert f.apply("rien à voir") is None

    def test_correspondance_partielle_renvoie_none(self) -> None:
        # .match() ancre au début : "xxabc" ne matche pas ^(?P<a>x+)$.
        f = RegexFilter(r"^(?P<a>x+)$")
        assert f.apply("xxabc") is None


class TestKeyValueFilter:
    def test_apply_basique(self) -> None:
        kv = KeyValueFilter()
        assert kv.apply("user=admin action=login result=ok") == {
            "user": "admin",
            "action": "login",
            "result": "ok",
        }

    def test_apply_avec_prefixe(self) -> None:
        kv = KeyValueFilter(prefix="kv_")
        assert kv.apply("a=1 b=2") == {"kv_a": "1", "kv_b": "2"}

    def test_segment_sans_value_split_ignore(self) -> None:
        # "key1,key2" sans "=" : segment ignoré, pas d'erreur.
        kv = KeyValueFilter()
        assert kv.apply("key1,key2 a=1") == {"a": "1"}

    def test_segments_vides_ignores(self) -> None:
        # ",a=1" avec field_split="," : le segment vide n'a pas de "=" → ignoré.
        kv = KeyValueFilter(field_split=",")
        assert kv.apply(",a=1") == {"a": "1"}

    def test_cle_vide_ignoree(self) -> None:
        kv = KeyValueFilter()
        assert kv.apply("=1 a=2") == {"a": "2"}

    def test_cle_repetee_liste_de_valeurs(self) -> None:
        # Comme le plugin kv officiel : une clé répétée vaut la liste de ses valeurs.
        kv = KeyValueFilter()
        assert kv.apply("tag=a tag=b tag=a") == {"tag": ["a", "b", "a"]}

    def test_cle_repetee_allow_duplicate_values_false(self) -> None:
        kv = KeyValueFilter(allow_duplicate_values=False)
        assert kv.apply("tag=a tag=b tag=a") == {"tag": ["a", "b"]}

    def test_include_keys_filtre_avant_prefixe(self) -> None:
        kv = KeyValueFilter(prefix="kv_", include_keys=["user"])
        assert kv.apply("user=admin action=login") == {"kv_user": "admin"}

    def test_exclude_keys(self) -> None:
        kv = KeyValueFilter(exclude_keys=["secret"])
        assert kv.apply("user=admin secret=x") == {"user": "admin"}

    def test_trim_key_et_trim_value(self) -> None:
        # trim_key retire les apostrophes des clés, trim_value les guillemets des valeurs.
        kv = KeyValueFilter(trim_key="'", trim_value='"')
        assert kv.apply("'a'=\"1\" b=2") == {"a": "1", "b": "2"}

    def test_cle_devenant_vide_apres_trim_ignoree(self) -> None:
        kv = KeyValueFilter(trim_key="'")
        # "''=1" : clé devenue vide après trim → ignorée.
        assert kv.apply("''=1 a=2") == {"a": "2"}

    def test_separateurs_personnalises(self) -> None:
        kv = KeyValueFilter(field_split=",", value_split=":")
        assert kv.apply("a:1,b:2") == {"a": "1", "b": "2"}


class TestDropFilter:
    def test_mode_contains_insensible_a_la_casse(self) -> None:
        df = DropFilter(words=["Sudo"], mode="contains", case_sensitive=False)
        assert df.matches("sudo pam_unix") is True

    def test_mode_equals(self) -> None:
        df = DropFilter(words=["health"], mode="equals")
        assert df.matches("health") is True
        assert df.matches("healthcheck") is False

    def test_mode_inconnu_leve(self) -> None:
        with pytest.raises(ValueError):
            DropFilter(words=["x"], mode="regexp")

    def test_case_sensitive(self) -> None:
        df = DropFilter(words=["sudo"], mode="contains", case_sensitive=True)
        assert df.matches("sudo") is True
        assert df.matches("SUDO") is False


class TestLoadWordList:
    def test_lecture_fichier(self, tmp_path) -> None:
        f = tmp_path / "blocklist.txt"
        f.write_text("# commentaire\n\nword1\nword2\n", encoding="utf-8")
        assert load_word_list(str(f)) == ["word1", "word2"]


class TestApplyMutate:
    def test_convert_integer(self) -> None:
        doc = {"port": "22"}
        apply_mutate(doc, {"convert": {"port": "integer"}})
        assert doc["port"] == 22

    def test_convert_type_inconnu_laisse_la_valeur_inchangee(self) -> None:
        # convert_value ne lève jamais : type inconnu → champ inchangé,
        # aucun tag (log DEBUG), comme une conversion impossible.
        doc = {"a": "x"}
        apply_mutate(doc, {"convert": {"a": "blob"}})
        assert doc == {"a": "x"}
        assert "tags" not in doc

    def test_convert_impossible_laisse_la_valeur_inchangee(self) -> None:
        doc = {"a": "abc"}
        apply_mutate(doc, {"convert": {"a": "integer"}})
        assert doc == {"a": "abc"}

    def test_uppercase_et_lowercase(self) -> None:
        doc = {"a": "mIxEd", "b": "UP"}
        apply_mutate(doc, {"uppercase": ["a"], "lowercase": ["b"]})
        assert doc == {"a": "MIXED", "b": "up"}

    def test_remove_field(self) -> None:
        doc = {"a": 1, "b": 2}
        apply_mutate(doc, {"remove_field": ["a"]})
        assert doc == {"b": 2}

    def test_add_field(self) -> None:
        doc = {"h": "srv1"}
        apply_mutate(doc, {"add_field": {"target": "x"}})
        assert doc["target"] == "x"

    def test_add_field_avec_gabarit(self) -> None:
        # %{h} est rendu depuis le document (syntaxe sprintf de Logstash).
        doc = {"h": "srv1"}
        apply_mutate(doc, {"add_field": {"target": "%{h}-log"}})
        assert doc["target"] == "srv1-log"

    def test_rename(self) -> None:
        doc = {"a": 1}
        apply_mutate(doc, {"rename": {"a": "b"}})
        assert doc == {"b": 1}

    def test_gsub_liste_plate_de_triplets(self) -> None:
        # gsub : [champ, motif, remplacement, ...] — liste plate, pas imbriquée.
        doc = {"msg": "hello world"}
        apply_mutate(doc, {"gsub": ["msg", "world", "team"]})
        assert doc["msg"] == "hello team"

    def test_option_inconnue_ignorer_silencieusement(self) -> None:
        # Les options non interprétées sont ignorées (log DEBUG), sans tag.
        doc = {"a": 1}
        apply_mutate(doc, {"option_inexistante": {"x": 1}})
        assert doc == {"a": 1}
        assert "tags" not in doc

    def test_add_tag_sans_doublon(self) -> None:
        doc = {"tags": ["x"]}
        apply_mutate(doc, {"add_tag": ["y", "x"]})
        assert doc["tags"] == ["x", "y"]

    def test_tag_on_failure_sur_exception_reelle(self) -> None:
        # Cas réel documenté : split avec séparateur vide lève ValueError
        # → opérations suivantes sautées + tag _mutate_error (comme le
        # `rescue` du plugin officiel), pas de raise.
        doc = {"a": "x y"}
        apply_mutate(doc, {"split": {"a": ""}, "add_field": {"b": "1"}})
        assert doc == {"a": "x y", "tags": ["_mutate_error"]}


class TestFilterPipeline:
    def test_chaine_dissect_puis_drop(self) -> None:
        # dissect extrait, puis drop bloque les messages de monitoring.
        steps = [
            {
                "type": "dissect",
                "source": "message",
                "pattern": "%{src} %{level} %{msg}",
                "target": "parsed",
            },
            {
                "type": "drop",
                "source": "message",
                "words": ["healthcheck"],
                "mode": "contains",
            },
        ]
        pipeline = FilterPipeline(steps)
        doc_ok = {"message": "app1 INFO connexion ok"}
        doc_drop = {"message": "app1 INFO healthcheck"}
        assert pipeline.apply(doc_ok) is True
        assert doc_ok["parsed"] == {"src": "app1", "level": "INFO", "msg": "connexion ok"}
        assert pipeline.apply(doc_drop) is False

    def test_mutate_dans_la_chaine(self) -> None:
        # Chemin imbriqué : syntaxe à crochets %{...} ("[parsed][port]"),
        # pas "parsed.port" (clé plate).
        steps = [{"type": "mutate", "convert": {"[parsed][port]": "integer"}}]
        pipeline = FilterPipeline(steps)
        doc = {"parsed": {"port": "22"}}
        pipeline.apply(doc)
        assert doc["parsed"]["port"] == 22

    def test_step_type_inconnu_leve(self) -> None:
        with pytest.raises(ValueError):
            FilterPipeline([{"type": "inexistant", "source": "message"}])
