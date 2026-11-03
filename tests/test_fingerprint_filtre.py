# SPDX-License-Identifier: GPL-3.0-or-later
"""Issue #114 : filtre `fingerprint` natif (`fingerprint.FingerprintFilter`) dans `--pipeline-conf`."""

from __future__ import annotations

import base64
import hashlib
import hmac
import uuid

import pytest

from pystashtemplate.fingerprint import FingerprintFilter, _valeur_texte
from pystashtemplate.interpreter import PipelineInterpreter


def _conf(doc: dict, corps: str = "") -> dict:
    PipelineInterpreter(f"filter {{ fingerprint {{ {corps} }} }}").apply(doc)
    return doc


class TestMethodes:
    def test_defauts_sha256_message_fingerprint(self) -> None:
        assert _conf({"message": "abc"})["fingerprint"] == hashlib.sha256(b"abc").hexdigest()

    @pytest.mark.parametrize("methode", ["SHA1", "SHA384", "SHA512", "MD5", "sha1"])
    def test_condensats(self, methode: str) -> None:
        doc = _conf({"message": "abc"}, f'method => "{methode}"')
        assert doc["fingerprint"] == hashlib.new(methode.lower(), b"abc").hexdigest()

    def test_hmac_et_base64(self) -> None:
        attendu = hmac.new(b"k", b"abc", "sha256")
        assert _conf({"message": "abc"}, 'key => "k"')["fingerprint"] == attendu.hexdigest()
        doc = _conf({"message": "abc"}, 'key => "k" base64encode => true')
        assert doc["fingerprint"] == base64.b64encode(attendu.digest()).decode()
        doc = _conf({"message": "abc"}, "base64encode => true")
        assert doc["fingerprint"] == base64.b64encode(hashlib.sha256(b"abc").digest()).decode()

    def test_murmur3_comme_logstash(self) -> None:
        # MurmurHash3_x86_32("abc", graine 0) = 0xB3DD93FA
        assert _conf({"message": "abc"}, 'method => "MURMUR3"')["fingerprint"] == 3017643002

    def test_uuid(self) -> None:
        uuid.UUID(_conf({"message": "abc"}, 'method => "UUID"')["fingerprint"])

    def test_ipv4_network(self) -> None:
        assert (
            _conf({"ip": "10.1.2.3"}, 'method => "IPV4_NETWORK" key => 24 source => "ip"')["fingerprint"] == "10.1.2.0"
        )
        assert _conf({"ip": "10.1.2.3"}, 'method => "IPV4_NETWORK" source => "ip"')["fingerprint"] == "10.1.2.3"
        assert _conf({"ip": "pas une ip"}, 'method => "IPV4_NETWORK" source => "ip"')["fingerprint"] is None

    def test_methode_inconnue_sans_effet(self) -> None:
        with pytest.raises(ValueError, match="inconnue"):
            FingerprintFilter({"method": "XX"})
        interp = PipelineInterpreter('filter { fingerprint { method => "XX" add_tag => ["t"] } }')
        for _ in range(2):
            doc = {"message": "abc"}
            interp.apply(doc)
            assert doc == {"message": "abc"}
        assert len(interp._fingerprint_cache) == 1


class TestSources:
    def test_concatenate_sources_triees(self) -> None:
        doc = _conf({"b": 2, "a": "1"}, 'source => ["b", "a", "absent"] concatenate_sources => true method => "SHA1"')
        assert doc["fingerprint"] == hashlib.sha1(b"|a|1|absent||b|2|").hexdigest()

    def test_concatenate_all_fields(self) -> None:
        doc = _conf({"b": True, "a": {"y": 1, "x": None}}, 'concatenate_all_fields => true method => "MD5"')
        assert doc["fingerprint"] == hashlib.md5(b'|a|{"x":null,"y":1}|b|true|').hexdigest()

    def test_dernier_champ_l_emporte_et_tableaux(self) -> None:
        doc = _conf({"a": "x", "b": "y"}, 'source => ["a", "b"] method => "MD5" target => "[h][v]"')
        assert doc["h"]["v"] == hashlib.md5(b"y").hexdigest()
        doc = _conf({"a": ["x", 1]}, 'source => "a" method => "MD5"')
        assert doc["fingerprint"] == [hashlib.md5(b"x").hexdigest(), hashlib.md5(b"1").hexdigest()]

    def test_source_absente_ni_ecriture_ni_options_communes(self) -> None:
        assert _conf({"autre": 1}, 'add_tag => ["t"]') == {"autre": 1}

    def test_options_communes_apres_succes(self) -> None:
        assert _conf({"message": "abc"}, 'add_tag => ["t"]')["tags"] == ["t"]

    def test_source_liste_mal_typee(self) -> None:
        assert FingerprintFilter({"source": ["a", 3]}).sources == ["a"]


@pytest.mark.parametrize(
    ("valeur", "texte"),
    [("a", "a"), (True, "true"), (False, "false"), (None, ""), (3, "3"), ([1, "a"], '[1,"a"]')],
)
def test_valeur_texte(valeur: object, texte: str) -> None:
    assert _valeur_texte(valeur) == texte


def test_kv_source_non_texte_ignoree() -> None:
    """Couvre la garde `source` non texte de `_apply_kv` (#113), arrivée sans test sur dev."""
    doc = {"message": "a=1"}
    PipelineInterpreter('filter { kv { source => ["message"] add_tag => ["t"] } }').apply(doc)
    assert doc == {"message": "a=1"}
