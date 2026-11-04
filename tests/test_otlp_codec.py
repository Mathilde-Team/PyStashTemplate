# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests du codage OTLP des journaux (issue #22) : valeurs, dates, sévérités, JSON et protobuf.

Le protobuf est relu par le décodeur minimal du module (`_champs`) ; le
décodage par un vrai récepteur est vérifié par le banc de conformité
(`test_integration_otlp.py`, OpenTelemetry Collector en conteneur).
"""

from __future__ import annotations

import json
import struct
from datetime import datetime, timedelta, timezone

import pytest

from pystashtemplate.outputs import otlp_codec as codec


def test_valeurs_scalaires() -> None:
    assert codec.valeur(None) == {}
    assert codec.valeur(True) == {"boolValue": True}
    assert codec.valeur(42) == {"intValue": "42"}
    assert codec.valeur(-(2**63)) == {"intValue": str(-(2**63))}
    assert codec.valeur(2**63) == {"stringValue": str(2**63)}
    assert codec.valeur(1.5) == {"doubleValue": 1.5}
    assert codec.valeur("a") == {"stringValue": "a"}
    assert codec.valeur(b"ab") == {"stringValue": "b'ab'"}


def test_valeurs_composees() -> None:
    assert codec.valeur({"a": [1, "x"]}) == {
        "kvlistValue": {
            "values": [{"key": "a", "value": {"arrayValue": {"values": [{"intValue": "1"}, {"stringValue": "x"}]}}}]
        }
    }
    assert codec.valeur((1,)) == {"arrayValue": {"values": [{"intValue": "1"}]}}


@pytest.mark.parametrize(
    ("entree", "attendu"),
    [
        ("2026-11-04T18:00:00Z", 1793815200 * 10**9),
        ("2026-11-04T18:00:00.5+00:00", 1793815200 * 10**9 + 500_000_000),
        ("2026-11-04T18:00:00", 1793815200 * 10**9),
        (datetime(2026, 11, 4, 19, tzinfo=timezone(timedelta(hours=1))), 1793815200 * 10**9),
        (1793815200, 1793815200 * 10**9),
        (1793815200.25, 1793815200 * 10**9 + 250_000_000),
        (1793815200123, 1793815200123 * 10**6),
        (1793815200123456, 1793815200123456 * 10**3),
        (1793815200123456789, 1793815200123456789),
        ("hier", None),
        (float("nan"), None),
        (-1, None),
        (True, None),
        (None, None),
        ([1], None),
    ],
)
def test_horodatage_ns(entree, attendu) -> None:
    assert codec.horodatage_ns(entree) == attendu


@pytest.mark.parametrize(
    ("entree", "attendu"),
    [
        ("INFO", (9, "INFO")),
        (" warning ", (13, " warning ")),
        ("emerg", (21, "emerg")),
        ("bizarre", (0, "bizarre")),
        (0, (21, "0")),
        (3, (17, "3")),
        (7, (5, "7")),
        (8, (0, "8")),
        (None, (0, "")),
        (False, (0, "")),
    ],
)
def test_severite(entree, attendu) -> None:
    assert codec.severite(entree) == attendu


def test_identifiants_hex() -> None:
    assert codec.identifiant_hex("0123456789ABCDEF", 8) == "0123456789abcdef"
    assert codec.identifiant_hex("0" * 16, 8) is None
    assert codec.identifiant_hex("zz" * 8, 8) is None
    assert codec.identifiant_hex("ab", 8) is None
    assert codec.identifiant_hex(12, 8) is None


def test_corps_json_strict() -> None:
    assert json.loads(codec.corps_json({"a": "é"})) == {"a": "é"}
    with pytest.raises(codec.ErreurCodageOtlp):
        codec.corps_json({"a": float("nan")})


REQUETE = {
    "resourceLogs": [
        {
            "resource": {"attributes": codec.attributs({"service.name": "s"})},
            "scopeLogs": [
                {
                    "scope": {"name": "p", "version": "1"},
                    "logRecords": [
                        {
                            "timeUnixNano": "5",
                            "observedTimeUnixNano": "6",
                            "severityNumber": 9,
                            "severityText": "info",
                            "body": {"stringValue": ""},
                            "attributes": codec.attributs(
                                {"b": False, "i": -1, "d": 0.5, "l": [1], "k": {"x": None}, "s": "t"}
                            ),
                            "traceId": "01" * 16,
                            "spanId": "02" * 8,
                        },
                        {},
                    ],
                }
            ],
        }
    ]
}


def test_corps_protobuf_structure() -> None:
    donnees = codec.corps_protobuf(REQUETE)
    (rl,) = codec._champs(donnees)[1]
    rl = codec._champs(rl)
    ressource = codec._champs(rl[1][0])
    kv = codec._champs(ressource[1][0])
    assert kv[1] == [b"service.name"]
    assert codec._champs(kv[2][0]) == {1: [b"s"]}
    sl = codec._champs(rl[2][0])
    assert codec._champs(sl[1][0]) == {1: [b"p"], 2: [b"1"]}
    premier, second = sl[2]
    assert second == b""
    journal = codec._champs(premier)
    assert journal[2] == [9]
    assert journal[3] == [b"info"]
    assert codec._champs(journal[5][0]) == {1: [b""]}
    assert journal[9] == [bytes.fromhex("01" * 16)]
    assert journal[10] == [bytes.fromhex("02" * 8)]
    valeurs = {codec._champs(a)[1][0]: codec._champs(codec._champs(a)[2][0]) for a in journal[6]}
    assert valeurs[b"b"] == {2: [0]}
    assert valeurs[b"i"] == {3: [2**64 - 1]}
    assert valeurs[b"s"] == {1: [b"t"]}
    assert codec._champs(valeurs[b"l"][5][0]) == {1: [codec._pb_valeur({"intValue": "1"})]}
    kvlist = codec._champs(codec._champs(valeurs[b"k"][6][0])[1][0])
    assert kvlist == {1: [b"x"], 2: [b""]}
    assert 4 not in valeurs[b"d"]  # double (fil 64 bits) ignoré par le décodeur minimal


def test_corps_protobuf_horodatages_fixed64() -> None:
    donnees = codec._pb_journal({"timeUnixNano": "5", "observedTimeUnixNano": "6"})
    assert donnees == b"\x09" + struct.pack("<Q", 5) + b"\x59" + struct.pack("<Q", 6)
    assert codec._pb_valeur({"doubleValue": 0.5}) == b"\x21" + struct.pack("<d", 0.5)
    assert codec._pb_valeur({}) == b""


def test_corps_protobuf_vide() -> None:
    assert codec.corps_protobuf({}) == b""
    assert codec.corps_protobuf({"resourceLogs": [{}]}) == b"\x0a\x02\x0a\x00"


def test_succes_partiel_protobuf() -> None:
    partiel = b"\x08" + codec._varint(3) + codec._chaine(2, "refusés")
    assert codec.succes_partiel_protobuf(codec._delimite(1, partiel)) == (3, "refusés")
    assert codec.succes_partiel_protobuf(b"") == (0, "")
    assert codec.succes_partiel_protobuf(b"\x0a\x00") == (0, "")
    negatif = codec._delimite(1, b"\x08" + codec._varint(-1))
    assert codec.succes_partiel_protobuf(negatif) == (-1, "")
    # champs 32 et 64 bits inconnus sautés
    assert codec.succes_partiel_protobuf(b"\x15abcd\x19abcdefgh") == (0, "")


@pytest.mark.parametrize("donnees", [b"\x0a\x05ab", b"\x08", b"\x0b", b"\x08" + b"\xff" * 11])
def test_succes_partiel_protobuf_illisible(donnees: bytes) -> None:
    with pytest.raises(ValueError):
        codec.succes_partiel_protobuf(donnees)


def test_succes_partiel_json() -> None:
    assert codec.succes_partiel_json({}) == (0, "")
    assert codec.succes_partiel_json({"partialSuccess": {}}) == (0, "")
    assert codec.succes_partiel_json({"partialSuccess": {"rejectedLogRecords": "2", "errorMessage": "m"}}) == (2, "m")


@pytest.mark.parametrize("corps", [[], {"partialSuccess": "x"}, {"partialSuccess": {"rejectedLogRecords": "a"}}])
def test_succes_partiel_json_illisible(corps) -> None:
    with pytest.raises(ValueError):
        codec.succes_partiel_json(corps)
