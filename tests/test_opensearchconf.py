# SPDX-License-Identifier: GPL-3.0-or-later
"""Options de connexion du bloc `opensearch{}` (`pystashtemplate.opensearchconf`, issue #110)."""

from __future__ import annotations

import argparse

import pytest

from pystashtemplate.opensearchconf import (
    apply_connection_options,
    collect_connection_options,
    conf_auth,
    conf_host_urls,
)


def test_booleen_en_chaine():
    assert collect_connection_options([("opensearch", {"ssl": "TRUE", "ssl_certificate_verification": "false"})]) == {
        "ssl": True,
        "ssl_certificate_verification": False,
    }


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"ssl": "oui"}, "true ou false"),
        ({"hosts": []}, "liste d'URLs"),
        ({"hosts": [1]}, "liste d'URLs"),
        ({"cacert": ""}, "chaîne non vide"),
        ({"user": "u"}, "vont ensemble"),
        ({"password": "p"}, "vont ensemble"),
        ({"hosts": ["${OS_HOST}"]}, "non remplacée"),
        ({"password": "${PW}", "user": "u"}, "non remplacée"),
    ],
)
def test_options_invalides(options, message):
    with pytest.raises(ValueError, match=message):
        collect_connection_options([("opensearch", options)])


def test_blocs_contradictoires_refuses():
    with pytest.raises(ValueError, match="différente"):
        collect_connection_options([("opensearch", {"hosts": "a"}), ("elasticsearch", {"hosts": ["b"]})])


def test_blocs_identiques_et_autres_plugins_acceptes():
    plugins = [
        ("opensearch", {"hosts": "a", "index": "x"}),
        ("stdout", {"hosts": "z"}),
        ("opensearch", {"hosts": ["a"]}),
    ]
    assert collect_connection_options(plugins) == {"hosts": ["a"]}


def test_ssl_contredit_le_schema():
    with pytest.raises(ValueError, match="https"):
        conf_host_urls({"hosts": ["https://a"], "ssl": False})
    with pytest.raises(ValueError, match="http://"):
        conf_host_urls({"hosts": ["http://a"], "ssl": True})


def test_urls_normalisees():
    assert conf_host_urls({}) is None
    assert conf_host_urls({"hosts": ["//a:1", "b", "HTTP://c"]}) == ["http://a:1", "http://b", "HTTP://c"]
    assert conf_host_urls({"hosts": ["//a"], "ssl": True}) == ["https://a"]


def test_conf_auth_et_apply_sans_options():
    assert conf_auth({}) is None
    args = _args()
    effective = apply_connection_options(args, {})
    assert effective is not args
    assert effective.opensearch_host is None
    assert effective.no_verify_certs is False


def _args(**kw):
    valeurs = {
        "opensearch_host": None,
        "no_verify_certs": False,
        "cacert": None,
        "tls_certificate": None,
        "tls_key": None,
    }
    return argparse.Namespace(**{**valeurs, **kw})


def test_apply_complete_les_options_absentes():
    options = collect_connection_options(
        [
            (
                "opensearch",
                {
                    "hosts": ["os1:9200"],
                    "ssl": True,
                    "ssl_certificate_verification": False,
                    "user": "u",
                    "password": "p",
                    "cacert": "/ca.pem",
                    "tls_certificate": "/c.pem",
                    "tls_key": "/k.pem",
                },
            )
        ]
    )
    effective = apply_connection_options(_args(), options)
    assert effective.opensearch_host == ["https://os1:9200"]
    assert effective.no_verify_certs is True
    assert (effective.cacert, effective.tls_certificate, effective.tls_key) == ("/ca.pem", "/c.pem", "/k.pem")
    assert conf_auth(options) == ("u", "p")


def test_ligne_de_commande_prioritaire():
    options = {"hosts": ["a"], "cacert": "/conf.pem"}
    effective = apply_connection_options(_args(opensearch_host=["https://cli:9200"], cacert="/cli.pem"), options)
    assert effective.opensearch_host == ["https://cli:9200"]
    assert effective.cacert == "/cli.pem"
