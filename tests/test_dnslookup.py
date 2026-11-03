# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de `src/dnslookup.py` et du plugin `dns {}` natif des `.conf` (issue #121).

Aucun vrai serveur DNS : `RSYSLOGSTACH_DNS=off` (conftest) coupe le réseau,
les résolveurs sont des doubles ou des fichiers hosts temporaires, et le
client dnspython est remplacé là où il est testé.
"""

from __future__ import annotations

from types import SimpleNamespace

import dns.exception
import dns.resolver
import pytest

import pystashtemplate.dnslookup as dnslookup
from pystashtemplate.dnslookup import (
    CacheTTL,
    DelaiDepasse,
    DnsFilter,
    FichierHosts,
    ServeurDns,
    _vers_ascii,
    _vers_unicode,
    est_adresse,
    serveurs_normalises,
)
from pystashtemplate.interpreter import PipelineInterpreter


class Double:
    """Résolveur de test : tables fixes, erreurs programmables, appels comptés."""

    def __init__(self, adresses: dict | None = None, noms: dict | None = None, erreurs: list | None = None) -> None:
        self._adresses = adresses or {}
        self._noms = noms or {}
        self._erreurs = list(erreurs or [])
        self.appels = 0

    def _peut_echouer(self) -> None:
        self.appels += 1
        if self._erreurs:
            raise self._erreurs.pop(0)

    def adresses(self, nom: str) -> list[str]:
        self._peut_echouer()
        return self._adresses.get(nom, [])

    def noms(self, adresse: str) -> list[str]:
        self._peut_echouer()
        return self._noms.get(adresse, [])


DOUBLE = {
    "adresses": {"web.local": ["10.0.0.1", "10.0.0.2"], "xn--caf-dma.fr": ["10.0.0.9"]},
    "noms": {"10.0.0.1": ["web.local"]},
}


def _dns(options: dict, doc: dict, *resolveurs: Double) -> bool:
    """Applique `DnsFilter(options)` avec les résolveurs donnés (un double par défaut)."""
    return DnsFilter(options, resolveurs=list(resolveurs) or [Double(**DOUBLE)]).apply(doc)


class TestResolve:
    def test_append(self) -> None:
        doc = {"h": "web.local"}
        assert _dns({"resolve": "h"}, doc)
        assert doc == {"h": ["web.local", "10.0.0.1"]}

    def test_replace_et_tableau(self) -> None:
        doc = {"a": "web.local", "b": ["web.local"]}
        assert _dns({"resolve": ["a", "b"], "action": "replace"}, doc)
        assert doc == {"a": "10.0.0.1", "b": ["10.0.0.1"]}

    def test_append_tableau(self) -> None:
        doc = {"h": ["web.local"]}
        assert _dns({"resolve": ["h"]}, doc)
        assert doc == {"h": ["web.local", "10.0.0.1"]}

    def test_idn(self) -> None:
        doc = {"h": "café.fr"}
        assert _dns({"resolve": ["h"], "action": "replace"}, doc)
        assert doc == {"h": "10.0.0.9"}

    def test_premier_resolveur_qui_repond(self) -> None:
        vide, plein = Double(), Double(adresses={"x": ["1.1.1.1"]})
        doc = {"h": "x"}
        assert _dns({"resolve": ["h"], "action": "replace"}, doc, vide, plein)
        assert doc == {"h": "1.1.1.1"}

    def test_champ_absent_saute(self) -> None:
        doc = {"h": "web.local", "n": None}
        assert _dns({"resolve": ["absent", "n", "h"], "action": "replace"}, doc)
        assert doc["h"] == "10.0.0.1"

    @pytest.mark.parametrize("valeur", [["a", "b"], [], 42])
    def test_valeurs_refusees(self, valeur: object) -> None:
        doc = {"h": valeur, "z": "web.local"}
        assert not _dns({"resolve": ["h", "z"], "action": "replace"}, doc)
        assert doc == {"h": valeur, "z": "web.local"}

    def test_inconnu_arrete(self) -> None:
        doc = {"h": "inconnu", "r": "10.0.0.1"}
        assert not _dns({"resolve": ["h"], "reverse": ["r"], "action": "replace"}, doc)
        assert doc == {"h": "inconnu", "r": "10.0.0.1"}

    def test_nom_idn_invalide(self) -> None:
        doc = {"h": "a" * 70 + ".fr"}
        assert not _dns({"resolve": ["h"]}, doc)


class TestReverse:
    def test_replace(self) -> None:
        doc = {"ip": "10.0.0.1"}
        assert _dns({"reverse": ["ip"], "action": "replace"}, doc)
        assert doc == {"ip": "web.local"}

    def test_resolve_puis_reverse(self) -> None:
        doc = {"h": "web.local", "ip": "10.0.0.1"}
        assert _dns({"resolve": "h", "reverse": "ip", "action": "replace"}, doc)
        assert doc == {"h": "10.0.0.1", "ip": "web.local"}

    @pytest.mark.parametrize("valeur", ["pas-une-ip", "fe80::1%eth0", ["1.2.3.4", "5.6.7.8"], 3])
    def test_refus(self, valeur: object) -> None:
        doc = {"ip": valeur}
        assert not _dns({"reverse": ["ip"]}, doc)
        assert doc == {"ip": valeur}

    def test_inconnue(self) -> None:
        doc = {"ip": "10.9.9.9"}
        assert not _dns({"reverse": ["ip"]}, doc)
        assert doc == {"ip": "10.9.9.9"}

    def test_idn_inverse(self) -> None:
        doc = {"ip": "10.0.0.9"}
        assert _dns({"reverse": ["ip"], "action": "replace"}, doc, Double(noms={"10.0.0.9": ["xn--caf-dma.fr"]}))
        assert doc == {"ip": "café.fr"}


class TestEssaisEtCaches:
    def test_delai_depasse_tag_apres_essais(self) -> None:
        double = Double(erreurs=[DelaiDepasse("t")] * 3)
        doc = {"h": "web.local"}
        assert not _dns({"resolve": ["h"]}, doc, double)
        assert double.appels == 3
        assert doc["tags"] == ["_dnstimeout"]

    def test_delai_depasse_puis_reponse(self) -> None:
        double = Double(adresses={"x": ["1.1.1.1"]}, erreurs=[DelaiDepasse("t"), OSError("réseau")])
        doc = {"h": "x"}
        assert _dns({"resolve": ["h"], "action": "replace", "max_retries": "2"}, doc, double)
        assert doc == {"h": "1.1.1.1"} and double.appels == 3

    def test_erreur_reseau_sans_tag(self) -> None:
        double = Double(erreurs=[OSError("réseau")])
        doc = {"h": "x"}
        assert not _dns({"resolve": ["h"], "max_retries": 0, "tag_on_timeout": "t"}, doc, double)
        assert "tags" not in doc

    def test_erreur_dns_inattendue(self) -> None:
        doc = {"h": "x"}
        assert not _dns({"resolve": ["h"]}, doc, Double(erreurs=[dns.exception.DNSException("x")]))

    def test_cache_des_succes(self) -> None:
        double = Double(adresses={"x": ["1.1.1.1"]})
        filtre = DnsFilter({"resolve": ["h"], "hit_cache_size": 10}, resolveurs=[double])
        for _ in range(3):
            assert filtre.apply({"h": "x"})
        assert double.appels == 1

    def test_cache_des_echecs_et_expiration(self) -> None:
        maintenant = [0.0]
        double = Double()
        filtre = DnsFilter(
            {"resolve": ["h"], "failed_cache_size": 10, "failed_cache_ttl": 5},
            resolveurs=[double],
            horloge=lambda: maintenant[0],
        )
        assert not filtre.apply({"h": "x"})
        assert not filtre.apply({"h": "x"})
        assert double.appels == 1
        maintenant[0] = 6.0
        assert not filtre.apply({"h": "x"})
        assert double.appels == 2

    def test_cache_des_echecs_sur_delai(self) -> None:
        double = Double(erreurs=[DelaiDepasse("t")])
        filtre = DnsFilter({"resolve": ["h"], "failed_cache_size": 1, "max_retries": 0}, resolveurs=[double])
        assert not filtre.apply({"h": "x"})
        doc = {"h": "x"}
        assert not filtre.apply(doc)
        assert double.appels == 1 and "tags" not in doc

    def test_cache_ttl_lru(self) -> None:
        cache = CacheTTL(2, 10, horloge=lambda: 0.0)
        cache.ecrire("a", 1)
        cache.ecrire("b", 2)
        assert cache.lire("a") == 1
        cache.ecrire("c", 3)
        assert (cache.lire("a"), cache.lire("b"), cache.lire("c")) == (1, None, 3)


class TestFichierHosts:
    def test_lecture(self, tmp_path) -> None:
        hosts = tmp_path / "hosts"
        hosts.write_text(
            "# commentaire\n10.0.0.1 web.local web  # fin\n10.0.0.2 web.local\n"
            "::0:1 six\nbizarre nom-bizarre\nseul\n\n",
            encoding="utf-8",
        )
        fichier = FichierHosts(str(hosts))
        assert fichier.adresses("web.local") == ["10.0.0.1", "10.0.0.2"]
        assert fichier.noms("10.0.0.1") == ["web.local", "web"]
        assert fichier.noms("::1") == ["six"]
        assert fichier.adresses("seul") == []
        assert fichier.noms("bizarre") == ["nom-bizarre"]

    def test_absent(self, tmp_path) -> None:
        assert FichierHosts(str(tmp_path / "absent")).adresses("x") == []

    def test_ordre_des_resolveurs(self, tmp_path, monkeypatch) -> None:
        a, b, systeme = tmp_path / "a", tmp_path / "b", tmp_path / "systeme"
        a.write_text("10.0.0.1 x\n", encoding="utf-8")
        b.write_text("10.0.0.2 x\n10.0.0.2 y\n", encoding="utf-8")
        systeme.write_text("10.0.0.3 x\n10.0.0.3 y\n10.0.0.3 z\n", encoding="utf-8")
        monkeypatch.setattr(dnslookup, "HOSTS_SYSTEME", str(systeme))
        doc = {"x": "x", "y": "y", "z": "z", "w": "w"}
        filtre = DnsFilter({"resolve": ["x", "y", "z"], "action": "replace", "hostsfile": [str(a), str(b)]})
        assert filtre.apply(doc)
        assert doc == {"x": "10.0.0.1", "y": "10.0.0.2", "z": "10.0.0.3", "w": "w"}
        assert len(filtre._resolveurs_actifs) == 3


class TestServeurDns:
    @staticmethod
    def _client(monkeypatch, reponses: dict) -> list:
        """Remplace `Resolver.resolve` par une table `(nom, type) → réponse ou exception`."""
        appels: list = []

        def resolve(self, nom, type_, search=True):
            appels.append((nom, type_, self.nameservers, self.lifetime))
            reponse = reponses.get((nom, type_), dns.resolver.NoAnswer())
            if isinstance(reponse, Exception):
                raise reponse
            return [SimpleNamespace(to_text=lambda v=v: v) for v in reponse]

        monkeypatch.setattr(dns.resolver.Resolver, "resolve", resolve)
        monkeypatch.delenv("RSYSLOGSTACH_DNS")
        return appels

    def test_a_puis_aaaa_et_ptr(self, monkeypatch) -> None:
        appels = self._client(
            monkeypatch,
            {("v6.fr", "AAAA"): ["::1"], ("1.0.0.10.in-addr.arpa.", "PTR"): ["web.local."]},
        )
        serveur = ServeurDns(0.5, serveurs_normalises({"address": ["192.0.2.53"], "search": "lan", "ndots": "2"}))
        assert list(serveur.adresses("v6.fr")) == ["::1"]
        assert serveur.noms("10.0.0.1") == ["web.local"]
        assert appels[0] == ("v6.fr", "A", ["192.0.2.53"], 0.5)
        assert serveur._client().ndots == 2

    def test_nxdomain_et_delai(self, monkeypatch) -> None:
        self._client(monkeypatch, {("x", "A"): dns.resolver.NXDOMAIN(), ("t", "A"): dns.exception.Timeout()})
        serveur = ServeurDns(0.1, serveurs_normalises("192.0.2.53"))
        assert list(serveur.adresses("x")) == []
        with pytest.raises(DelaiDepasse):
            list(serveur.adresses("t"))

    def test_configuration_systeme(self, monkeypatch) -> None:
        self._client(monkeypatch, {("x", "A"): ["10.1.1.1"]})
        monkeypatch.setattr(dns.resolver.Resolver, "read_resolv_conf", lambda self, f: None)
        filtre = DnsFilter({"resolve": ["h"], "action": "replace", "timeout": 1})
        assert isinstance(filtre._resolveurs_actifs[-1], ServeurDns)
        doc = {"h": "x"}
        assert filtre.apply(doc)
        assert doc == {"h": "10.1.1.1"}

    def test_coupe_par_variable(self) -> None:
        assert ServeurDns(0.5, serveurs_normalises("192.0.2.53")).noms("10.0.0.1") == []


class TestOptions:
    def test_serveurs_normalises(self) -> None:
        assert serveurs_normalises(["1.1.1.1", "8.8.8.8"]) == {
            "nameserver": ["1.1.1.1", "8.8.8.8"],
            "search": [],
            "ndots": 1,
        }

    @pytest.mark.parametrize(
        "nameserver",
        [
            {"search": ["a"]},
            {"address": []},
            {"address": "x", "search": list("abcdefg")},
            {"address": "x", "ndots": "z"},
            {"address": "x", "ndots": -1},
            {"address": "x", "port": 53},
        ],
    )
    def test_serveurs_invalides(self, nameserver: object) -> None:
        with pytest.raises(ValueError, match="dns"):
            serveurs_normalises(nameserver)

    @pytest.mark.parametrize(
        "options",
        [
            {"action": "supprimer"},
            {"resolve": [1]},
            {"timeout": "${LOGSTASH_DNS_TIMEOUT}"},
            {"hit_cache_size": None},
            {"nameserver": {"search": "x"}},
        ],
    )
    def test_invalides(self, options: dict) -> None:
        with pytest.raises(ValueError, match="dns"):
            DnsFilter(options)

    def test_resolveurs_par_defaut(self) -> None:
        assert len(DnsFilter({"nameserver": "192.0.2.53"})._resolveurs_actifs) == 2
        assert len(DnsFilter({"nameserver": []})._resolveurs_actifs) == 1
        assert DnsFilter({}).apply({"h": "x"})

    def test_adresses_et_idn(self) -> None:
        assert est_adresse("10.0.0.1") and est_adresse("::1") and not est_adresse("010.0.0.1")
        assert _vers_ascii("café.fr") == "xn--caf-dma.fr"
        assert _vers_unicode("xn--caf-dma.fr") == "café.fr"
        assert _vers_unicode("xn--zz.fr") == "xn--zz.fr"
        assert _vers_unicode("é") == "é"


class TestInterpreteur:
    def test_conf_hostsfile(self, tmp_path) -> None:
        hosts = tmp_path / "hosts"
        hosts.write_text("10.0.0.1 web.local\n", encoding="utf-8")
        options = f'reverse => ["[host][ip]"] action => "replace" hostsfile => ["{hosts}"] add_tag => ["dns"]'
        conf = f"filter {{ dns {{ {options} }} }}"
        doc = {"host": {"ip": "10.0.0.1"}}
        PipelineInterpreter(conf).apply(doc)
        assert doc == {"host": {"ip": "web.local"}, "tags": ["dns"]}

    def test_variable_non_resolue_refus_au_chargement(self) -> None:
        # #122 : `${VAR}` non défini sans valeur par défaut est une erreur
        # de configuration au chargement du `.conf`, comme Logstash —
        # avant #122 le bloc était silencieusement ignoré.
        conf = (
            'filter { dns { reverse => ["ip"] hit_cache_size => "${LOGSTASH_DNS_HIT_CACHE_SIZE}" add_tag => ["dns"] } }'
        )
        with pytest.raises(ValueError, match="LOGSTASH_DNS_HIT_CACHE_SIZE"):
            PipelineInterpreter(conf)

    def test_variable_avec_defaut_active_le_bloc(self, tmp_path) -> None:
        # `${VAR:défaut}` : la valeur par défaut sert à la construction.
        hosts = tmp_path / "hosts"
        hosts.write_text("10.0.0.1 web.local\n", encoding="utf-8")
        conf = (
            "filter { dns {"
            ' reverse => ["ip"] action => "replace"'
            ' hostsfile => ["' + str(hosts) + '"]'
            ' hit_cache_size => "${LOGSTASH_DNS_HIT_CACHE_SIZE:100}"'
            ' add_tag => ["dns"] } }'
        )
        interp = PipelineInterpreter(conf)
        doc = {"ip": "10.0.0.1"}
        interp.apply(doc)
        assert doc == {"ip": "web.local", "tags": ["dns"]}
