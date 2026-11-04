# SPDX-License-Identifier: GPL-3.0-or-later
"""Options de connexion lues dans le bloc `opensearch{}` d'un `.conf` Logstash.

Rapatrié de RsysLogstach (issue #125 du connecteur, PyStashTemplate#110).

`output { opensearch { hosts => ["os1:9200"] ssl => true user => "u" ... } }`
reprend les options de connexion de logstash-output-opensearch
(`lib/logstash/plugin_mixins/opensearch/api_configs.rb`) :

- `hosts` : URL ou liste d'URLs ; sans schéma, `https` si `ssl => true`,
  `http` sinon (défaut Logstash) ; port 9200 par défaut ;
- `ssl` : booléen ; s'il est donné et contredit le schéma d'une URL
  (`ssl => true` avec `http://`), erreur de configuration comme Logstash ;
- `ssl_certificate_verification => false` : certificats non vérifiés ;
- `user` / `password` : authentification basique (les deux ensemble) ;
- `cacert`, `tls_certificate`, `tls_key` : chemins PEM (mTLS).

Un outil n'a qu'un client OpenSearch : si plusieurs blocs `opensearch{}` /
`elasticsearch{}` donnent des valeurs différentes pour une même option,
le démarrage échoue. La ligne de commande reste prioritaire, de même que
`OPENSEARCH_USER` / `OPENSEARCH_PASSWORD` sur `user` / `password`.
Les valeurs ne sont pas des gabarits sprintf (Logstash ne les rend pas).
Une valeur qui contient encore `${VAR}` (variable d'environnement non
remplacée, ) est refusée plutôt que de se connecter à un hôte
nommé littéralement `${VAR}`.
"""

from __future__ import annotations

import argparse
from typing import Any

from loguru import logger

from pystashtemplate.tracing import traced

INDEX_PLUGINS = frozenset({"opensearch", "elasticsearch"})
CONNECTION_OPTIONS = (
    "hosts",
    "user",
    "password",
    "ssl",
    "ssl_certificate_verification",
    "cacert",
    "tls_certificate",
    "tls_key",
)
_BOOLEAN_OPTIONS = ("ssl", "ssl_certificate_verification")


@traced
def collect_connection_options(plugins: list[tuple[str, dict[str, Any]]]) -> dict[str, Any]:
    """Options de connexion des plugins `opensearch` / `elasticsearch`, validées et fusionnées.

    Raises:
        ValueError: type invalide, `${VAR}` non remplacée, `user` sans
            `password` (ou l'inverse), ou valeurs différentes d'un bloc à l'autre.
    """
    merged: dict[str, Any] = {}
    for name, options in plugins:
        if name not in INDEX_PLUGINS:
            continue
        for option in CONNECTION_OPTIONS:
            if option not in options:
                continue
            value = _check_value(name, option, options[option])
            if option in merged and merged[option] != value:
                raise ValueError(
                    f"output {name} : option {option!r} différente d'un bloc à l'autre ; "
                    "une seule connexion OpenSearch est possible"
                )
            merged[option] = value
    if ("user" in merged) != ("password" in merged):
        raise ValueError("output opensearch : 'user' et 'password' vont ensemble")
    logger.debug("collect_connection_options | options={}", sorted(merged))
    return merged


@traced
def _check_value(plugin: str, option: str, value: Any) -> Any:
    """Contrôle le type d'une option de connexion et la normalise (`hosts` en liste)."""
    if option == "hosts":
        hosts = [value] if isinstance(value, str) else value
        if not isinstance(hosts, list) or not hosts or not all(isinstance(h, str) and h for h in hosts):
            raise ValueError(f"output {plugin} : 'hosts' attend une URL ou une liste d'URLs")
        checked: Any = hosts
    elif option in _BOOLEAN_OPTIONS:
        checked = _to_bool(plugin, option, value)
    else:
        if not isinstance(value, str) or not value:
            raise ValueError(f"output {plugin} : {option!r} attend une chaîne non vide")
        checked = value
    texts = checked if isinstance(checked, list) else [checked]
    if any(isinstance(t, str) and "${" in t for t in texts):
        raise ValueError(f"output {plugin} : {option!r} contient une variable ${{…}} non remplacée")
    logger.debug("_check_value | {} {}", plugin, option)
    return checked


@traced
def _to_bool(plugin: str, option: str, value: Any) -> bool:
    """Booléen Logstash : `true` / `false`, littéral ou chaîne."""
    if isinstance(value, bool):
        logger.debug("_to_bool | {}={}", option, value)
        return value
    if isinstance(value, str) and value.lower() in ("true", "false"):
        logger.debug("_to_bool | {}={!r} (chaîne)", option, value)
        return value.lower() == "true"
    raise ValueError(f"output {plugin} : {option!r} attend true ou false")


@traced
def conf_host_urls(options: dict[str, Any]) -> list[str] | None:
    """URLs complètes (avec schéma) des `hosts` du `.conf`, ou None s'il n'y en a pas.

    Raises:
        ValueError: `ssl` explicite qui contredit le schéma d'une URL.
    """
    if "hosts" not in options:
        logger.debug("conf_host_urls | pas de hosts")
        return None
    ssl = options.get("ssl")
    default_scheme = "https" if ssl else "http"
    urls: list[str] = []
    for host in options["hosts"]:
        if "://" in host:
            url = host
        elif host.startswith("//"):
            url = f"{default_scheme}:{host}"
        else:
            url = f"{default_scheme}://{host}"
        scheme = url.split("://", 1)[0].lower()
        if ssl is not None and (scheme == "https") != ssl:
            raise ValueError(f"output opensearch : ssl => {str(ssl).lower()} mais l'URL {host!r} est en {scheme}://")
        urls.append(url)
    logger.debug("conf_host_urls | {} URL(s)", len(urls))
    return urls


@traced
def apply_connection_options(args: argparse.Namespace, options: dict[str, Any]) -> argparse.Namespace:
    """Copie de `args` complétée par les options du `.conf` absentes de la ligne de commande."""
    effective = argparse.Namespace(**vars(args))
    if args.opensearch_host is None:
        effective.opensearch_host = conf_host_urls(options)
    if options.get("ssl_certificate_verification") is False:
        effective.no_verify_certs = True
    for option, attribute in (("cacert", "cacert"), ("tls_certificate", "tls_certificate"), ("tls_key", "tls_key")):
        if getattr(args, attribute) is None and option in options:
            setattr(effective, attribute, options[option])
    logger.debug("apply_connection_options | hosts={}", effective.opensearch_host)
    return effective


@traced
def conf_auth(options: dict[str, Any]) -> tuple[str, str] | None:
    """Couple (`user`, `password`) du `.conf`, ou None."""
    if "user" in options:
        logger.debug("conf_auth | user et password présents")
        return (options["user"], options["password"])
    logger.debug("conf_auth | aucune")
    return None
