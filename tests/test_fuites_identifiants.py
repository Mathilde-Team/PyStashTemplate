# SPDX-License-Identifier: GPL-3.0-or-later
"""Aucun secret dans les journaux, même écrit dans l'URL (critère de #5).

Audit du 04/11 : `hosts => "http://user:secret@hote"` faisait sortir le
mot de passe en clair dans le DEBUG de sortie en exception des
décorateurs de traçage (httpx cite l'URL dans ses messages) et dans le
DEBUG de signature de la sortie `doris`. Ces tests passent toutes les
sorties HTTP du registre, avec un secret dans l'URL et dans chaque
option d'authentification qu'elles acceptent, sur un faux serveur qui
répond 401, 400, 429 puis 500 ; aucun message, à aucun niveau, ne doit
contenir le secret.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

import httpx
import pytest
from loguru import logger

from pystashtemplate.outputs import REGISTRY
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput
from pystashtemplate.tracing import atraced, masquer_identifiants, traced

SECRET = "S3cr3tXYZ"
SORTIES_HTTP = sorted(nom for nom, cls in REGISTRY.items() if issubclass(cls, ElasticsearchBulkOutput))


class TestMasquerIdentifiants:
    @pytest.mark.parametrize(
        ("texte", "attendu"),
        [
            ("http://user:secret@hote:9200/x", "http://***@hote:9200/x"),
            ("https://secret@hote", "https://***@hote"),
            ("redis://:secret@redis:6379/0", "redis://***@redis:6379/0"),
            ("amqp://u:p%40ss@mq/vhost", "amqp://***@mq/vhost"),
            ("for url 'http://u:s@h/ready'", "for url 'http://***@h/ready'"),
            ("a http://u:s@h1 b HTTP://v:t@h2", "a http://***@h1 b HTTP://***@h2"),
            ("http://hote:9200/x", "http://hote:9200/x"),
            ("courriel user@exemple.fr", "courriel user@exemple.fr"),
            ("http://hote/chemin@version", "http://hote/chemin@version"),
        ],
    )
    def test_masquage(self, texte: str, attendu: str) -> None:
        assert masquer_identifiants(texte) == attendu

    def test_accepte_tout_objet(self) -> None:
        assert masquer_identifiants(httpx.URL("http://u:s@h/")) == "http://***@h/"
        assert masquer_identifiants(["http://u:s@h"]) == "['http://***@h']"


def _journaux(action) -> list[str]:
    messages: list[str] = []
    sink = logger.add(lambda m: messages.append(str(m)), level="TRACE")
    try:
        action()
    finally:
        logger.remove(sink)
    return messages


class TestDecorateurs:
    def test_traced_masque_le_message_d_exception(self) -> None:
        @traced
        def echoue() -> None:
            raise ValueError(f"for url 'http://u:{SECRET}@h/'")

        def action() -> None:
            with pytest.raises(ValueError):
                echoue()

        messages = _journaux(action)
        assert any("http://***@h/" in m for m in messages)
        assert not any(SECRET in m for m in messages)

    def test_atraced_masque_le_message_d_exception(self) -> None:
        @atraced
        async def echoue() -> None:
            raise ValueError(f"for url 'http://u:{SECRET}@h/'")

        def action() -> None:
            with pytest.raises(ValueError):
                asyncio.run(echoue())

        messages = _journaux(action)
        assert any("http://***@h/" in m for m in messages)
        assert not any(SECRET in m for m in messages)


def _options(nom: str, cls: type) -> dict[str, Any]:
    """Options avec le secret dans l'URL et dans les options d'authentification acceptées."""
    if nom == "http":
        return {"url": f"http://u:{SECRET}@srv:1/x", "headers": {"Authorization": f"Bearer {SECRET}"}}
    options: dict[str, Any] = {"hosts": f"http://u:{SECRET}@srv:1", "retry_max": 0, "retry_backoff": 0}
    propres = set()
    for classe in cls.__mro__:
        propres |= set(getattr(classe, "OPTIONS_PROPRES", None) or {})
    if "api_key" in propres:
        options["api_key"] = SECRET
    elif "token" in propres:
        options["token"] = SECRET
    else:
        options.update(user="u", password=SECRET)
    return options


def _construire(nom: str, transport: httpx.MockTransport) -> ElasticsearchBulkOutput:
    cls = REGISTRY[nom]
    options = _options(nom, cls)
    if nom == "parseable":
        options["stream"] = "s"
    for _ in range(5):
        try:
            return cls(options, transport=transport)
        except ValueError as exc:
            requise = re.search(r"option « (\w+) » est requise", str(exc))
            if requise is None:
                raise
            options[requise.group(1)] = "t"
    raise AssertionError(nom)  # pragma: no cover


@pytest.mark.parametrize("statut", [401, 400, 429, 500])
@pytest.mark.parametrize("nom", SORTIES_HTTP)
def test_aucune_fuite_de_secret(nom: str, statut: int) -> None:
    sortie = _construire(nom, httpx.MockTransport(lambda requete: httpx.Response(statut, text="erreur")))

    async def scenario() -> None:
        try:
            try:
                await sortie.envoyer({"message": "m"})
                await sortie.flush()
            except Exception:  # noqa: BLE001 - seul le contenu des journaux compte ici
                pass
            try:
                await sortie.health()
            except Exception:  # noqa: BLE001
                pass
        finally:
            await sortie.close()

    messages = _journaux(lambda: asyncio.run(scenario()))
    assert messages, "aucun journal : le test ne prouverait rien"
    fuites = [m for m in messages if SECRET in m]
    assert fuites == []


def test_inventaire_des_sorties_http() -> None:
    assert len(SORTIES_HTTP) >= 15
