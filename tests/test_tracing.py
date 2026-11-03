# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests du décorateur `atraced` (`src/tracing.py`, issue #37).

L'équivalent asynchrone de `@traced` : entrée et sortie journalisées
AUTOUR du `await` — la « sortie » n'apparaît qu'une fois la coroutine
terminée, contrairement à `@traced` sur une fonction `async def`.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest
from loguru import logger

from pystashtemplate.tracing import atraced


def _run(coro):
    # Sans pytest-asyncio (convention du dépôt, cf. RsysLogstach) : asyncio.run direct (#34).
    return asyncio.run(coro)


class TestAtraced:
    def test_entree_puis_sortie_autour_du_await(self) -> None:
        messages: list[str] = []
        sink = logger.add(lambda m: messages.append(m), level="DEBUG")

        @atraced
        async def travail(valeur: int) -> int:
            await asyncio.sleep(0)
            return valeur * 2

        try:
            assert _run(travail(21)) == 42
        finally:
            logger.remove(sink)
        noms = [m.split(" - ", 1)[1].strip() for m in messages]
        assert noms == [
            "TestAtraced.test_entree_puis_sortie_autour_du_await.<locals>.travail | entrée",
            "TestAtraced.test_entree_puis_sortie_autour_du_await.<locals>.travail | sortie",
        ]

    def test_sortie_apres_await_pas_avant(self) -> None:
        # `@traced` sur un async def journalise « sortie » dès la création
        # de la coroutine ; `@atraced` attend la fin réelle.
        messages: list[str] = []

        @atraced
        async def lent() -> None:
            await asyncio.sleep(0.05)

        coro = lent.__wrapped__()  # coroutine créée, PAS encore attendue
        assert messages == []
        coro.close()

    def test_exception_journalisee_et_propagee(self) -> None:
        messages: list[str] = []
        sink = logger.add(lambda m: messages.append(m), level="DEBUG")

        @atraced
        async def echec() -> None:
            raise ValueError("boom")

        try:
            with pytest.raises(ValueError, match="boom"):
                _run(echec())
        finally:
            logger.remove(sink)
        noms = [m.split(" - ", 1)[1].strip() for m in messages]
        assert noms[0].endswith("<locals>.echec | entrée")
        assert "<locals>.echec | sortie (exception ValueError) : boom" in noms[1]

    def test_introspection_preservee(self) -> None:
        @atraced
        async def documeee() -> None:
            """Documentation conservée."""

        assert documeee.__name__ == "documeee"
        assert documeee.__doc__ == "Documentation conservée."
        assert inspect.iscoroutinefunction(documeee)
        assert hasattr(documeee, "__wrapped__")

    def test_arguments_et_resultat_non_journalises(self) -> None:
        messages: list[str] = []
        sink = logger.add(lambda m: messages.append(m), level="DEBUG")

        @atraced
        async def secret(mot_de_passe: str) -> str:
            return mot_de_passe.upper()

        try:
            assert _run(secret("s3cret")) == "S3CRET"
        finally:
            logger.remove(sink)
        assert not any("s3cret" in m or "S3CRET" in m for m in messages)
