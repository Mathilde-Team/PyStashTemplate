# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests du décorateur `atraced` (`src/tracing.py`, issue #37).

L'équivalent asynchrone de `@traced` : entrée et sortie journalisées
AUTOUR du `await` — la « sortie » n'apparaît qu'une fois la coroutine
terminée, contrairement à `@traced` sur une fonction `async def`.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
from pathlib import Path

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


def _coroutines_de_src() -> list[tuple[str, ast.AsyncFunctionDef]]:
    """Toutes les fonctions `async def` de `src/pystashtemplate` (chemin, nœud)."""
    racine = Path(__file__).resolve().parents[1] / "src" / "pystashtemplate"
    trouvees: list[tuple[str, ast.AsyncFunctionDef]] = []
    for chemin in sorted(racine.rglob("*.py")):
        arbre = ast.parse(chemin.read_text(encoding="utf-8"))
        for noeud in ast.walk(arbre):
            if isinstance(noeud, ast.AsyncFunctionDef):
                trouvees.append((f"{chemin.relative_to(racine)}:{noeud.lineno} {noeud.name}", noeud))
    return trouvees


def _decorateurs(noeud: ast.AsyncFunctionDef) -> set[str]:
    return {ast.unparse(d).split("(")[0].rsplit(".", 1)[-1] for d in noeud.decorator_list}


def _exemptee(noeud: ast.AsyncFunctionDef) -> bool:
    """Méthode abstraite, signature de Protocol (corps vide), générateur asynchrone, ou `atraced` lui-même."""
    if "abstractmethod" in _decorateurs(noeud) or noeud.name == "wrapper":
        return True
    corps = [n for n in noeud.body if not (isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant))]
    if not corps:
        return True
    # `atraced` attend une coroutine : il ne s'applique pas à un générateur asynchrone (`yield`).
    return any(isinstance(n, (ast.Yield, ast.YieldFrom)) for n in ast.walk(noeud))


class TestRegleAtraced:
    """Critère de #37 : toutes les fonctions `async def` de `src/` sont décorées `@atraced`."""

    def test_inventaire_non_vide(self) -> None:
        assert len(_coroutines_de_src()) > 100

    def test_aucun_traced_sur_une_coroutine(self) -> None:
        # `@traced` sur une coroutine journalise la « sortie » avant même l'`await`.
        fautives = [nom for nom, noeud in _coroutines_de_src() if "traced" in _decorateurs(noeud)]
        assert fautives == []

    def test_toutes_les_coroutines_sont_atraced(self) -> None:
        manquantes = [
            nom for nom, noeud in _coroutines_de_src() if not _exemptee(noeud) and "atraced" not in _decorateurs(noeud)
        ]
        assert manquantes == []

    def test_exemptions(self) -> None:
        source = (
            "async def protocole(self):\n    '''Signature seule.'''\n"
            "async def generateur(self):\n    yield 1\n"
            "async def normale(self):\n    return 1\n"
        )
        noeuds = [n for n in ast.parse(source).body if isinstance(n, ast.AsyncFunctionDef)]
        assert [_exemptee(n) for n in noeuds] == [True, True, False]
