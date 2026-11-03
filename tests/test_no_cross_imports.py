# SPDX-License-Identifier: GPL-3.0-or-later
"""Découpage #111 : le graphe de dépendances des modules de la bibliothèque
reste acyclique, et la bibliothèque n'importe jamais le connecteur.

Adapté de RsysLogstach (issue #25, vérification via `ast` — règle
documentée dans PATTERNS.md, « Absence de dépendances circulaires »).

Vérifie en plus les invariants cités par les docstrings :

- `tracing` est une feuille (aucun import interne) et `util` n'importe
  que `tracing` ;
- `enrichment` ne dépend que de `tracing` ;
- `i18n` ne dépend que de `tracing` ;
- aucun module de la bibliothèque n'importe le connecteur
  (omprog, indexer, dlq, metrics, opensearchconf) : c'est le sens du
  découpage #111 — le connecteur dépend de la bibliothèque, jamais
  l'inverse.
"""

from __future__ import annotations

import ast
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src" / "pystashtemplate"
MODULES = sorted(p.stem for p in SRC.glob("*.py") if p.stem != "__init__")
MODULES_CONNECTEUR = {"omprog", "indexer", "dlq", "metrics", "opensearchconf"}


def _imports_of(module_name: str) -> set[str]:
    """Imports internes (vers un autre module du paquet) d'un module, via ast."""
    tree = ast.parse((SRC / f"{module_name}.py").read_text(encoding="utf-8"))
    internal: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            internal.update(alias.name.split(".")[1] for alias in node.names if alias.name.startswith("pystashtemplate."))
        elif isinstance(node, ast.ImportFrom) and node.module:
            # `from pystashtemplate.X import ...` → X
            if node.module.startswith("pystashtemplate"):
                internal.add(node.module.split(".")[1] or "")
    return internal & set(MODULES)


def test_modules_de_src_sont_bien_couverts() -> None:
    # Garde-fou : si un module est ajouté/renommé dans le paquet, ce test l'exige ici.
    assert MODULES, "aucun module trouvé dans src/pystashtemplate/"


def test_graphe_sans_cycle() -> None:
    # Détection de cycle par DFS : depuis chaque module, on ne doit jamais
    # retomber sur un nœud déjà présent dans le chemin courant.
    graph = {m: _imports_of(m) for m in MODULES}
    for start in MODULES:
        visited: set[str] = set()
        stack: list[tuple[str, frozenset[str]]] = [(start, frozenset())]
        while stack:
            current, path = stack.pop()
            for dep in graph[current]:
                assert dep not in path, f"cycle détecté : {' → '.join([*sorted(path), dep])} → {start}"
                if dep not in visited:
                    visited.add(dep)
                    stack.append((dep, path | {current}))


def test_tracing_est_une_feuille_et_util_ne_depend_que_de_tracing() -> None:
    # tracing.py : aucun module interne importé ; util.py : tracing uniquement.
    assert _imports_of("tracing") == set(), (
        f"tracing ne devrait importer aucun module interne (trouvé: {_imports_of('tracing')})"
    )
    assert _imports_of("util") <= {"tracing"}, f"util ne devrait importer que tracing (trouvé: {_imports_of('util')})"


def test_enrichment_ne_depend_que_de_tracing() -> None:
    # Docstring d'enrichment.py : « Ne dépend que de `tracing` ».
    assert _imports_of("enrichment") <= {"tracing"}


def test_i18n_ne_depend_que_de_tracing() -> None:
    # Docstring d'i18n.py (issue #38) : importé par filters et le connecteur,
    # il ne doit dépendre d'aucun d'eux.
    assert _imports_of("i18n") <= {"tracing"}


def test_bibliotheque_n_importe_jamais_le_connecteur() -> None:
    # Découpage #111 : le connecteur (omprog, indexer, dlq, metrics,
    # opensearchconf) reste dans RsysLogstach et dépend de la bibliothèque —
    # jamais l'inverse.
    for m in MODULES:
        arbre = ast.parse((SRC / f"{m}.py").read_text(encoding="utf-8"))
        for node in ast.walk(arbre):
            noms: list[str] = []
            if isinstance(node, ast.Import):
                noms = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                noms = [node.module]
            for nom in noms:
                for c in MODULES_CONNECTEUR:
                    assert not re.match(rf"^(pystashtemplate\.)?{c}\b", nom), f"{m} importe le connecteur ({nom})"


import re  # noqa: E402
