# SPDX-License-Identifier: GPL-3.0-or-later
"""Sorties agnostiques : contrat `Output`, capacités, registre (chantier #4).

Interface documentée dans `pystashtemplate/outputs/base.py` : cycle
`open`/`send`/`flush`/`close` asynchrone, capacités déclarées par sortie,
registre `REGISTRY` branché sur `PipelineInterpreter.output_plugins` et
sortie factice `null`.
"""

from __future__ import annotations

import asyncio

import pytest

from pystashtemplate.interpreter import PipelineInterpreter
from pystashtemplate.outputs import (
    ACTIONS,
    REGISTRY,
    Capacites,
    NullOutput,
    Output,
    compiler_sorties,
    enregistrer,
    sorties_du_pipeline,
)


class TestCapacites:
    def test_defauts(self) -> None:
        # Cas minimal d'une sortie : elle écrit (index), accepte un _id,
        # répond globalement, sans contrainte de taille.
        capacites = Capacites()
        assert capacites.actions == frozenset({"index"})
        assert capacites.accepte_id_document is True
        assert capacites.accuse_par_element is False
        assert capacites.taille_max_requete is None

    def test_supporte(self) -> None:
        capacites = Capacites(actions=frozenset({"index", "create"}))
        assert capacites.supporte("index") is True
        assert capacites.supporte("delete") is False

    def test_action_inconnue_refusee(self) -> None:
        # Une coquille dans une sous-classe doit sauter aux yeux.
        with pytest.raises(ValueError, match="actions de sortie inconnues.*upsert"):
            Capacites(actions=frozenset({"index", "upsert"}))


def _run(coro):
    # Sans pytest-asyncio (convention du dépôt, cf. RsysLogstach) : asyncio.run direct.
    return asyncio.run(coro)


class TestNullOutput:
    def test_cycle_complet(self) -> None:
        async def scenario() -> None:
            sortie = NullOutput({"n_importe": "quoi"})
            assert sortie.nom_plugin == "null"
            assert sortie.options == {"n_importe": "quoi"}
            assert sortie.ouverte is False
            await sortie.open()
            assert sortie.ouverte is True
            await sortie.send([{"message": "un"}, {"message": "deux"}])
            await sortie.send([{"message": "trois"}])
            assert [e["message"] for e in sortie.livres] == ["un", "deux", "trois"]
            await sortie.close()
            assert sortie.ouverte is False

        _run(scenario())

    def test_async_with(self) -> None:
        # Le contrat doit être utilisable comme gestionnaire asynchrone.
        async def scenario() -> list[dict]:
            async with NullOutput({}) as sortie:
                assert sortie.ouverte is True
                await sortie.send([{"message": "coucou"}])
            assert sortie.ouverte is False
            return sortie.livres

        assert _run(scenario()) == [{"message": "coucou"}]

    def test_async_with_exception(self) -> None:
        # Une exception dans le bloc doit fermer la sortie ET remonter.
        async def scenario() -> bool:
            with pytest.raises(RuntimeError, match="boom"):
                async with NullOutput({}) as sortie:
                    raise RuntimeError("boom")
            return sortie.ouverte

        assert _run(scenario()) is False

    def test_flush_sans_effet(self) -> None:
        # Les sorties sans tampon n'ont rien à vider : le défaut ne fait rien.
        async def scenario() -> None:
            sortie = NullOutput({})
            await sortie.open()
            await sortie.flush()
            await sortie.close()

        _run(scenario())

    def test_flush_et_close_par_defaut(self) -> None:
        # `flush()` et `close()` ont un défaut sans effet : une sous-classe
        # minimale n'a que `open()` et `send()` à écrire.

        class SortieMinimale(Output):
            nom_plugin = "minimale"

            async def open(self) -> None:
                return None

            async def send(self, lot: list[dict]) -> None:
                return None

        async def scenario() -> None:
            sortie = SortieMinimale({})
            await sortie.open()
            await sortie.flush()
            await sortie.close()

        _run(scenario())

    def test_capacites_declarees(self) -> None:
        # `null` accepte tout, pour servir de cible de test universelle.
        assert NullOutput.capacites.actions == ACTIONS
        assert NullOutput.capacites.accepte_id_document is True
        assert NullOutput.capacites.accuse_par_element is True

    def test_output_abstraite(self) -> None:
        # Le contrat ne s'instancie pas : une sous-classe doit exister.
        with pytest.raises(TypeError, match="abstract"):
            Output({})  # type: ignore[abstract]


class TestRegistre:
    def test_null_enregistree(self) -> None:
        assert REGISTRY["null"] is NullOutput

    def test_enregistrer(self) -> None:
        # Une sortie externe peut rejoindre le registre sans toucher au paquet.

        class SortieExterne(Output):
            nom_plugin = "externe"

            async def open(self) -> None:
                return None

            async def send(self, lot: list[dict]) -> None:
                return None

        try:
            assert enregistrer(SortieExterne) is SortieExterne
            assert REGISTRY["externe"] is SortieExterne
            [sortie] = compiler_sorties([("externe", {})])
            assert isinstance(sortie, SortieExterne)
        finally:
            del REGISTRY["externe"]

    def test_plugin_inconnu(self) -> None:
        # Erreur claire à la compilation, traduite, qui nomme le fautif
        # et les sorties disponibles.
        with pytest.raises(ValueError, match="Sortie inconnue : « inconnu ».*disponibles : .*null"):
            compiler_sorties([("null", {}), ("inconnu", {"hosts": ["x"]})])


class TestBranchementInterpreteur:
    def test_sorties_du_pipeline(self) -> None:
        # `output_plugins` du .conf → instances du registre : branches `if`
        # comprises, options brutes transmises.
        interpreter = PipelineInterpreter(
            """
            output {
              if [gravite] == "haute" {
                null { priorite => "haute" }
              } else {
                null { priorite => "basse" }
              }
            }
            """
        )
        sorties = sorties_du_pipeline(interpreter)
        assert len(sorties) == 2
        assert all(isinstance(sortie, NullOutput) for sortie in sorties)
        assert [sortie.options.get("priorite") for sortie in sorties] == ["haute", "basse"]

    def test_plugin_inconnu_du_pipeline(self) -> None:
        # Un `.conf` avec une sortie non enregistrée est refusé à la
        # compilation du pipeline, pas à l'envoi du premier message.
        interpreter = PipelineInterpreter("output { moteur_a_vapeur {} }")
        with pytest.raises(ValueError, match="moteur_a_vapeur"):
            sorties_du_pipeline(interpreter)

    def test_pipeline_sans_sortie(self) -> None:
        # Un `.conf` sans `output{}` compile zéro sortie (le connecteur
        # garde alors sa destination statique, cf. `has_output`).
        interpreter = PipelineInterpreter("filter { mutate { add_field => {a => 1} } }")
        assert sorties_du_pipeline(interpreter) == []
