# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests du socle des sorties agnostiques (issue #4) : interface `Output`,
capacités, registre, sortie factice `null` et compilation des sorties
d'un pipeline — erreur claire et traduite pour un plugin inconnu.

Les méthodes asynchrones sont pilotées par `asyncio.run()` : le dépôt
n'utilise pas pytest-asyncio (l'issue #38 généralisera les tests
asyncio).
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
    SortieInconnueError,
    compiler_sorties,
    enregistrer,
)


def _pipeline_avec_sortie(plugin: str, options: str = "") -> PipelineInterpreter:
    return PipelineInterpreter(f"output {{ {plugin} {{ {options} }} }}")


class TestCapacites:
    def test_valeurs_par_defaut(self) -> None:
        capacites = Capacites()
        assert capacites.actions == frozenset(ACTIONS)
        assert capacites.accepte_id is True
        assert capacites.accuse_par_evenement is True
        assert capacites.taille_max_requete is None

    def test_sous_ensemble_declare(self) -> None:
        capacites = Capacites(
            actions=frozenset({"create"}), accepte_id=False, accuse_par_evenement=False, taille_max_requete=10_485_760
        )
        assert capacites.verifie_action("create") is None  # supportée : sans effet
        with pytest.raises(ValueError, match="non supportée par cette sortie"):
            capacites.verifie_action("index")

    def test_verifie_action_accepte(self) -> None:
        Capacites().verifie_action("index")  # ne lève pas


class TestSortieNull:
    def test_insrite_au_registre(self) -> None:
        assert REGISTRY["null"] is NullOutput

    def test_cycle_de_vie_sans_effet(self) -> None:
        async def scenario() -> list[str]:
            async with NullOutput({}) as sortie:
                await sortie.send([{"message": "un"}, {"message": "deux"}])
                await sortie.flush()
            return ["ok"]

        assert asyncio.run(scenario()) == ["ok"]


class TestCompilerSorties:
    def test_une_sortie_par_bloc_output(self) -> None:
        interp = _pipeline_avec_sortie("null")
        sorties = compiler_sorties(interp)
        assert len(sorties) == 1
        assert isinstance(sorties[0], NullOutput)
        interp.close()

    def test_options_du_bloc_transmises(self) -> None:
        interp = _pipeline_avec_sortie("null", 'champ => "valeur"')
        (sortie,) = compiler_sorties(interp)
        assert sortie.options == {"champ": "valeur"}
        interp.close()

    def test_pipeline_sans_output_donne_rien(self) -> None:
        interp = PipelineInterpreter("filter { mutate { add_field => {a => 1} } }")
        assert compiler_sorties(interp) == []
        interp.close()

    def test_plugin_inconnu_erreur_claire_et_traduite(self) -> None:
        interp = _pipeline_avec_sortie("inexistant")
        with pytest.raises(SortieInconnueError) as excinfo:
            compiler_sorties(interp)
        message = str(excinfo.value)
        assert "« inexistant »" in message
        assert "null" in message  # les sorties disponibles sont nommées
        assert excinfo.value.plugin == "inexistant"
        assert excinfo.value.disponibles == ["null"]
        interp.close()

    def test_erreur_transmise_en_valueerror(self) -> None:
        interp = _pipeline_avec_sortie("inconnu")
        with pytest.raises(ValueError):
            compiler_sorties(interp)
        interp.close()


class TestAsyncWith:
    def test_open_et_close_appelles(self) -> None:
        etats: list[str] = []

        class SortieEspionne(Output):
            plugin = "espionne"

            async def open(self) -> None:
                etats.append("open")

            async def send(self, batch) -> None:
                etats.append("send")

            async def flush(self) -> None:
                etats.append("flush")

            async def close(self) -> None:
                etats.append("close")

        async def scenario() -> None:
            async with SortieEspionne({}) as sortie:
                await sortie.send([])
                await sortie.flush()

        asyncio.run(scenario())
        assert etats == ["open", "send", "flush", "close"]

    def test_close_meme_sur_exception(self) -> None:
        etats: list[str] = []

        class SortieQuiEchoue(Output):
            plugin = "echoue"

            async def open(self) -> None:
                etats.append("open")

            async def send(self, batch) -> None:
                raise RuntimeError("panne")

            async def flush(self) -> None:
                etats.append("flush")

            async def close(self) -> None:
                etats.append("close")

        async def scenario() -> None:
            async with SortieQuiEchoue({}) as sortie:
                await sortie.send([])

        with pytest.raises(RuntimeError, match="panne"):
            asyncio.run(scenario())
        assert etats == ["open", "close"]


class TestRegistre:
    def test_sortie_abstraite_non_instanciable(self) -> None:
        with pytest.raises(TypeError):
            Output({})  # type: ignore[abstract]

    def test_double_inscription_refusee(self) -> None:
        @enregistrer
        class SortieDejaPrise(Output):
            plugin = "doublon"

            async def open(self) -> None:
                pass

            async def send(self, batch) -> None:
                pass

            async def flush(self) -> None:
                pass

            async def close(self) -> None:
                pass

        try:
            with pytest.raises(ValueError, match="déjà inscrite"):
                enregistrer(SortieDejaPrise)
        finally:
            del REGISTRY["doublon"]
