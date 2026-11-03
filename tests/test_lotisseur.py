# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests du lotisseur de sorties (issue #6) : lots par nombre/taille/délai,
contre-pression, concurrence limitée, arrêt propre sans perte d'événement.

Les méthodes asynchrones sont pilotées par `asyncio.run()` (le dépôt
n'utilise pas pytest-asyncio, l'issue #38 s'en charge).
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

import pytest

from pystashtemplate.outputs import Capacites, Output
from pystashtemplate.outputs.lotisseur import Lotisseur, MoteurSorties, _taille_document


class SortieEspionne(Output):
    """Sortie qui enregistre les lots reçus, avec envoi ralentissable."""

    plugin: ClassVar[str] = "espionne"
    capacites: ClassVar[Capacites] = Capacites()

    def __init__(self, options: dict[str, Any] | None = None, *, delai_envoi: float = 0.0) -> None:
        super().__init__(dict(options or {}))
        self.lots: list[list[dict]] = []
        self.delai_envoi = delai_envoi
        self.en_cours = 0
        self.pic_concurrence = 0

    async def open(self) -> None:
        pass

    async def send(self, batch) -> None:
        self.en_cours += 1
        self.pic_concurrence = max(self.pic_concurrence, self.en_cours)
        if self.delai_envoi:
            await asyncio.sleep(self.delai_envoi)
        self.lots.append(list(batch))
        self.en_cours -= 1

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


def _documents(n: int) -> list[dict]:
    return [{"message": f"événement {i}"} for i in range(n)]


class TestLots:
    def test_lot_par_nombre(self) -> None:
        async def scenario() -> None:
            sortie = SortieEspionne()
            async with Lotisseur(sortie, {"batch_size": 3, "flush_interval": 60}) as lotisseur:
                for document in _documents(7):
                    await lotisseur.ajouter(document)
            # 7 documents, lots de 3 : 3+3 partis, le reste (1) part à l'arrêt.
            tailles = [len(lot) for lot in sortie.lots]
            assert tailles == [3, 3, 1]
            assert [d for lot in sortie.lots for d in lot] == _documents(7)

        asyncio.run(scenario())

    def test_lot_par_taille(self) -> None:
        async def scenario() -> None:
            sortie = SortieEspionne()
            gros = {"message": "x" * 100}
            async with Lotisseur(sortie, {"batch_size": 100, "batch_bytes": 150, "flush_interval": 60}) as lotisseur:
                await lotisseur.ajouter(gros)
                await lotisseur.ajouter(gros)
                await lotisseur.ajouter(gros)
            tailles = [len(lot) for lot in sortie.lots]
            assert tailles == [2, 1]  # deux documents ~100 o dépassent 150 o ensemble

        asyncio.run(scenario())

    def test_lot_par_delai(self) -> None:
        async def scenario() -> None:
            sortie = SortieEspionne()
            async with Lotisseur(sortie, {"batch_size": 100, "flush_interval": 0.05}) as lotisseur:
                await lotisseur.ajouter({"message": "seul"})
                await asyncio.sleep(0.3)
            assert [len(lot) for lot in sortie.lots] == [1]  # parti sans être plein

        asyncio.run(scenario())

    def test_lot_vide_jamais_envoye(self) -> None:
        async def scenario() -> None:
            sortie = SortieEspionne()
            lotisseur = Lotisseur(sortie, {"batch_size": 10})
            await lotisseur.demarrer()
            await lotisseur.arreter()  # aucun document : aucun envoi
            assert sortie.lots == []

        asyncio.run(scenario())

    def test_taille_document_non_serialisable(self) -> None:
        document: dict[str, Any] = {}
        document["moi"] = document  # référence circulaire

        assert _taille_document({"ok": 1}) > 0
        assert _taille_document(document) == 0  # json échoue : 0 octet, sans lever


class TestContrePression:
    def test_file_pleine_bloque_puis_reprend(self) -> None:
        async def scenario() -> None:
            sortie = SortieEspionne(delai_envoi=0.15)
            # batch_size=1, max_concurrency=1 : le 1er document occupe l'unique
            # créneau d'envoi pendant 0,15 s ; le 2e bloque la boucle sur le
            # sémaphore, le 3e remplit la file (taille 1) et le 4e bloque
            # `ajouter()` : la contre-pression remonte jusqu'à l'appelant.
            async with Lotisseur(
                sortie, {"batch_size": 1, "taille_file": 1, "max_concurrency": 1, "flush_interval": 60}
            ) as lotisseur:
                await lotisseur.ajouter({"n": 1})
                await lotisseur.ajouter({"n": 2})
                await lotisseur.ajouter({"n": 3})
                bloque = asyncio.Event()

                async def ajoute_bloque() -> None:
                    await lotisseur.ajouter({"n": 4})
                    bloque.set()

                tache = asyncio.create_task(ajoute_bloque())
                await asyncio.sleep(0.03)
                assert not bloque.is_set()  # contre-pression : l'ajout attend
                await asyncio.wait_for(bloque.wait(), 3)
                await tache
            assert sum(len(lot) for lot in sortie.lots) == 4

        asyncio.run(scenario())

    def test_ajout_refuse_apres_arret(self) -> None:
        async def scenario() -> None:
            lotisseur = Lotisseur(SortieEspionne(), {"batch_size": 10})
            await lotisseur.demarrer()
            await lotisseur.arreter()
            with pytest.raises(RuntimeError, match="arrêté"):
                await lotisseur.ajouter({"n": 1})

        asyncio.run(scenario())

    def test_arret_sans_demarrage_refuse(self) -> None:
        async def scenario() -> None:
            lotisseur = Lotisseur(SortieEspionne())
            with pytest.raises(RuntimeError, match="jamais démarré"):
                await lotisseur.arreter()

        asyncio.run(scenario())


class TestConcurrence:
    def test_max_concurrence_respecte(self) -> None:
        async def scenario() -> None:
            sortie = SortieEspionne(delai_envoi=0.05)
            async with Lotisseur(sortie, {"batch_size": 1, "max_concurrency": 2, "flush_interval": 60}) as lotisseur:
                for document in _documents(6):
                    await lotisseur.ajouter(document)
                await asyncio.sleep(0.3)
            assert sortie.pic_concurrence <= 2
            assert sortie.pic_concurrence == 2  # et la parallélisation a bien eu lieu

        asyncio.run(scenario())


class TestRobustesse:
    def test_echec_envoi_journalise_sans_casser_la_boucle(self) -> None:
        class SortieFragile(SortieEspionne):
            def __init__(self) -> None:
                super().__init__()
                self.appels = 0

            async def send(self, batch) -> None:
                self.appels += 1
                if self.appels == 1:
                    raise RuntimeError("sortie indisponible")
                await super().send(batch)

        async def scenario() -> None:
            sortie = SortieFragile()
            async with Lotisseur(sortie, {"batch_size": 1, "flush_interval": 60}) as lotisseur:
                for document in _documents(2):
                    await lotisseur.ajouter(document)
            # Le 1er lot échoue (journalisé), le 2e passe : la boucle survit.
            assert sortie.appels == 2
            assert [d["message"] for lot in sortie.lots for d in lot] == ["événement 1"]

        asyncio.run(scenario())

    def test_demarrer_idempotent(self) -> None:
        async def scenario() -> None:
            lotisseur = Lotisseur(SortieEspionne())
            await lotisseur.demarrer()
            premiere = lotisseur._tache
            await lotisseur.demarrer()  # déjà lancé : rien de neuf
            assert lotisseur._tache is premiere
            await lotisseur.arreter()

        asyncio.run(scenario())

    def test_arret_via_contexte_sans_demarrage(self) -> None:
        async def scenario() -> None:
            lotisseur = Lotisseur(SortieEspionne())
            # `__aexit__` sans `demarrer` : rien à arrêter, aucune erreur.
            await lotisseur.__aexit__(None, None, None)
            assert lotisseur._tache is None

        asyncio.run(scenario())


class TestArret:
    def test_arret_propre_sans_perte(self) -> None:
        async def scenario() -> None:
            sortie = SortieEspionne()
            async with Lotisseur(sortie, {"batch_size": 3, "flush_interval": 60}) as lotisseur:
                for document in _documents(10):
                    await lotisseur.ajouter(document)
            # 10 documents : rien ne doit être perdu, dans l'ordre.
            assert [d for lot in sortie.lots for d in lot] == _documents(10)

        asyncio.run(scenario())

    def test_arret_force_au_dela_du_delai(self) -> None:
        async def scenario() -> None:
            sortie = SortieEspionne(delai_envoi=5.0)  # bloqué
            lotisseur = Lotisseur(sortie, {"batch_size": 1, "delai_arret": 0.1, "flush_interval": 60})
            await lotisseur.demarrer()
            await lotisseur.ajouter({"n": 1})
            await asyncio.sleep(0.05)  # laisse le lot partir vers l'envoi bloqué
            await lotisseur.arreter()  # délai maximal dépassé : arrêt forcé
            assert lotisseur._tache.done()

        asyncio.run(scenario())


class TestMoteurSorties:
    def test_routage_par_indices(self) -> None:
        async def scenario() -> None:
            sorties = [SortieEspionne() for _ in range(3)]
            moteur = MoteurSorties(sorties, [{"batch_size": 10}, {"batch_size": 10}, {"batch_size": 10}])
            await moteur.demarrer()
            await moteur.emettre({"message": "pour tous"})
            await moteur.emettre({"message": "pour la 0"}, indices=[0])
            await moteur.emettre({"message": "pour la 2"}, indices=[2])
            await moteur.arreter()
            # Documents reçus par sortie : 2 / 1 / 2 (pas le nombre de lots).
            assert [sum(len(lot) for lot in s.lots) for s in sorties] == [2, 1, 2]

        asyncio.run(scenario())

    def test_options_par_sortie(self) -> None:
        async def scenario() -> None:
            sorties = [SortieEspionne(), SortieEspionne()]
            moteur = MoteurSorties(sorties, [{"batch_size": 1}, {"batch_size": 10}])
            await moteur.demarrer()
            await moteur.emettre({"n": 1})
            await moteur.emettre({"n": 2})
            await asyncio.sleep(0.05)
            # La sortie 0 (batch_size=1) a déjà envoyé 2 lots d'1 document,
            # la sortie 1 (batch_size=10) attend encore.
            assert [len(s.lots) for s in sorties] == [2, 0]
            await moteur.arreter()

        asyncio.run(scenario())


class TestOptions:
    @pytest.mark.parametrize(
        "option", ["batch_size", "flush_interval", "taille_file", "max_concurrency", "delai_arret"]
    )
    def test_option_nulle_ou_negative_refusee(self, option: str) -> None:
        for valeur in (0, -1):
            with pytest.raises(ValueError, match=option):
                Lotisseur(SortieEspionne(), {option: valeur})

    def test_batch_bytes_nul_refuse(self) -> None:
        with pytest.raises(ValueError, match="batch_bytes"):
            Lotisseur(SortieEspionne(), {"batch_bytes": 0})
