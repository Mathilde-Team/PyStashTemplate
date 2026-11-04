# SPDX-License-Identifier: GPL-3.0-or-later
"""Tests de la DLQ générique des sorties (issue #7).

Nom de la sortie et raison dans chaque entrée, compatibilité avec le
format historique de RsysLogstach, échecs transitoires épuisés envoyés
en DLQ (option `dlq_transitoires`), relecture et rejeu des entrées.
Faux serveur `httpx.MockTransport` : aucune requête réseau réelle ;
`asyncio.run` directement (pas de pytest-asyncio, convention du dépôt).
"""

from __future__ import annotations

import asyncio
import gzip
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any, ClassVar

import httpx
import pytest

from pystashtemplate.dlq import (
    REASON_PERMANENT,
    REASON_TRANSIENT_EXHAUSTED,
    LocalDeadLetterQueue,
    read_entries,
)
from pystashtemplate.outputs import Output
from pystashtemplate.outputs.elasticsearch_bulk import ElasticsearchBulkOutput, SendOutcome
from pystashtemplate.outputs.fichier import FileOutput
from pystashtemplate.outputs.null import NullOutput
from pystashtemplate.outputs.opensearch import OpenSearchOutput
from pystashtemplate.outputs.rejeu import RapportRejeu, rejouer_dlq

CLES_HISTORIQUES = ["@dlq_timestamp", "@dlq_index", "@dlq_error", "document"]


class FauxServeur:
    """Répond à chaque POST /_bulk selon un scénario (voir test_elasticsearch_bulk)."""

    def __init__(self, scenario: list[tuple[str, Any]]) -> None:
        self.scenario = list(scenario)
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        kind, payload = self.scenario.pop(0) if self.scenario else ("ok", None)
        if kind == "reseau":
            raise httpx.ConnectError("connexion refusée (test)")
        if kind == "statut":
            return httpx.Response(payload, text="")
        if kind == "refus":
            return httpx.Response(200, json={"items": [{"index": {"status": 400, "error": {"type": payload}}}]})
        nombre = request.read().decode("utf-8").count("\n") // 2
        return httpx.Response(200, json={"items": [{"index": {"status": 201}}] * nombre})


def bulk(
    serveur: FauxServeur,
    dlq: LocalDeadLetterQueue | None = None,
    classe: type = ElasticsearchBulkOutput,
    **options: Any,
) -> ElasticsearchBulkOutput:
    """Sortie _bulk branchée sur le faux serveur."""
    return classe(
        {"hosts": "https://localhost:9200", "index": "logs-test", "retry_backoff": 0.0, **options},
        transport=httpx.MockTransport(serveur.handler),
        dlq=dlq,
    )


# --- Format des entrées ----------------------------------------------------


def test_entree_sans_sortie_ni_raison_garde_le_format_historique(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    asyncio.run(dlq.write({"m": 1}, index="i", error="e"))
    entree = json.loads((tmp_path / "dlq.jsonl").read_text())
    assert list(entree) == CLES_HISTORIQUES


def test_entree_porte_la_sortie_et_la_raison(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    asyncio.run(dlq.write({"m": 1}, index="i", error="e", output="es-prod", reason=REASON_PERMANENT))
    entree = json.loads((tmp_path / "dlq.jsonl").read_text())
    assert entree["@dlq_output"] == "es-prod"
    assert entree["@dlq_reason"] == "permanent"
    assert list(entree)[-1] == "document"


# --- Nom de la sortie --------------------------------------------------------


def test_nom_de_sortie_option_id_sinon_plugin_sinon_classe() -> None:
    assert NullOutput({"id": "jetable"}).nom == "jetable"
    assert NullOutput({}).nom == "null"
    assert bulk(FauxServeur([]), classe=OpenSearchOutput).nom == "opensearch"
    assert bulk(FauxServeur([])).nom == "ElasticsearchBulkOutput"


def test_option_id_acceptee_par_la_famille_bulk() -> None:
    assert bulk(FauxServeur([]), id="principal").nom == "principal"


# --- Échecs permanents ----------------------------------------------------------


def test_echec_permanent_bulk_inscrit_sortie_et_raison(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    s = bulk(FauxServeur([("refus", "mapper_parsing_exception")]), dlq, classe=OpenSearchOutput, id="os")
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.DEAD_LETTERED
    (entree,) = read_entries(tmp_path / "dlq.jsonl")
    assert entree["@dlq_output"] == "os"
    assert entree["@dlq_reason"] == REASON_PERMANENT
    assert entree["@dlq_index"] == "logs-test"


def test_echec_permanent_fichier_inscrit_sortie_et_raison(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sortie = FileOutput({"path": str(tmp_path / "t.ndjson"), "dlq_path": str(tmp_path / "dlq.jsonl")})

    def _panique(chemin_rendu: str, document: Any) -> str:
        raise OSError("disque plein")

    monkeypatch.setattr(sortie, "_ecrire", _panique)
    asyncio.run(sortie.send([{"message": "a"}]))
    (entree,) = read_entries(tmp_path / "dlq.jsonl")
    assert entree["@dlq_output"] == "file"
    assert entree["@dlq_reason"] == REASON_PERMANENT


# --- Échecs transitoires épuisés --------------------------------------------------


@pytest.mark.parametrize("panne", [("reseau", None), ("statut", 503), ("statut", 429)])
def test_transitoire_epuise_va_en_dlq_avec_l_option(tmp_path: Path, panne: tuple[str, Any]) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    serveur = FauxServeur([panne, panne])
    s = bulk(serveur, dlq, retry_max=1, dlq_transitoires=True)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.DEAD_LETTERED
    assert len(serveur.requests) == 2  # les réessais ont bien eu lieu avant la DLQ
    (entree,) = read_entries(tmp_path / "dlq.jsonl")
    assert entree["@dlq_reason"] == REASON_TRANSIENT_EXHAUSTED
    assert "réessais épuisés" in entree["@dlq_error"]


@pytest.mark.parametrize("panne", [("reseau", None), ("statut", 503)])
def test_transitoire_epuise_reste_retryable_par_defaut(tmp_path: Path, panne: tuple[str, Any]) -> None:
    # Parité RsysLogstach : sans l'option, l'appelant (rsyslog) retient le document.
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    s = bulk(FauxServeur([panne]), dlq)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.RETRYABLE
    assert read_entries(tmp_path / "dlq.jsonl") == []


def test_transitoire_epuise_sans_dlq_reste_retryable() -> None:
    s = bulk(FauxServeur([("reseau", None)]), dlq_transitoires=True)
    assert asyncio.run(s.envoyer({"m": "x"})) is SendOutcome.RETRYABLE


def test_lot_entier_en_dlq_apres_transitoire(tmp_path: Path) -> None:
    dlq = LocalDeadLetterQueue(str(tmp_path / "dlq.jsonl"))
    s = bulk(FauxServeur([("statut", 502)]), dlq, batch_size=2, dlq_transitoires=True)
    assert asyncio.run(s.envoyer({"m": 1})) is SendOutcome.EN_ATTENTE
    assert asyncio.run(s.envoyer({"m": 2})) is SendOutcome.DEAD_LETTERED
    assert [e["document"]["m"] for e in read_entries(tmp_path / "dlq.jsonl")] == [1, 2]


# --- Relecture ---------------------------------------------------------------------


def test_read_entries_fichier_absent(tmp_path: Path) -> None:
    assert read_entries(tmp_path / "absent.jsonl") == []


def test_read_entries_lit_l_ancien_et_le_nouveau_format_et_le_gzip(tmp_path: Path) -> None:
    ancien = {"@dlq_timestamp": "t", "@dlq_index": "i", "@dlq_error": "e", "document": {"a": 1}}
    nouveau = {**ancien, "@dlq_output": "o", "@dlq_reason": "permanent"}
    lignes = json.dumps(ancien) + "\n\n" + json.dumps(nouveau) + "\n"
    (tmp_path / "d.jsonl").write_text(lignes, encoding="utf-8")
    with gzip.open(tmp_path / "d.jsonl.20260101T000000Z.gz", "wt", encoding="utf-8") as f:
        f.write(lignes)
    assert read_entries(tmp_path / "d.jsonl") == [ancien, nouveau]
    assert read_entries(tmp_path / "d.jsonl.20260101T000000Z.gz") == [ancien, nouveau]


@pytest.mark.parametrize(
    ("ligne", "motif"), [("{pas du json", "illisible"), ('{"x": 1}', "document"), ("[1]", "document")]
)
def test_read_entries_refuse_une_ligne_invalide(tmp_path: Path, ligne: str, motif: str) -> None:
    (tmp_path / "d.jsonl").write_text(ligne + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match=motif):
        read_entries(tmp_path / "d.jsonl")


# --- Rejeu ---------------------------------------------------------------------------


def _remplir(chemin: Path, documents: list[dict], **extra: Any) -> None:
    """Écrit une DLQ contenant ces documents (format enrichi)."""
    dlq = LocalDeadLetterQueue(str(chemin))
    for document in documents:
        asyncio.run(dlq.write(document, index="logs-origine", error="e", **extra))


def test_rejeu_tout_accepte_supprime_le_fichier(tmp_path: Path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    _remplir(chemin, [{"m": 1}, {"m": 2}], output="es", reason=REASON_PERMANENT)
    serveur = FauxServeur([])
    rapport = asyncio.run(rejouer_dlq(chemin, bulk(serveur)))
    assert rapport == RapportRejeu(rejouees=2, conservees=0, erreurs=[])
    assert not chemin.exists()
    # L'index d'origine est réutilisé, pas le motif de la sortie.
    assert '"_index":"logs-origine"' in serveur.requests[0].read().decode().replace(" ", "")


def test_rejeu_partiel_conserve_les_refus_au_format_d_origine(tmp_path: Path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    _remplir(chemin, [{"m": 1}, {"m": 2}, {"m": 3}])
    avant = read_entries(chemin)
    serveur = FauxServeur([("ok", None), ("refus", "mapper"), ("reseau", None)])
    rapport = asyncio.run(rejouer_dlq(str(chemin), bulk(serveur)))
    assert (rapport.rejouees, rapport.conservees) == (1, 2)
    assert rapport.erreurs == ["retryable", "retryable"]
    assert read_entries(chemin) == avant[1:]


def test_rejeu_vers_la_dlq_meme_fichier_sans_doublon(tmp_path: Path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    _remplir(chemin, [{"m": 1}])
    s = bulk(FauxServeur([("refus", "mapper")]), LocalDeadLetterQueue(str(chemin)))
    rapport = asyncio.run(rejouer_dlq(chemin, s))
    assert rapport.erreurs == ["dead_lettered"]
    assert len(read_entries(chemin)) == 1


def test_rejeu_d_un_fichier_rotate_gzip(tmp_path: Path) -> None:
    courant = tmp_path / "dlq.jsonl"
    _remplir(courant, [{"m": 1}, {"m": 2}])
    rotate = tmp_path / "dlq.jsonl.20260101T000000Z.gz"
    with gzip.open(rotate, "wb") as f:
        f.write(courant.read_bytes())
    courant.unlink()
    rapport = asyncio.run(rejouer_dlq(rotate, bulk(FauxServeur([("refus", "x"), ("ok", None)]))))
    assert (rapport.rejouees, rapport.conservees) == (1, 1)
    assert not rotate.exists()
    restant = tmp_path / "dlq.jsonl.20260101T000000Z"
    assert [e["document"]["m"] for e in read_entries(restant)] == [1]


def test_rejeu_fichier_vide_ne_touche_pas_la_sortie(tmp_path: Path) -> None:
    class Interdite(NullOutput):
        async def open(self) -> None:
            raise AssertionError("la sortie ne doit pas être ouverte")

    assert asyncio.run(rejouer_dlq(tmp_path / "absent.jsonl", Interdite({}))) == RapportRejeu()


class SortieCapricieuse(Output):
    """Sortie sans `envoyer` : refuse les documents marqués `refus`."""

    plugin: ClassVar[str] = "capricieuse"

    def __init__(self, options: dict[str, Any]) -> None:
        super().__init__(options)
        self.recus: list[dict] = []
        self.ouverte = self.fermee = False

    async def open(self) -> None:
        self.ouverte = True

    async def send(self, batch: Sequence[Any]) -> None:
        for document in batch:
            if document.get("refus"):
                raise RuntimeError("refusé par la cible")
            self.recus.append(dict(document))

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        self.fermee = True


def test_rejeu_vers_une_sortie_send_seul(tmp_path: Path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    _remplir(chemin, [{"m": 1}, {"m": 2, "refus": True}])
    cible = SortieCapricieuse({})
    rapport = asyncio.run(rejouer_dlq(chemin, cible))
    assert (rapport.rejouees, rapport.conservees) == (1, 1)
    assert rapport.erreurs == ["refusé par la cible"]
    assert cible.recus == [{"m": 1}]
    assert cible.ouverte and cible.fermee
    assert [e["document"]["m"] for e in read_entries(chemin)] == [2]


def test_rejeu_refuse_un_lot_tamponne(tmp_path: Path) -> None:
    chemin = tmp_path / "dlq.jsonl"
    _remplir(chemin, [{"m": 1}])
    with pytest.raises(ValueError, match="batch_size"):
        asyncio.run(rejouer_dlq(chemin, bulk(FauxServeur([]), batch_size=10)))
    assert len(read_entries(chemin)) == 1


def test_rejeu_gzip_refuse_d_ecraser_un_fichier_existant(tmp_path: Path) -> None:
    rotate = tmp_path / "dlq.jsonl.20260101T000000Z.gz"
    with gzip.open(rotate, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"@dlq_index": "i", "document": {"m": 1}}) + "\n")
    existant = tmp_path / "dlq.jsonl.20260101T000000Z"
    existant.write_text("à garder\n", encoding="utf-8")
    with pytest.raises(ValueError, match="écrasé"):
        asyncio.run(rejouer_dlq(rotate, bulk(FauxServeur([]))))
    assert existant.read_text(encoding="utf-8") == "à garder\n"
    assert rotate.exists()
