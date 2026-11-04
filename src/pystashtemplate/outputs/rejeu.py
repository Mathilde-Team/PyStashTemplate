# SPDX-License-Identifier: GPL-3.0-or-later
"""Rejeu des entrées d'une dead letter queue vers une sortie (issue #7).

Une fois la cause d'un rejet corrigée (mapping ajusté, destination
revenue…), `rejouer_dlq()` renvoie les documents d'un fichier DLQ vers
une sortie du socle. Le rejeu est agnostique de la destination :

- une sortie qui rapporte un résultat par document (méthode
  ``envoyer``, famille _bulk) reçoit chaque document avec l'index
  d'origine (``@dlq_index``) et le résultat décide : indexé = rejoué,
  autre résultat = conservé ;
- toute autre sortie reçoit le document par ``send([doc])`` : rejoué
  si aucune exception n'est levée, conservé sinon.

Rien n'est perdu : le fichier est réécrit (remplacement atomique) avec
les seules entrées conservées, au format d'origine ; il est supprimé
quand tout a été rejoué. Les fichiers rotatés compressés (``.gz``) se
relisent aussi ; ils sont réécrits non compressés à côté
(``<nom>`` sans ``.gz``) quand des entrées restent.

Une sortie à résultat par document doit avoir ``batch_size`` à 1 (le
résultat d'un document seulement tamponné n'est pas connu). Si la sortie
a sa propre DLQ, un document de nouveau rejeté y est écrit ; quand c'est
le fichier même en cours de rejeu, la réécriture finale l'écrase par
l'entrée d'origine conservée (pas de doublon). Les filtres ne sont pas
rejoués : les documents de la DLQ ont déjà été transformés avant leur
échec.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from loguru import logger

from pystashtemplate.dlq import read_entries
from pystashtemplate.i18n import _
from pystashtemplate.outputs.base import Output
from pystashtemplate.tracing import atraced, traced


@dataclass
class RapportRejeu:
    """Bilan d'un rejeu : entrées rejouées, conservées, et erreurs vues."""

    rejouees: int = 0
    conservees: int = 0
    erreurs: list[str] = field(default_factory=list)


@traced
def _reecrire(chemin: Path, entrees: list[dict]) -> Path | None:
    """Réécrit le fichier DLQ avec les seules entrées conservées (remplacement atomique).

    Returns:
        Le fichier écrit, ou None s'il ne reste rien (fichier supprimé).
    """
    cible = chemin.with_suffix("") if chemin.suffix == ".gz" else chemin
    if not entrees:
        chemin.unlink(missing_ok=True)
        logger.debug("_reecrire : plus aucune entrée, {} supprimé", chemin)
        return None
    descripteur, temporaire = tempfile.mkstemp(dir=cible.parent, prefix=f".{cible.name}.", suffix=".rejeu")
    with os.fdopen(descripteur, "w", encoding="utf-8") as handle:
        for entree in entrees:
            handle.write(json.dumps(entree, ensure_ascii=False) + "\n")
    os.replace(temporaire, cible)
    if cible != chemin:
        chemin.unlink(missing_ok=True)
    logger.debug("_reecrire : {} entrée(s) conservée(s) dans {}", len(entrees), cible)
    return cible


@atraced
async def _rejouer_entree(sortie: Output, entree: dict) -> str | None:
    """Renvoie un document ; None s'il est accepté, sinon la raison du refus."""
    document = dict(entree["document"])
    envoyer = getattr(sortie, "envoyer", None)
    try:
        if envoyer is not None:
            resultat = await envoyer(document, index=entree.get("@dlq_index"))
            await sortie.flush()
            valeur = getattr(resultat, "value", resultat)
            if valeur != "indexed":
                logger.debug("_rejouer_entree : refusé ({})", valeur)
                return str(valeur)
        else:
            await sortie.send([document])
            await sortie.flush()
    except Exception as exc:  # noqa: BLE001 — toute erreur conserve l'entrée
        logger.warning(_("rejeu DLQ : document conservé erreur={error}"), error=exc)
        return str(exc)
    logger.debug("_rejouer_entree : accepté")
    return None


@atraced
async def rejouer_dlq(chemin: str | Path, sortie: Output) -> RapportRejeu:
    """Rejoue les entrées d'un fichier DLQ vers une sortie déjà construite.

    La sortie est ouverte au début et fermée à la fin. Les entrées
    refusées restent dans le fichier (voir la docstring du module).

    Args:
        chemin: Fichier DLQ courant ou rotaté (``.gz``).
        sortie: Sortie du socle vers laquelle renvoyer les documents.

    Returns:
        Le bilan du rejeu.

    Raises:
        ValueError: fichier DLQ illisible, ou sortie à résultat par
            document avec ``batch_size`` différent de 1 (rien n'est rejoué).
    """
    if hasattr(sortie, "envoyer") and int(sortie.options.get("batch_size", 1)) != 1:
        raise ValueError(_("rejeu DLQ : la sortie doit avoir batch_size à 1"))
    fichier = Path(chemin)
    entrees = read_entries(fichier)
    rapport = RapportRejeu()
    if not entrees:
        return rapport
    conservees: list[dict] = []
    await sortie.open()
    try:
        for entree in entrees:
            refus = await _rejouer_entree(sortie, entree)
            if refus is None:
                rapport.rejouees += 1
            else:
                conservees.append(entree)
                rapport.erreurs.append(refus)
    finally:
        await sortie.close()
    rapport.conservees = len(conservees)
    _reecrire(fichier, conservees)
    logger.info(
        _("rejeu DLQ terminé rejouées={rejouees} conservées={conservees}"),
        rejouees=rapport.rejouees,
        conservees=rapport.conservees,
    )
    return rapport
