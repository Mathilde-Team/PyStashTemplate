# SPDX-License-Identifier: GPL-3.0-or-later
"""Métriques des sorties (issue #10), sans dépendance à une bibliothèque de métriques.

Chaque sortie porte un `observateur` (nul par défaut : aucun coût) qu'elle
prévient à chaque événement mesurable :

- `evenements(sortie, issue, nombre)` : documents par issue, parmi
  `ISSUES` — ``indexed`` (accepté par la cible), ``dead_lettered`` (écrit
  en DLQ), ``retryable`` (rendu à l'appelant, qui le renverra) et
  ``rejected`` (refus définitif de la cible, compté en plus de son issue
  finale : DLQ ou rendu à l'appelant sans DLQ) ;
- `reessai(sortie)` : une retente d'une requête en échec transitoire ;
- `latence(sortie, secondes)` : durée d'une requête vers la cible (réussie
  ou non) ;
- `file(sortie, taille)` : documents en attente dans le tampon de la sortie.

`CompteursSortie` est l'implémentation fournie : compteurs en mémoire par
sortie, `instantane()` pour les tests et les tableaux de bord, et
`exposition()` au format texte Prometheus (contenu d'un `/metrics`). Un
connecteur qui a déjà ses métriques (RsysLogstach, `prometheus_client`)
branche plutôt un adaptateur qui implémente `ObservateurSortie` sur ses
compteurs existants : ses noms de métriques ne changent pas (voir
`docs/sorties.md`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from loguru import logger

from pystashtemplate.i18n import _

ISSUES = ("indexed", "dead_lettered", "retryable", "rejected")

PREFIXE = "pystashtemplate_output"


@runtime_checkable
class ObservateurSortie(Protocol):
    """Ce qu'une sortie prévient de ses envois (voir le docstring du module)."""

    def evenements(self, sortie: str, issue: str, nombre: int = 1) -> None:
        """`nombre` documents de la sortie `sortie` ont eu l'issue `issue` (`ISSUES`)."""

    def reessai(self, sortie: str) -> None:
        """La sortie retente une requête après un échec transitoire."""

    def latence(self, sortie: str, secondes: float) -> None:
        """Durée d'une requête de la sortie vers sa cible."""

    def file(self, sortie: str, taille: int) -> None:
        """Documents en attente dans le tampon de la sortie."""


class ObservateurNul:
    """Observateur par défaut : ignore tout (une sortie sans métriques ne paie rien)."""

    def evenements(self, sortie: str, issue: str, nombre: int = 1) -> None:
        """Ignoré."""

    def reessai(self, sortie: str) -> None:
        """Ignoré."""

    def latence(self, sortie: str, secondes: float) -> None:
        """Ignoré."""

    def file(self, sortie: str, taille: int) -> None:
        """Ignoré."""


OBSERVATEUR_NUL = ObservateurNul()


@dataclass
class _Compteurs:
    """Compteurs d'une sortie."""

    evenements: dict[str, int] = field(default_factory=lambda: dict.fromkeys(ISSUES, 0))
    reessais: int = 0
    requetes: int = 0
    duree_totale: float = 0.0
    duree_max: float = 0.0
    file: int = 0


def _etiquette(valeur: str) -> str:
    """Valeur d'étiquette Prometheus échappée (``\\``, ``"``, saut de ligne)."""
    logger.debug("_etiquette | entrée")
    echappee = valeur.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
    logger.debug("_etiquette | sortie")
    return echappee


class CompteursSortie:
    """Compteurs en mémoire, par nom de sortie (`Output.nom`)."""

    def __init__(self) -> None:
        """Aucune sortie observée au départ."""
        logger.debug("CompteursSortie.__init__ | entrée")
        self._par_sortie: dict[str, _Compteurs] = {}
        logger.debug("CompteursSortie.__init__ | sortie")

    def _de(self, sortie: str) -> _Compteurs:
        """Compteurs de `sortie`, créés à la première observation."""
        logger.debug("CompteursSortie._de | entrée ({})", sortie)
        compteurs = self._par_sortie.setdefault(sortie, _Compteurs())
        logger.debug("CompteursSortie._de | sortie")
        return compteurs

    def evenements(self, sortie: str, issue: str, nombre: int = 1) -> None:
        """Ajoute `nombre` documents à l'issue `issue` de la sortie.

        Raises:
            ValueError: `issue` n'est pas dans `ISSUES`.
        """
        logger.debug("CompteursSortie.evenements | entrée ({} {} × {})", sortie, issue, nombre)
        if issue not in ISSUES:
            logger.warning(_("métriques : issue inconnue {issue}"), issue=issue)
            raise ValueError(_("issue de métrique inconnue : {} (attendu : {})").format(issue, ", ".join(ISSUES)))
        self._de(sortie).evenements[issue] += nombre
        logger.debug("CompteursSortie.evenements | sortie")

    def reessai(self, sortie: str) -> None:
        """Compte une retente."""
        logger.debug("CompteursSortie.reessai | entrée ({})", sortie)
        self._de(sortie).reessais += 1
        logger.debug("CompteursSortie.reessai | sortie")

    def latence(self, sortie: str, secondes: float) -> None:
        """Compte une requête et sa durée (total et maximum)."""
        logger.debug("CompteursSortie.latence | entrée ({} {:.6f} s)", sortie, secondes)
        compteurs = self._de(sortie)
        compteurs.requetes += 1
        compteurs.duree_totale += secondes
        compteurs.duree_max = max(compteurs.duree_max, secondes)
        logger.debug("CompteursSortie.latence | sortie")

    def file(self, sortie: str, taille: int) -> None:
        """Retient la taille courante du tampon de la sortie."""
        logger.debug("CompteursSortie.file | entrée ({} {})", sortie, taille)
        self._de(sortie).file = taille
        logger.debug("CompteursSortie.file | sortie")

    def instantane(self) -> dict[str, dict[str, object]]:
        """Copie des compteurs : ``{sortie: {"indexed": …, "reessais": …, …}}``."""
        logger.debug("CompteursSortie.instantane | entrée")
        resultat: dict[str, dict[str, object]] = {
            nom: {
                **c.evenements,
                "reessais": c.reessais,
                "requetes": c.requetes,
                "duree_totale": c.duree_totale,
                "duree_max": c.duree_max,
                "file": c.file,
            }
            for nom, c in sorted(self._par_sortie.items())
        }
        logger.debug("CompteursSortie.instantane | sortie ({} sortie(s))", len(resultat))
        return resultat

    def exposition(self, prefixe: str = PREFIXE) -> str:
        """Exposition texte Prometheus (format 0.0.4) de tous les compteurs.

        Métriques : ``<prefixe>_events_total{output,outcome}``,
        ``<prefixe>_retries_total{output}``,
        ``<prefixe>_request_duration_seconds{output}`` (résumé :
        ``_count`` et ``_sum``) et ``<prefixe>_queue_size{output}``.
        """
        logger.debug("CompteursSortie.exposition | entrée")
        lignes = [
            f"# HELP {prefixe}_events_total Documents traités par sortie, par issue",
            f"# TYPE {prefixe}_events_total counter",
        ]
        par_sortie = sorted(self._par_sortie.items())
        for nom, c in par_sortie:
            for issue in ISSUES:
                lignes.append(
                    f'{prefixe}_events_total{{output="{_etiquette(nom)}",outcome="{issue}"}} {c.evenements[issue]}'
                )
        lignes += [
            f"# HELP {prefixe}_retries_total Retentes après un échec transitoire",
            f"# TYPE {prefixe}_retries_total counter",
            *(f'{prefixe}_retries_total{{output="{_etiquette(nom)}"}} {c.reessais}' for nom, c in par_sortie),
            f"# HELP {prefixe}_request_duration_seconds Durée des requêtes vers la cible",
            f"# TYPE {prefixe}_request_duration_seconds summary",
        ]
        for nom, c in par_sortie:
            lignes.append(f'{prefixe}_request_duration_seconds_count{{output="{_etiquette(nom)}"}} {c.requetes}')
            lignes.append(f'{prefixe}_request_duration_seconds_sum{{output="{_etiquette(nom)}"}} {c.duree_totale}')
        lignes += [
            f"# HELP {prefixe}_queue_size Documents en attente dans le tampon de la sortie",
            f"# TYPE {prefixe}_queue_size gauge",
            *(f'{prefixe}_queue_size{{output="{_etiquette(nom)}"}} {c.file}' for nom, c in par_sortie),
        ]
        texte = "\n".join(lignes) + "\n"
        logger.debug("CompteursSortie.exposition | sortie ({} ligne(s))", len(lignes))
        return texte
