# SPDX-License-Identifier: GPL-3.0-or-later
"""Schéma de configuration commun des sorties (issue #8).

Objectif de MathildeDec : « seule la configuration du ``output{}`` diffère
légèrement d'un système à l'autre ». Ce module déclare **une seule fois**
les options communes à toutes les sorties (`hosts`, authentification,
`ssl_*`, `compression`, `batch_size`, `batch_bytes`, `flush_interval`,
`max_concurrency`, `retry_max`, `retry_backoff`, `dlq_path`), leurs types,
leurs bornes et leurs valeurs par défaut ; chaque sortie déclare en plus
ses options propres (`OPTIONS_PROPRES`). La validation a lieu **à la
compilation** du pipeline — l'instanciation par `compiler_sorties()` passe
par `Output.__init__` qui appelle `valider_options()` : une option
inconnue (faute de frappe) ou une valeur invalide échoue franchement,
avec un message traduit, avant toute connexion.

Les noms d'options reprennent ceux du plugin Logstash correspondant quand
il existe (`hosts`, `user`, `ssl`, `batch_size`…), pour qu'un `.conf`
Logstash fonctionne sans retouche. Les valeurs par défaut sont
documentées dans `docs/sorties.md` (tableau des options communes).

Fonctions PURES : aucune dépendance à Electron… à rien du tout, en fait —
seulement `i18n._` pour les messages traduits.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loguru import logger

from pystashtemplate.i18n import _
from pystashtemplate.tracing import traced


@dataclass(frozen=True)
class OptionSpec:
    """Spécification d'une option de bloc ``output{}`` (nom, type, bornes).

    Attributs:
        nom: Nom de l'option, tel qu'écrit dans le ``.conf``.
        types: Types Python acceptés (un seul pour la plupart des options ;
            ``hosts`` accepte une chaîne ou une liste, ``flush_interval``
            un entier ou un flottant).
        defaut: Valeur par défaut documentée (``None`` : pas de défaut,
            l'option est absente du ``.conf`` si l'utilisateur n'y touche pas).
        minimum: Borne inférieure inclusive pour les nombres (ex.
            ``batch_size >= 1``) ; ``None`` : pas de borne.
        valeurs: Valeurs autorisées pour les énumérations (ex. codecs
            ``json_lines``/``rubydebug``) ; ``None`` : pas d'énumération.
        requis: `True` si la sortie refuse de compiler sans cette option
            (ex. ``path`` de la sortie ``file``).
        description: Une ligne de documentation, reprise dans la table de
            ``docs/sorties.md``.
    """

    nom: str
    types: tuple[type, ...]
    defaut: Any = None
    minimum: float | None = None
    valeurs: tuple[str, ...] | None = None
    requis: bool = False
    description: str = ""


# Les options communes à toutes les sorties, dans l'ordre de la
# documentation. Chaque sortie les accepte ; celles qui n'en font rien
# les ignorent silencieusement (un pipeline portable n'a pas à retirer
# ses options pour changer de destination).
OPTIONS_COMMUNES: dict[str, OptionSpec] = {
    "hosts": OptionSpec(
        "hosts",
        (str, list, tuple),
        description="URL(s) du ou des nœuds cibles (chaîne seule ou liste)",
    ),
    "user": OptionSpec("user", (str,), description="Utilisateur de l'authentification basique"),
    "password": OptionSpec("password", (str,), description="Mot de passe de l'authentification basique"),
    "ssl": OptionSpec("ssl", (bool,), True, description="Vérifier le certificat TLS du serveur (vrai par défaut)"),
    "cacert": OptionSpec(
        "cacert",
        (str,),
        description="Autorité de certification (PEM) pour vérifier le serveur",
    ),
    "ssl_certificate": OptionSpec("ssl_certificate", (str,), description="Certificat client TLS (mTLS)"),
    "ssl_key": OptionSpec("ssl_key", (str,), description="Clé privée du certificat client TLS"),
    "compression": OptionSpec("compression", (bool,), False, description="Compresser les requêtes (faux par défaut)"),
    "batch_size": OptionSpec(
        "batch_size",
        (int,),
        1,
        minimum=1,
        description="Documents tamponnés avant envoi (1 par défaut)",
    ),
    "batch_bytes": OptionSpec(
        "batch_bytes",
        (int,),
        10_485_760,
        minimum=1,
        description="Taille maximale d'un lot en octets (10 Mio par défaut)",
    ),
    "flush_interval": OptionSpec(
        "flush_interval",
        (int, float),
        1.0,
        minimum=0,
        description="Secondes entre deux poussées du tampon (1,0 par défaut)",
    ),
    "max_concurrency": OptionSpec(
        "max_concurrency",
        (int,),
        4,
        minimum=1,
        description="Envois parallèles au maximum (4 par défaut)",
    ),
    "retry_max": OptionSpec(
        "retry_max", (int,), 0, minimum=0, description="Réessais avant échec définitif (0 par défaut)"
    ),
    "retry_backoff": OptionSpec(
        "retry_backoff",
        (int, float),
        1.0,
        minimum=0,
        description="Secondes initiales d'attente entre réessais, exponentielles (1,0 par défaut)",
    ),
    "dlq_path": OptionSpec("dlq_path", (str,), description="Fichier de la DLQ locale (échecs définitifs)"),
    # Option commune de Logstash : identifiant du bloc output{}, repris
    # comme nom de sortie dans les entrées de DLQ (#7).
    "id": OptionSpec("id", (str,), description="Identifiant du bloc output (nom de sortie dans la DLQ)"),
}


@traced
def valider_options(
    plugin: str,
    options: dict[str, Any],
    options_propres: dict[str, OptionSpec] | None = None,
) -> None:
    """Valide les options d'un bloc ``output{}`` : communes + propres.

    Appelée par `Output.__init__` (donc par `compiler_sorties()`, à la
    compilation du pipeline, avant toute connexion). Chaque règle cassée
    lève `ValueError` avec un message traduit nommant le plugin et
    l'option en cause :

    - option inconnue (ni commune, ni déclarée par la sortie — une faute
      de frappe mérite un échec franc plutôt qu'un silence) ;
    - type inattendu (ex. ``batch_size: "beaucoup"``) ;
    - valeur hors bornes (ex. ``retry_max: -1``) ou hors énumération
      (ex. ``codec: "yaml"``) ;
    - option requise absente.

    Args:
        plugin: Nom du plugin (pour nommer le coupable dans l'erreur).
        options: Options du bloc, telles que rendues par l'interpréteur.
        options_propres: Options déclarées par la sortie, en plus des
            communes.

    Raises:
        ValueError: Option inconnue, valeur invalide ou option requise
            absente — le message est traduit et nomme le plugin.
    """
    propres = options_propres or {}
    connues: dict[str, OptionSpec] = dict(OPTIONS_COMMUNES)
    connues.update(propres)
    logger.debug("valider_options | plugin={} options={}", plugin, sorted(options))
    for nom, valeur in options.items():
        if nom not in connues:
            logger.debug("valider_options | option inconnue : {}", nom)
            raise ValueError(
                _("option « {} » inconnue pour la sortie « {} » (options valides : {})").format(
                    nom, plugin, ", ".join(sorted(connues))
                )
            )
        _valider_valeur(plugin, connues[nom], valeur)
    for nom, spec in propres.items():
        if spec.requis and nom not in options:
            logger.debug("valider_options | option requise absente : {}", nom)
            raise ValueError(_("sortie « {} » : l'option « {} » est requise").format(plugin, nom))
    logger.debug("valider_options | {} option(s) valide(s)", len(options))


@traced
def _valider_valeur(plugin: str, spec: OptionSpec, valeur: Any) -> None:
    """Valide une valeur contre sa spécification (type, bornes, énumération).

    Raises:
        ValueError: Type inattendu, valeur hors bornes ou hors énumération
            — message traduit nommant le plugin et l'option.
    """
    if not isinstance(valeur, spec.types):
        raise ValueError(
            _("sortie « {} » : option « {} » — type {} attendu (reçu : {})").format(
                plugin, spec.nom, " ou ".join(t.__name__ for t in spec.types), type(valeur).__name__
            )
        )
    if spec.minimum is not None and isinstance(valeur, (int, float)):
        if valeur < spec.minimum:
            raise ValueError(
                _("sortie « {} » : option « {} » — la valeur doit être au moins {} (reçu : {})").format(
                    plugin, spec.nom, spec.minimum, valeur
                )
            )
    if spec.valeurs is not None and str(valeur) not in spec.valeurs:
        raise ValueError(
            _("sortie « {} » : option « {} » — valeurs valides : {} (reçu : {})").format(
                plugin, spec.nom, ", ".join(spec.valeurs), valeur
            )
        )


@traced
def defauts_communs() -> dict[str, Any]:
    """Les valeurs par défaut des options communes, pour la documentation.

    Returns:
        Un dictionnaire nom → défaut (les options sans défaut sont
        absentes) — la table de ``docs/sorties.md`` en est tirée.
    """
    return {spec.nom: spec.defaut for spec in OPTIONS_COMMUNES.values() if spec.defaut is not None}
