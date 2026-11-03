#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
"""État des traductions de ``lang/`` (issue #38, sur le modèle de Gcm4
``scripts/i18n_report.py``).

Lit ``lang/LINGUAS`` (seule liste des langues) et compte, pour chaque
``lang/<locale>.po``, les entrées traduites, approximatives (« fuzzy »,
ignorées à l'exécution) et vides ; vérifie que le catalogue compilé
``lang/locale/<locale>/LC_MESSAGES/pystashtemplate.mo`` se charge. L'en-tête
(msgid vide) et les entrées obsolètes (``#~``) sont ignorés. Le français
est la langue source : ``fr_FR`` reste vide, ce n'est pas un manque.

Usage :

    scripts/i18n_report.py              # résumé et tableau texte
    scripts/i18n_report.py --markdown   # résumé et tableau Markdown

Dans GitHub Actions, la version Markdown est aussi ajoutée au résumé du job
(``$GITHUB_STEP_SUMMARY``). Le rapport est informatif : le code de retour
vaut 0, sauf si un catalogue listé manque ou ne se lit pas (2).
"""

from __future__ import annotations

import argparse
import ast
import gettext
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

ROOT = Path(__file__).resolve().parent.parent
LANG_DIR = ROOT / "lang"
DOMAIN = "pystashtemplate"
SOURCE_LOCALE = "fr_FR"


@dataclass(frozen=True)
class Stats:
    """Compteurs d'un catalogue."""

    locale: str
    translated: int
    fuzzy: int
    untranslated: int
    compiled: bool

    @property
    def total(self) -> int:
        """Nombre d'entrées actives (hors en-tête et obsolètes)."""
        logger.debug("Stats.total | {}", self.locale)
        return self.translated + self.fuzzy + self.untranslated

    @property
    def percent(self) -> float:
        """Part des entrées traduites et utilisables, en pourcentage."""
        logger.debug("Stats.percent | {}", self.locale)
        return 100.0 * self.translated / self.total if self.total else 0.0


def read_linguas(lang_dir: Path = LANG_DIR) -> list[str]:
    """Locales déclarées dans ``LINGUAS`` (commentaires ignorés), dans l'ordre du fichier."""
    logger.debug("read_linguas() | lang_dir={}", lang_dir)
    locales = []
    for line in (lang_dir / "LINGUAS").read_text(encoding="utf-8").splitlines():
        locales.extend(line.split("#", 1)[0].split())
    logger.debug("read_linguas() -> {} locale(s)", len(locales))
    return locales


def parse_po(text: str) -> list[tuple[str, list[str], bool]]:
    """Entrées actives d'un catalogue : (msgid, msgstr[], fuzzy), en-tête compris."""
    logger.debug("parse_po() | {} caractères", len(text))
    entries: list[dict] = []
    pending_fuzzy = False
    current = ""  # dernier mot-clé lu : msgctxt, msgid, msgid_plural, msgstr
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#~"):
            continue
        if line.startswith("#,"):
            pending_fuzzy = "fuzzy" in line[2:].replace(",", " ").split()
            continue
        if line.startswith("#"):
            continue
        if line.startswith('"'):  # suite de la chaîne précédente
            if current == "msgid":
                entries[-1]["msgid"] += ast.literal_eval(line)
            elif current == "msgstr":
                entries[-1]["msgstr"][-1] += ast.literal_eval(line)
            continue
        keyword, _sep, rest = line.partition(" ")
        if keyword == "msgctxt" or (keyword == "msgid" and current != "msgctxt"):
            entries.append({"msgid": "", "msgstr": [], "fuzzy": pending_fuzzy})
            pending_fuzzy = False
        if keyword == "msgid":
            entries[-1]["msgid"] = ast.literal_eval(rest)
        elif keyword.startswith("msgstr"):
            entries[-1]["msgstr"].append(ast.literal_eval(rest))
            keyword = "msgstr"
        current = keyword
    result = [(e["msgid"], e["msgstr"], e["fuzzy"]) for e in entries]
    logger.debug("parse_po() -> {} entrée(s)", len(result))
    return result


def is_compiled(lang_dir: Path, locale: str) -> bool:
    """Le ``.mo`` suivi de la langue existe et se charge."""
    logger.debug("is_compiled() | {}", locale)
    path = lang_dir / "locale" / locale / "LC_MESSAGES" / f"{DOMAIN}.mo"
    try:
        with path.open("rb") as fh:
            gettext.GNUTranslations(fh)
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        logger.warning("catalogue compilé {} inutilisable : {}", path.name, exc)
        logger.debug("is_compiled() -> False")
        return False
    logger.debug("is_compiled() -> True")
    return True


def catalog_stats(locale: str, text: str, compiled: bool = True) -> Stats:
    """Compte les entrées traduites, fuzzy et vides d'un catalogue (hors en-tête)."""
    logger.debug("catalog_stats() | locale={}", locale)
    translated = fuzzy = untranslated = 0
    for msgid, msgstrs, is_fuzzy in parse_po(text):
        if msgid == "":
            continue
        if is_fuzzy:
            fuzzy += 1
        elif msgstrs and all(msgstrs):
            translated += 1
        else:
            untranslated += 1
    stats = Stats(locale, translated, fuzzy, untranslated, compiled)
    logger.debug("catalog_stats() -> {}", stats)
    return stats


def collect(lang_dir: Path = LANG_DIR) -> tuple[list[Stats], list[str]]:
    """Statistiques des langues de ``LINGUAS``, triées par taux décroissant, et erreurs."""
    logger.debug("collect() | lang_dir={}", lang_dir)
    stats, errors = [], []
    for locale in read_linguas(lang_dir):
        path = lang_dir / f"{locale}.po"
        try:
            text = path.read_text(encoding="utf-8")
            stats.append(catalog_stats(locale, text, is_compiled(lang_dir, locale)))
        except (OSError, UnicodeDecodeError, ValueError, SyntaxError) as exc:
            logger.warning("catalogue {} illisible : {}", path.name, exc)
            errors.append(f"lang/{locale}.po : {exc}")
    stats.sort(key=lambda s: (-s.percent, s.locale))
    logger.debug("collect() -> {} langue(s), {} erreur(s)", len(stats), len(errors))
    return stats, errors


def summary_lines(stats: list[Stats], errors: list[str]) -> list[tuple[str, str]]:
    """Synthèse : (libellé, valeur), comme demandé par l'issue #38."""
    logger.debug("summary_lines() | {} langue(s)", len(stats))
    declared = len(stats) + len(errors)
    targets = [s for s in stats if s.locale != SOURCE_LOCALE]
    lines = [
        ("Langues", str(declared)),
        ("Chaînes par catalogue", str(stats[0].total if stats else 0)),
        ("Traduites", str(sum(s.translated for s in targets))),
        ("Non traduites", str(sum(s.untranslated for s in targets))),
        ("Fuzzy", str(sum(s.fuzzy for s in targets))),
        ("Locales OK", f"{len(stats)}/{declared}"),
        ("Catalogues compilés", f"{sum(1 for s in stats if s.compiled)}/{declared}"),
    ]
    logger.debug("summary_lines() -> {} ligne(s)", len(lines))
    return lines


def render(stats: list[Stats], errors: list[str], markdown: bool) -> str:
    """Résumé puis tableau par langue, en texte ou en Markdown."""
    logger.debug("render() | {} langue(s) markdown={}", len(stats), markdown)
    summary = summary_lines(stats, errors)
    note = f"Langue source : {SOURCE_LOCALE} (msgid), exclue des totaux de traduction."
    if markdown:
        lines = ["## Internationalisation", "", note, "", "| | |", "|---|---:|"]
        lines += [f"| {label} | {value} |" for label, value in summary]
        lines += ["", "| Langue | Traduites | Fuzzy | Vides | Taux | .mo |", "|---|---:|---:|---:|---:|---|"]
        lines += [
            f"| `{s.locale}` | {s.translated} | {s.fuzzy} | {s.untranslated} | {s.percent:.1f} % | "
            f"{'oui' if s.compiled else 'non'} |"
            for s in stats
        ]
    else:
        lines = ["Internationalisation", "-" * 30]
        lines += [f"{label + ' :':<24}{value:>6}" for label, value in summary]
        lines += [note, "", f"{'Langue':<8} {'Traduites':>9} {'Fuzzy':>6} {'Vides':>6} {'Taux':>7}  .mo"]
        lines += [
            f"{s.locale:<8} {s.translated:>9} {s.fuzzy:>6} {s.untranslated:>6} {s.percent:>6.1f}%  "
            f"{'oui' if s.compiled else 'non'}"
            for s in stats
        ]
    report = "\n".join(lines) + "\n"
    logger.debug("render() -> {} ligne(s)", len(lines))
    return report


def main(argv: list[str] | None = None) -> int:
    """Point d'entrée : 0, ou 2 si un catalogue manque ou est illisible."""
    logger.trace("main() appelée (avant configuration des puits)")
    parser = argparse.ArgumentParser(description="État des traductions (lang/*.po)")
    parser.add_argument("--markdown", action="store_true", help="tableau Markdown")
    parser.add_argument("--lang-dir", type=Path, default=LANG_DIR, help="dossier des catalogues")
    args = parser.parse_args(argv)
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if os.environ.get("RSYSLOGSTACH_DEBUG") else "WARNING")
    logger.debug("main() | markdown={} lang_dir={}", args.markdown, args.lang_dir)
    stats, errors = collect(args.lang_dir)
    sys.stdout.write(render(stats, errors, args.markdown))
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as fh:
            fh.write(render(stats, errors, markdown=True))
    for error in errors:
        print(f"::error::{error}", file=sys.stderr)
    code = 2 if errors else 0
    logger.debug("main() -> {}", code)
    return code


if __name__ == "__main__":
    sys.exit(main())
