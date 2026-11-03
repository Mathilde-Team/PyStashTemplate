#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# scripts/i18n-update.sh -- maintenance des catalogues gettext (issue #38,
# sur le modèle de Gcm4 scripts/i18n-update.sh).
#
#   scripts/i18n-update.sh              xgettext -> lang/messages.pot,
#                                       msgmerge lang/*.po (crée les .po
#                                       manquants avec msginit),
#                                       msgfmt --check, puis compilation
#                                       des .mo suivis par git
#   scripts/i18n-update.sh --check      vérifie sans rien écrire (CI) :
#                                       .pot et .po à jour par rapport aux
#                                       sources (dates ignorées), catalogues
#                                       valides, .mo suivis identiques à la
#                                       compilation des .po
#   scripts/i18n-update.sh --compile [DIR]
#                                       msgfmt --check puis compilation en
#                                       DIR/<locale>/LC_MESSAGES/pystashtemplate.mo
#                                       (défaut : lang/locale, les .mo suivis)
#
# Langues : lang/LINGUAS, nulle part ailleurs (ni dans le code, ni dans
# pyproject.toml : la roue embarque tout lang/locale/ sous share/locale).
# Les nouvelles chaînes arrivent avec msgstr "" : aucune traduction
# automatique n'est insérée. Langue source : le français (msgid).
# Sources : fichiers Python suivis par git sous src/pystashtemplate/.
#
# Prérequis : gettext (apt-get install gettext / dnf install gettext).

set -euo pipefail
export LC_ALL=C.UTF-8

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LANG_DIR="$ROOT/lang"
POT="$LANG_DIR/messages.pot"
DOMAIN="pystashtemplate"
MODE="update"
MO_DIR="$LANG_DIR/locale"
DEST="$MO_DIR"

case "${1:-}" in
    "") ;;
    --check) MODE="check" ;;
    --compile) MODE="compile"; DEST="${2:-$DEST}" ;;
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    *) echo "option inconnue : $1" >&2; exit 2 ;;
esac

for tool in xgettext msgmerge msgfmt msginit msgfilter msgattrib msgcat msgunfmt; do
    command -v "$tool" >/dev/null || { echo "gettext requis ($tool introuvable)" >&2; exit 2; }
done

linguas() { sed -e 's/#.*//' -e 's/[[:space:]]//g' "$LANG_DIR/LINGUAS" | grep -v '^$'; }

# Contenu comparable : sans les dates de génération, sans les coupures de
# lignes (deux versions de gettext ne coupent pas au même endroit) et sans
# les marques de format que xgettext déduit lui-même du msgid
# (python-format, python-brace-format) : gettext 0.21 ne marque pas « {} »
# positionnel, gettext 1.0 si. Les marques venues des sources
# (« xgettext:no-... ») et « fuzzy » restent comparées.
normalize() {
    grep -vE '^"(POT-Creation-Date|PO-Revision-Date):' "$1" | msgcat --no-wrap --sort-by-file - 2>/dev/null \
        | awk '/^#,/ { n = split(substr($0, 3), f, ","); out = "";
                       for (i = 1; i <= n; i++) { g = f[i]; gsub(/ /, "", g);
                           if (g != "python-format" && g != "python-brace-format") out = out (out == "" ? "" : ",") " " g }
                       if (out != "") print "#," out; next }
                { print }' || true
}

sources() {
    (cd "$ROOT" && git ls-files 'src/pystashtemplate/*.py' 'src/pystashtemplate/**/*.py' | sort -u)
}

generate_pot() {
    local out="$1"
    (cd "$ROOT" && sources | xgettext --files-from=- --output="$out" \
        --language=Python --from-code=UTF-8 \
        --keyword=_ --keyword=N_ --keyword=ngettext:1,2 \
        --add-comments=TRANSLATORS: --add-location=file --sort-by-file --no-wrap \
        --package-name="RsysLogstach" \
        --msgid-bugs-address="https://github.com/Mathilde-Team/RsysLogstach/issues" \
        --copyright-holder="Mathilde-Team" 2>"$TMP/xgettext.log") \
        || { cat "$TMP/xgettext.log" >&2; exit 1; }
    # xgettext laisse CHARSET dans l'en-tête du modèle ; les .po héritent d'UTF-8.
    sed -i 's/charset=CHARSET/charset=UTF-8/' "$out"
}

merge_po() {  # $1 = .po existant, $2 = .pot, $3 = sortie
    msgmerge --quiet --add-location=file --sort-by-file --no-wrap --previous --output-file="$3" "$1" "$2"
}

same() { diff -q <(normalize "$1") <(normalize "$2") >/dev/null; }

# Clés d'un catalogue : msgctxt/msgid/msgid_plural des entrées actives.
# Pour le contrôle --check d'un .po, c'est la seule chose comparée : le
# rapprochement approximatif (« fuzzy ») de msgmerge varie d'une version de
# gettext à l'autre, alors que l'ensemble des chaînes à traduire, lui, est
# déterminé par messages.pot.
keys() {
    msgattrib --no-obsolete --no-wrap "$1" 2>/dev/null | grep -E '^(msgctxt|msgid|msgid_plural) ' | LC_ALL=C sort
}

# Contenu d'un .mo (ou d'un .po compilé), comparable d'une version de
# msgfmt à l'autre : msgunfmt trie les entrées et ignore l'agencement binaire.
mo_content() { msgunfmt --no-wrap "$1" 2>/dev/null; }

msgfmt_check() {  # $1 = .po, $2 = sortie
    # Les en-têtes Last-Translator/Language-Team laissés à leur valeur par
    # défaut ne sont que des avertissements : on ne les affiche pas.
    msgfmt --check --output-file="$2" "$1" 2> >(grep -v 'still has the initial default value' >&2)
}

check_all() {
    local rc=0 po loc
    for loc in $(linguas); do
        po="$LANG_DIR/$loc.po"
        if [ ! -f "$po" ]; then echo "manquant : lang/$loc.po (lancer scripts/i18n-update.sh)" >&2; rc=1; continue; fi
        msgfmt_check "$po" /dev/null || { echo "invalide : lang/$loc.po" >&2; rc=1; }
    done
    for po in "$LANG_DIR"/*.po; do
        [ -e "$po" ] || continue
        loc="$(basename "$po" .po)"
        linguas | grep -qx "$loc" || { echo "lang/$loc.po absent de lang/LINGUAS" >&2; rc=1; }
    done
    return $rc
}

compile_all() {  # $1 = dossier de sortie
    local loc
    for loc in $(linguas); do
        mkdir -p "$1/$loc/LC_MESSAGES"
        msgfmt_check "$LANG_DIR/$loc.po" "$1/$loc/LC_MESSAGES/$DOMAIN.mo"
    done
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

case "$MODE" in
update)
    generate_pot "$TMP/messages.pot"
    if [ -f "$POT" ] && same "$POT" "$TMP/messages.pot"; then
        echo "lang/messages.pot inchangé"
    else
        cp "$TMP/messages.pot" "$POT"; echo "lang/messages.pot mis à jour ($(grep -c '^msgid ' "$POT") entrées)"
    fi
    for loc in $(linguas); do
        po="$LANG_DIR/$loc.po"
        if [ ! -f "$po" ]; then
            msginit --no-translator --no-wrap --locale="$loc" --input="$POT" --output-file="$po" 2>/dev/null
            sed -i 's/charset=ASCII/charset=UTF-8/' "$po"
            # msginit recopie les msgid pour l'anglais : ce seraient de fausses
            # traductions. Tout repart vide.
            msgfilter --keep-header --no-wrap --input="$po" --output-file="$po" true
            # Code complet de lang/LINGUAS dans l'en-tête (msginit l'abrège
            # parfois : « fr » pour fr_FR, selon la version de gettext).
            sed -i -E "s/^\"Language: [^\\]*/\"Language: $loc/" "$po"
            if grep -q 'nplurals=INTEGER' "$po" || ! grep -q '^"Plural-Forms: nplurals=[0-9]' "$po"; then
                echo "lang/$loc.po créé : msginit ne connaît pas les pluriels de '$loc'," \
                     "renseigner l'en-tête Plural-Forms avant de relancer" >&2
                exit 1
            fi
            echo "lang/$loc.po créé"
        fi
        merge_po "$po" "$POT" "$TMP/$loc.po"
        if ! same "$po" "$TMP/$loc.po"; then
            cp "$TMP/$loc.po" "$po"; echo "lang/$loc.po mis à jour"
        fi
    done
    check_all
    # Les .mo suivis ne sont réécrits que si leur contenu change (un .mo
    # recompilé par une autre version de msgfmt n'est pas un changement).
    compile_all "$TMP/mo"
    for loc in $(linguas); do
        mo="$MO_DIR/$loc/LC_MESSAGES/$DOMAIN.mo"
        if [ ! -f "$mo" ] || ! diff -q <(mo_content "$mo") <(mo_content "$TMP/mo/$loc/LC_MESSAGES/$DOMAIN.mo") >/dev/null; then
            mkdir -p "$(dirname "$mo")"; cp "$TMP/mo/$loc/LC_MESSAGES/$DOMAIN.mo" "$mo"; echo "lang/locale/$loc/LC_MESSAGES/$DOMAIN.mo compilé"
        fi
    done
    ;;
check)
    rc=0
    generate_pot "$TMP/messages.pot"
    if [ ! -f "$POT" ] || ! diff -u <(normalize "$POT") <(normalize "$TMP/messages.pot") >"$TMP/pot.diff"; then
        echo "::error::lang/messages.pot n'est plus à jour avec les sources : lancer scripts/i18n-update.sh" >&2
        head -60 "$TMP/pot.diff" >&2 || true
        rc=1
    fi
    for loc in $(linguas); do
        po="$LANG_DIR/$loc.po"
        [ -f "$po" ] || continue
        if ! diff -u <(keys "$POT") <(keys "$po") >"$TMP/$loc.diff"; then
            echo "::error::lang/$loc.po n'est plus synchronisé avec messages.pot : lancer scripts/i18n-update.sh" >&2
            head -20 "$TMP/$loc.diff" >&2 || true
            rc=1
        fi
    done
    check_all || rc=1
    if [ $rc -eq 0 ]; then
        compile_all "$TMP/mo"
        for loc in $(linguas); do
            mo="$MO_DIR/$loc/LC_MESSAGES/$DOMAIN.mo"
            if [ ! -f "$mo" ] || ! diff -q <(mo_content "$mo") <(mo_content "$TMP/mo/$loc/LC_MESSAGES/$DOMAIN.mo") >/dev/null; then
                echo "::error::lang/locale/$loc/LC_MESSAGES/$DOMAIN.mo absent ou différent de lang/$loc.po : lancer scripts/i18n-update.sh" >&2
                rc=1
            fi
        done
    fi
    [ $rc -eq 0 ] && echo "catalogues à jour : $(linguas | wc -l) langue(s)"
    exit $rc
    ;;
compile)
    check_all
    compile_all "$DEST"
    echo "$(linguas | wc -l) catalogue(s) compilé(s) dans $DEST"
    ;;
esac
