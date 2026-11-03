# SPDX-License-Identifier: GPL-3.0-or-later
"""Filtre `xml` façon Logstash pour les pipelines `.conf` (issue #121).

`filter { xml { source => "message" target => "doc" } }` reprend
logstash-filter-xml (`xml.rb`) : deux traitements indépendants de la même
chaîne XML.

- `xpath => { "expression" => "[champ]" }` : la chaîne est analysée en mode
  tolérant (comme Nokogiri, option `RECOVER`), chaque expression est
  évaluée et ses résultats sont convertis en texte comme le `to_s` de
  Ruby : un élément donne son XML, un nœud texte son texte échappé, un
  attribut sa valeur, un nombre `3.0`, un booléen `true` (faux : ignoré).
  Les résultats vont dans le champ sous forme de tableau (texte seul si un
  seul résultat et `force_array => false`). Préfixes : ceux de l'élément
  racine (l'espace par défaut sous le préfixe `xmlns`), ou ceux de
  `namespaces` s'il est donné ; `remove_namespaces => true` les retire
  tous avant l'évaluation.
- `store_xml => true` (défaut) : la chaîne est analysée en mode strict et
  convertie comme XmlSimple (`xml_in`) avec les options de Logstash :
  attributs et éléments enfants dans le même objet, enfants en tableaux
  (`force_array`, vrai par défaut), texte seul en chaîne (ou
  `{"content": …}` avec `force_content` ou des attributs), éléments vides
  omis (`suppress_empty`, vrai par défaut). L'élément racine n'apparaît
  pas. `target` est alors requis.

Une valeur absente ne fait rien, une chaîne vide non plus ; un tableau de
longueur différente de 1, une valeur non textuelle, une erreur d'analyse
ou une expression XPath invalide ajoutent le tag `_xmlparsefailure`.
`parse_options` (`strict`, `no_error`, `no_warning`… séparés par `,` ou
`|`) remplace le mode tolérant de l'analyse XPath.

Sécurité : les entités externes, le chargement de DTD et XInclude ne sont
jamais actifs ; `parse_options` qui les demande (`noent`, `dtdload`,
`dtdattr`, `dtdvalid`, `xinclude`) est refusé (bloc ignoré). L'accès
réseau reste coupé même sans `nonet`.
"""

from __future__ import annotations

import math
import re
from typing import Any
from xml.sax.saxutils import escape

from loguru import logger
from lxml import etree

from pystashtemplate.templates import _MISSING, assign_field, parse_field_path, resolve_field
from pystashtemplate.tracing import traced
from pystashtemplate.util import add_failure_tags

ECHEC = "_xmlparsefailure"
CONTENU = "content"
ANONYME = "anon"

# Options de Nokogiri::XML::ParseOptions acceptées (après retrait des `_` et
# passage en majuscules) et leur effet sur l'analyseur lxml.
_OPTIONS_ANALYSE: dict[str, dict[str, bool]] = {
    "STRICT": {},
    "RECOVER": {"recover": True},
    "NOERROR": {},
    "NOWARNING": {},
    "PEDANTIC": {},
    "NOBLANKS": {"remove_blank_text": True},
    "SAX1": {},
    "NONET": {},
    "NODICT": {},
    "NSCLEAN": {"ns_clean": True},
    "NOCDATA": {},
    "NOXINCNODE": {},
    "COMPACT": {"compact": True},
    "OLD10": {},
    "NOBASEFIX": {},
    "HUGE": {"huge_tree": True},
    "BIGLINES": {},
    "DEFAULTXML": {"recover": True},
}
_OPTIONS_REFUSEES = {"NOENT", "DTDLOAD", "DTDATTR", "DTDVALID", "XINCLUDE"}


@traced
def options_analyse(texte: str | None) -> dict[str, bool]:
    """Traduit `parse_options` en arguments de `lxml.etree.XMLParser`.

    Sans option : mode tolérant (`DEFAULT_XML` de Nokogiri). Avec des
    options : mode strict, sauf si `recover` en fait partie.

    Raises:
        ValueError: option inconnue, ou refusée pour la sécurité.
    """
    if texte is None:
        return {"recover": True}
    arguments: dict[str, bool] = {}
    for brut in re.split(r"[,|]", texte):
        nom = brut.strip().replace("_", "").upper()
        if not nom:
            continue
        if nom in _OPTIONS_REFUSEES:
            raise ValueError(f"xml : parse_options {brut.strip()!r} refusé (entités, DTD ou XInclude)")
        if nom not in _OPTIONS_ANALYSE:
            raise ValueError(f"xml : parse_options inconnu {brut.strip()!r}")
        arguments.update(_OPTIONS_ANALYSE[nom])
    logger.debug("options_analyse({!r}) | {}", texte, arguments)
    return arguments


@traced
def analyseur(**options: bool) -> etree.XMLParser:
    """Analyseur lxml sûr : ni réseau, ni entités, ni DTD, entrée en UTF-8."""
    return etree.XMLParser(
        encoding="utf-8",
        no_network=True,
        resolve_entities=False,
        load_dtd=False,
        dtd_validation=False,
        **options,
    )


@traced
def _texte_ruby(resultat: Any) -> str | None:
    """`to_s` Ruby d'un résultat XPath Nokogiri ; None si Logstash l'ignore."""
    if resultat is None or resultat is False:
        texte = None
    elif resultat is True:
        texte = "true"
    elif isinstance(resultat, float):
        if math.isnan(resultat):
            texte = "NaN"
        elif math.isinf(resultat):
            texte = "Infinity" if resultat > 0 else "-Infinity"
        else:
            texte = repr(resultat)
    elif isinstance(resultat, str):
        texte = (
            escape(resultat)
            if getattr(resultat, "is_text", False) or getattr(resultat, "is_tail", False)
            else str(resultat)
        )
    else:
        texte = etree.tostring(resultat, encoding=str, with_tail=False)
    logger.debug("_texte_ruby | {!r}", texte)
    return texte


@traced
def _retirer_espaces_de_noms(racine: Any) -> None:
    """`remove_namespaces!` de Nokogiri : noms locaux partout, déclarations retirées."""
    for element in racine.iter():
        if isinstance(element.tag, str):
            element.tag = etree.QName(element).localname
            for nom in list(element.attrib):
                if nom.startswith("{"):
                    element.attrib[etree.QName(nom).localname] = element.attrib.pop(nom)
    etree.cleanup_namespaces(racine)
    logger.debug("_retirer_espaces_de_noms | fait")


class _XmlSimple:
    """`XmlSimple.xml_in` avec les options de Logstash (KeyAttr vide, sans KeepRoot)."""

    @traced
    def __init__(self, force_array: bool, force_content: bool, suppress_empty: bool) -> None:
        """Mémorise ForceArray, ForceContent et SuppressEmpty."""
        self._force_array = force_array
        self._force_content = force_content
        self._suppress_empty = suppress_empty

    @traced
    def convertir(self, racine: Any) -> Any:
        """Convertit l'élément racine (sans son nom)."""
        return self._replier(racine)

    @traced
    def _attributs(self, element: Any) -> dict[str, Any]:
        """Attributs de REXML : déclarations `xmlns` comprises, noms préfixés."""
        parent = element.getparent()
        herites = parent.nsmap if parent is not None else {}
        attributs: dict[str, Any] = {}
        for prefixe, uri in element.nsmap.items():
            if herites.get(prefixe) != uri:
                attributs["xmlns" if prefixe is None else f"xmlns:{prefixe}"] = uri
        prefixes = {uri: prefixe for prefixe, uri in element.nsmap.items() if prefixe is not None}
        prefixes["http://www.w3.org/XML/1998/namespace"] = "xml"
        for nom, valeur in element.attrib.items():
            if nom.startswith("{"):
                qname = etree.QName(nom)
                prefixe = prefixes.get(qname.namespace)
                nom = f"{prefixe}:{qname.localname}" if prefixe else qname.localname
            attributs[nom] = valeur
        logger.debug("_attributs | {}", attributs)
        return attributs

    @traced
    def _textes(self, element: Any) -> list[str]:
        """Nœuds texte directs de l'élément (texte initial et queues des enfants)."""
        textes = [element.text] if element.text is not None else []
        textes.extend(enfant.tail for enfant in element if enfant.tail is not None)
        logger.debug("_textes | {}", len(textes))
        return textes

    @traced
    def _replier(self, element: Any) -> Any:
        """`collapse` de XmlSimple."""
        resultat: Any = self._attributs(element)
        a_des_attributs = bool(resultat)
        enfants = [enfant for enfant in element if isinstance(enfant.tag, str)]
        textes = self._textes(element)
        if enfants:
            for enfant in enfants:
                valeur = self._replier(enfant)
                if _vide(valeur) and not a_des_attributs and self._suppress_empty:
                    continue
                self._fusionner(resultat, etree.QName(enfant).localname, valeur)
            if textes and "".join(textes).strip():
                contenu = [_echapper_rexml(texte) for texte in textes]
                resultat[CONTENU] = contenu[0] if len(contenu) == 1 else contenu
        elif textes:
            return self._replier_texte(resultat, a_des_attributs, "".join(textes))
        tableaux = sum(1 for valeur in resultat.values() if isinstance(valeur, list))
        if tableaux == 1 and isinstance(resultat.get(ANONYME), list):
            return resultat[ANONYME]
        if not resultat and self._suppress_empty:
            logger.debug("_replier | vide supprimé")
            return None
        logger.debug("_replier | {} clé(s)", len(resultat))
        return resultat

    @traced
    def _replier_texte(self, resultat: dict[str, Any], a_des_attributs: bool, valeur: str) -> Any:
        """`collapse_text_node` : texte seul en chaîne, ou sous `content`."""
        if _vide(valeur) and not a_des_attributs:
            return {}
        if a_des_attributs or self._force_content:
            return self._fusionner(resultat, CONTENU, valeur)
        return valeur

    @traced
    def _fusionner(self, objet: dict[str, Any], cle: str, valeur: Any) -> dict[str, Any]:
        """`merge` : la 2e occurrence d'une clé transforme sa valeur en tableau."""
        if cle in objet:
            if isinstance(objet[cle], list):
                objet[cle].append(valeur)
            else:
                objet[cle] = [objet[cle], valeur]
        elif isinstance(valeur, list) or (self._force_array and cle != CONTENU):
            objet[cle] = [valeur]
        else:
            objet[cle] = valeur
        logger.debug("_fusionner({}) | fait", cle)
        return objet


@traced
def _vide(valeur: Any) -> bool:
    """`empty` de XmlSimple : None, objet vide ou texte sans caractère visible."""
    if isinstance(valeur, dict):
        return not valeur
    if isinstance(valeur, str):
        return not valeur.strip()
    return valeur is None


@traced
def _echapper_rexml(texte: str) -> str:
    """`Text#to_s` de REXML (contenu mixte) : `&`, `<` et `>` échappés."""
    return escape(texte)


class XmlFilter:
    """Bloc `xml {}` d'un `.conf` (voir la docstring du module)."""

    @traced
    def __init__(self, options: dict[str, Any]) -> None:
        """Valide les options.

        Raises:
            ValueError: `source` absent, `target` absent avec `store_xml`,
                `xpath` ou `namespaces` mal formés, `parse_options` inconnu
                ou refusé.
        """
        source = options.get("source")
        if not isinstance(source, str) or not source:
            raise ValueError("xml : 'source' (chaîne) est requis")
        self._source = parse_field_path(source)
        self._store_xml = _booleen(options.get("store_xml", True))
        target = options.get("target")
        if self._store_xml and (not isinstance(target, str) or not target):
            raise ValueError("xml : 'target' est requis quand 'store_xml' est vrai")
        self._cible = parse_field_path(target) if self._store_xml else []
        self._xpath = [(expr, parse_field_path(dest)) for expr, dest in _paires(options.get("xpath", {}), "xpath")]
        self._namespaces = dict(_paires(options.get("namespaces", {}), "namespaces"))
        self._remove_namespaces = _booleen(options.get("remove_namespaces", False))
        self._force_array = _booleen(options.get("force_array", True))
        self._simple = _XmlSimple(
            self._force_array,
            _booleen(options.get("force_content", False)),
            _booleen(options.get("suppress_empty", True)),
        )
        parse_options = options.get("parse_options")
        self._analyseur_xpath = analyseur(**options_analyse(None if parse_options is None else str(parse_options)))
        self._analyseur_strict = analyseur()

    @traced
    def apply(self, doc: dict[str, Any]) -> bool:
        """Applique le filtre ; True si une valeur a été extraite (options communes)."""
        valeur = resolve_field(doc, self._source)
        if valeur is _MISSING or valeur is None or valeur is False:
            logger.debug("xml : source absente")
            return False
        if isinstance(valeur, list):
            if len(valeur) != 1:
                return self._echec(doc, "tableau de longueur différente de 1")
            valeur = valeur[0]
        if not isinstance(valeur, str):
            return self._echec(doc, f"valeur non textuelle ({type(valeur).__name__})")
        if not valeur.strip():
            logger.debug("xml : chaîne vide")
            return False
        octets = valeur.encode("utf-8")
        try:
            trouve = self._appliquer_xpath(doc, octets)
        except (etree.XMLSyntaxError, etree.XPathError) as exc:
            logger.debug("xml : analyse XPath impossible : {}", exc)
            return self._echec(doc, str(exc))
        if self._store_xml:
            try:
                racine = etree.fromstring(octets, self._analyseur_strict)
            except etree.XMLSyntaxError as exc:
                logger.debug("xml : analyse stricte impossible : {}", exc)
                return self._echec(doc, str(exc))
            assign_field(doc, self._cible, self._simple.convertir(racine))
            trouve = True
        logger.debug("xml : appliqué | trouvé={}", trouve)
        return trouve

    @traced
    def _appliquer_xpath(self, doc: dict[str, Any], octets: bytes) -> bool:
        """Évalue chaque expression `xpath` et écrit les résultats ; True si au moins un."""
        racine = etree.fromstring(octets, self._analyseur_xpath)
        if racine is None or not self._xpath:
            logger.debug("_appliquer_xpath | rien à évaluer")
            return False
        if self._remove_namespaces:
            _retirer_espaces_de_noms(racine)
        if self._namespaces:
            prefixes = self._namespaces
        else:
            prefixes = {("xmlns" if p is None else p): uri for p, uri in racine.nsmap.items()}
        arbre = racine.getroottree()
        trouve = False
        for expression, destination in self._xpath:
            resultat = arbre.xpath(expression, namespaces=prefixes)
            elements = resultat if isinstance(resultat, list) else [resultat]
            donnees = [t for t in (_texte_ruby(e) for e in elements) if t is not None]
            trouve = trouve or bool(donnees)
            if len(donnees) == 1 and not self._force_array:
                assign_field(doc, destination, donnees[0])
            elif donnees:
                assign_field(doc, destination, donnees)
        logger.debug("_appliquer_xpath | trouvé={}", trouve)
        return trouve

    @traced
    def _echec(self, doc: dict[str, Any], raison: str) -> bool:
        """Ajoute `_xmlparsefailure` ; le filtre n'est pas réussi."""
        logger.debug("xml : échec ({})", raison)
        add_failure_tags(doc, [ECHEC])
        return False


@traced
def _booleen(valeur: Any) -> bool:
    """Booléen d'une option `.conf` (`true`/`false` en texte ou natif)."""
    if isinstance(valeur, bool):
        return valeur
    texte = str(valeur).strip().lower()
    if texte not in ("true", "false"):
        raise ValueError(f"xml : booléen attendu, reçu {valeur!r}")
    return texte == "true"


@traced
def _paires(valeur: Any, nom: str) -> list[tuple[str, str]]:
    """Option hash (`{a => b}`) ou tableau pair (`[a, b, c, d]`) en paires de textes."""
    if isinstance(valeur, dict):
        paires = list(valeur.items())
    elif isinstance(valeur, list) and len(valeur) % 2 == 0:
        paires = list(zip(valeur[::2], valeur[1::2], strict=True))
    else:
        raise ValueError(f"xml : {nom!r} doit être un hash")
    if not all(isinstance(c, str) and isinstance(v, str) for c, v in paires):
        raise ValueError(f"xml : {nom!r} attend des textes")
    logger.debug("_paires({}) | {}", nom, len(paires))
    return paires
