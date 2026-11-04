# SPDX-License-Identifier: GPL-3.0-or-later
"""Sortie Elasticsearch (issue #13) : le client _bulk commun, produit d'origine.

Comme la sortie OpenSearch (#12), tout le travail d'envoi est dans le
client commun (#11) : chemin `_bulk` natif (`POST /_bulk`), les quatre
actions, `_id` accepté, erreurs rapportées élément par élément
(`items[]`). Ce module n'ajoute que ce qui est propre à Elasticsearch :

- **clé d'API** (`api_key`) : en-tête `Authorization: ApiKey <clé>`. La
  valeur est soit la forme encodée que rend Elasticsearch (champ
  `encoded` de `POST /_security/api_key`), soit `id:api_key` comme dans
  la sortie Logstash — encodée alors en base64 ici ;
- **jeton porteur** (`token`) : en-tête `Authorization: Bearer <jeton>`
  (jeton de service, OAuth/OIDC) ;
- une seule authentification à la fois : `user`/`password`, `api_key`
  et `token` s'excluent (erreur dès la configuration) ;
- **data streams** (`data_stream => true`) : seule l'action `create`
  est acceptée par Elasticsearch. L'action par défaut `index` devient
  `create` ; `update` et `delete` sont refusées avant tout envoi.
  Sans `index`, le flux visé suit le schéma de nommage d'Elastic, comme
  la sortie Logstash : `<data_stream_type>-<data_stream_dataset>-
  <data_stream_namespace>` (`logs-generic-default` par défaut).

Les secrets passent par `${VAR}` dans le `.conf` ; ils ne sont jamais
journalisés (seul le type d'authentification l'est).

## Gabarit de pipeline d'exemple

```
output {
  elasticsearch {
    hosts   => ["https://elasticsearch:9200"]
    index   => "logs-%Y.%m.%d"
    api_key => "${ES_API_KEY}"
    cacert  => "/etc/ssl/certs/elasticsearch-ca.pem"
  }
}
```
"""

from __future__ import annotations

import base64
from typing import Any, ClassVar

from loguru import logger

from pystashtemplate.dlq import LocalDeadLetterQueue
from pystashtemplate.i18n import _
from pystashtemplate.outputs import Capacites, enregistrer
from pystashtemplate.outputs.elasticsearch_bulk import ActionInvalideError, ElasticsearchBulkOutput, SendOutcome
from pystashtemplate.tracing import atraced, traced

# Schéma de nommage des data streams d'Elastic (type-dataset-namespace),
# valeurs par défaut identiques à la sortie Logstash elasticsearch.
_DATA_STREAM_DEFAUTS = {
    "data_stream_type": "logs",
    "data_stream_dataset": "generic",
    "data_stream_namespace": "default",
}


@enregistrer
class ElasticsearchOutput(ElasticsearchBulkOutput):
    """Sortie Elasticsearch : client _bulk commun, clé d'API, jeton, data streams.

    Capacités complètes du produit : `index`, `create`, `update`,
    `delete` ; `_id` accepté ; accusé de réception par élément.
    """

    plugin = "elasticsearch"
    chemin_bulk = ""
    capacites = Capacites(
        actions=frozenset({"index", "create", "update", "delete"}),
        accepte_id=True,
        accuse_par_evenement=True,
        taille_max_requete=None,
    )
    OPTIONS_CONNUES: ClassVar[frozenset[str]] = ElasticsearchBulkOutput.OPTIONS_CONNUES | frozenset(
        {"api_key", "token", "data_stream", *_DATA_STREAM_DEFAUTS}
    )

    @traced
    def __init__(
        self,
        options: dict[str, Any],
        *,
        dlq: LocalDeadLetterQueue | None = None,
        transport: Any = None,
    ) -> None:
        """Valide l'authentification et le mode data stream, puis prépare le client commun.

        Raises:
            ValueError: plusieurs authentifications à la fois, option
                inconnue, ou action `update`/`delete` avec `data_stream`.
        """
        options = dict(options)
        auths = [nom for nom in ("user", "api_key", "token") if options.get(nom)]
        if len(auths) > 1:
            raise ValueError(
                _(
                    "sortie elasticsearch : une seule authentification parmi user/password, api_key et token ({})"
                ).format(", ".join(auths))
            )
        self._data_stream = bool(options.get("data_stream", False))
        if self._data_stream:
            action = str(options.get("action", "create"))
            if action not in ("index", "create"):
                raise ValueError(
                    _("sortie elasticsearch : data_stream n'accepte que l'action create ({})").format(action)
                )
            options["action"] = "create"
            if not options.get("index"):
                options["index"] = "-".join(
                    str(options.get(cle) or defaut) for cle, defaut in _DATA_STREAM_DEFAUTS.items()
                )
        elif any(cle in options for cle in _DATA_STREAM_DEFAUTS):
            raise ValueError(_("sortie elasticsearch : data_stream_type/dataset/namespace exigent data_stream => true"))
        super().__init__(options, dlq=dlq, transport=transport)
        logger.debug("ElasticsearchOutput : authentification {}", auths[0] if auths else "aucune")

    @traced
    def _en_tetes_d_options(self) -> dict[str, str]:
        """En-têtes du client commun, plus `Authorization` pour `api_key` ou `token`."""
        en_tetes = super()._en_tetes_d_options()
        if self.options.get("api_key"):
            en_tetes["Authorization"] = f"ApiKey {_cle_encodee(str(self.options['api_key']))}"
        elif self.options.get("token"):
            en_tetes["Authorization"] = f"Bearer {self.options['token']}"
        return en_tetes

    @atraced
    async def envoyer(self, doc: dict[str, Any], *, action: str = "index", **options: Any) -> SendOutcome:
        """`envoyer` du client commun ; en data stream, `index` devient `create`.

        Raises:
            ActionInvalideError: `update` ou `delete` vers un data stream
                (refus d'Elasticsearch, levé avant tout envoi) ; et les cas
                du client commun.
        """
        if self._data_stream:
            if action not in ("index", "create"):
                raise ActionInvalideError(f"data stream : action {action!r} refusée (create seule)")
            action = "create"
        return await super().envoyer(doc, action=action, **options)


@traced
def _cle_encodee(cle: str) -> str:
    """Forme encodée d'une clé d'API : `id:api_key` encodé en base64, sinon telle quelle."""
    if ":" in cle:
        return base64.b64encode(cle.encode("utf-8")).decode("ascii")
    return cle
