"""Resource-subscription routing and origination (σ4; MCP
``resources/subscribe``).

A client subscribes to a resource by URI; the party that owns the resource then
emits ``notifications/resources/updated`` when it changes. yamp handles both
roles from one seam, mirroring tasks (δ19 routing plus σ3 origination):

- Proxy role: ``resources/subscribe`` and ``resources/unsubscribe`` carry a
  namespaced URI (``backend__uri``, like ``resources/read``). yamp reverse-
  resolves it to the owning backend and forwards with the backend's own URI; the
  backend's ``notifications/resources/updated`` is re-namespaced so the client
  sees the same ``backend__uri`` it holds (the mirror of ``tasks.namespace_event``).
- Server role: a subscription whose URI does not resolve to a backend is
  registered in a per-connection registry. When the resource changes,
  ``publish_resource_update`` fans out the notification only to the subscribed
  URIs, so an unsubscribed resource costs nothing.

Registrations are per-connection node-local state (like the σ3 task store); a
cross-node shared registry is a fleet concern (Track F), not this increment.
"""

from __future__ import annotations

from . import namespace

SUBSCRIBE_METHOD = "resources/subscribe"
UNSUBSCRIBE_METHOD = "resources/unsubscribe"
# Server/backend -> client: the resource changed. One method for both roles.
UPDATED_METHOD = "notifications/resources/updated"
SUBSCRIBE_METHODS = frozenset({SUBSCRIBE_METHOD, UNSUBSCRIBE_METHOD})


def is_subscribe_method(method: str | None) -> bool:
    """Whether a method is a resource subscribe/unsubscribe request."""
    return method in SUBSCRIBE_METHODS


def updated_notification(uri: str) -> dict:
    """The ``notifications/resources/updated`` a server originates for a changed
    resource (σ4). The proxy role never builds this; it re-namespaces the
    backend's own with :func:`namespace_updated`."""
    return {"jsonrpc": "2.0", "method": UPDATED_METHOD, "params": {"uri": uri}}


def namespace_updated(message: dict, backend_id: str) -> dict:
    """Re-namespace the ``uri`` in a backend's ``notifications/resources/updated``
    so the client sees the same ``backend__uri`` it holds (the mirror of
    ``tasks.namespace_event``). A message without a string ``params.uri`` is
    returned unchanged."""
    params = message.get("params")
    if not isinstance(params, dict) or not isinstance(params.get("uri"), str):
        return message
    out = dict(message)
    out_params = dict(params)
    out_params["uri"] = namespace.prefix_uri(backend_id, params["uri"])
    out["params"] = out_params
    return out


class Subscriptions:
    """The server's per-connection resource-subscription registry (σ4). A set of
    subscribed URIs: ``subscribe`` adds, ``unsubscribe`` removes, and the update
    fan-out consults membership so an unsubscribed resource is never sent. A
    re-subscribe is idempotent (a set holds each URI once)."""

    def __init__(self) -> None:
        self._uris: set[str] = set()

    def subscribe(self, uri: str) -> None:
        self._uris.add(uri)

    def unsubscribe(self, uri: str) -> bool:
        """Remove a subscription. Returns whether it was present."""
        if uri in self._uris:
            self._uris.discard(uri)
            return True
        return False

    def contains(self, uri: str) -> bool:
        return uri in self._uris

    def count(self) -> int:
        return len(self._uris)
