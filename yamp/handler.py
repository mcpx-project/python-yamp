"""Local request handlers and their registry (draft §5.3/§5.7).

A ``Handler`` is a local source of tools that can originate a response, rather
than routing to a backend. It gives yamp a single dispatch seam: a request whose
namespaced name resolves to a handler is served locally (server behavior), and
one that resolves to a backend is routed (proxy behavior). Handlers share the
backend namespace discipline, so each carries a reserved id and its tools are
exposed as ``id__tool``. This is the substrate the drafts' Reverse and
Conversion modes need: ``RestToMcp`` is a Conversion handler, and meta-tools
such as ``yamp__backends`` are built-in handlers.
"""

from __future__ import annotations

import json
from typing import Awaitable, Callable, Protocol, runtime_checkable

from . import namespace


@runtime_checkable
class Handler(Protocol):
    """A local, namespaced source of tools. ``call_tool`` receives the original
    (un-prefixed) name; the registry maps the prefix to the handler."""

    id: str

    def list_tools(self) -> list[dict]: ...

    async def call_tool(self, name: str, arguments: dict) -> dict: ...


class Registry:
    """Maps a reserved id to its handler and namespaces the handlers' tools.

    Consulted before the backend routing table (SEP dispatch order): a tool name
    whose prefix is a handler id is served locally.
    """

    def __init__(self, handlers: list[Handler] | None = None) -> None:
        self._handlers: dict[str, Handler] = {}
        for handler in handlers or []:
            if not namespace.valid_backend_id(handler.id):
                raise ValueError(f"invalid handler id: {handler.id!r}")
            if handler.id in self._handlers:
                raise ValueError(f"duplicate handler id: {handler.id!r}")
            self._handlers[handler.id] = handler

    def ids(self) -> set[str]:
        return set(self._handlers)

    def handler_for(self, id: str) -> Handler | None:
        return self._handlers.get(id)

    def list_tools(self) -> list[dict]:
        """Every handler's tools, namespaced under the handler id."""
        tools: list[dict] = []
        for handler in self._handlers.values():
            for tool in handler.list_tools():
                entry = dict(tool)
                entry["name"] = namespace.prefix(handler.id, tool["name"])
                tools.append(entry)
        return tools


class BackendsHandler:
    """A built-in meta-tool (``yamp__backends``) that reports the proxy's own
    backends. It originates its response entirely inside yamp, demonstrating the
    server side of the dispatch seam."""

    def __init__(self, provider: Callable[[], list[dict]], id: str = "yamp") -> None:
        self.id = id
        self._provider = provider

    def list_tools(self) -> list[dict]:
        return [
            {
                "name": "backends",
                "description": "List the backends this proxy fronts and their availability",
                "inputSchema": {"type": "object", "properties": {}},
            }
        ]

    async def call_tool(self, name: str, arguments: dict) -> dict:
        backends = self._provider()
        return {"content": [{"type": "text", "text": json.dumps(backends)}]}


# The signature a handler's async call takes; kept here so both the protocol and
# concrete handlers name one type.
CallTool = Callable[[str, dict], Awaitable[dict]]


def build_registry(config, backends_provider: Callable[[], list[dict]]) -> Registry:
    """Build a :class:`Registry` from a ``HandlerConfig`` (δ17).

    Each configured REST handler becomes a served ``RestToMcp`` (Conversion
    mode); ``metaTools`` adds the ``yamp__backends`` handler, which reports the
    proxy's backends via ``backends_provider``.
    """
    from .rest import RestToMcp

    handlers: list[Handler] = []
    for spec in config.rest:
        handlers.append(RestToMcp({"baseUrl": spec.base_url, "operations": spec.operations}, id=spec.id))
    if config.meta_tools:
        handlers.append(BackendsHandler(provider=backends_provider))
    return Registry(handlers)
