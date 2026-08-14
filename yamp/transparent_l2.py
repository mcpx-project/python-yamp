"""Transparent mode, Level 2 (SEP §10.3/§10.4 L2, §10.5, §7.1).

A Level 2 transparent intermediary parses bodies. It augments ``_meta`` with
proxy-hop tracing (appending to any existing hops), may filter the capability
surface on ``server/discover``, namespaces when routing to multiple backends,
and may optionally perform a dual handshake in stateful mode.

This increment deliberately reuses the earlier layers rather than reimplement
them: the stateless envelope and backend from δ3, the namespace from δ2, the
forward handshake from δ1, and the Level 1 passthrough from δ4.
"""

from __future__ import annotations

import json
from typing import Callable

from . import namespace
from .errors import HEADER_MISMATCH
from .forward import ForwardProxy
from .observability import PROXY_HOPS_KEY, append_hop, proxy_hop
from .stateless import (
    INVALID_PARAMS,
    METHOD_NOT_FOUND,
    StatelessBackend,
    StatelessRequest,
    StatelessResponse,
    decode_request,
    encode_response,
)


def header_body_mismatch(request: StatelessRequest) -> str | None:
    """Return the field that disagrees, or None if header and body agree.

    SEP-2243: a Level 2 intermediary parses the body, so it MUST verify the
    transport headers (``Mcp-Method`` / ``Mcp-Name``, modeled here as the
    envelope ``method`` / ``name``) agree with the body. Otherwise header-based
    routing or policy can be bypassed by a divergent body. An empty or opaque
    body carries nothing to check.
    """
    if not request.body:
        return None
    try:
        message = json.loads(request.body)
    except (ValueError, TypeError):
        return None  # opaque body: nothing to validate against
    if not isinstance(message, dict):
        return None
    body_method = message.get("method")
    if body_method is not None and body_method != request.method:
        return "method"
    if request.method == "tools/call":
        params = message.get("params")
        # A hostile body may carry a non-object `params` (null, list, scalar);
        # treat it as "no name to check" rather than crashing, matching the Rust
        # arm's `params.get("name")` which yields None on a non-object.
        body_name = params.get("name") if isinstance(params, dict) else None
        if body_name is not None and body_name != request.name:
            return "name"
    return None
from .transparent import TransparentL1
from .transport.base import Transport

# Hop tracing helpers live in `observability`; re-exported here for the layer's
# existing importers.
__all__ = [
    "PROXY_HOPS_KEY",
    "append_hop",
    "proxy_hop",
    "header_body_mismatch",
    "TransparentL2Stateless",
    "TransparentL2Stateful",
]


class TransparentL2Stateless:
    def __init__(
        self,
        client: Transport,
        backends: list[StatelessBackend],
        tool_filter: Callable[[str], bool] | None = None,
    ) -> None:
        self._client = client
        self._backends = {backend.id: backend for backend in backends}
        self._tool_filter = tool_filter

    async def serve(self) -> None:
        while True:
            raw = await self._client.receive()
            if raw is None:
                break
            request = decode_request(raw)
            # L2 parses the body and augments _meta with hop tracing (§7.1),
            # appending to any hops already present.
            request.meta = append_hop(request.meta)
            response = await self._handle(request)
            await self._client.send(encode_response(response))
        for backend in self._backends.values():
            await backend.close()

    async def _handle(self, request: StatelessRequest) -> StatelessResponse:
        mismatch = header_body_mismatch(request)
        if mismatch is not None:
            return StatelessResponse(
                meta=request.meta,
                body=json.dumps({"error": {"code": HEADER_MISMATCH, "message": f"header/body {mismatch} mismatch"}}),
            )
        if request.method == "server/discover":
            return await self._discover(request)
        if request.method == "tools/call":
            return await self._call(request)
        return StatelessResponse(
            meta=request.meta,
            body=json.dumps({"error": {"code": METHOD_NOT_FOUND, "message": request.method}}),
        )

    async def _discover(self, request: StatelessRequest) -> StatelessResponse:
        tools: list[dict] = []
        for backend in self._backends.values():
            response = await backend.exchange(
                StatelessRequest("server/discover", None, request.meta)
            )
            for tool in json.loads(response.body).get("tools", []):
                if self._tool_filter is not None and not self._tool_filter(tool["name"]):
                    continue  # L2 may filter the capability surface (§10.4.2)
                entry = dict(tool)
                entry["name"] = namespace.prefix(backend.id, tool["name"])
                tools.append(entry)
        return StatelessResponse(meta=request.meta, body=json.dumps({"tools": tools}))

    async def _call(self, request: StatelessRequest) -> StatelessResponse:
        resolved = namespace.split(request.name or "")
        if resolved is None or resolved[0] not in self._backends:
            return StatelessResponse(
                meta=request.meta,
                body=json.dumps({"error": {"code": INVALID_PARAMS, "message": f"unknown tool: {request.name}"}}),
            )
        backend_id, original = resolved
        forwarded = StatelessRequest("tools/call", original, request.meta, request.body)
        return await self._backends[backend_id].exchange(forwarded)


class TransparentL2Stateful:
    """Stateful Level 2: a per-connection choice between a dual handshake
    (follows §2.2 via the δ1 forward proxy) and Level 1 passthrough (δ4)."""

    def __init__(self, client: Transport, backend: Transport, dual_handshake: bool) -> None:
        self._client = client
        self._backend = backend
        self._dual_handshake = dual_handshake
        self.dual_handshake_performed = False

    async def serve(self) -> None:
        if self._dual_handshake:
            await ForwardProxy(self._client, self._backend).serve()
            self.dual_handshake_performed = True
        else:
            await TransparentL1(self._client, self._backend).serve()
            self.dual_handshake_performed = False
