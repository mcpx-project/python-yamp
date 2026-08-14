"""Forward proxy, stateless mode (SEP §2.1, MCP 2026-07-28 and later).

Stateless mode has no initialize handshake and no session identifiers. Each
request is independent. Routing uses the transport-level ``Mcp-Method`` and
``Mcp-Name`` headers, so the proxy never parses the application body to decide
where a request goes. Per-request ``_meta`` carries client identity; the proxy
injects its own identity when forwarding to a backend and forwards the
backend's ``_meta`` back to the client.

The wire envelope modeled here (``method``, ``name``, ``meta``, ``body``) is
transport-level, analogous to HTTP headers plus body under SEP-2243. The
``body`` string is the opaque application payload: the router treats it as
bytes and never decodes it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from . import namespace, version
from .forward import PROXY_SERVER_INFO
from .jsonrpc import INVALID_PARAMS, METHOD_NOT_FOUND
from .transport.base import Transport
from .version import PROTOCOL_VERSION_META_KEY, UNSUPPORTED_PROTOCOL_VERSION

CLIENT_INFO_META_KEY = "io.modelcontextprotocol/clientInfo"


@dataclass
class StatelessRequest:
    method: str
    name: str | None
    meta: dict = field(default_factory=dict)
    body: str = ""


@dataclass
class StatelessResponse:
    meta: dict = field(default_factory=dict)
    body: str = ""


def encode_request(request: StatelessRequest) -> bytes:
    return json.dumps(
        {"method": request.method, "name": request.name, "meta": request.meta, "body": request.body}
    ).encode("utf-8")


def decode_request(raw: bytes) -> StatelessRequest:
    data = json.loads(raw)
    return StatelessRequest(data["method"], data.get("name"), data.get("meta", {}), data.get("body", ""))


def encode_response(response: StatelessResponse) -> bytes:
    return json.dumps({"meta": response.meta, "body": response.body}).encode("utf-8")


def decode_response(raw: bytes) -> StatelessResponse:
    data = json.loads(raw)
    return StatelessResponse(data.get("meta", {}), data.get("body", ""))


def _error_body(code: int, message: str, data: dict | None = None) -> str:
    error: dict = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return json.dumps({"error": error})


class StatelessBackend:
    def __init__(self, id: str, transport: Transport) -> None:
        if not namespace.valid_backend_id(id):
            raise ValueError(f"invalid backend id: {id!r}")
        self.id = id
        self._transport = transport

    async def exchange(self, request: StatelessRequest) -> StatelessResponse:
        await self._transport.send(encode_request(request))
        raw = await self._transport.receive()
        if raw is None:
            raise ConnectionError(f"backend {self.id} closed")
        return decode_response(raw)

    async def close(self) -> None:
        await self._transport.send_eof()


class StatelessForwarder:
    def __init__(self, client: Transport, backends: list[StatelessBackend]) -> None:
        self._client = client
        self._backends = {backend.id: backend for backend in backends}

    async def serve(self) -> None:
        while True:
            raw = await self._client.receive()
            if raw is None:
                break
            request = decode_request(raw)
            response = await self._handle(request)
            await self._client.send(encode_response(response))
        for backend in self._backends.values():
            await backend.close()

    async def _handle(self, request: StatelessRequest) -> StatelessResponse:
        # Each stateless request is self-describing: negotiate its declared
        # protocol version before routing (SEP-2575). A version the proxy cannot
        # serve is rejected here rather than forwarded, since statelessness has
        # no handshake to fall back on.
        requested = request.meta.get(PROTOCOL_VERSION_META_KEY) if isinstance(request.meta, dict) else None
        negotiated = version.negotiate(requested)
        if negotiated is None:
            return StatelessResponse(
                meta=self._proxy_meta(),
                body=_error_body(
                    UNSUPPORTED_PROTOCOL_VERSION,
                    f"unsupported protocol version: {requested}",
                    version.unsupported_error_data(requested),
                ),
            )
        if request.method == "server/discover":
            return await self._discover()
        if request.method == "tools/call":
            return await self._call(request, negotiated)
        return StatelessResponse(
            meta=self._proxy_meta(),
            body=_error_body(METHOD_NOT_FOUND, f"method not routable: {request.method}"),
        )

    async def _discover(self) -> StatelessResponse:
        tools: list[dict] = []
        for backend in self._backends.values():
            response = await backend.exchange(
                StatelessRequest("server/discover", None, self._proxy_meta())
            )
            for tool in json.loads(response.body).get("tools", []):
                entry = dict(tool)
                entry["name"] = namespace.prefix(backend.id, tool["name"])
                tools.append(entry)
        return StatelessResponse(meta=self._proxy_meta(), body=json.dumps({"tools": tools}))

    async def _call(self, request: StatelessRequest, negotiated: str) -> StatelessResponse:
        # Route on the Mcp-Name header only. The body is never decoded here.
        resolved = namespace.split(request.name or "")
        if resolved is None or resolved[0] not in self._backends:
            return StatelessResponse(
                meta=self._proxy_meta(),
                body=_error_body(INVALID_PARAMS, f"unknown tool: {request.name}"),
            )
        backend_id, original = resolved
        # Carry the client's _meta forward (capabilities and any client fields),
        # inject the proxy identity, and pin the negotiated version so the
        # backend sees a self-describing request (SEP-2575).
        forwarded_meta = dict(request.meta)
        forwarded_meta[CLIENT_INFO_META_KEY] = PROXY_SERVER_INFO
        forwarded_meta[PROTOCOL_VERSION_META_KEY] = negotiated
        forwarded = StatelessRequest(
            method="tools/call",
            name=original,
            meta=forwarded_meta,
            body=request.body,  # forwarded unchanged, never parsed
        )
        return await self._backends[backend_id].exchange(forwarded)

    @staticmethod
    def _proxy_meta() -> dict:
        return {CLIENT_INFO_META_KEY: PROXY_SERVER_INFO}
