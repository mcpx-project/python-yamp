"""Forward proxy, stateful mode (SEP §2.2, draft §5.2).

The proxy is explicitly addressed by the client and is protocol-aware. It
performs an independent ``initialize`` / ``notifications/initialized``
handshake with the backend (dual handshake), then composes a single
client-facing ``initialize`` response whose ``serverInfo`` identifies the
proxy and whose ``protocolVersion`` is the proxy's own highest. Backend
identity is never exposed to the client. After the handshake, messages are
forwarded unchanged (single backend; namespacing arrives in δ2).
"""

from __future__ import annotations

from . import jsonrpc
from .jsonrpc import INVALID_REQUEST
from .relay import Relay
from .transport.base import Transport
from .version import STATEFUL_PROTOCOL_VERSION

# The version the stateful served path advertises (SEP §2.2: the intermediary's
# own highest). Sourced from the single version module; kept here as the name
# every layer already imports.
PROXY_PROTOCOL_VERSION = STATEFUL_PROTOCOL_VERSION
# The proxy's identity, single-sourced here (the repo's "proxy identity in
# forward" rule) so observability and the REST adapter do not re-derive it.
PROXY_NAME = "yamp"
PROXY_VERSION = "0.0.0"
PROXY_SERVER_INFO = {"name": PROXY_NAME, "version": PROXY_VERSION}


class HandshakeError(Exception):
    """Raised when the client does not open with a valid initialize."""


class ForwardProxy:
    def __init__(self, client: Transport, backend: Transport) -> None:
        self._client = client
        self._backend = backend
        # Kept internal; never sent to the client.
        self.backend_server_info: jsonrpc.Message | None = None

    async def serve(self) -> None:
        if await self._handshake():
            await Relay(self._client, self._backend).run()

    async def _handshake(self) -> bool:
        raw = await self._client.receive()
        if raw is None:
            return False
        client_init = jsonrpc.decode(raw)
        if jsonrpc.method_of(client_init) != "initialize":
            await self._client.send(
                jsonrpc.encode(
                    jsonrpc.error(
                        client_init.get("id"), INVALID_REQUEST, "expected initialize"
                    )
                )
            )
            raise HandshakeError("first client message was not initialize")

        backend_caps = await self._backend_handshake()

        await self._client.send(
            jsonrpc.encode(
                jsonrpc.result(
                    client_init.get("id"),
                    {
                        "protocolVersion": PROXY_PROTOCOL_VERSION,
                        "capabilities": backend_caps,
                        "serverInfo": PROXY_SERVER_INFO,
                    },
                )
            )
        )
        # Consume the client's notifications/initialized to complete the
        # client-facing handshake. The backend was initialized independently.
        await self._client.receive()
        return True

    async def _backend_handshake(self) -> jsonrpc.Message:
        await self._backend.send(
            jsonrpc.encode(
                jsonrpc.request(
                    1,
                    "initialize",
                    {
                        "protocolVersion": PROXY_PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": PROXY_SERVER_INFO,
                    },
                )
            )
        )
        raw = await self._backend.receive()
        if raw is None:
            raise HandshakeError("backend closed during initialize")
        backend_init = jsonrpc.decode(raw)
        backend_result = backend_init.get("result", {})
        self.backend_server_info = backend_result.get("serverInfo")
        await self._backend.send(
            jsonrpc.encode(jsonrpc.notification("notifications/initialized"))
        )
        return backend_result.get("capabilities", {})
