"""Transparent mode, Level 1 (SEP §10, transport-aware).

A Level 1 transparent intermediary intercepts traffic at the network layer and
forwards it byte-faithfully. It inspects only the transport-level headers
(``Mcp-Method``, ``Mcp-Name``) to observe, log, or block; it never parses the
application body, performs no initialize handshake, and does no namespacing.
The original destination recovered from the intercepted socket selects the
backend for the connection.

The message envelope modeled here (``headers`` plus an opaque ``body`` string)
stands in for an HTTP request under SEP-2243: reading ``headers`` is free at the
transport layer, while ``body`` is never decoded. Forwarding sends the original
bytes unchanged, so no body modification is possible.
"""

from __future__ import annotations

import asyncio
import json
from typing import Iterable, Protocol

from .errors import POLICY_DENIED
from .transport.base import Transport


def encode_envelope(headers: dict, body: str) -> bytes:
    return json.dumps({"headers": headers, "body": body}).encode("utf-8")


def peek_headers(raw: bytes) -> dict:
    """Read transport headers without decoding the application body."""
    return json.loads(raw).get("headers", {})


def recover_original_destination(sock: object) -> tuple[str, int]:
    """Recover the pre-DNAT destination of an intercepted socket.

    On Linux this reads ``SO_ORIGINAL_DST`` (or the TPROXY equivalent). It
    requires a real intercepted socket and is therefore integration-only; it is
    never exercised by the in-process unit tests.
    """
    raise NotImplementedError("SO_ORIGINAL_DST recovery is integration-only")


def select_backend(destination: tuple[str, int], table: dict[tuple[str, int], Transport]) -> Transport:
    """Select the backend transport for a recovered original destination."""
    if destination not in table:
        raise KeyError(f"no backend for destination {destination}")
    return table[destination]


class Policy(Protocol):
    def allow(self, headers: dict) -> bool: ...


class AllowAll:
    def allow(self, headers: dict) -> bool:
        return True


class HeaderPolicy:
    """Blocks by ``Mcp-Method`` / ``Mcp-Name`` headers only, never the body."""

    def __init__(
        self,
        blocked_methods: Iterable[str] = (),
        blocked_names: Iterable[str] = (),
    ) -> None:
        self._blocked_methods = set(blocked_methods)
        self._blocked_names = set(blocked_names)

    def allow(self, headers: dict) -> bool:
        if headers.get("Mcp-Method") in self._blocked_methods:
            return False
        if headers.get("Mcp-Name") in self._blocked_names:
            return False
        return True


def _blocked_response() -> bytes:
    return encode_envelope(
        {"Mcp-Status": "blocked"},
        json.dumps({"error": {"code": POLICY_DENIED, "message": "blocked by policy"}}),
    )


class TransparentL1:
    def __init__(self, client: Transport, backend: Transport, policy: Policy | None = None) -> None:
        self._client = client
        self._backend = backend
        self._policy = policy or AllowAll()
        self.blocked = 0

    async def serve(self) -> None:
        await asyncio.gather(self._client_to_backend(), self._backend_to_client())

    async def _client_to_backend(self) -> None:
        while True:
            raw = await self._client.receive()
            if raw is None:
                await self._backend.send_eof()
                return
            headers = peek_headers(raw)
            if not self._policy.allow(headers):
                self.blocked += 1
                await self._client.send(_blocked_response())
                continue
            await self._backend.send(raw)  # forwarded unmodified

    async def _backend_to_client(self) -> None:
        while True:
            raw = await self._backend.receive()
            if raw is None:
                await self._client.send_eof()
                return
            await self._client.send(raw)  # forwarded unmodified
