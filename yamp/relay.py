"""Layer 1 relay (draft §5.1, Relay mode).

Forwards messages bidirectionally between a client-facing and a backend-facing
transport without inspecting or modifying payloads, and without any
initialization handshake of its own. It may bridge differing framings (stdio
line framing to HTTP content-length framing) while preserving message
boundaries and payload bytes.
"""

from __future__ import annotations

import asyncio

from .transport.base import Transport


class Relay:
    def __init__(self, client: Transport, backend: Transport) -> None:
        self._client = client
        self._backend = backend

    async def run(self) -> None:
        """Pump both directions until either side reaches end of stream."""
        await asyncio.gather(
            self._pump(self._client, self._backend),
            self._pump(self._backend, self._client),
        )

    @staticmethod
    async def _pump(src: Transport, dst: Transport) -> None:
        while True:
            message = await src.receive()
            if message is None:
                await dst.send_eof()
                return
            await dst.send(message)
