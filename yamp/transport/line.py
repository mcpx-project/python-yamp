"""Newline-delimited framing (MCP stdio transport).

One JSON-RPC message per line. The payload is the line without its trailing
newline, forwarded unchanged.
"""

from __future__ import annotations

import asyncio

from .base import Transport


class LineTransport(Transport):
    async def receive(self) -> bytes | None:
        try:
            line = await self._reader.readuntil(b"\n")
        except asyncio.IncompleteReadError as exc:
            # EOF: a non-empty ``partial`` is a final unterminated message;
            # an empty one is a clean end of stream.
            return exc.partial or None
        return line[:-1]

    async def send(self, payload: bytes) -> None:
        await self._writer.write(payload + b"\n")
