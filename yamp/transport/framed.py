"""Content-Length framing (HTTP / LSP-style message boundary).

``Content-Length: N\\r\\n\\r\\n`` followed by exactly N payload bytes. This
models the message boundary of an HTTP-based MCP transport, so the δ0 bridge
can relay between stdio line framing and an HTTP-style framing while preserving
boundaries. Because the boundary is a byte count, payloads that themselves
contain ``\\r\\n`` or the text ``Content-Length`` are carried unharmed.
"""

from __future__ import annotations

import asyncio

from .base import Transport

# A declared frame larger than this is rejected before any buffer is sized, so a
# hostile or corrupt Content-Length cannot force a huge allocation. Internal
# framed messages are small; 64 MiB is generous headroom.
MAX_FRAME_BYTES = 64 * 1024 * 1024


def parse_content_length(header: bytes) -> int:
    for line in header.split(b"\r\n"):
        name, sep, value = line.partition(b":")
        if sep and name.strip().lower() == b"content-length":
            length = int(value.strip())
            if length < 0:
                raise ValueError("negative Content-Length")
            if length > MAX_FRAME_BYTES:
                raise ValueError("Content-Length exceeds maximum")
            return length
    raise ValueError("missing Content-Length header")


class FramedTransport(Transport):
    async def receive(self) -> bytes | None:
        try:
            header = await self._reader.readuntil(b"\r\n\r\n")
        except asyncio.IncompleteReadError:
            return None
        length = parse_content_length(header)
        try:
            body = await self._reader.readexactly(length)
        except asyncio.IncompleteReadError as exc:
            return exc.partial or None
        return body

    async def send(self, payload: bytes) -> None:
        header = b"Content-Length: %d\r\n\r\n" % len(payload)
        await self._writer.write(header + payload)
