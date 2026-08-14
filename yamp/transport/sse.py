"""Server-Sent Events framing (MCP HTTP+SSE transport).

Each message is one SSE event: one or more ``data:`` lines terminated by a
blank line. This is the server-to-client half of the MCP HTTP+SSE transport and
the streaming half of Streamable HTTP. Because a transport's read and write
sides are independent, this framing composes with the others: the relay and the
router bridge SSE to stdio or content-length with no change.

Only the ``data`` field carries the payload. Comment lines (starting with
``:``) and other fields (``event``, ``id``, ``retry``) are ignored on read, per
the SSE specification.
"""

from __future__ import annotations

import asyncio

from .base import Transport


class SseTransport(Transport):
    async def receive(self) -> bytes | None:
        data: list[bytes] = []
        while True:
            try:
                line = await self._reader.readuntil(b"\n")
            except asyncio.IncompleteReadError as exc:
                # End of stream. A well-formed SSE stream ends each event with a
                # blank line, so any accumulated data here is a final unterminated
                # event; return it, otherwise report end of stream.
                partial = exc.partial.rstrip(b"\r\n")
                if partial.startswith(b"data:"):
                    data.append(_field_value(partial))
                return b"\n".join(data) if data else None
            line = line.rstrip(b"\r\n")
            if line == b"":
                if data:
                    return b"\n".join(data)
                continue  # blank line with no data, skip
            if line.startswith(b":"):
                continue  # comment
            if line.startswith(b"data:"):
                data.append(_field_value(line))
            # other fields (event, id, retry) are ignored

    async def send(self, payload: bytes) -> None:
        out = bytearray()
        for part in payload.split(b"\n"):
            out += b"data: " + part + b"\n"
        out += b"\n"  # blank line terminates the event
        await self._writer.write(bytes(out))


def _field_value(line: bytes) -> bytes:
    value = line[len(b"data:"):]
    if value.startswith(b" "):
        value = value[1:]  # SSE strips one leading space after the colon
    return value
