"""Transport interface (draft §6.1, Layer 1).

A transport frames an opaque payload, a serialized JSON-RPC envelope, on the
wire. In increment δ0 it never inspects payload contents. The relay is
byte-faithful, so the exact payload bytes of every message are preserved
across a framing bridge.
"""

from __future__ import annotations

import asyncio
from typing import Protocol, runtime_checkable


@runtime_checkable
class WriteEnd(Protocol):
    """The write half of a byte channel."""

    async def write(self, data: bytes) -> None: ...

    def write_eof(self) -> None: ...


class Transport:
    """Base transport over an ``asyncio.StreamReader`` and a ``WriteEnd``.

    Subclasses implement one framing by overriding :meth:`receive` and
    :meth:`send`. The payload handed to :meth:`send` and returned by
    :meth:`receive` is the message body only, never the framing.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: WriteEnd) -> None:
        self._reader = reader
        self._writer = writer

    async def receive(self) -> bytes | None:
        """Return the next message payload, or ``None`` at end of stream."""
        raise NotImplementedError

    async def send(self, payload: bytes) -> None:
        """Frame and write one message payload."""
        raise NotImplementedError

    async def send_eof(self) -> None:
        """Signal end of stream to the peer on the write side."""
        self._writer.write_eof()
