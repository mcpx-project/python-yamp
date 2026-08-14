"""In-process byte pipe used to wire transports together.

The read end is an ``asyncio.StreamReader``; the write end feeds bytes into
that reader. Two pipes form a duplex connection. This is both the test
substrate and the seam the relay pumps across, so it is production code, not a
test helper.
"""

from __future__ import annotations

import asyncio

_DEFAULT_LIMIT = 1 << 20  # 1 MiB readuntil window


class MemoryPipe:
    """One-directional in-process byte pipe.

    Implements the :class:`~yamp.transport.base.WriteEnd` protocol on the write
    side and exposes an ``asyncio.StreamReader`` on the read side.
    """

    def __init__(self, limit: int = _DEFAULT_LIMIT) -> None:
        self.reader = asyncio.StreamReader(limit=limit)

    async def write(self, data: bytes) -> None:
        self.reader.feed_data(data)

    def write_eof(self) -> None:
        self.reader.feed_eof()
