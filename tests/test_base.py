import asyncio

import pytest

from yamp.transport.base import Transport
from yamp.transport.memory import MemoryPipe


def test_base_transport_is_abstract():
    async def scenario():
        transport = Transport(MemoryPipe().reader, MemoryPipe())
        with pytest.raises(NotImplementedError):
            await transport.receive()
        with pytest.raises(NotImplementedError):
            await transport.send(b"x")

    asyncio.run(scenario())
