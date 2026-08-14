import asyncio

from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


def _line(reader_pipe: MemoryPipe, write_pipe: MemoryPipe) -> LineTransport:
    return LineTransport(reader_pipe.reader, write_pipe)


def test_round_trip_single_message():
    async def scenario():
        wire = MemoryPipe()
        sink = MemoryPipe()
        transport = _line(wire, sink)
        await wire.write(b'{"jsonrpc":"2.0","id":1}\n')
        wire.write_eof()
        assert await transport.receive() == b'{"jsonrpc":"2.0","id":1}'
        assert await transport.receive() is None

    asyncio.run(scenario())


def test_multiple_messages_coalesced_in_one_chunk():
    async def scenario():
        wire = MemoryPipe()
        transport = _line(wire, MemoryPipe())
        await wire.write(b"a\nbb\nccc\n")
        wire.write_eof()
        assert await transport.receive() == b"a"
        assert await transport.receive() == b"bb"
        assert await transport.receive() == b"ccc"
        assert await transport.receive() is None

    asyncio.run(scenario())


def test_fragmented_delivery_preserves_boundary():
    async def scenario():
        wire = MemoryPipe()
        transport = _line(wire, MemoryPipe())
        for byte in b"hello\n":
            await wire.write(bytes([byte]))
        wire.write_eof()
        assert await transport.receive() == b"hello"
        assert await transport.receive() is None

    asyncio.run(scenario())


def test_unterminated_trailing_bytes_returned_then_eof():
    async def scenario():
        wire = MemoryPipe()
        transport = _line(wire, MemoryPipe())
        await wire.write(b"done\ntail-no-newline")
        wire.write_eof()
        assert await transport.receive() == b"done"
        assert await transport.receive() == b"tail-no-newline"
        assert await transport.receive() is None

    asyncio.run(scenario())


def test_send_appends_newline():
    async def scenario():
        sink = MemoryPipe()
        transport = _line(MemoryPipe(), sink)
        await transport.send(b"payload")
        sink.write_eof()
        assert await sink.reader.read() == b"payload\n"

    asyncio.run(scenario())


def test_send_eof_propagates():
    async def scenario():
        sink = MemoryPipe()
        transport = _line(MemoryPipe(), sink)
        await transport.send_eof()
        assert await sink.reader.read() == b""

    asyncio.run(scenario())
