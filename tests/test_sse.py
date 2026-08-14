import asyncio

from yamp.relay import Relay
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe
from yamp.transport.sse import SseTransport


def _sse(reader_pipe, write_pipe):
    return SseTransport(reader_pipe.reader, write_pipe)


def test_round_trip_single_message():
    async def scenario():
        wire = MemoryPipe()
        transport = _sse(wire, MemoryPipe())
        await wire.write(b'data: {"jsonrpc":"2.0","id":1}\n\n')
        wire.write_eof()
        assert await transport.receive() == b'{"jsonrpc":"2.0","id":1}'
        assert await transport.receive() is None

    asyncio.run(scenario())


def test_send_encodes_event():
    async def scenario():
        sink = MemoryPipe()
        transport = _sse(MemoryPipe(), sink)
        await transport.send(b'{"x":1}')
        sink.write_eof()
        assert await sink.reader.read() == b'data: {"x":1}\n\n'

    asyncio.run(scenario())


def test_multiline_payload_round_trips():
    async def scenario():
        pipe = MemoryPipe()
        transport = _sse(pipe, pipe)
        await transport.send(b"line1\nline2")
        pipe.write_eof()
        # two data: lines, rejoined with a newline on receive
        assert await transport.receive() == b"line1\nline2"

    asyncio.run(scenario())


def test_comments_and_other_fields_ignored():
    async def scenario():
        wire = MemoryPipe()
        transport = _sse(wire, MemoryPipe())
        await wire.write(b": keepalive\nevent: message\nid: 7\ndata: payload\n\n")
        wire.write_eof()
        assert await transport.receive() == b"payload"

    asyncio.run(scenario())


def test_leading_blank_lines_skipped():
    async def scenario():
        wire = MemoryPipe()
        transport = _sse(wire, MemoryPipe())
        await wire.write(b"\n\ndata: x\n\n")  # blank lines before any data
        wire.write_eof()
        assert await transport.receive() == b"x"

    asyncio.run(scenario())


def test_leading_space_stripped_once():
    async def scenario():
        wire = MemoryPipe()
        transport = _sse(wire, MemoryPipe())
        await wire.write(b"data:nospace\n\ndata:  onespace\n\n")
        wire.write_eof()
        assert await transport.receive() == b"nospace"
        assert await transport.receive() == b" onespace"

    asyncio.run(scenario())


def test_fragmented_and_multiple_events():
    async def scenario():
        wire = MemoryPipe()
        transport = _sse(wire, MemoryPipe())
        for byte in b"data: a\n\ndata: b\n\n":
            await wire.write(bytes([byte]))
        wire.write_eof()
        assert await transport.receive() == b"a"
        assert await transport.receive() == b"b"
        assert await transport.receive() is None

    asyncio.run(scenario())


def test_unterminated_final_event_returned():
    async def scenario():
        wire = MemoryPipe()
        transport = _sse(wire, MemoryPipe())
        await wire.write(b"data: done\n\ndata: tail")  # no trailing blank line
        wire.write_eof()
        assert await transport.receive() == b"done"
        assert await transport.receive() == b"tail"
        assert await transport.receive() is None

    asyncio.run(scenario())


# ---- mixing transports: bridge SSE to stdio ----

async def _line_echo(read_pipe, write_pipe):
    transport = LineTransport(read_pipe.reader, write_pipe)
    while True:
        message = await transport.receive()
        if message is None:
            await transport.send_eof()
            return
        await transport.send(message)


def test_relay_bridges_sse_client_to_stdio_backend():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()  # client speaks SSE
        r2b, b2r = MemoryPipe(), MemoryPipe()  # backend speaks stdio (line)
        relay = Relay(
            client=SseTransport(c2r.reader, r2c),
            backend=LineTransport(b2r.reader, r2b),
        )
        backend = asyncio.create_task(_line_echo(r2b, b2r))
        relay_task = asyncio.create_task(relay.run())

        client = SseTransport(r2c.reader, c2r)
        await client.send(b'{"jsonrpc":"2.0","id":1,"method":"ping"}')
        echoed = await client.receive()
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(relay_task, backend), timeout=5)
        return echoed

    assert asyncio.run(scenario()) == b'{"jsonrpc":"2.0","id":1,"method":"ping"}'
