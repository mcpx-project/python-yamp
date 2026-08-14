import asyncio

import pytest

from yamp.transport.framed import FramedTransport, parse_content_length
from yamp.transport.memory import MemoryPipe


def _framed(reader_pipe: MemoryPipe, write_pipe: MemoryPipe) -> FramedTransport:
    return FramedTransport(reader_pipe.reader, write_pipe)


def test_round_trip_single_message():
    async def scenario():
        wire = MemoryPipe()
        transport = _framed(wire, MemoryPipe())
        await wire.write(b"Content-Length: 3\r\n\r\nabc")
        wire.write_eof()
        assert await transport.receive() == b"abc"
        assert await transport.receive() is None

    asyncio.run(scenario())


def test_multiple_messages():
    async def scenario():
        wire = MemoryPipe()
        transport = _framed(wire, MemoryPipe())
        await wire.write(
            b"Content-Length: 1\r\n\r\nx"
            b"Content-Length: 2\r\n\r\nyz"
        )
        wire.write_eof()
        assert await transport.receive() == b"x"
        assert await transport.receive() == b"yz"
        assert await transport.receive() is None

    asyncio.run(scenario())


def test_fragmented_header_and_body():
    async def scenario():
        wire = MemoryPipe()
        transport = _framed(wire, MemoryPipe())
        for byte in b"Content-Length: 5\r\n\r\nhello":
            await wire.write(bytes([byte]))
        wire.write_eof()
        assert await transport.receive() == b"hello"

    asyncio.run(scenario())


def test_eof_before_header_returns_none():
    async def scenario():
        wire = MemoryPipe()
        transport = _framed(wire, MemoryPipe())
        wire.write_eof()
        assert await transport.receive() is None

    asyncio.run(scenario())


def test_truncated_body_returns_partial_then_none():
    async def scenario():
        wire = MemoryPipe()
        transport = _framed(wire, MemoryPipe())
        await wire.write(b"Content-Length: 10\r\n\r\nshort")
        wire.write_eof()
        assert await transport.receive() == b"short"
        assert await transport.receive() is None

    asyncio.run(scenario())


def test_payload_containing_framing_bytes_is_preserved():
    async def scenario():
        wire = MemoryPipe()
        transport = _framed(wire, MemoryPipe())
        payload = b'{"x":"Content-Length: 9\r\n\r\nnot-a-header"}'
        await wire.write(b"Content-Length: %d\r\n\r\n" % len(payload) + payload)
        wire.write_eof()
        assert await transport.receive() == payload

    asyncio.run(scenario())


def test_send_frames_with_content_length():
    async def scenario():
        sink = MemoryPipe()
        transport = _framed(MemoryPipe(), sink)
        await transport.send(b"abcd")
        sink.write_eof()
        assert await sink.reader.read() == b"Content-Length: 4\r\n\r\nabcd"

    asyncio.run(scenario())


def test_parse_content_length_case_insensitive():
    assert parse_content_length(b"content-length: 42\r\n\r\n") == 42
    assert parse_content_length(b"CONTENT-LENGTH:  7 \r\n\r\n") == 7


def test_parse_content_length_missing_raises():
    with pytest.raises(ValueError):
        parse_content_length(b"X-Other: 1\r\n\r\n")
