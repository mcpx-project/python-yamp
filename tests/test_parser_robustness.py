"""Parser-robustness / fuzz tests for the hand-rolled framings.

Feeds malformed and adversarial byte streams through the Content-Length, SSE,
and line decoders and asserts they degrade gracefully: a bounded result (payload,
partial, or None at EOF) or a controlled ValueError, never a hang or an
unbounded allocation. Mirrors the Rust arm's tests/parser_robustness.rs.
"""

import asyncio
import random

import pytest

from yamp.transport.framed import MAX_FRAME_BYTES, FramedTransport, parse_content_length
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe
from yamp.transport.sse import SseTransport


def _reader(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


async def _receive(transport_cls, data: bytes):
    transport = transport_cls(_reader(data), MemoryPipe())
    return await transport.receive()


# --- Content-Length ---------------------------------------------------------


def test_content_length_rejects_negative():
    with pytest.raises(ValueError):
        parse_content_length(b"Content-Length: -1\r\n\r\n")


def test_content_length_rejects_non_numeric():
    with pytest.raises(ValueError):
        parse_content_length(b"Content-Length: abc\r\n\r\n")


def test_content_length_rejects_oversized():
    huge = MAX_FRAME_BYTES + 1
    with pytest.raises(ValueError):
        parse_content_length(f"Content-Length: {huge}\r\n\r\n".encode())


def test_content_length_at_the_cap_is_accepted():
    assert parse_content_length(f"Content-Length: {MAX_FRAME_BYTES}\r\n\r\n".encode()) == MAX_FRAME_BYTES


def test_content_length_ignores_junk_headers_before_the_real_one():
    async def scenario():
        return await _receive(FramedTransport, b"X-Junk: nonsense\r\nContent-Length: 2\r\n\r\nhi")

    assert asyncio.run(scenario()) == b"hi"


def test_content_length_no_terminator_is_eof():
    async def scenario():
        return await _receive(FramedTransport, b"Content-Length: 5\r\n")  # no blank line

    assert asyncio.run(scenario()) is None


# --- SSE --------------------------------------------------------------------


def test_sse_comments_only_is_eof():
    async def scenario():
        return await _receive(SseTransport, b": keepalive\n: another\n\n")

    assert asyncio.run(scenario()) is None


def test_sse_joins_multiple_data_lines():
    async def scenario():
        return await _receive(SseTransport, b"data: a\ndata: b\n\n")

    assert asyncio.run(scenario()) == b"a\nb"


def test_sse_unterminated_event_at_eof_is_returned():
    async def scenario():
        return await _receive(SseTransport, b"data: tail")  # no blank line, no newline

    assert asyncio.run(scenario()) == b"tail"


def test_sse_ignores_unknown_fields():
    async def scenario():
        return await _receive(SseTransport, b"event: message\nid: 7\nretry: 100\ndata: x\n\n")

    assert asyncio.run(scenario()) == b"x"


# --- line -------------------------------------------------------------------


def test_line_unterminated_at_eof_is_partial():
    async def scenario():
        return await _receive(LineTransport, b"no newline here")

    assert asyncio.run(scenario()) == b"no newline here"


def test_line_empty_stream_is_eof():
    async def scenario():
        return await _receive(LineTransport, b"")

    assert asyncio.run(scenario()) is None


# --- fuzz sweep -------------------------------------------------------------


def test_random_bytes_never_hang_or_crash_unexpectedly():
    # Every decoder must, on arbitrary bytes ending in EOF, either return a
    # bytes/None result or raise a controlled ValueError; nothing else, and it
    # must always terminate (the stream is EOF-bounded).
    rng = random.Random(0xC0FFEE)
    alphabet = b'{}":,\r\n data:Content-Length 0123456789xyz'

    async def scenario():
        for _ in range(400):
            length = rng.randint(0, 64)
            blob = bytes(rng.choice(alphabet) for _ in range(length))
            for cls in (FramedTransport, SseTransport, LineTransport):
                try:
                    out = await asyncio.wait_for(_receive(cls, blob), timeout=2.0)
                except ValueError:
                    continue  # controlled: malformed Content-Length
                assert out is None or isinstance(out, (bytes, bytearray))

    asyncio.run(scenario())
