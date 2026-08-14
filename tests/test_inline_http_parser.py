"""Adversarial-input tests for the inline HTTP request parsers in the served
entrypoints (test-infra gap #4).

serve_http and serve_streamable parse Content-Length themselves (with a default)
rather than through the framing decoder, so they carry their own copy of the
allocation-amplification vector the framing decoder closed. These tests feed
malformed and hostile headers through the real parser functions and assert each
degrades to a bounded result (a body, or a clean close), never a hang or an
unbounded allocation.
"""

import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # python/ for the entrypoints

import serve_http
import serve_streamable
from yamp.transport.framed import MAX_FRAME_BYTES


class _Reader:
    """A minimal in-memory reader matching the asyncio.StreamReader surface the
    inline parsers use (readuntil, readexactly), with no event-loop dependency."""

    def __init__(self, data: bytes) -> None:
        self._buf = data
        self._pos = 0

    async def readuntil(self, sep: bytes) -> bytes:
        idx = self._buf.find(sep, self._pos)
        if idx == -1:
            partial, self._pos = self._buf[self._pos :], len(self._buf)
            raise asyncio.IncompleteReadError(partial, None)
        end = idx + len(sep)
        chunk, self._pos = self._buf[self._pos : end], end
        return chunk

    async def readexactly(self, n: int) -> bytes:
        if self._pos + n > len(self._buf):
            partial, self._pos = self._buf[self._pos :], len(self._buf)
            raise asyncio.IncompleteReadError(partial, n)
        chunk, self._pos = self._buf[self._pos : self._pos + n], self._pos + n
        return chunk


def _run(coro_fn, data: bytes):
    return asyncio.run(coro_fn(_Reader(data)))


# --- serve_http.content_length (pure) -------------------------------------


def test_content_length_normal_and_absent():
    assert serve_http.content_length(b"Content-Length: 12\r\n\r\n") == 12
    assert serve_http.content_length(b"Host: x\r\n\r\n") == 0  # absent -> no body


@pytest.mark.parametrize(
    "header",
    [
        b"Content-Length: abc\r\n\r\n",  # non-numeric
        b"Content-Length: -5\r\n\r\n",  # negative
        b"Content-Length: 999999999999\r\n\r\n",  # over MAX_FRAME_BYTES
        (b"Content-Length: %d\r\n\r\n" % (MAX_FRAME_BYTES + 1)),  # one past the cap
    ],
)
def test_content_length_rejects_hostile_values(header):
    with pytest.raises(ValueError):
        serve_http.content_length(header)


def test_content_length_accepts_the_cap_exactly():
    header = b"Content-Length: %d\r\n\r\n" % MAX_FRAME_BYTES
    assert serve_http.content_length(header) == MAX_FRAME_BYTES


# --- serve_http.read_http_message (async, bounded) ------------------------


def test_read_http_message_reads_declared_body():
    body = b'{"jsonrpc":"2.0"}'
    raw = b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n" % len(body) + body
    assert _run(serve_http.read_http_message, raw) == body


def test_read_http_message_rejects_oversized_length_without_reading():
    # A hostile Content-Length must not drive an unbounded readexactly; the
    # parser rejects it and the caller gets None (a clean close).
    raw = b"HTTP/1.1 200 OK\r\nContent-Length: 999999999999\r\n\r\nX"
    assert _run(serve_http.read_http_message, raw) is None


def test_read_http_message_none_at_eof():
    assert _run(serve_http.read_http_message, b"") is None


# --- serve_http.route (credential stripping) and backend_post (pooling) ---


def test_route_strips_client_credential_and_resolves(monkeypatch):
    seen = {}

    async def fake_post(host, port, body):
        seen["host"], seen["port"], seen["body"] = host, port, body
        return b'{"ok":1}'

    monkeypatch.setattr(serve_http, "backend_post", fake_post)
    backends = {"b0": ("127.0.0.1", 9101)}

    # A client credential in _meta must be dropped before the call is forwarded.
    msg = {
        "jsonrpc": "2.0", "id": "1", "method": "tools/call",
        "params": {"name": "b0__search", "_meta": {"authorization": "Bearer secret"}},
    }
    out = asyncio.run(serve_http.route(json.dumps(msg).encode(), backends))
    assert out == b'{"ok":1}'
    forwarded = json.loads(seen["body"])
    assert forwarded["params"]["name"] == "search"  # namespace prefix stripped
    assert "authorization" not in forwarded["params"]["_meta"]

    # A call with no _meta forwards fine (exercises the no-credential path).
    msg2 = {"jsonrpc": "2.0", "id": "2", "method": "tools/call", "params": {"name": "b0__list"}}
    asyncio.run(serve_http.route(json.dumps(msg2).encode(), backends))
    assert json.loads(seen["body"])["params"]["name"] == "list"


def test_route_rejects_unknown_backend_and_bad_json():
    from yamp.errors import INVALID_PARAMS

    backends = {"b0": ("127.0.0.1", 9101)}
    unknown = asyncio.run(serve_http.route(b'{"params":{"name":"zzz__x"}}', backends))
    assert json.loads(unknown)["error"]["code"] == INVALID_PARAMS
    bad = asyncio.run(serve_http.route(b"not-json", backends))
    assert json.loads(bad)["error"]["code"] == INVALID_PARAMS


def test_backend_post_drops_dead_connection(monkeypatch):
    serve_http.pools.clear()

    class _Writer:
        def __init__(self):
            self.closed = False

        def write(self, data):
            pass

        async def drain(self):
            pass

        def close(self):
            self.closed = True

    writer = _Writer()
    # A reader at EOF makes read_http_message return None (backend closed).
    reader = _Reader(b"")

    async def fake_open(host, port):
        return (reader, writer)

    monkeypatch.setattr(serve_http.asyncio, "open_connection", fake_open)
    out = asyncio.run(serve_http.backend_post("h", 1, b"{}"))
    assert out == b"{}"
    assert writer.closed is True
    assert serve_http.pools.get(("h", 1), []) == []  # dead connection not pooled


# --- serve_streamable.read_request (async, bounded) -----------------------


def test_read_request_parses_normal_post():
    body = b'{"method":"initialize"}'
    raw = b"POST /mcp HTTP/1.1\r\nContent-Length: %d\r\n\r\n" % len(body) + body
    method, path, headers, got = _run(serve_streamable.read_request, raw)
    assert (method, path, got) == ("POST", "/mcp", body)


@pytest.mark.parametrize(
    "raw",
    [
        b"POST /mcp HTTP/1.1\r\nContent-Length: 999999999999\r\n\r\nX",  # over cap
        b"POST /mcp HTTP/1.1\r\nContent-Length: -1\r\n\r\n",  # negative
        b"POST /mcp HTTP/1.1\r\nContent-Length: abc\r\n\r\n",  # non-numeric
        b"GARBLED-NO-SPACES\r\n\r\n",  # malformed request line
    ],
)
def test_read_request_rejects_hostile_input(raw):
    assert _run(serve_streamable.read_request, raw) is None


def test_read_request_none_at_eof():
    assert _run(serve_streamable.read_request, b"") is None
