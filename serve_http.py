"""Run a yamp stateless proxy over HTTP (Streamable-HTTP request/response).

Each POST to /mcp carries one JSON-RPC message. The proxy reads the tool name,
resolves the namespace prefix to a backend, forwards the call over HTTP with a
pooled keep-alive connection, and returns the backend's response. This is the
stateless request/response path of MCP Streamable HTTP; server-initiated SSE is
not handled here.

Usage:
  python serve_http.py --listen 127.0.0.1:9100 --backend b0=http://127.0.0.1:9101/mcp
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))

from yamp import auth, media, namespace
from yamp.errors import INVALID_PARAMS, SERVER_NOT_AVAILABLE
from yamp.transport.framed import MAX_FRAME_BYTES

pools: dict[tuple[str, int], list] = {}


def content_length(header: bytes) -> int:
    # Bounded, defensive parse for the inline HTTP path (this entrypoint parses
    # Content-Length itself rather than through the framing decoder). An absent
    # header means no body (0). A present value that is non-numeric, negative, or
    # over MAX_FRAME_BYTES is rejected so a hostile header cannot force an
    # unbounded read, matching the framing decoder's cap.
    for line in header.split(b"\r\n"):
        name, sep, value = line.partition(b":")
        if sep and name.strip().lower() == b"content-length":
            length = int(value.strip())  # ValueError on non-numeric input
            if length < 0:
                raise ValueError("negative Content-Length")
            if length > MAX_FRAME_BYTES:
                raise ValueError("Content-Length exceeds maximum")
            return length
    return 0


def header_value(header: bytes, name: bytes) -> str | None:
    for line in header.split(b"\r\n"):
        field, sep, value = line.partition(b":")
        if sep and field.strip().lower() == name:
            return value.strip().decode()
    return None


async def read_http_message(reader) -> bytes | None:
    try:
        header = await reader.readuntil(b"\r\n\r\n")
    except (asyncio.IncompleteReadError, ConnectionResetError):
        return None
    try:
        length = content_length(header)  # ValueError on a hostile Content-Length
    except ValueError:
        return None
    return await reader.readexactly(length) if length else b""


async def backend_post(host: str, port: int, body: bytes) -> bytes:
    key = (host, port)
    pool = pools.setdefault(key, [])
    conn = pool.pop() if pool else await asyncio.open_connection(host, port)
    reader, writer = conn
    request = (
        b"POST /mcp HTTP/1.1\r\nHost: %s\r\nContent-Type: application/json\r\n"
        b"Content-Length: %d\r\nConnection: keep-alive\r\n\r\n"
        % (host.encode(), len(body))
    ) + body
    writer.write(request)
    await writer.drain()
    response = await read_http_message(reader)
    if response is None:
        # The backend closed the keep-alive connection: close our end and do not
        # return it to the pool, or the next request reuses a dead socket.
        writer.close()
        return b"{}"
    pool.append((reader, writer))
    return response


def error_body(code: int) -> bytes:
    return json.dumps({"jsonrpc": "2.0", "error": {"code": code}}).encode()


def make_handler(backends: dict[str, tuple[str, int]]):
    async def handle(reader, writer):
        try:
            while True:
                try:
                    header = await reader.readuntil(b"\r\n\r\n")
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    break
                try:
                    length = content_length(header)  # bounded; rejects hostile lengths
                except ValueError:
                    break
                body = await reader.readexactly(length) if length else b""
                out = await route(body, backends)
                # SEP-2357: answer in application/mcp+json when the client
                # accepts it, else application/json.
                content_type = media.response_content_type(header_value(header, b"accept"))
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: %s\r\n"
                    b"Content-Length: %d\r\nConnection: keep-alive\r\n\r\n"
                    % (content_type.encode(), len(out))
                    + out
                )
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            writer.close()

    return handle


async def route(body: bytes, backends: dict[str, tuple[str, int]]) -> bytes:
    try:
        message = json.loads(body)
    except ValueError:
        return error_body(INVALID_PARAMS)
    params = message.get("params") or {}
    resolved = namespace.split(params.get("name", ""))
    if resolved is None or resolved[0] not in backends:
        return error_body(INVALID_PARAMS)
    backend_id, original = resolved
    params["name"] = original
    # Confused-deputy defense (SEP §13.1): drop the client's credential from the
    # forwarded _meta so it never reaches a backend, mirroring the router path.
    if "_meta" in params:
        params["_meta"] = auth.forward_meta(params["_meta"], None)
    message["params"] = params
    host, port = backends[backend_id]
    try:
        return await backend_post(host, port, json.dumps(message).encode())
    except Exception:
        return error_body(SERVER_NOT_AVAILABLE)


def parse_backend(spec: str):
    backend_id, url = spec.split("=", 1)
    parsed = urlparse(url)
    return backend_id, (parsed.hostname, parsed.port)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", required=True)
    parser.add_argument("--backend", action="append", default=[])
    args = parser.parse_args()

    backends = dict(parse_backend(b) for b in args.backend)
    host, port = args.listen.rsplit(":", 1)
    server = await asyncio.start_server(make_handler(backends), host, int(port))
    print(f"listening on {args.listen}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
