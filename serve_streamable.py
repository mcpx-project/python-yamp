"""Serve yamp as an MCP Streamable HTTP proxy.

This wraps the real ForwardRouter in HTTP. A client POSTs JSON-RPC messages to
/mcp. On initialize the server creates a session, runs a ForwardRouter for it
(fed through an in-memory pipe so all the core handshake, routing, and
namespacing logic is reused unchanged), assigns an Mcp-Session-Id, and returns
the composed initialize response. Later POSTs carrying the session id are routed
through that session's router. Requests get a JSON response; notifications get
202. DELETE ends the session.

Backends connect over TCP with stdio (newline) framing. Server-initiated SSE
streams (GET /mcp) are not implemented here; this is the request/response path.

Usage:
  python serve_streamable.py --listen 127.0.0.1:9100 --backend b0=127.0.0.1:9101
"""

import argparse
import asyncio
import contextlib
import json
import signal
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from yamp import config as cfg
from yamp import jsonrpc, routing, security, status, tap
from yamp.config import BackendConfig, ProxyConfig, Resilience, from_dict, load_config, parse_address
from yamp.errors import INVALID_PARAMS, NO_SESSION, UNAUTHORIZED
from yamp.policy import BearerAuthenticator, PolicyLayer
from yamp.transport.framed import MAX_FRAME_BYTES
from yamp.resilience import CircuitBreaker
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def _connect_failover(addresses):
    """Try each address in order; return (reader, writer) or None if all fail."""
    for address in addresses:
        host, port = parse_address(address)
        try:
            return await asyncio.open_connection(host, port)
        except OSError:
            continue
    return None


class WriterEnd:
    def __init__(self, writer):
        self._writer = writer

    async def write(self, data):
        self._writer.write(data)
        await self._writer.drain()

    def write_eof(self):
        try:
            self._writer.write_eof()
        except Exception:
            pass


class Session:
    """A running ForwardRouter fed request-by-request over an in-memory pipe."""

    def __init__(self):
        self.lock = asyncio.Lock()
        self.outbound: asyncio.Queue[bytes] = asyncio.Queue()

    async def push(self, message: bytes) -> None:
        """Enqueue a server-to-client message for the GET SSE stream."""
        await self.outbound.put(message)

    @classmethod
    async def create(cls, backend_configs, resilience=None):
        self = cls()
        c2r, r2c = MemoryPipe(), MemoryPipe()
        self._client = LineTransport(r2c.reader, c2r)  # our side: send -> c2r, recv <- r2c
        self._backend_writers = []
        backends = []
        resilient = resilience is not None and resilience.enabled
        for backend in backend_configs:
            conn = await _connect_failover(backend.addresses)
            if conn is None:
                # Cannot reach any address. In resilient mode leave it out; the
                # health loop cannot recover a backend with no connection.
                if resilient:
                    continue
                raise ConnectionError(f"backend {backend.id}: all addresses failed")
            reader, writer = conn
            self._backend_writers.append(writer)
            breaker = (
                CircuitBreaker(resilience.failure_threshold, resilience.reset_timeout)
                if resilient
                else None
            )
            timeout = resilience.request_timeout if resilient else None
            backends.append(
                Backend(backend.id, LineTransport(reader, WriterEnd(writer)), breaker=breaker, request_timeout=timeout)
            )

        async def on_server(backend_id, message):
            # A backend-initiated message goes to this session's SSE stream.
            await self.push(jsonrpc.encode(message))

        health = resilience.health_interval if resilient else None
        router = ForwardRouter(
            LineTransport(c2r.reader, r2c), backends, on_server_message=on_server, health_interval=health
        )
        self._task = asyncio.create_task(router.serve())
        return self

    async def request(self, body: bytes) -> bytes:
        async with self.lock:
            await self._client.send(body)
            return await self._client.receive()

    async def notify(self, body: bytes) -> None:
        async with self.lock:
            await self._client.send(body)

    async def close(self):
        await self._client.send_eof()
        try:
            await asyncio.wait_for(self._task, 2)
        except (asyncio.TimeoutError, Exception):
            self._task.cancel()
        for writer in self._backend_writers:
            writer.close()


async def read_request(reader):
    try:
        head = await reader.readuntil(b"\r\n\r\n")
    except (asyncio.IncompleteReadError, ConnectionResetError):
        return None
    lines = head.split(b"\r\n")
    # A malformed request line (no method/path/version) is a bad request: close.
    try:
        method, path, _ = lines[0].decode("latin-1").split(" ", 2)
    except ValueError:
        return None
    headers = {}
    for line in lines[1:]:
        name, sep, value = line.partition(b":")
        if sep:
            headers[name.decode("latin-1").strip().lower()] = value.decode("latin-1").strip()
    # Bounded, defensive parse (this entrypoint parses Content-Length itself
    # rather than through the framing decoder): a non-numeric, negative, or
    # over-MAX_FRAME_BYTES value is rejected so a hostile header cannot force an
    # unbounded read, matching the framing decoder's cap.
    try:
        length = int(headers.get("content-length", "0"))
    except ValueError:
        return None
    if length < 0 or length > MAX_FRAME_BYTES:
        return None
    body = await reader.readexactly(length) if length else b""
    return method, path, headers, body


async def write_response(writer, status, headers, body):
    out = [f"HTTP/1.1 {status}"]
    for name, value in headers.items():
        out.append(f"{name}: {value}")
    out.append(f"Content-Length: {len(body)}")
    out.append("Connection: keep-alive")
    writer.write(("\r\n".join(out) + "\r\n\r\n").encode("latin-1") + body)
    await writer.drain()


def make_handler(state, sessions, tap_enabled=False):
    # ``state`` is a mutable holder ({"backends", "resilience", "policy"}) so a SIGHUP
    # reload can swap the config new connections see without dropping in-flight ones.
    async def handle(reader, writer):
        try:
            while True:
                request = await read_request(reader)
                if request is None:
                    break
                method, path, headers, body = request
                if method == "GET" and path == "/.well-known/mcp":
                    # Publish the proxy's Server Card for discovery (SEP-2127).
                    card = json.dumps(routing.server_card()).encode()
                    await write_response(writer, "200 OK", {"Content-Type": "application/json"}, card)
                    continue
                if method == "GET" and path == "/status":
                    # Read-only operational status (Track U). Sorted keys and compact
                    # separators so the bytes match the Rust arm's serde_json map
                    # serialization (sorted, no spaces).
                    snap = json.dumps(
                        status.snapshot([b.id for b in state["backends"]], len(sessions)),
                        sort_keys=True, separators=(",", ":"),
                    ).encode()
                    await write_response(writer, "200 OK", {"Content-Type": "application/json"}, snap)
                    continue
                if path != "/mcp":
                    await write_response(writer, "404 Not Found", {}, b"")
                    continue
                sid = headers.get("mcp-session-id")
                if method == "GET":
                    # Server-to-client SSE stream (Streamable HTTP GET /mcp).
                    session = sessions.get(sid)
                    if session is None:
                        await write_response(writer, "404 Not Found", {}, b"")
                        continue
                    await _serve_sse(writer, session)
                    return  # this connection is the stream until the client leaves
                if method == "DELETE":
                    session = sessions.pop(sid, None)
                    if session is not None:
                        await session.close()
                    await write_response(writer, "200 OK", {}, b"")
                    continue

                try:
                    message = json.loads(body) if body else {}
                except ValueError:
                    # A malformed JSON POST must get a 400, not silently kill the
                    # connection (JSONDecodeError is not in the handler's except).
                    await write_response(writer, "400 Bad Request", {"Content-Type": "application/json"},
                                         json.dumps({"error": {"code": INVALID_PARAMS, "message": "invalid JSON"}}).encode())
                    continue
                if tap_enabled:
                    # Redacting live capture (Track U): never surface a credential.
                    print(json.dumps(tap.capture("c2s", message), sort_keys=True, separators=(",", ":")), file=sys.stderr, flush=True)
                is_request = "id" in message and "method" in message

                if sid is None and message.get("method") == "initialize":
                    policy = state["policy"]
                    if policy is not None and not policy.authorize_client({"Authorization": headers.get("authorization", "")}):
                        await write_response(writer, "401 Unauthorized", {"Content-Type": "application/json"},
                                             json.dumps({"error": {"code": UNAUTHORIZED, "message": "unauthorized"}}).encode())
                        continue
                    session = await Session.create(state["backends"], state["resilience"])
                    sid = uuid.uuid4().hex
                    sessions[sid] = session
                    response = await session.request(body)
                    await write_response(
                        writer, "200 OK",
                        {"Content-Type": "application/json", "Mcp-Session-Id": sid},
                        response,
                    )
                elif sid in sessions:
                    session = sessions[sid]
                    if is_request:
                        response = await session.request(body)
                        await write_response(writer, "200 OK", {"Content-Type": "application/json"}, response)
                    else:
                        await session.notify(body)
                        await write_response(writer, "202 Accepted", {}, b"")
                else:
                    await write_response(writer, "400 Bad Request", {"Content-Type": "application/json"},
                                         json.dumps({"error": {"code": NO_SESSION, "message": "no session"}}).encode())
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            writer.close()

    return handle


async def _serve_sse(writer, session):
    writer.write(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
        b"Cache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n"
    )
    await writer.drain()
    # Stream backend-initiated messages routed to this session, with a keepalive
    # comment when idle.
    try:
        while True:
            try:
                message = await asyncio.wait_for(session.outbound.get(), timeout=15.0)
            except asyncio.TimeoutError:
                writer.write(b": keepalive\n\n")
                await writer.drain()
                continue
            writer.write(b"data: " + message + b"\n\n")
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError):
        pass


def parse_backend(spec):
    backend_id, addr = spec.split("=", 1)
    return BackendConfig(id=backend_id, addresses=[addr])


def _policy_for(config: ProxyConfig):
    return PolicyLayer(client_authenticator=BearerAuthenticator(set(config.client_tokens))) if config.client_tokens else None


async def serve(config: ProxyConfig, insecure: bool = False, tap_enabled: bool = False, config_path: str | None = None):
    sessions = {}
    host, port = parse_address(config.listen)
    # Secure default (U7): refuse a non-loopback bind without client auth unless the
    # operator explicitly opts out with --insecure.
    refusal = security.guard_bind(config.listen, bool(config.client_tokens), insecure)
    if refusal is not None:
        print(f"error: {refusal}", file=sys.stderr, flush=True)
        sys.exit(2)
    # A mutable holder so a SIGHUP reload swaps the config new connections use while
    # in-flight sessions keep their own and finish uninterrupted (U5, zero dropped).
    state = {"backends": config.backends, "resilience": config.resilience, "policy": _policy_for(config)}
    handler = make_handler(state, sessions, tap_enabled)

    def reload():
        # Validate the new document before swapping; a bad reload is rejected and the
        # running config is kept, so a typo never takes the server down.
        if config_path is None:
            return
        try:
            raw = json.loads(Path(config_path).read_text())
        except (OSError, ValueError) as exc:
            print(f"reload rejected: {exc}", file=sys.stderr, flush=True)
            return
        diagnosis = cfg.diagnose(raw)
        if diagnosis is not None:
            print(f"reload rejected: {diagnosis['slug']}: {diagnosis['message']}", file=sys.stderr, flush=True)
            return
        new_config = cfg.from_dict(raw)
        state["backends"] = new_config.backends
        state["resilience"] = new_config.resilience
        state["policy"] = _policy_for(new_config)
        print("config reloaded", flush=True)

    if config_path is not None:
        with contextlib.suppress(NotImplementedError, ValueError):
            asyncio.get_running_loop().add_signal_handler(signal.SIGHUP, reload)

    server = await asyncio.start_server(handler, host, port)
    print(f"listening on {config.listen}", flush=True)
    async with server:
        await server.serve_forever()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="JSON config file (SEP schema)")
    parser.add_argument("--listen")
    parser.add_argument("--backend", action="append", default=[])
    parser.add_argument("--insecure", action="store_true", help="allow a non-loopback bind without client auth")
    parser.add_argument("--tap", action="store_true", help="print a redacted capture of each client request to stderr")
    args = parser.parse_args()
    if args.config:
        config = load_config(args.config)
    else:
        config = ProxyConfig(listen=args.listen, backends=[parse_backend(b) for b in args.backend], resilience=Resilience())
    await serve(config, args.insecure, args.tap, config_path=args.config)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
