"""Run a yamp forward proxy over TCP.

Each accepted client connection gets its own ForwardRouter, which opens its own
connections to the backends and runs the MCP handshake. This is the entrypoint
the external load harness (../bench) drives.

Usage:
  python serve.py --listen 127.0.0.1:9100 --backend b0=127.0.0.1:9101 --backend b1=127.0.0.1:9102
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from yamp import security
from yamp.cache import ListCache
from yamp.config import HandlerConfig, load_config, parse_address
from yamp.handler import build_registry
from yamp.router import Backend, ForwardRouter
from yamp.signing import AuditLog
from yamp.transport.line import LineTransport


class WriterEnd:
    """Adapt an asyncio.StreamWriter to yamp's WriteEnd."""

    def __init__(self, writer: asyncio.StreamWriter) -> None:
        self._writer = writer

    async def write(self, data: bytes) -> None:
        self._writer.write(data)
        await self._writer.drain()

    def write_eof(self) -> None:
        try:
            self._writer.write_eof()
        except Exception:
            pass


async def handle_client(reader, writer, backend_specs, cache, handlers=None, namespacing=None, tokens=None, audit=None):
    connections = []
    backends = []
    tokens = tokens or {}
    try:
        for backend_id, host, port in backend_specs:
            b_reader, b_writer = await asyncio.open_connection(host, port)
            connections.append(b_writer)
            backends.append(Backend(backend_id, LineTransport(b_reader, WriterEnd(b_writer)), token=tokens.get(backend_id)))
        # Local handlers (Conversion, meta-tools) served alongside the backends.
        registry = None
        if handlers is not None:
            registry = build_registry(
                handlers, backends_provider=lambda: [{"id": b.id} for b in backends]
            )
        # One cache shared across every client connection, so repeated list
        # fetches by many clients collapse to O(backends) (SEP §6).
        router = ForwardRouter(
            LineTransport(reader, WriterEnd(writer)),
            backends,
            cache=cache,
            registry=registry,
            namespacing=namespacing,
            audit=audit,
        )
        await router.serve()
    except Exception as exc:
        # A per-connection failure (client disconnect, malformed handshake) must
        # not take down the server, but it should surface rather than vanish.
        print(f"connection closed on error: {exc!r}", file=sys.stderr, flush=True)
    finally:
        writer.close()
        for b_writer in connections:
            b_writer.close()


def parse_backend(spec):
    backend_id, addr = spec.split("=", 1)
    host, port = addr.rsplit(":", 1)
    return backend_id, host, int(port)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen")
    parser.add_argument("--backend", action="append", default=[])
    parser.add_argument("--config")
    parser.add_argument("--insecure", action="store_true", help="allow a non-loopback bind without client auth")
    args = parser.parse_args()

    handlers = None
    namespacing = None
    tokens = {}
    audit = None
    has_client_auth = False
    if args.config:
        config = load_config(args.config)
        listen = args.listen or config.listen
        specs = [(b.id, *parse_address(b.addresses[0])) for b in config.backends]
        tokens = {b.id: b.token for b in config.backends if b.token}
        handlers = config.handlers
        namespacing = config.namespacing
        has_client_auth = bool(config.client_tokens)
        # A config audit secret enables one accountability log shared across
        # every connection, so the hash chain spans the whole proxy (SEP-2828).
        if config.audit_secret:
            audit = AuditLog(config.audit_secret)
    else:
        listen = args.listen
        specs = [parse_backend(b) for b in args.backend]

    # Secure default (U7): refuse to expose a non-loopback listener without client
    # auth unless the operator explicitly opts out with --insecure.
    refusal = security.guard_bind(listen, has_client_auth, args.insecure)
    if refusal is not None:
        print(f"error: {refusal}", file=sys.stderr, flush=True)
        sys.exit(2)

    cache = ListCache()
    if audit is not None:
        print("accountability log enabled", flush=True)
    host, port = listen.rsplit(":", 1)
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, specs, cache, handlers, namespacing, tokens, audit), host, int(port)
    )
    print(f"listening on {listen}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
