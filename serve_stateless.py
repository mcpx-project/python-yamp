"""Run a yamp stateless forward proxy over TCP.

The stateless counterpart of serve.py. Each accepted client connection gets its
own StatelessForwarder, which opens its own connections to the backends. There
is no initialize handshake and no session id: every request is self-describing,
routed on its Mcp-Name header, and carries its protocol version in _meta
(SEP-2575). server/discover composes the backends' tool surfaces.

Usage:
  python serve_stateless.py --listen 127.0.0.1:9100 --backend b0=127.0.0.1:9101 --backend b1=127.0.0.1:9102
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from yamp.stateless import StatelessBackend, StatelessForwarder
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


async def handle_client(reader, writer, backend_specs):
    connections = []
    backends = []
    try:
        for backend_id, host, port in backend_specs:
            b_reader, b_writer = await asyncio.open_connection(host, port)
            connections.append(b_writer)
            backends.append(
                StatelessBackend(backend_id, LineTransport(b_reader, WriterEnd(b_writer)))
            )
        forwarder = StatelessForwarder(LineTransport(reader, WriterEnd(writer)), backends)
        await forwarder.serve()
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
    parser.add_argument("--listen", required=True)
    parser.add_argument("--backend", action="append", default=[])
    args = parser.parse_args()

    specs = [parse_backend(b) for b in args.backend]
    host, port = args.listen.rsplit(":", 1)
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, specs), host, int(port)
    )
    print(f"listening on {args.listen}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
