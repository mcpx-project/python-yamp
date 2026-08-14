"""δ-testinfra: automated e2e for the stateless served entrypoint (serve_stateless).

Boots serve_stateless.py's per-connection handler against two live stub stateless
backends over real TCP sockets and drives a full server/discover -> tools/call as
a client, asserting the composed namespaced surface, a routed call with the
backend prefix stripped, and per-request protocol-version negotiation
(SEP-2575). Mirrors the Rust arm's tests/e2e_stateless.rs.
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # python/ for the entrypoint

import serve_stateless
from yamp.stateless import (
    StatelessRequest,
    StatelessResponse,
    decode_request,
    decode_response,
    encode_request,
    encode_response,
)
from yamp.transport.line import LineTransport
from yamp.version import PROTOCOL_VERSION_META_KEY, STATELESS_PROTOCOL_VERSION


def _stub_backend(name, tools):
    async def backend(reader, writer):
        transport = LineTransport(reader, serve_stateless.WriterEnd(writer))
        while True:
            raw = await transport.receive()
            if raw is None:
                return
            request = decode_request(raw)
            if request.method == "server/discover":
                body = json.dumps({"tools": [{"name": t} for t in tools]})
                await transport.send(encode_response(StatelessResponse(meta={"backend": name}, body=body)))
            elif request.method == "tools/call":
                # Echo the tool name (already stripped of the prefix) and the
                # negotiated version the proxy pinned into _meta.
                pinned = request.meta.get(PROTOCOL_VERSION_META_KEY)
                await transport.send(
                    encode_response(StatelessResponse(meta={"backend": name}, body=f"echoed:{request.name}:{pinned}"))
                )

    return backend


def test_e2e_stateless_discover_and_call():
    async def scenario():
        b0_server = await asyncio.start_server(_stub_backend("b0", ["echo"]), "127.0.0.1", 0)
        b0 = b0_server.sockets[0].getsockname()[1]
        b1_server = await asyncio.start_server(_stub_backend("b1", ["echo"]), "127.0.0.1", 0)
        b1 = b1_server.sockets[0].getsockname()[1]

        specs = [("b0", "127.0.0.1", b0), ("b1", "127.0.0.1", b1)]
        proxy = await asyncio.start_server(lambda r, w: serve_stateless.handle_client(r, w, specs), "127.0.0.1", 0)
        pport = proxy.sockets[0].getsockname()[1]

        reader, writer = await asyncio.open_connection("127.0.0.1", pport)
        client = LineTransport(reader, serve_stateless.WriterEnd(writer))

        async def exchange(request):
            await client.send(encode_request(request))
            return decode_response(await asyncio.wait_for(client.receive(), timeout=5))

        version_meta = {PROTOCOL_VERSION_META_KEY: STATELESS_PROTOCOL_VERSION}
        discover = await exchange(StatelessRequest("server/discover", None, dict(version_meta)))
        called = await exchange(StatelessRequest("tools/call", "b0__echo", dict(version_meta), body="{}"))

        writer.close()
        for server_obj in (proxy, b0_server, b1_server):
            server_obj.close()
            await server_obj.wait_closed()
        return discover, called

    discover, called = asyncio.run(scenario())
    names = {t["name"] for t in json.loads(discover.body)["tools"]}
    assert names == {"b0__echo", "b1__echo"}  # two backends -> prefixed surface
    # Routed to b0 with the prefix stripped and the negotiated version pinned.
    assert called.body == f"echoed:echo:{STATELESS_PROTOCOL_VERSION}"
