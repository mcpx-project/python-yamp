"""Runnable example: multiple proxies behind a transparent proxy.

Topology (all in-process, stateless mode):

    client -> outer (Transparent L2) -> inner (Transparent L2) -> leafA, leafB

The outer proxy's one backend is the inner proxy. The inner proxy's backends
are two leaf servers. This shows three things at once:

  1. A proxy can sit behind a transparent proxy: its backend is another proxy.
  2. Namespaces chain. A leaf tool `x` becomes `inner__leafA__x` at the client.
  3. Hop tracing accumulates. Each Level 2 hop appends to `_meta.proxy-hops`
     without replacing what is there, so a leaf sees two hops, not one.

Run:  cd python && ../.venv/bin/python examples/chained_proxies.py
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from yamp.observability import PROXY_HOPS_KEY
from yamp.stateless import (
    StatelessBackend,
    StatelessRequest,
    StatelessResponse,
    decode_request,
    decode_response,
    encode_request,
    encode_response,
)
from yamp.transparent_l2 import TransparentL2Stateless
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe

seen_hops = {}  # leaf name -> the number of proxy hops it received


async def leaf(read_pipe, write_pipe, name, tools):
    """A minimal stateless MCP server. It answers server/discover and records
    how many proxy hops the request carried."""
    t = LineTransport(read_pipe.reader, write_pipe)
    while True:
        raw = await t.receive()
        if raw is None:
            await t.send_eof()
            return
        req = decode_request(raw)
        seen_hops[name] = len(req.meta.get(PROXY_HOPS_KEY, []))
        body = json.dumps({"tools": [{"name": tool} for tool in tools]})
        await t.send(encode_response(StatelessResponse(meta={}, body=body)))


async def main():
    # Inner proxy in front of two leaf servers.
    ic2p, ip2c = MemoryPipe(), MemoryPipe()
    inner_backends, leaf_tasks = [], []
    for bid, tools in [("leafA", ["x"]), ("leafB", ["y"])]:
        p2b, b2p = MemoryPipe(), MemoryPipe()
        inner_backends.append(StatelessBackend(bid, LineTransport(b2p.reader, p2b)))
        leaf_tasks.append(asyncio.create_task(leaf(p2b, b2p, bid, tools)))
    inner = TransparentL2Stateless(LineTransport(ic2p.reader, ip2c), inner_backends)
    # A transport that talks to the inner proxy as if it were a client.
    inner_as_backend = LineTransport(ip2c.reader, ic2p)

    # Outer transparent proxy whose single backend is the inner proxy.
    oc2p, op2c = MemoryPipe(), MemoryPipe()
    outer = TransparentL2Stateless(
        LineTransport(oc2p.reader, op2c),
        [StatelessBackend("inner", inner_as_backend)],
    )
    client = LineTransport(op2c.reader, oc2p)

    tasks = [asyncio.create_task(outer.serve()), asyncio.create_task(inner.serve()), *leaf_tasks]

    await client.send(encode_request(StatelessRequest("server/discover", None, {})))
    resp = decode_response(await client.receive())
    tools = [t["name"] for t in json.loads(resp.body)["tools"]]
    print("client sees chained-namespaced tools:")
    print(json.dumps(tools, indent=2))
    print("proxy hops each leaf received:", json.dumps(seen_hops))

    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)


if __name__ == "__main__":
    asyncio.run(main())
