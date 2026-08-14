"""Router integration for server variants and variant-bound cursors (SEP-2053).

Drives the served ForwardRouter against mock backends that advertise variants and
paginate, asserting the four proxy obligations: compose the offered variants,
forward the selection, mint a variant-bound composite cursor, and reject a
continuation used under the wrong variant. Mirrors the Rust arm's
tests/router_variants.rs.
"""

import asyncio

from yamp import jsonrpc, variants
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe

EXT = variants.EXTENSION_ID
KEY = variants.SERVER_VARIANT_META_KEY


async def _mock(read_pipe, write_pipe, name, variant_ids, paginates):
    transport = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await transport.receive())
    caps = {"tools": {}}
    if variant_ids:
        caps["extensions"] = {EXT: {"availableVariants": [{"id": v} for v in variant_ids]}}
    await transport.send(
        jsonrpc.encode(jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": caps, "serverInfo": {"name": name}}))
    )
    await transport.receive()  # notifications/initialized
    while True:
        raw = await transport.receive()
        if raw is None:
            await transport.send_eof()
            return
        message = jsonrpc.decode(raw)
        if message["method"] == "tools/list":
            params = message.get("params", {})
            variant = (params.get("_meta") or {}).get(KEY, "none")
            cursor = params.get("cursor")
            if paginates and cursor is None:
                # Page 1: echo the received variant into the tool name and page.
                body = {"tools": [{"name": f"{name}_{variant}_p1"}], "nextCursor": "backend-p2"}
            elif paginates and cursor == "backend-p2":
                body = {"tools": [{"name": f"{name}_{variant}_p2"}]}
            else:
                body = {"tools": [{"name": f"{name}_{variant}"}]}
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], body)))


class _Session:
    def __init__(self, specs):
        # specs: list of (id, variant_ids, paginates)
        self.specs = specs

    async def __aenter__(self):
        self._c2r, self._r2c = MemoryPipe(), MemoryPipe()
        backends, self._tasks = [], []
        for bid, vids, paginates in self.specs:
            pr2b, b2pr = MemoryPipe(), MemoryPipe()
            backends.append(Backend(bid, LineTransport(b2pr.reader, pr2b)))
            self._tasks.append(asyncio.create_task(_mock(pr2b, b2pr, bid, vids, paginates)))
        router = ForwardRouter(LineTransport(self._c2r.reader, self._r2c), backends)
        self._router_task = asyncio.create_task(router.serve())
        self.client = LineTransport(self._r2c.reader, self._c2r)
        await self.client.send(
            jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
        )
        self.init = jsonrpc.decode(await self.client.receive())
        await self.client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        return self

    async def __aexit__(self, *exc):
        await self.client.send_eof()
        await asyncio.wait_for(asyncio.gather(self._router_task, *self._tasks), timeout=15)

    async def call(self, method, params):
        await self.client.send(jsonrpc.encode(jsonrpc.request("r", method, params)))
        return jsonrpc.decode(await self.client.receive())


def _run(coro):
    return asyncio.run(coro)


def test_handshake_composes_offered_variants_by_intersection():
    async def go():
        async with _Session([("b0", ["a", "b", "c"], False), ("b1", ["b", "c", "d"], False)]) as s:
            offered = s.init["result"]["capabilities"]["extensions"][EXT]["availableVariants"]
            assert [v["id"] for v in offered] == ["b", "c"]
    _run(go())


def test_no_variant_extension_when_disjoint():
    async def go():
        async with _Session([("b0", ["a"], False), ("b1", ["b"], False)]) as s:
            assert EXT not in s.init["result"]["capabilities"].get("extensions", {})
    _run(go())


def test_selected_variant_forwarded_to_backends():
    async def go():
        async with _Session([("b0", ["a", "b"], False), ("b1", ["a", "b"], False)]) as s:
            resp = await s.call("tools/list", {"_meta": {KEY: "b"}})
            names = [t["name"] for t in resp["result"]["tools"]]
            assert "b0__b0_b" in names and "b1__b1_b" in names  # both backends saw variant "b"
    _run(go())


def test_unknown_variant_rejected():
    async def go():
        async with _Session([("b0", ["a", "b"], False)]) as s:
            resp = await s.call("tools/list", {"_meta": {KEY: "zzz"}})
            assert resp["error"]["code"] == jsonrpc.INVALID_PARAMS
            assert resp["error"]["data"]["availableVariants"] == ["a", "b"]
    _run(go())


def test_variant_selected_but_unsupported_rejected():
    async def go():
        async with _Session([("b0", [], False)]) as s:  # backend has no variants
            resp = await s.call("tools/list", {"_meta": {KEY: "b"}})
            assert "not supported" in resp["error"]["message"]
    _run(go())


def test_composite_cursor_paginates_and_binds_variant():
    async def go():
        async with _Session([("b0", ["a", "b"], True), ("b1", ["a", "b"], False)]) as s:
            page1 = await s.call("tools/list", {"_meta": {KEY: "a"}})
            cursor = page1["result"]["nextCursor"]
            # The composite cursor is opaque and binds the active variant.
            assert cursor.startswith(variants.CURSOR_PREFIX)
            variant, backends = variants.resolve_cursor(cursor)
            assert variant == "a" and set(backends) == {"b0"}  # only the paginating backend
            # Continuation under the same variant reaches page 2 of that backend.
            page2 = await s.call("tools/list", {"_meta": {KEY: "a"}, "cursor": cursor})
            names = [t["name"] for t in page2["result"]["tools"]]
            assert names == ["b0__b0_a_p2"]
            assert "nextCursor" not in page2["result"]
    _run(go())


def test_cursor_rejected_under_wrong_variant():
    async def go():
        async with _Session([("b0", ["a", "b"], True)]) as s:
            page1 = await s.call("tools/list", {"_meta": {KEY: "a"}})
            cursor = page1["result"]["nextCursor"]
            resp = await s.call("tools/list", {"_meta": {KEY: "b"}, "cursor": cursor})
            assert resp["error"]["message"] == "Cursor invalid for requested variant"
            assert resp["error"]["data"] == {"cursorVariant": "a", "requestedVariant": "b"}
    _run(go())


def test_unknown_cursor_rejected_by_aggregator():
    async def go():
        async with _Session([("b0", [], False), ("b1", [], False)]) as s:
            resp = await s.call("tools/list", {"cursor": "not-a-proxy-cursor"})
            assert resp["error"]["message"] == "unknown cursor"
    _run(go())


def test_single_backend_passes_raw_cursor_through():
    # With one backend there is nothing to disambiguate (SEP §5.3), so a raw
    # backend cursor is forwarded straight through rather than rejected.
    async def go():
        async with _Session([("b0", [], True)]) as s:
            resp = await s.call("tools/list", {"cursor": "backend-p2"})
            names = [t["name"] for t in resp["result"]["tools"]]
            assert names == ["b0_none_p2"]  # single mode keeps the unprefixed name
    _run(go())
