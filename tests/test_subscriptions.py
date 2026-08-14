"""σ4 resource subscriptions (Python arm). Mirrors the Rust arm.

Two roles from one seam (like tasks: δ19 routing + σ3 origination):
- Proxy role: resources/subscribe|unsubscribe reverse-resolve their namespaced
  URI to the owning backend and forward with the backend's own URI; the backend's
  notifications/resources/updated is re-namespaced so the client sees the same
  backend__uri it holds.
- Server role: a subscribe whose URI resolves to no backend is registered in a
  per-connection registry, and publish_resource_update fans out the notification
  only to subscribed URIs.
"""

import asyncio

from yamp import jsonrpc, subscriptions
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.jsonrpc import INVALID_PARAMS
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


# --- pure module surface ---


def test_is_subscribe_method():
    assert subscriptions.is_subscribe_method("resources/subscribe")
    assert subscriptions.is_subscribe_method("resources/unsubscribe")
    assert not subscriptions.is_subscribe_method("resources/read")
    assert not subscriptions.is_subscribe_method(None)


def test_updated_notification_shape():
    assert subscriptions.updated_notification("file:///x") == {
        "jsonrpc": "2.0",
        "method": "notifications/resources/updated",
        "params": {"uri": "file:///x"},
    }


def test_namespace_updated_renames_uri():
    msg = {"jsonrpc": "2.0", "method": subscriptions.UPDATED_METHOD, "params": {"uri": "file:///reports/q3.md", "title": "Q3"}}
    out = subscriptions.namespace_updated(msg, "docs")
    assert out["params"]["uri"] == "file:///docs/reports/q3.md"
    assert out["params"]["title"] == "Q3"  # other fields preserved
    assert msg["params"]["uri"] == "file:///reports/q3.md"  # input not mutated


def test_namespace_updated_ignores_uriless():
    no_params = {"method": subscriptions.UPDATED_METHOD}
    assert subscriptions.namespace_updated(no_params, "docs") == no_params
    no_uri = {"method": subscriptions.UPDATED_METHOD, "params": {"seq": 3}}
    assert subscriptions.namespace_updated(no_uri, "docs") == no_uri


def test_subscriptions_registry():
    reg = subscriptions.Subscriptions()
    assert reg.count() == 0 and not reg.contains("u1")
    reg.subscribe("u1")
    reg.subscribe("u1")  # idempotent
    assert reg.contains("u1") and reg.count() == 1
    assert reg.unsubscribe("u1") is True
    assert reg.unsubscribe("u1") is False  # already gone
    assert not reg.contains("u1")


# --- proxy role: route subscribe to the owning backend, re-namespace updates ---

BACKENDS = ["docs", "wiki"]


async def _resource_backend(read_pipe, write_pipe, name, log):
    t = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await t.receive())
    await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"resources": {"subscribe": True}}, "serverInfo": {"name": name}})))
    await t.receive()
    while True:
        raw = await t.receive()
        if raw is None:
            await t.send_eof()
            return
        m = jsonrpc.decode(raw)
        log.append(m)
        if m["method"] in (subscriptions.SUBSCRIBE_METHOD, subscriptions.UNSUBSCRIBE_METHOD):
            await t.send(jsonrpc.encode(jsonrpc.result(m["id"], {})))
            if m["method"] == subscriptions.SUBSCRIBE_METHOD:
                # Emit a change for the just-subscribed resource, using the
                # backend's own (un-prefixed) uri.
                await t.send(jsonrpc.encode(jsonrpc.notification(subscriptions.UPDATED_METHOD, {"uri": m["params"]["uri"]})))


def test_subscribe_routes_and_updated_is_renamespaced():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        logs = {name: [] for name in BACKENDS}
        objs, task_list = [], []
        for name in BACKENDS:
            pr2b, b2pr = MemoryPipe(), MemoryPipe()
            objs.append(Backend(name, LineTransport(b2pr.reader, pr2b)))
            task_list.append(asyncio.create_task(_resource_backend(pr2b, b2pr, name, logs[name])))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), objs)
        rt = asyncio.create_task(router.serve())
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))

        await client.send(jsonrpc.encode(jsonrpc.request("s", subscriptions.SUBSCRIBE_METHOD, {"uri": "file:///docs/reports/q3.md"})))
        ack, updated = None, None
        while ack is None or updated is None:
            msg = jsonrpc.decode(await client.receive())
            if msg.get("id") == "s":
                ack = msg
            elif msg.get("method") == subscriptions.UPDATED_METHOD:
                updated = msg
        # An unknown resource (no backend, server subs off) is rejected.
        await client.send(jsonrpc.encode(jsonrpc.request("u", subscriptions.SUBSCRIBE_METHOD, {"uri": "file:///ghost/x"})))
        unknown = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(rt, *task_list), timeout=5)
        return ack, updated, unknown, logs

    ack, updated, unknown, logs = asyncio.run(scenario())
    assert "result" in ack  # subscribe acknowledged
    # docs saw the subscribe with its own un-prefixed uri; wiki untouched.
    docs_sub = [m for m in logs["docs"] if m["method"] == subscriptions.SUBSCRIBE_METHOD]
    assert docs_sub and docs_sub[0]["params"]["uri"] == "file:///reports/q3.md"
    assert not any(m["method"] == subscriptions.SUBSCRIBE_METHOD for m in logs["wiki"])
    # The backend's updated notification is re-namespaced to what the client holds.
    assert updated["params"]["uri"] == "file:///docs/reports/q3.md"
    assert unknown["error"]["code"] == INVALID_PARAMS


def test_unsubscribe_routes_to_backend():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        logs = {name: [] for name in BACKENDS}
        objs, task_list = [], []
        for name in BACKENDS:
            pr2b, b2pr = MemoryPipe(), MemoryPipe()
            objs.append(Backend(name, LineTransport(b2pr.reader, pr2b)))
            task_list.append(asyncio.create_task(_resource_backend(pr2b, b2pr, name, logs[name])))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), objs)
        rt = asyncio.create_task(router.serve())
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send(jsonrpc.encode(jsonrpc.request("u", subscriptions.UNSUBSCRIBE_METHOD, {"uri": "file:///wiki/p"})))
        ack = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(rt, *task_list), timeout=5)
        return ack, logs

    ack, logs = asyncio.run(scenario())
    assert "result" in ack
    wiki_unsub = [m for m in logs["wiki"] if m["method"] == subscriptions.UNSUBSCRIBE_METHOD]
    assert wiki_unsub and wiki_unsub[0]["params"]["uri"] == "file:///p"


def test_single_backend_subscribe_passes_uri_through():
    # A single backend passes names through (SEP §5.3): the uri is neither
    # namespaced on subscribe nor re-namespaced on the updated notification.
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        log = []
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [Backend("only", LineTransport(b2pr.reader, pr2b))])
        rt = asyncio.create_task(router.serve())
        bt = asyncio.create_task(_resource_backend(pr2b, b2pr, "only", log))
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send(jsonrpc.encode(jsonrpc.request("s", subscriptions.SUBSCRIBE_METHOD, {"uri": "file:///reports/q3.md"})))
        ack, updated = None, None
        while ack is None or updated is None:
            msg = jsonrpc.decode(await client.receive())
            if msg.get("id") == "s":
                ack = msg
            elif msg.get("method") == subscriptions.UPDATED_METHOD:
                updated = msg
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(rt, bt), timeout=5)
        return updated, log

    updated, log = asyncio.run(scenario())
    sub = [m for m in log if m["method"] == subscriptions.SUBSCRIBE_METHOD][0]
    assert sub["params"]["uri"] == "file:///reports/q3.md"  # passed through unchanged
    assert updated["params"]["uri"] == "file:///reports/q3.md"  # not re-namespaced


def test_subscribe_backend_error_passes_through():
    async def erroring_backend(read_pipe, write_pipe):
        t = LineTransport(read_pipe.reader, write_pipe)
        init = jsonrpc.decode(await t.receive())
        await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"resources": {"subscribe": True}}, "serverInfo": {"name": "docs"}})))
        await t.receive()
        while True:
            raw = await t.receive()
            if raw is None:
                await t.send_eof()
                return
            m = jsonrpc.decode(raw)
            await t.send(jsonrpc.encode(jsonrpc.error(m["id"], -32050, "cannot subscribe")))

    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        # Two backends so the uri is namespaced and reverse-resolves to docs.
        pr2b2, b2pr2 = MemoryPipe(), MemoryPipe()
        objs = [Backend("docs", LineTransport(b2pr.reader, pr2b)), Backend("wiki", LineTransport(b2pr2.reader, pr2b2))]
        rt = asyncio.create_task(ForwardRouter(LineTransport(c2r.reader, r2c), objs).serve())
        bt = asyncio.create_task(erroring_backend(pr2b, b2pr))
        bt2 = asyncio.create_task(_resource_backend(pr2b2, b2pr2, "wiki", []))
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send(jsonrpc.encode(jsonrpc.request("s", subscriptions.SUBSCRIBE_METHOD, {"uri": "file:///docs/x"})))
        r = jsonrpc.decode(await client.receive())
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(rt, bt, bt2), timeout=5)
        return r

    r = asyncio.run(scenario())
    assert r["error"]["code"] == -32050  # backend error passed through unchanged


def test_subscribe_resilient_failure_then_breaker_open():
    from yamp.resilience import SERVER_NOT_AVAILABLE, CircuitBreaker

    async def dying_backend(read_pipe, write_pipe):
        t = LineTransport(read_pipe.reader, write_pipe)
        init = jsonrpc.decode(await t.receive())
        await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"resources": {"subscribe": True}}, "serverInfo": {"name": "docs"}})))
        await t.receive()
        await t.receive()  # the subscribe request
        await t.send_eof()  # die mid-request

    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        pr2b2, b2pr2 = MemoryPipe(), MemoryPipe()
        objs = [
            Backend("docs", LineTransport(b2pr.reader, pr2b), breaker=CircuitBreaker(failure_threshold=1)),
            Backend("wiki", LineTransport(b2pr2.reader, pr2b2), breaker=CircuitBreaker(failure_threshold=1)),
        ]
        rt = asyncio.create_task(ForwardRouter(LineTransport(c2r.reader, r2c), objs).serve())
        bt = asyncio.create_task(dying_backend(pr2b, b2pr))
        bt2 = asyncio.create_task(_resource_backend(pr2b2, b2pr2, "wiki", []))
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))

        async def call(id, uri):
            await client.send(jsonrpc.encode(jsonrpc.request(id, subscriptions.SUBSCRIBE_METHOD, {"uri": uri})))
            while True:  # drain any breaker-driven list_changed notification
                msg = jsonrpc.decode(await client.receive())
                if msg.get("id") == id:
                    return msg

        failed = await call("s1", "file:///docs/x")  # backend dies -> failure path
        open_breaker = await call("s2", "file:///docs/y")  # breaker now open -> unavailable
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(rt, bt, bt2), timeout=5)
        return failed, open_breaker

    failed, open_breaker = asyncio.run(scenario())
    assert failed["error"]["code"] == SERVER_NOT_AVAILABLE
    assert open_breaker["error"]["code"] == SERVER_NOT_AVAILABLE


def test_subscribe_non_resilient_failure_propagates():
    # Without a breaker, a backend that dies mid-subscribe propagates the failure
    # rather than swallowing it.
    async def dying(read_pipe, write_pipe):
        t = LineTransport(read_pipe.reader, write_pipe)
        init = jsonrpc.decode(await t.receive())
        await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"resources": {"subscribe": True}}, "serverInfo": {"name": "only"}})))
        await t.receive()
        await t.receive()  # the subscribe request
        await t.send_eof()  # die with no breaker

    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        rt = asyncio.create_task(ForwardRouter(LineTransport(c2r.reader, r2c), [Backend("only", LineTransport(b2pr.reader, pr2b))]).serve())
        bt = asyncio.create_task(dying(pr2b, b2pr))
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send(jsonrpc.encode(jsonrpc.request("s", subscriptions.SUBSCRIBE_METHOD, {"uri": "file:///x"})))
        return await asyncio.wait_for(asyncio.gather(rt, bt, return_exceptions=True), timeout=5)

    results = asyncio.run(scenario())
    assert any(isinstance(r, Exception) for r in results)  # the failure propagated
