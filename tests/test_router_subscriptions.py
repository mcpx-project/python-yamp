"""σ4 server-side resource subscriptions (Python arm). Mirrors the Rust arm.

With ``set_resource_subscriptions(True)`` a ``resources/subscribe`` whose URI
resolves to no backend is registered in a per-connection registry (an empty
result is returned), and ``publish_resource_update`` fans out
``notifications/resources/updated`` only to the subscribed URIs. Off by default,
so such a subscribe is rejected as an unknown resource.
"""

import asyncio

from yamp import jsonrpc, subscriptions
from yamp.jsonrpc import INVALID_PARAMS
from yamp.router import ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


def _new(on=True):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    router = ForwardRouter(LineTransport(c2r.reader, r2c), []).set_resource_subscriptions(on)
    client = LineTransport(r2c.reader, c2r)
    return router, client


async def _handshake(client):
    await client.send(jsonrpc.encode(jsonrpc.request("i", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))


async def _req(client, id, method, params):
    await client.send(jsonrpc.encode(jsonrpc.request(id, method, params)))
    return jsonrpc.decode(await client.receive())


def test_subscribe_registers_and_publish_fans_out():
    async def scenario():
        router, client = _new()
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        ack = await _req(client, "s", subscriptions.SUBSCRIBE_METHOD, {"uri": "mem://counter"})
        # Publish to a subscribed and an unsubscribed uri.
        sent_sub = await router.publish_resource_update("mem://counter")
        sent_other = await router.publish_resource_update("mem://unseen")
        note = jsonrpc.decode(await client.receive())  # only the subscribed one arrives
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return ack, sent_sub, sent_other, note

    ack, sent_sub, sent_other, note = asyncio.run(scenario())
    assert set(ack["result"]) <= {"_meta"}  # empty result (only the proxy trace hop)
    assert sent_sub is True and sent_other is False  # only subscribed uris fan out
    assert note["method"] == subscriptions.UPDATED_METHOD
    assert note["params"]["uri"] == "mem://counter"


def test_unsubscribe_stops_updates():
    async def scenario():
        router, client = _new()
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        await _req(client, "s", subscriptions.SUBSCRIBE_METHOD, {"uri": "mem://x"})
        await _req(client, "u", subscriptions.UNSUBSCRIBE_METHOD, {"uri": "mem://x"})
        sent = await router.publish_resource_update("mem://x")
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return sent

    assert asyncio.run(scenario()) is False  # no longer subscribed


def test_off_by_default_rejects_local_subscribe():
    async def scenario():
        router, client = _new(on=False)
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        r = await _req(client, "s", subscriptions.SUBSCRIBE_METHOD, {"uri": "mem://x"})
        sent = await router.publish_resource_update("mem://x")  # nothing was registered
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return r, sent

    r, sent = asyncio.run(scenario())
    assert r["error"]["code"] == INVALID_PARAMS
    assert sent is False
