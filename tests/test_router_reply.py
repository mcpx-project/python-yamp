"""δ14 bidirectional reply-routing and notification forwarding (Python arm).

Mirrors the Rust arm. Pins two bugs the increment fixes: a client's reply to a
backend-initiated request must route back to the originating backend, and a
client notification must be forwarded onward rather than dropped.
"""

import asyncio

from yamp import jsonrpc
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


async def _handshake(client):
    await client.send(
        jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}}))
    )
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))


def _make_backend(name, on_ready=None, tool_result=None):
    async def backend(read_pipe, write_pipe, log):
        t = LineTransport(read_pipe.reader, write_pipe)
        init = jsonrpc.decode(await t.receive())
        await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"capabilities": {"tools": {}}, "serverInfo": {"name": name}})))
        await t.receive()  # notifications/initialized
        if on_ready is not None:
            await t.send(jsonrpc.encode(on_ready))
        while True:
            raw = await t.receive()
            if raw is None:
                await t.send_eof()
                return
            msg = jsonrpc.decode(raw)
            log.append(msg)
            if msg.get("method") == "tools/call":
                result = tool_result if tool_result is not None else {"content": [{"type": "text", "text": "ok"}]}
                await t.send(jsonrpc.encode(jsonrpc.result(msg["id"], result)))

    return backend


async def _run(backend_coro, client_steps, backend_id="b"):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    pr2b, b2pr = MemoryPipe(), MemoryPipe()
    log: list = []
    backend = Backend(backend_id, LineTransport(b2pr.reader, pr2b))
    router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend])
    router_task = asyncio.create_task(router.serve())
    backend_task = asyncio.create_task(backend_coro(pr2b, b2pr, log))
    client = LineTransport(r2c.reader, c2r)
    await _handshake(client)
    result = await client_steps(client)
    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(router_task, backend_task), timeout=5)
    return log, result


def test_server_initiated_request_reply_routes_back_to_backend():
    # A backend pushes a sampling request; the client's reply must reach the
    # backend with the backend's own id restored.
    sampling = jsonrpc.request("bkreq-1", "sampling/createMessage", {"prompt": "hi"})

    async def client_steps(client):
        pushed = jsonrpc.decode(await client.receive())
        assert pushed["method"] == "sampling/createMessage"
        assert pushed["id"].startswith("srv-")  # proxy minted a unique client id
        assert pushed["id"] != "bkreq-1"
        await client.send(jsonrpc.encode(jsonrpc.result(pushed["id"], {"content": "sampled"})))
        # A normal call afterwards proves the channel is still healthy.
        await client.send(jsonrpc.encode(jsonrpc.request("c2", "tools/call", {"name": "b__x"})))
        return jsonrpc.decode(await client.receive())

    log, called = asyncio.run(_run(_make_backend("b", on_ready=sampling), client_steps))
    # The backend received the reply with its ORIGINAL id, not the proxy's.
    replies = [m for m in log if m.get("id") == "bkreq-1" and "result" in m]
    assert replies and replies[0]["result"] == {"content": "sampled"}
    assert called["result"]["content"][0]["text"] == "ok"


def test_client_notification_forwarded_to_backend():
    async def client_steps(client):
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/progress", {"progress": 0.5})))
        # Follow with a call so the notification has certainly been processed
        # before teardown.
        await client.send(jsonrpc.encode(jsonrpc.request("c2", "tools/call", {"name": "b__x"})))
        return jsonrpc.decode(await client.receive())

    log, _called = asyncio.run(_run(_make_backend("b"), client_steps))
    progress = [m for m in log if m.get("method") == "notifications/progress"]
    assert progress and progress[0]["params"]["progress"] == 0.5


def test_client_cancellation_routes_to_holding_backend_with_original_id():
    # The backend initiates a request; the client cancels it. The proxy must
    # deliver the cancellation to that backend with the backend's own id
    # restored, not broadcast it (SEP §5.1, SEP-2260/2322).
    sampling = jsonrpc.request("bkreq-1", "sampling/createMessage", {"prompt": "hi"})

    async def client_steps(client):
        pushed = jsonrpc.decode(await client.receive())
        srv_id = pushed["id"]
        # Cancel the proxy-facing id; the backend must see its own "bkreq-1".
        await client.send(
            jsonrpc.encode(jsonrpc.notification("notifications/cancelled", {"requestId": srv_id, "reason": "user aborted"}))
        )
        # A cancel for an id the proxy never minted must be dropped, not routed.
        await client.send(
            jsonrpc.encode(jsonrpc.notification("notifications/cancelled", {"requestId": "never-seen"}))
        )
        # A trailing call flushes the pipeline so both notifications are handled.
        await client.send(jsonrpc.encode(jsonrpc.request("c2", "tools/call", {"name": "b__x"})))
        return jsonrpc.decode(await client.receive())

    log, _called = asyncio.run(_run(_make_backend("b", on_ready=sampling), client_steps))
    cancels = [m for m in log if m.get("method") == "notifications/cancelled"]
    # Exactly one cancellation reached the backend: the tracked one, translated.
    assert len(cancels) == 1
    assert cancels[0]["params"]["requestId"] == "bkreq-1"
    assert cancels[0]["params"]["reason"] == "user aborted"


def test_stray_client_response_is_dropped_not_errored():
    async def client_steps(client):
        # A response with an id the proxy never minted: dropped silently.
        await client.send(jsonrpc.encode(jsonrpc.result("no-such-id", {"x": 1})))
        await client.send(jsonrpc.encode(jsonrpc.request("c2", "tools/call", {"name": "b__x"})))
        return jsonrpc.decode(await client.receive())

    _log, called = asyncio.run(_run(_make_backend("b"), client_steps))
    # The next call still succeeds; the stray reply did not derail routing.
    assert called["result"]["content"][0]["text"] == "ok"


def test_notification_forward_survives_dead_backend():
    # If forwarding to a backend fails (its pipe is closed), the router must not
    # crash; the notification is best-effort.
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backend = Backend("b", LineTransport(b2pr.reader, pr2b))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend])
        router_task = asyncio.create_task(router.serve())
        backend_task = asyncio.create_task(_make_backend("b")(pr2b, b2pr, []))
        client = LineTransport(r2c.reader, c2r)
        await _handshake(client)
        # Close the backend's read pipe so any further send into it raises.
        pr2b.write_eof()
        await asyncio.sleep(0.01)
        # Forwarding this notification hits the send failure and swallows it.
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/progress", {"progress": 1.0})))
        await asyncio.sleep(0.01)
        await client.send_eof()
        # The router stays alive and shuts down cleanly.
        await asyncio.wait_for(asyncio.gather(router_task, backend_task), timeout=5)
        return True

    assert asyncio.run(scenario())


def test_mrtr_result_and_request_state_pass_through_verbatim():
    # An MRTR-style result carries a resultType and an opaque requestState the
    # proxy MUST NOT parse. It and a follow-up request that echoes requestState
    # must pass through unchanged.
    opaque = {"token": "abc123", "nested": [1, 2, {"k": "v"}]}
    mrtr_result = {"resultType": "input_required", "requestState": opaque, "content": []}

    async def client_steps(client):
        await client.send(jsonrpc.encode(jsonrpc.request("c2", "tools/call", {"name": "b__x"})))
        first = jsonrpc.decode(await client.receive())
        # Client echoes requestState verbatim in a follow-up request.
        await client.send(
            jsonrpc.encode(jsonrpc.request("c3", "tools/call", {"name": "b__x", "arguments": {"requestState": opaque}}))
        )
        second = jsonrpc.decode(await client.receive())
        return first, second

    log, (first, second) = asyncio.run(_run(_make_backend("b", tool_result=mrtr_result), client_steps))
    # The result reached the client with resultType and requestState intact.
    assert first["result"]["resultType"] == "input_required"
    assert first["result"]["requestState"] == opaque
    # The backend saw the follow-up request with requestState byte-identical.
    follow = [m for m in log if m.get("params", {}).get("arguments", {}).get("requestState")]
    assert follow and follow[0]["params"]["arguments"]["requestState"] == opaque
