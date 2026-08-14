import asyncio

from yamp import jsonrpc, tasks
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.jsonrpc import INVALID_PARAMS
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


def test_is_task_result():
    assert tasks.is_task_result({"resultType": "task", "taskId": "t1"})
    assert not tasks.is_task_result({"resultType": "complete"})
    assert not tasks.is_task_result("nope")


def test_namespace_task_id():
    out = tasks.namespace_task_id({"resultType": "task", "taskId": "t1", "task": {"taskId": "t1", "status": "working"}}, "gh")
    assert out["taskId"] == "gh__t1"
    assert out["task"]["taskId"] == "gh__t1"
    assert out["task"]["status"] == "working"  # other fields preserved


def test_resolve_task():
    assert tasks.resolve_task("gh__t1") == ("gh", "t1")
    assert tasks.resolve_task("nodelim") is None


def test_tasks_stream_is_a_read_task_method():
    # SEP-2694: tasks/stream routes like the other task methods and is a read.
    assert "tasks/stream" in tasks.TASKS_METHODS
    assert "tasks/stream" in tasks.TASK_READ_METHODS


def test_namespace_event_renames_task_id():
    event = {"method": tasks.TASK_EVENT_METHOD, "params": {"taskId": "T-9", "seq": 0, "type": "log"}}
    out = tasks.namespace_event(event, "gh")
    assert out["params"]["taskId"] == "gh__T-9"
    assert out["params"]["seq"] == 0 and out["params"]["type"] == "log"  # other fields preserved
    assert event["params"]["taskId"] == "T-9"  # input not mutated


def test_namespace_event_ignores_eventless_messages():
    assert tasks.namespace_event({"method": tasks.TASK_EVENT_METHOD}, "gh") == {"method": tasks.TASK_EVENT_METHOD}
    no_task = {"method": tasks.TASK_EVENT_METHOD, "params": {"seq": 3}}
    assert tasks.namespace_event(no_task, "gh") == no_task


def test_approval_gated_handle_is_an_ordinary_task():
    # SEP-2848: an approval-gated call returns a working task handle, which is an
    # ordinary task the proxy namespaces and routes; no new routing is needed.
    handle = {"resultType": "task", "taskId": "A-1", "status": "working"}
    assert tasks.is_task_result(handle)
    assert tasks.namespace_task_id(handle, "gh")["taskId"] == "gh__A-1"


BACKENDS = [("gh", ["run"]), ("gl", ["run"])]


async def _task_backend(read_pipe, write_pipe, name, log):
    t = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await t.receive())
    await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}, "extensions": {"io.modelcontextprotocol/tasks": {}}}, "serverInfo": {"name": name}})))
    await t.receive()
    while True:
        raw = await t.receive()
        if raw is None:
            await t.send_eof()
            return
        m = jsonrpc.decode(raw)
        log.append(m)
        if m["method"] == "tools/call":
            # Return a task handle with the backend's own opaque id.
            await t.send(jsonrpc.encode(jsonrpc.result(m["id"], {"resultType": "task", "taskId": "T-99", "task": {"taskId": "T-99", "status": "working"}})))
        elif m["method"] == "tasks/cancel":
            await t.send(jsonrpc.encode(jsonrpc.error(m["id"], -32050, "cannot cancel")))
        elif m["method"] in tasks.TASKS_METHODS:
            await t.send(jsonrpc.encode(jsonrpc.result(m["id"], {"resultType": "complete", "taskId": m["params"]["taskId"], "status": "completed", "handledBy": name})))


def test_task_creation_namespaced_and_tasks_get_routes_back():
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        logs = {name: [] for name, _ in BACKENDS}
        objs, task_list = [], []
        for name, _ in BACKENDS:
            pr2b, b2pr = MemoryPipe(), MemoryPipe()
            objs.append(Backend(name, LineTransport(b2pr.reader, pr2b)))
            task_list.append(asyncio.create_task(_task_backend(pr2b, b2pr, name, logs[name])))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), objs)
        rt = asyncio.create_task(router.serve())
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))

        async def call(id, method, params):
            await client.send(jsonrpc.encode(jsonrpc.request(id, method, params)))
            return jsonrpc.decode(await client.receive())

        created = await call("t", "tools/call", {"name": "gl__run", "arguments": {}})
        got = await call("g", "tasks/get", {"taskId": created["result"]["taskId"]})
        unknown = await call("u", "tasks/get", {"taskId": "zz__nope"})
        cancelled = await call("x", "tasks/cancel", {"taskId": created["result"]["taskId"]})
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(rt, *task_list), timeout=5)
        return created, got, unknown, cancelled, logs

    created, got, unknown, cancelled, logs = asyncio.run(scenario())
    assert cancelled["error"]["code"] == -32050  # backend error passed through
    # The created task id is namespaced under the backend that made it.
    assert created["result"]["taskId"] == "gl__T-99"
    assert created["result"]["task"]["taskId"] == "gl__T-99"
    # tasks/get routes to gl (the state holder) with the un-prefixed id.
    assert got["result"]["handledBy"] == "gl"
    assert got["result"]["taskId"] == "gl__T-99"  # re-namespaced for the client
    gl_task_get = [m for m in logs["gl"] if m["method"] == "tasks/get"]
    assert gl_task_get and gl_task_get[0]["params"]["taskId"] == "T-99"
    assert not any(m["method"] == "tasks/get" for m in logs["gh"])  # gh untouched
    # An unresolvable task id is rejected, not routed.
    assert unknown["error"]["code"] == INVALID_PARAMS


def test_tasks_stream_routes_and_renames_events():
    # SEP-2694: tasks/stream routes to the task's backend with the backend's own
    # id and the resume cursor preserved; the events the backend then emits are
    # re-namespaced so the client sees the backend__taskId it holds.
    async def stream_backend(read_pipe, write_pipe, log):
        t = LineTransport(read_pipe.reader, write_pipe)
        init = jsonrpc.decode(await t.receive())
        await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": "gh"}})))
        await t.receive()
        while True:
            raw = await t.receive()
            if raw is None:
                await t.send_eof()
                return
            m = jsonrpc.decode(raw)
            log.append(m)
            if m["method"] == "tools/call":
                await t.send(jsonrpc.encode(jsonrpc.result(m["id"], {"resultType": "task", "taskId": "T-7"})))
            elif m["method"] == "tasks/stream":
                tid = m["params"]["taskId"]
                for seq in (0, 1):
                    await t.send(jsonrpc.encode(jsonrpc.notification(tasks.TASK_EVENT_METHOD, {"taskId": tid, "seq": seq, "type": "log"})))
                await t.send(jsonrpc.encode(jsonrpc.result(m["id"], {})))  # stream closed

    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        log = []
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [Backend("gh", LineTransport(b2pr.reader, pr2b))])
        rt = asyncio.create_task(router.serve())
        bt = asyncio.create_task(stream_backend(pr2b, b2pr, log))
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send(jsonrpc.encode(jsonrpc.request("t", "tools/call", {"name": "run", "arguments": {}})))
        created = jsonrpc.decode(await client.receive())
        # Open the stream from the last seen sequence (resume cursor).
        await client.send(jsonrpc.encode(jsonrpc.request("s", "tasks/stream", {"taskId": created["result"]["taskId"], "after": 4})))
        # The client sees: two events, then the tasks/stream response.
        events, response = [], None
        while response is None:
            msg = jsonrpc.decode(await client.receive())
            if msg.get("method") == tasks.TASK_EVENT_METHOD:
                events.append(msg)
            elif msg.get("id") == "s":
                response = msg
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(rt, bt), timeout=5)
        return created, events, response, log

    created, events, response, log = asyncio.run(scenario())
    assert created["result"]["taskId"] == "gh__T-7"  # single backend still namespaces task ids
    # tasks/stream forwarded the backend's own id and the resume cursor.
    stream_req = [m for m in log if m["method"] == "tasks/stream"][0]
    assert stream_req["params"]["taskId"] == "T-7"
    assert stream_req["params"]["after"] == 4
    # Each event's taskId is re-namespaced to what the client holds.
    assert [e["params"]["taskId"] for e in events] == ["gh__T-7", "gh__T-7"]
    assert [e["params"]["seq"] for e in events] == [0, 1]
    assert "result" in response  # stream closed with an empty result


def test_task_routing_resilient_failure():
    # A resilient router: the backend holding the task dies on tasks/get, then
    # its breaker keeps subsequent same-task requests off it.
    from yamp.resilience import SERVER_NOT_AVAILABLE, CircuitBreaker

    async def dying_task_backend(read_pipe, write_pipe):
        t = LineTransport(read_pipe.reader, write_pipe)
        init = jsonrpc.decode(await t.receive())
        await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": "gh"}})))
        await t.receive()
        while True:
            raw = await t.receive()
            if raw is None:
                await t.send_eof()
                return
            m = jsonrpc.decode(raw)
            if m["method"] == "tools/call":
                await t.send(jsonrpc.encode(jsonrpc.result(m["id"], {"resultType": "task", "taskId": "T-1", "task": {"taskId": "T-1"}})))
            elif m["method"] == "tasks/get":
                await t.send_eof()  # die mid-request
                return

    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backend = Backend("gh", LineTransport(b2pr.reader, pr2b), breaker=CircuitBreaker(failure_threshold=1))
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [backend])
        rt = asyncio.create_task(router.serve())
        bt = asyncio.create_task(dying_task_backend(pr2b, b2pr))
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))

        async def call(id, method, params):
            await client.send(jsonrpc.encode(jsonrpc.request(id, method, params)))
            while True:  # drain any breaker-driven list_changed notification
                msg = jsonrpc.decode(await client.receive())
                if msg.get("id") == id:
                    return msg

        created = await call("t", "tools/call", {"name": "gh__run", "arguments": {}})
        failed = await call("g1", "tasks/get", {"taskId": created["result"]["taskId"]})  # backend dies -> failure path
        open_breaker = await call("g2", "tasks/get", {"taskId": created["result"]["taskId"]})  # breaker now open
        await client.send_eof()
        await asyncio.wait_for(asyncio.gather(rt, bt), timeout=5)
        return failed, open_breaker

    failed, open_breaker = asyncio.run(scenario())
    assert failed["error"]["code"] == SERVER_NOT_AVAILABLE  # request failed
    assert open_breaker["error"]["code"] == SERVER_NOT_AVAILABLE  # breaker open


def test_task_routing_non_resilient_propagates():
    # Without a breaker, a backend that dies mid-task propagates the failure
    # rather than swallowing it.
    async def dying(read_pipe, write_pipe):
        t = LineTransport(read_pipe.reader, write_pipe)
        init = jsonrpc.decode(await t.receive())
        await t.send(jsonrpc.encode(jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": "gh"}})))
        await t.receive()
        while True:
            raw = await t.receive()
            if raw is None:
                await t.send_eof()
                return
            m = jsonrpc.decode(raw)
            if m["method"] == "tools/call":
                await t.send(jsonrpc.encode(jsonrpc.result(m["id"], {"resultType": "task", "taskId": "T-1"})))
            else:
                await t.send_eof()  # die on tasks/get with no breaker
                return

    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [Backend("gh", LineTransport(b2pr.reader, pr2b))])
        rt = asyncio.create_task(router.serve())
        bt = asyncio.create_task(dying(pr2b, b2pr))
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("c1", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        await client.receive()
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
        await client.send(jsonrpc.encode(jsonrpc.request("t", "tools/call", {"name": "gh__run", "arguments": {}})))
        created = jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.request("g", "tasks/get", {"taskId": created["result"]["taskId"]})))
        results = await asyncio.wait_for(asyncio.gather(rt, bt, return_exceptions=True), timeout=5)
        return results

    results = asyncio.run(scenario())
    assert any(isinstance(r, Exception) for r in results)  # the failure propagated


# --- Server-side task origination (σ3) ---


def test_is_task_augmented():
    assert tasks.is_task_augmented({"_meta": {tasks.TASK_META_KEY: {}}}) is True
    assert tasks.is_task_augmented({"_meta": {tasks.TASK_META_KEY: {"ttl": 30}}}) is True
    assert tasks.is_task_augmented({"_meta": {"other": 1}}) is False
    assert tasks.is_task_augmented({"_meta": {}}) is False
    assert tasks.is_task_augmented({}) is False


def test_new_task_id_has_no_delimiter():
    assert tasks.new_task_id(1) == "task-1"
    assert tasks.new_task_id(42) == "task-42"
    assert "__" not in tasks.new_task_id(7)  # never collides with backend__taskId


def test_task_handle_shape():
    assert tasks.task_handle("task-1", tasks.STATUS_WORKING) == {"resultType": "task", "taskId": "task-1", "status": "working"}
    completed = tasks.task_handle("task-2", tasks.STATUS_COMPLETED, result={"content": []})
    assert completed == {"resultType": "task", "taskId": "task-2", "status": "completed", "result": {"content": []}}
    failed = tasks.task_handle("task-3", tasks.STATUS_FAILED, error={"code": -32603})
    assert failed == {"resultType": "task", "taskId": "task-3", "status": "failed", "error": {"code": -32603}}


def test_server_tasks_store_lifecycle():
    store = tasks.ServerTasks()
    a = store.create()
    b = store.create()
    assert (a, b) == ("task-1", "task-2")
    assert store.contains(a) and not store.contains("task-9")
    assert store.get(a) == {"status": "working"}

    store.complete(a, {"content": [{"type": "text", "text": "ok"}]})
    assert store.get(a) == {"status": "completed", "result": {"content": [{"type": "text", "text": "ok"}]}}

    # Terminal states are final: a completed task does not transition again.
    store.cancel(a)
    assert store.get(a)["status"] == "completed"

    # cancel a working task transitions; cancel a terminal one returns False.
    assert store.cancel(b) is True
    assert store.get(b) == {"status": "cancelled"}
    assert store.cancel(b) is False

    c = store.create()
    store.fail(c, {"code": -32603})
    assert store.get(c) == {"status": "failed", "error": {"code": -32603}}
    store.complete(c, {"x": 1})  # no transition from failed
    assert store.get(c)["status"] == "failed"
