"""σ3 server-side task origination (Python arm). Mirrors the Rust arm.

With ``set_server_tasks(True)`` a task-augmented tools/call that resolves to a
local handler returns a ``working`` handle at once, runs in the background, and
its later ``tasks/get``/``tasks/cancel`` are served from the store: a finished
call is ``completed`` with its result, a raising one ``failed``, a cancelled one
``cancelled``. Off by default, so a task-augmented call is answered synchronously.
"""

import asyncio

from yamp import jsonrpc, tasks
from yamp.errors import INVALID_PARAMS
from yamp.handler import Registry
from yamp.router import ForwardRouter
from yamp.schema import validate_call_args  # noqa: F401  (keep schema import path exercised)
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe

AUG = {"_meta": {tasks.TASK_META_KEY: {}}}


class TaskHandler:
    id = "srv"

    def __init__(self):
        self.enter = {}
        self.gate = {}

    def list_tools(self):
        return [
            {"name": "fast", "inputSchema": {"type": "object"}},
            {"name": "block", "inputSchema": {"type": "object"}},
            {"name": "boom", "inputSchema": {"type": "object"}},
            {"name": "strict", "inputSchema": {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}},
        ]

    async def call_tool(self, name, arguments):
        if name == "boom":
            raise RuntimeError("handler blew up")
        if name == "block":
            key = arguments["k"]
            self.enter[key].set()
            await self.gate[key].wait()
        return {"content": [{"type": "text", "text": name}]}


def _new(handler, server_tasks=True, validate=False):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    router = ForwardRouter(LineTransport(c2r.reader, r2c), [], registry=Registry([handler])).set_server_tasks(server_tasks).set_validate_schemas(validate)
    client = LineTransport(r2c.reader, c2r)
    return router, client


async def _handshake(client):
    await client.send(jsonrpc.encode(jsonrpc.request("i", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
    await client.receive()
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))


async def _call(client, id, name, arguments, meta=None):
    params = {"name": name, "arguments": arguments}
    if meta is not None:
        params["_meta"] = meta["_meta"]
    await client.send(jsonrpc.encode(jsonrpc.request(id, "tools/call", params)))
    return jsonrpc.decode(await client.receive())


async def _task(client, id, method, task_id):
    await client.send(jsonrpc.encode(jsonrpc.request(id, method, {"taskId": task_id})))
    return jsonrpc.decode(await client.receive())


async def _poll_done(client, task_id):
    for i in range(100):
        r = await _task(client, f"g{i}", "tasks/get", task_id)
        if r["result"]["status"] != tasks.STATUS_WORKING:
            return r["result"]
    raise AssertionError("task never left working")


def test_originate_then_complete():
    async def scenario():
        router, client = _new(TaskHandler())
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        handle = await _call(client, "A", "srv__fast", {}, meta=AUG)
        task_id = handle["result"]["taskId"]
        done = await _poll_done(client, task_id)
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return handle["result"], done

    working, done = asyncio.run(scenario())
    assert working["resultType"] == "task" and working["status"] == "working" and "__" not in working["taskId"]
    assert done["status"] == "completed"
    assert done["result"]["content"][0]["text"] == "fast"


def test_working_then_cancel():
    async def scenario():
        h = TaskHandler()
        h.enter["a"], h.gate["a"] = asyncio.Event(), asyncio.Event()  # gate never released
        router, client = _new(h)
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        handle = await _call(client, "A", "srv__block", {"k": "a"}, meta=AUG)
        task_id = handle["result"]["taskId"]
        await asyncio.wait_for(h.enter["a"].wait(), 5)
        pending = await _task(client, "g", "tasks/get", task_id)
        cancelled = await _task(client, "c", "tasks/cancel", task_id)
        after = await _task(client, "g2", "tasks/get", task_id)
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return pending["result"], cancelled["result"], after["result"]

    pending, cancelled, after = asyncio.run(scenario())
    assert pending["status"] == "working"
    assert cancelled["status"] == "cancelled"
    assert after["status"] == "cancelled"


def test_handler_exception_fails_the_task():
    async def scenario():
        router, client = _new(TaskHandler())
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        handle = await _call(client, "A", "srv__boom", {}, meta=AUG)
        done = await _poll_done(client, handle["result"]["taskId"])
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return done

    done = asyncio.run(scenario())
    assert done["status"] == "failed"
    assert done["error"]["data"]["errorId"] == "E5000"


def test_schema_rejected_call_fails_the_task():
    async def scenario():
        # validate on: the strict tool's bad args make route_call return a -32602,
        # which the task records as its failure error.
        router, client = _new(TaskHandler(), validate=True)
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        handle = await _call(client, "A", "srv__strict", {}, meta=AUG)
        done = await _poll_done(client, handle["result"]["taskId"])
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return done

    done = asyncio.run(scenario())
    assert done["status"] == "failed"
    assert done["error"]["data"]["errorId"] == "E4002"  # invalid params


def test_cancel_after_completion_returns_the_terminal_handle():
    async def scenario():
        router, client = _new(TaskHandler())
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        handle = await _call(client, "A", "srv__fast", {}, meta=AUG)
        task_id = handle["result"]["taskId"]
        await _poll_done(client, task_id)  # ensure completed (handle already popped)
        cancelled = await _task(client, "c", "tasks/cancel", task_id)
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return cancelled["result"]

    # Cancel on an already-completed task is a no-op: the completed handle stands.
    assert asyncio.run(scenario())["status"] == "completed"


def test_unknown_task_id_is_rejected():
    async def scenario():
        router, client = _new(TaskHandler())
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        r = await _task(client, "g", "tasks/get", "task-999")  # never created
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return r

    r = asyncio.run(scenario())
    assert r["error"]["code"] == INVALID_PARAMS


def test_off_by_default_answers_synchronously():
    async def scenario():
        router, client = _new(TaskHandler(), server_tasks=False)
        rt = asyncio.create_task(router.serve())
        await _handshake(client)
        r = await _call(client, "A", "srv__fast", {}, meta=AUG)  # augmented, but tasks off
        await client.send_eof()
        await asyncio.wait_for(rt, 5)
        return r["result"]

    result = asyncio.run(scenario())
    # No task handle: the call was answered directly with the tool result.
    assert result.get("resultType") != "task"
    assert result["content"][0]["text"] == "fast"
