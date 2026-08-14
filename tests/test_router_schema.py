"""Schema validation of server-originated calls (σ1). Mirrors the Rust arm.

With ``set_validate_schemas(True)`` the router validates a local handler's
``tools/call`` arguments against the tool's ``inputSchema`` (a bad input is a
client-class ``-32602``) and its result against ``outputSchema`` before it leaves
(a bad output is a server-class ``-32603``). Both errors carry the normalized
``errorId``. Validation is off by default, so a bad input then reaches the
handler unchecked. Only the local-handler branch validates; the proxy role is
untouched.
"""

import asyncio

from yamp import jsonrpc
from yamp.errors import INTERNAL_ERROR, INVALID_PARAMS
from yamp.handler import Registry
from yamp.router import ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe


class SchemaHandler:
    """A server tool with declared schemas. ``add`` requires an integer ``n`` and
    promises an integer ``doubled``; ``bad_out`` accepts anything but returns a
    result that violates its own ``outputSchema``."""

    id = "srv"

    def list_tools(self):
        return [
            {
                "name": "add",
                "inputSchema": {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]},
                "outputSchema": {"type": "object", "properties": {"doubled": {"type": "integer"}}, "required": ["doubled"]},
            },
            {
                "name": "bad_out",
                "inputSchema": {"type": "object"},
                "outputSchema": {"type": "object", "properties": {"doubled": {"type": "integer"}}, "required": ["doubled"]},
            },
        ]

    async def call_tool(self, name, arguments):
        if name == "bad_out":
            return {"content": [], "structuredContent": {"doubled": "not-an-int"}}
        n = arguments.get("n", 0)
        return {"content": [{"type": "text", "text": "ok"}], "structuredContent": {"doubled": n * 2}}


def _drive(validate):
    async def scenario():
        c2r, r2c = MemoryPipe(), MemoryPipe()
        router = ForwardRouter(LineTransport(c2r.reader, r2c), [], registry=Registry([SchemaHandler()])).set_validate_schemas(validate)
        router_task = asyncio.create_task(router.serve())
        client = LineTransport(r2c.reader, c2r)
        await client.send(jsonrpc.encode(jsonrpc.request("i", "initialize", {"protocolVersion": "x", "capabilities": {}, "clientInfo": {}})))
        jsonrpc.decode(await client.receive())
        await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))

        async def call(id, name, args):
            await client.send(jsonrpc.encode(jsonrpc.request(id, "tools/call", {"name": name, "arguments": args})))
            return jsonrpc.decode(await client.receive())

        good = await call("g", "srv__add", {"n": 3})
        bad_in = await call("b", "srv__add", {})
        bad_out = await call("o", "srv__bad_out", {})
        await client.send_eof()
        await asyncio.wait_for(router_task, timeout=5)
        return good, bad_in, bad_out

    return asyncio.run(scenario())


def test_validates_input_and_output_when_enabled():
    good, bad_in, bad_out = _drive(True)

    # Valid input, valid output: the call succeeds and the typed result crosses.
    assert good["result"]["structuredContent"]["doubled"] == 6

    # Missing required `n`: rejected before the handler runs, client-class.
    assert bad_in["error"]["code"] == INVALID_PARAMS
    assert bad_in["error"]["data"]["errorId"] == "E4002"
    assert "result" not in bad_in

    # Handler produced output violating its own outputSchema: server-class.
    assert bad_out["error"]["code"] == INTERNAL_ERROR
    assert bad_out["error"]["data"]["errorId"] == "E5000"


def test_off_by_default_passes_bad_input_through():
    good, bad_in, _bad_out = _drive(False)

    # With validation off the handler runs unchecked: the good call still works,
    assert good["result"]["structuredContent"]["doubled"] == 6
    # and a call the schema would reject reaches the handler and returns a result.
    assert "error" not in bad_in
    assert bad_in["result"]["structuredContent"]["doubled"] == 0
