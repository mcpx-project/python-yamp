"""Generate the cross-arm golden-flow corpus.

Where `differential-corpus.json` pins pure functions, this pins whole message
flows: each scenario drives the served `ForwardRouter` through an
initialize -> ... exchange against scripted in-process backends, and records the
exact sequence of client-facing messages the proxy emits. The Rust arm
(`rust/tests/flow.rs`) replays the same scenarios through its router and asserts
it produces the identical message sequence, so the two data planes are pinned to
behave identically end to end, not just per pure function.

Determinism: the client-facing trace adds only a proxy hop (no random
traceparent, which the proxy attaches to backend requests only), request ids are
fixed, and backends answer in list order, so the recorded flow is reproducible
and cross-arm identical. Run from the repo root:

    python python/tools/gen_flow_corpus.py
"""

import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from yamp import filters, jsonrpc
from yamp.forward import PROXY_PROTOCOL_VERSION
from yamp.router import Backend, ForwardRouter
from yamp.transport.line import LineTransport
from yamp.transport.memory import MemoryPipe

INIT_PARAMS = {"protocolVersion": "x", "capabilities": {}, "clientInfo": {"name": "flow"}}

# Each scenario: backends (id, tools, optional per-tool responses/capabilities)
# and a list of post-handshake client requests. The recorded `out` is the
# initialize response followed by each request's response.
SCENARIOS = [
    {
        "name": "single_backend_list_and_call",
        "backends": [{"id": "b0", "tools": ["echo"], "responses": {"echo": {"content": [{"type": "text", "text": "hi"}]}}}],
        "client": [
            {"method": "tools/list", "params": {}},
            {"method": "tools/call", "params": {"name": "echo", "arguments": {"x": 1}}},
        ],
    },
    {
        "name": "two_backends_merged_and_routed",
        "backends": [
            {"id": "gh", "tools": ["create_issue"], "responses": {"create_issue": {"content": [{"type": "text", "text": "gh-ok"}]}}},
            {"id": "fs", "tools": ["read"], "responses": {"read": {"content": [{"type": "text", "text": "fs-ok"}]}}},
        ],
        "client": [
            {"method": "tools/list", "params": {}},
            {"method": "tools/call", "params": {"name": "gh__create_issue", "arguments": {}}},
            {"method": "tools/call", "params": {"name": "fs__read", "arguments": {}}},
        ],
    },
    {
        "name": "unknown_tool_is_rejected",
        "backends": [{"id": "gh", "tools": ["create_issue"]}, {"id": "fs", "tools": ["read"]}],
        "client": [
            {"method": "tools/call", "params": {"name": "zz__nope", "arguments": {}}},
        ],
    },
    # ε5: reference filters run through the router; both arms build them from the
    # same declarative spec, so the extension host is proven identical end to end.
    {
        "name": "filter_denies_a_tool",
        "backends": [{"id": "gh", "tools": ["create_issue"]}],
        "filter": {"kind": "deny_tool", "tool": "create_issue", "reason": "blocked by policy"},
        "client": [{"method": "tools/call", "params": {"name": "create_issue", "arguments": {}}}],
    },
    {
        "name": "filter_redacts_an_argument",
        "backends": [{"id": "gh", "tools": ["create_issue"], "echo_args": True}],
        "filter": {"kind": "redact_arg", "arg": "secret", "to": "[redacted]"},
        "client": [{"method": "tools/call", "params": {"name": "create_issue", "arguments": {"secret": "raw", "keep": 1}}}],
    },
]


# Reference filters (ε5). Each is built from a declarative spec so the Rust arm
# constructs the identical filter and the flow output must match.
class _DenyTool(filters.Filter):
    def __init__(self, tool, reason):
        self._tool, self._reason = tool, reason

    def evaluate(self, hook, message):
        if message.get("params", {}).get("name") == self._tool:
            return {"kind": "deny", "reason": self._reason}
        return {"kind": "allow"}


class _RedactArg(filters.Filter):
    def __init__(self, arg, to):
        self._arg, self._to = arg, to

    def evaluate(self, hook, message):
        arguments = dict(message.get("params", {}).get("arguments", {}))
        arguments[self._arg] = self._to
        return {"kind": "mutate", "arguments": arguments}


def build_filter(spec):
    if spec["kind"] == "deny_tool":
        return filters.FilterChain([_DenyTool(spec["tool"], spec.get("reason", ""))])
    if spec["kind"] == "redact_arg":
        return filters.FilterChain([_RedactArg(spec["arg"], spec["to"])])
    raise ValueError(spec["kind"])


async def _mock_backend(read_pipe, write_pipe, spec):
    transport = LineTransport(read_pipe.reader, write_pipe)
    init = jsonrpc.decode(await transport.receive())
    caps = spec.get("capabilities", {"tools": {}})
    await transport.send(
        jsonrpc.encode(jsonrpc.result(init["id"], {"protocolVersion": PROXY_PROTOCOL_VERSION, "capabilities": caps, "serverInfo": {"name": spec["id"]}}))
    )
    await transport.receive()  # notifications/initialized
    while True:
        raw = await transport.receive()
        if raw is None:
            await transport.send_eof()
            return
        message = jsonrpc.decode(raw)
        method = message.get("method")
        if method == "tools/list":
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], {"tools": [{"name": n} for n in spec.get("tools", [])]})))
        elif method == "tools/call":
            name = message["params"]["name"]
            if spec.get("echo_args"):
                result = {"content": [{"type": "text", "text": name}], "arguments": message["params"].get("arguments", {})}
            else:
                result = spec.get("responses", {}).get(name, {"content": [{"type": "text", "text": name}]})
            await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], result)))


async def _drive(scenario):
    c2r, r2c = MemoryPipe(), MemoryPipe()
    backends, backend_tasks = [], []
    for spec in scenario["backends"]:
        pr2b, b2pr = MemoryPipe(), MemoryPipe()
        backends.append(Backend(spec["id"], LineTransport(b2pr.reader, pr2b)))
        backend_tasks.append(asyncio.create_task(_mock_backend(pr2b, b2pr, spec)))
    filter_chain = build_filter(scenario["filter"]) if "filter" in scenario else None
    router = ForwardRouter(LineTransport(c2r.reader, r2c), backends, filter_chain=filter_chain)
    router_task = asyncio.create_task(router.serve())
    client = LineTransport(r2c.reader, c2r)

    out = []
    await client.send(jsonrpc.encode(jsonrpc.request("init", "initialize", INIT_PARAMS)))
    out.append(jsonrpc.decode(await client.receive()))
    await client.send(jsonrpc.encode(jsonrpc.notification("notifications/initialized")))
    for index, request in enumerate(scenario["client"]):
        await client.send(jsonrpc.encode(jsonrpc.request(f"r{index}", request["method"], request.get("params", {}))))
        out.append(jsonrpc.decode(await client.receive()))
    await client.send_eof()
    await asyncio.wait_for(asyncio.gather(router_task, *backend_tasks), timeout=5)
    return out


def drive(scenario):
    """Run one scenario through the Python router, returning the client-facing
    message sequence."""
    return asyncio.run(_drive(scenario))


def build():
    flows = []
    for scenario in SCENARIOS:
        spec_in = {"backends": scenario["backends"], "client": scenario["client"]}
        if "filter" in scenario:
            spec_in["filter"] = scenario["filter"]
        flows.append({"name": scenario["name"], "in": spec_in, "out": drive(scenario)})
    return {"flows": flows}


def main():
    corpus = build()
    out_path = ROOT / "conformance" / "flow-corpus.json"
    out_path.write_text(json.dumps(corpus, indent=2, sort_keys=True) + "\n")
    print(f"wrote {len(corpus['flows'])} flows to {out_path}")


if __name__ == "__main__":
    main()
