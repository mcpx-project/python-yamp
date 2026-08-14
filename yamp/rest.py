"""REST-to-MCP conversion (Conversion mode, draft Section 5.7).

A ``RestToMcp`` adapter is a small MCP server that fronts a REST API described
by an operation manifest. The proxy connects to it as an ordinary backend, so
an MCP client (for example Claude) reaches a REST API through the proxy: each
REST operation shows up as a tool on ``tools/list``, and ``tools/call``
translates the arguments into an HTTP request.

The manifest is a practical subset of OpenAPI:

    {
      "baseUrl": "https://api.example.com",
      "operations": [
        {"name": "get_user", "method": "GET", "path": "/users/{id}",
         "parameters": [{"name": "id", "in": "path"}, {"name": "verbose", "in": "query"}]},
        {"name": "create_issue", "method": "POST", "path": "/issues", "body": ["title", "body"]}
      ]
    }

The HTTP client is injectable so the translation is testable without a network.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Awaitable, Callable

from . import jsonrpc, namespace
from .forward import PROXY_NAME, PROXY_PROTOCOL_VERSION, PROXY_VERSION
from .jsonrpc import METHOD_NOT_FOUND
from .transport.base import Transport

# (method, url, headers, body) -> (status, body)
HttpCall = Callable[[str, str, dict, bytes | None], Awaitable[tuple[int, bytes]]]

REST_SERVER_INFO = {"name": f"{PROXY_NAME}-rest", "version": PROXY_VERSION}


async def default_http_call(method: str, url: str, headers: dict, body: bytes | None) -> tuple[int, bytes]:
    """A real HTTP call using the standard library, off the event loop."""

    def blocking() -> tuple[int, bytes]:
        request = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    return await asyncio.get_running_loop().run_in_executor(None, blocking)


class RestToMcp:
    """A REST-fronting MCP surface. Usable two ways: as a backend over a
    transport (:meth:`serve`), or as a local :class:`~yamp.handler.Handler`
    served directly by the proxy (Conversion mode). Both share ``list_tools`` /
    ``call_tool``. The ``id`` is the reserved namespace when served as a handler.
    """

    def __init__(self, spec: dict, http_call: HttpCall = default_http_call, id: str = "rest") -> None:
        if not namespace.valid_backend_id(id):
            raise ValueError(f"invalid rest handler id: {id!r}")
        self.id = id
        self._base = spec["baseUrl"].rstrip("/")
        self._ops = {op["name"]: op for op in spec["operations"]}
        self._http = http_call

    def list_tools(self) -> list[dict]:
        tools = []
        for op in self._ops.values():
            properties: dict = {}
            for parameter in op.get("parameters", []):
                properties[parameter["name"]] = {"type": "string"}
            for field in op.get("body", []):
                properties[field] = {"type": "string"}
            tools.append({
                "name": op["name"],
                "description": op.get("description", ""),
                "inputSchema": {"type": "object", "properties": properties},
            })
        return tools

    async def call_tool(self, name: str, arguments: dict) -> dict:
        op = self._ops.get(name)
        if op is None:
            return {"content": [{"type": "text", "text": "unknown operation"}], "isError": True}
        args = arguments
        path = op["path"]
        query: list[tuple[str, str]] = []
        for parameter in op.get("parameters", []):
            value = args.get(parameter["name"])
            if value is None:
                continue
            if parameter["in"] == "path":
                # Percent-encode with no safe characters so a client-supplied
                # value cannot inject path segments (`../`) or break out of the
                # segment; the argument names one path variable, nothing more.
                encoded = urllib.parse.quote(str(value), safe="")
                path = path.replace("{" + parameter["name"] + "}", encoded)
            elif parameter["in"] == "query":
                query.append((parameter["name"], str(value)))
        url = self._base + path
        if query:
            # Percent-encode each key and value (RFC 3986, no safe characters) so
            # a value cannot smuggle extra `&key=` pairs. Both arms use the same
            # rule (space -> %20, not +) so the built URL is byte-identical.
            url += "?" + "&".join(
                f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}" for k, v in query
            )
        headers: dict = {}
        body = None
        if op.get("body"):
            headers["Content-Type"] = "application/json"
            body = json.dumps({field: args[field] for field in op["body"] if field in args}).encode()
        status, response = await self._http(op["method"], url, headers, body)
        return {"content": [{"type": "text", "text": response.decode("utf-8", "replace")}], "isError": status >= 400}

    async def serve(self, transport: Transport) -> None:
        init = jsonrpc.decode(await transport.receive())
        await transport.send(jsonrpc.encode(jsonrpc.result(init["id"], {
            "protocolVersion": PROXY_PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": REST_SERVER_INFO,
        })))
        await transport.receive()  # notifications/initialized
        while True:
            raw = await transport.receive()
            if raw is None:
                await transport.send_eof()
                return
            message = jsonrpc.decode(raw)
            method = message.get("method")
            if method == "tools/list":
                await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], {"tools": self.list_tools()})))
            elif method == "tools/call":
                params = message.get("params", {})
                result = await self.call_tool(params.get("name"), params.get("arguments", {}))
                await transport.send(jsonrpc.encode(jsonrpc.result(message["id"], result)))
            elif "id" in message:
                # An unknown request (has an id) must get a reply; otherwise the
                # client blocks forever waiting for it. Notifications (no id) are
                # ignored. Mirrors the Rust arm's rest serve loop.
                await transport.send(
                    jsonrpc.encode(
                        jsonrpc.error(message["id"], METHOD_NOT_FOUND, f"unknown method: {method}")
                    )
                )
