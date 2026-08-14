import asyncio
import json

import pytest

from yamp.handler import BackendsHandler, Handler, Registry


class StubHandler:
    def __init__(self, id, tools):
        self.id = id
        self._tools = tools

    def list_tools(self):
        return [{"name": t, "inputSchema": {"type": "object", "properties": {}}} for t in self._tools]

    async def call_tool(self, name, arguments):
        return {"content": [{"type": "text", "text": f"{self.id}:{name}:{arguments}"}]}


def test_registry_namespaces_tools_and_resolves():
    registry = Registry([StubHandler("a", ["x", "y"]), StubHandler("b", ["z"])])
    names = {t["name"] for t in registry.list_tools()}
    assert names == {"a__x", "a__y", "b__z"}
    assert registry.ids() == {"a", "b"}
    assert registry.handler_for("a").id == "a"
    assert registry.handler_for("missing") is None


def test_registry_rejects_invalid_and_duplicate_ids():
    with pytest.raises(ValueError):
        Registry([StubHandler("bad__id", ["x"])])  # delimiter in id
    with pytest.raises(ValueError):
        Registry([StubHandler("a", ["x"]), StubHandler("a", ["y"])])  # duplicate


def test_stub_handler_satisfies_protocol():
    assert isinstance(StubHandler("a", []), Handler)


def test_rest_handler_rejects_invalid_id():
    from yamp.rest import RestToMcp

    with pytest.raises(ValueError):
        RestToMcp({"baseUrl": "http://x", "operations": []}, id="bad__id")


def test_backends_handler_reports_backends():
    handler = BackendsHandler(provider=lambda: [{"id": "gh", "available": True}])
    assert handler.id == "yamp"
    tools = handler.list_tools()
    assert tools[0]["name"] == "backends"
    result = asyncio.run(handler.call_tool("backends", {}))
    assert json.loads(result["content"][0]["text"]) == [{"id": "gh", "available": True}]


def test_build_registry_from_config():
    from yamp.config import HandlerConfig, RestHandlerConfig
    from yamp.handler import build_registry

    config = HandlerConfig(
        meta_tools=True,
        rest=[RestHandlerConfig(id="gh", base_url="https://api.example.com", operations=[{"name": "get_user"}])],
    )
    registry = build_registry(config, backends_provider=lambda: [{"id": "b0"}])
    assert registry.ids() == {"gh", "yamp"}
    names = {t["name"] for t in registry.list_tools()}
    assert names == {"gh__get_user", "yamp__backends"}


def test_build_registry_without_meta_tools():
    from yamp.config import HandlerConfig
    from yamp.handler import build_registry

    registry = build_registry(HandlerConfig(), backends_provider=lambda: [])
    assert registry.ids() == set()
