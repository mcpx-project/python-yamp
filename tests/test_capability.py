import pytest

from yamp.capability import (
    DEFAULT_TOOL_THRESHOLD,
    PROXY_SEARCH_TOOL,
    compose,
    compose_capabilities,
    disclose,
    search_tool_definition,
    search_tools,
)


def test_compose_capabilities_any_backend_primitives():
    # tools/resources/prompts/logging/sampling: present if ANY backend has them,
    # sub-flags merged.
    composed = compose_capabilities(
        [
            {"tools": {"listChanged": True}, "sampling": {}},
            {"tools": {}, "logging": {}, "resources": {"subscribe": True}},
        ]
    )
    assert composed["tools"] == {"listChanged": True}  # merged sub-flags
    assert composed["sampling"] == {}  # any backend
    assert composed["logging"] == {}  # any backend
    assert composed["resources"] == {"subscribe": True}
    assert "prompts" not in composed  # no backend advertised prompts


def test_compose_capabilities_elicitation_follows_client():
    # elicitation is advertised only when the CLIENT supports it, never because a
    # backend does.
    with_client = compose_capabilities([{"tools": {}}], client_caps={"elicitation": {}})
    assert with_client["elicitation"] == {}
    without_client = compose_capabilities([{"tools": {}, "elicitation": {}}])
    assert "elicitation" not in without_client


def test_compose_capabilities_extensions_unioned():
    composed = compose_capabilities(
        [
            {"extensions": {"io.example/tasks": {"version": 1}}},
            {"extensions": {"io.example/ui": {"version": 2}}},
        ]
    )
    assert composed["extensions"] == {
        "io.example/tasks": {"version": 1},
        "io.example/ui": {"version": 2},
    }


def test_compose_capabilities_empty():
    assert compose_capabilities([]) == {}


def _tools(*names):
    return [{"name": n, "description": f"{n} tool"} for n in names]


def test_compose_union():
    result = compose([_tools("a", "b"), _tools("c")], "union")
    assert {t["name"] for t in result} == {"a", "b", "c"}


def test_compose_intersection():
    result = compose([_tools("a", "b"), _tools("b", "c")], "intersection")
    assert [t["name"] for t in result] == ["b"]


def test_compose_intersection_empty():
    assert compose([], "intersection") == []


def test_compose_curated():
    result = compose([_tools("a", "b"), _tools("c")], "curated", curated=["a", "c"])
    assert {t["name"] for t in result} == {"a", "c"}


def test_compose_unknown_mode_raises():
    with pytest.raises(ValueError):
        compose([], "nonsense")


def test_search_matches_name_and_description():
    tools = [
        {"name": "gh__create_issue", "description": "open an issue"},
        {"name": "gh__search", "description": "find code"},
    ]
    assert [t["name"] for t in search_tools("issue", tools)] == ["gh__create_issue"]
    assert [t["name"] for t in search_tools("find", tools)] == ["gh__search"]


def test_disclose_under_threshold_returns_all():
    tools = _tools(*[f"t{i}" for i in range(5)])
    advertised, has_search = disclose(tools, threshold=DEFAULT_TOOL_THRESHOLD)
    assert advertised == tools
    assert has_search is False


def test_disclose_over_threshold_adds_search_tool():
    tools = _tools(*[f"t{i}" for i in range(50)])
    advertised, has_search = disclose(tools, threshold=40)
    assert has_search is True
    assert len(advertised) == 41  # 40 curated + the search tool
    assert advertised[-1]["name"] == PROXY_SEARCH_TOOL


def test_search_tool_schema():
    schema = search_tool_definition()
    assert schema["name"] == PROXY_SEARCH_TOOL
    assert schema["inputSchema"]["required"] == ["query"]
