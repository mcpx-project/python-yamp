"""Layer 7 capability (SEP §2.3, §9, draft §6.7).

Capability composition across backends (union, intersection, curated) and
progressive disclosure: when the aggregated tool count exceeds a threshold, a
curated subset is advertised alongside a ``proxy__search_tools`` meta-tool that
lets the client find the rest.
"""

from __future__ import annotations

DEFAULT_TOOL_THRESHOLD = 40
PROXY_SEARCH_TOOL = "proxy__search_tools"


def search_tool_definition() -> dict:
    return {
        "name": PROXY_SEARCH_TOOL,
        "description": "Search for additional tools available through this proxy",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Keyword to search tool names and descriptions",
                }
            },
            "required": ["query"],
        },
    }


def compose(per_backend: list[list[dict]], mode: str = "union", curated: list[str] | None = None) -> list[dict]:
    if mode == "union":
        return [tool for backend in per_backend for tool in backend]
    if mode == "intersection":
        if not per_backend:
            return []
        common = set.intersection(*({tool["name"] for tool in backend} for backend in per_backend))
        seen: set[str] = set()
        out: list[dict] = []
        for backend in per_backend:
            for tool in backend:
                if tool["name"] in common and tool["name"] not in seen:
                    seen.add(tool["name"])
                    out.append(tool)
        return out
    if mode == "curated":
        allowed = set(curated or [])
        return [tool for backend in per_backend for tool in backend if tool["name"] in allowed]
    raise ValueError(f"unknown composition mode: {mode}")


def compose_capabilities(backend_caps: list[dict], client_caps: dict | None = None) -> dict:
    """Compose the client-facing server capabilities per SEP §2.3.

    Not a naive last-writer-wins union. ``tools``/``resources``/``prompts`` and
    ``logging``/``sampling`` are advertised if ANY backend advertises them, with
    their sub-flags merged. ``elicitation`` is advertised only if the CLIENT
    supports it (the proxy elicits from the client, not a backend).
    ``extensions`` are unioned across backends (SEP-2133); the per-backend
    support is preserved so a later handler can track it per tool.
    """
    composed: dict = {}
    for primitive in ("tools", "resources", "prompts", "logging", "sampling"):
        merged: dict = {}
        present = False
        for caps in backend_caps:
            if primitive in caps:
                present = True
                value = caps[primitive]
                if isinstance(value, dict):
                    merged.update(value)
        if present:
            composed[primitive] = merged
    # elicitation follows the client, not the backends (SEP §2.3).
    if client_caps and "elicitation" in client_caps:
        composed["elicitation"] = client_caps["elicitation"]
    # extensions: union across backends, keeping which backends declared each.
    extensions: dict = {}
    for caps in backend_caps:
        for name, value in (caps.get("extensions") or {}).items():
            extensions.setdefault(name, value)
    if extensions:
        composed["extensions"] = extensions
    return composed


def search_tools(query: str, tools: list[dict]) -> list[dict]:
    needle = query.lower()
    return [
        tool
        for tool in tools
        if needle in tool["name"].lower() or needle in tool.get("description", "").lower()
    ]


def disclose(tools: list[dict], threshold: int = DEFAULT_TOOL_THRESHOLD) -> tuple[list[dict], bool]:
    """Return the advertised tool surface and whether a search tool was added.

    Over the threshold, a curated prefix is advertised plus the search tool.
    """
    if len(tools) > threshold:
        return tools[:threshold] + [search_tool_definition()], True
    return list(tools), False
