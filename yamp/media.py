"""MCP media type negotiation (corpus SEP-2357).

SEP-2357 gives MCP-over-HTTP its own media type, ``application/mcp+json``, so a
gateway, load balancer, or WAF can identify MCP traffic without parsing the
body. yamp emits it when the client accepts it and falls back to
``application/json`` during the transition. The functions here are the single
source of that negotiation, shared by every HTTP entrypoint.
"""

from __future__ import annotations

MCP_JSON = "application/mcp+json"
JSON = "application/json"


def _tokens(header: str) -> list[str]:
    # Media type of each Accept element, without parameters (";q=..." etc.).
    return [part.split(";", 1)[0].strip() for part in header.split(",")]


def response_content_type(accept: str | None) -> str:
    """The Content-Type to answer with, given the request's ``Accept`` header.

    Prefer ``application/mcp+json`` when the client accepts it explicitly or via
    a wildcard; otherwise fall back to ``application/json``.
    """
    if not accept:
        return JSON
    tokens = _tokens(accept)
    if MCP_JSON in tokens or "application/*" in tokens or "*/*" in tokens:
        return MCP_JSON
    return JSON


def is_mcp_json(content_type: str | None) -> bool:
    """Whether a ``Content-Type`` names the MCP media type (params ignored)."""
    if not content_type:
        return False
    return content_type.split(";", 1)[0].strip() == MCP_JSON
