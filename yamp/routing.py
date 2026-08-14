"""Routing intelligence (corpus SEP-2564, SEP-2614, SEP-2127).

Three pieces of gateway logic the corpus turned into protocol:

- Server-side filtering (SEP-2564): a ``filter`` on list methods carries
  ``namePatterns`` so an aggregating gateway can drop non-matching results and
  push the filter down to backends.
- Keyword routing (SEP-2614): a backend declares ``keywords``; a filter that
  carries keyword hints lets the proxy pre-select only the backends that can
  match, cutting fan-out.
- Server Cards (SEP-2127): a pre-connection discovery document the proxy
  publishes about itself.
"""

from __future__ import annotations

from .forward import PROXY_SERVER_INFO
from .version import SUPPORTED_PROTOCOL_VERSIONS


def name_matches(name: str, patterns: list[str]) -> bool:
    """Whether ``name`` matches any glob-ish pattern. No patterns matches all.

    A single ``*`` is a wildcard segment: ``a*`` prefix, ``*a`` suffix, ``*a*``
    contains, ``*`` everything, otherwise an exact match.
    """
    if not patterns:
        return True
    return any(_glob(pattern, name) for pattern in patterns)


def _glob(pattern: str, name: str) -> bool:
    if pattern == "*":
        return True
    if pattern.startswith("*") and pattern.endswith("*"):
        return pattern[1:-1] in name
    if pattern.endswith("*"):
        return name.startswith(pattern[:-1])
    if pattern.startswith("*"):
        return name.endswith(pattern[1:])
    return name == pattern


def backend_selected(backend_keywords: list[str], filter_keywords: list[str]) -> bool:
    """Whether a backend should be queried for a keyword-filtered list.

    A backend is skipped only when it declares keywords and none intersect the
    filter's keywords. A backend without declared keywords is always queried,
    since the proxy cannot know its surface (SEP-2614).
    """
    if not backend_keywords or not filter_keywords:
        return True
    return bool(set(backend_keywords) & set(filter_keywords))


def server_card() -> dict:
    """The proxy's self-description for `.well-known` discovery (SEP-2127)."""
    return {
        "name": PROXY_SERVER_INFO["name"],
        "version": PROXY_SERVER_INFO["version"],
        "protocolVersions": list(SUPPORTED_PROTOCOL_VERSIONS),
        "transports": ["stdio", "streamable-http"],
        "role": "intermediary",
    }
