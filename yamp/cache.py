"""Capability-list cache (SEP §6, corpus SEP-2549).

List methods (``tools/list``, ``prompts/list``, ``resources/list``,
``server/discover``) are cacheable keyed on backend identity. A backend result
may carry SEP-2549 directives: ``ttlMs`` (freshness in milliseconds, ``0``
meaning immediately stale) and ``cacheScope`` (``"public"`` shareable across
principals, ``"private"`` never served to a different principal). A shared proxy
cache collapses repeated sub-agent list fetches from ``O(subagents × backends)``
to ``O(backends)``.

Invalidation (SEP §6.2): a backend's entries are dropped when it emits a
``list_changed`` notification and when its circuit breaker opens. The clock is
injectable so freshness is deterministic under test, mirroring the breaker.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

TTL_MS_KEY = "ttlMs"
CACHE_SCOPE_KEY = "cacheScope"
PUBLIC = "public"
PRIVATE = "private"
# Default freshness when a backend returns no ttlMs (draft §6.2: SHOULD support a
# configurable default TTL of 300 seconds).
DEFAULT_TTL_MS = 300_000


@dataclass
class _Entry:
    result: dict
    expires_at: float  # monotonic seconds
    scope: str
    principal: str | None  # set only for private entries


class ListCache:
    """A shared, principal-aware cache of backend list results.

    One entry per ``(backend_id, method)``. A public entry is served to any
    principal; a private entry is served only to the principal that stored it,
    so a shared gateway never leaks one user's private list to another
    (SEP-2549).
    """

    def __init__(
        self,
        now: Callable[[], float] = time.monotonic,
        default_ttl_ms: int = DEFAULT_TTL_MS,
    ) -> None:
        self._now = now
        self._default_ttl_ms = default_ttl_ms
        self._entries: dict[tuple[str, str], _Entry] = {}

    def get(self, backend_id: str, method: str, principal: str | None) -> dict | None:
        key = (backend_id, method)
        entry = self._entries.get(key)
        if entry is None:
            return None
        if self._now() >= entry.expires_at:
            del self._entries[key]
            return None
        if entry.scope == PRIVATE and entry.principal != principal:
            return None
        return entry.result

    def put(self, backend_id: str, method: str, principal: str | None, result: dict) -> None:
        ttl_ms = result.get(TTL_MS_KEY, self._default_ttl_ms)
        if not isinstance(ttl_ms, (int, float)) or isinstance(ttl_ms, bool):
            ttl_ms = self._default_ttl_ms
        if ttl_ms <= 0:
            # ttlMs = 0 means immediately stale: drop any prior entry, cache
            # nothing (SEP-2549).
            self._entries.pop((backend_id, method), None)
            return
        scope = result.get(CACHE_SCOPE_KEY, PUBLIC)
        if scope not in (PUBLIC, PRIVATE):
            scope = PUBLIC
        self._entries[(backend_id, method)] = _Entry(
            result=result,
            expires_at=self._now() + ttl_ms / 1000.0,
            scope=scope,
            principal=principal if scope == PRIVATE else None,
        )

    def invalidate_backend(self, backend_id: str) -> None:
        for key in [k for k in self._entries if k[0] == backend_id]:
            del self._entries[key]
