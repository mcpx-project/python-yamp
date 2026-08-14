"""Layer 4 resilience (SEP §8, draft §6.4).

A circuit breaker per backend, a resilient router that returns partial results
on fan-out when a backend is unavailable, rejects calls to open backends with
JSON-RPC ``-32003``, and never retries a (possibly side-effecting)
``tools/call``. The clock is injectable so the breaker's timing is
deterministic under test.
"""

from __future__ import annotations

import time
from enum import Enum
from typing import Awaitable, Callable, Iterable, Protocol

from . import jsonrpc, namespace
from .errors import SERVER_NOT_AVAILABLE  # single source; re-exported for existing importers
from .jsonrpc import INTERNAL_ERROR, INVALID_PARAMS

PROXY_PARTIAL_KEY = "io.modelcontextprotocol/proxy-partial"


def partial_meta(unavailable: Iterable[str], reason: str) -> dict:
    """The ``_meta`` payload announcing a partial list result: which backends
    were dropped from the surface and why. One object shape, shared by every
    path that omits a backend (circuit breaker or fan-out failure), so the wire
    contract does not depend on which layer noticed the outage."""
    return {
        PROXY_PARTIAL_KEY: {
            "unavailable_backends": sorted(set(unavailable)),
            "reason": reason,
        }
    }


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: float = 30.0,  # matches the config default (both arms)
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._threshold = failure_threshold
        self._reset = reset_timeout
        self._now = now
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at = 0.0

    @property
    def state(self) -> CircuitState:
        # An open breaker becomes half-open once the reset timeout elapses,
        # allowing a single trial request.
        if self._state is CircuitState.OPEN and self._now() - self._opened_at >= self._reset:
            self._state = CircuitState.HALF_OPEN
        return self._state

    def allow(self) -> bool:
        return self.state is not CircuitState.OPEN

    def record_success(self) -> None:
        self._failures = 0
        self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        if self.state is CircuitState.HALF_OPEN:
            self._trip()  # the trial failed; reopen immediately
            return
        self._failures += 1
        if self._failures >= self._threshold:
            self._trip()

    def _trip(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._now()
        self._failures = self._threshold


class BackendChannel(Protocol):
    id: str

    def request(self, method: str, params: jsonrpc.Message) -> Awaitable[jsonrpc.Message]: ...


class ManagedBackend:
    def __init__(self, channel: BackendChannel, breaker: CircuitBreaker | None = None) -> None:
        self.channel = channel
        self.breaker = breaker or CircuitBreaker()

    @property
    def id(self) -> str:
        return self.channel.id


class ResilientRouter:
    def __init__(self, backends: list[ManagedBackend]) -> None:
        self._backends = backends
        self._last_available = self._available_ids()

    def _available_ids(self) -> set[str]:
        return {backend.id for backend in self._backends if backend.breaker.allow()}

    async def tools_list(self, id: object) -> jsonrpc.Message:
        tools: list[jsonrpc.Message] = []
        unavailable: list[str] = []
        for backend in self._backends:
            if not backend.breaker.allow():
                unavailable.append(backend.id)
                continue
            try:
                response = await backend.channel.request("tools/list", {})
            except Exception:
                backend.breaker.record_failure()
                unavailable.append(backend.id)
                continue
            backend.breaker.record_success()
            for tool in response.get("result", {}).get("tools", []):
                entry = dict(tool)
                entry["name"] = namespace.prefix(backend.id, tool["name"])
                tools.append(entry)
        result: jsonrpc.Message = {"tools": tools}
        if unavailable:
            result["_meta"] = partial_meta(unavailable, "circuit_breaker_open")
        return jsonrpc.result(id, result)

    async def tools_call(self, message: jsonrpc.Message) -> jsonrpc.Message:
        params = message.get("params", {})
        resolved = namespace.split(params.get("name", ""))
        index = self._index_of(resolved[0]) if resolved else None
        if resolved is None or index is None:
            return jsonrpc.error(message["id"], INVALID_PARAMS, f"unknown tool: {params.get('name')}")
        backend = self._backends[index]
        if not backend.breaker.allow():
            return jsonrpc.error(
                message["id"], SERVER_NOT_AVAILABLE, f"backend {backend.id} unavailable"
            )
        forwarded = dict(params)
        forwarded["name"] = resolved[1]
        try:
            # Single attempt: tools/call may have side effects, so it is not
            # retried (draft §6.4).
            response = await backend.channel.request("tools/call", forwarded)
        except Exception:
            backend.breaker.record_failure()
            return jsonrpc.error(
                message["id"], SERVER_NOT_AVAILABLE, f"backend {backend.id} failed"
            )
        backend.breaker.record_success()
        if "result" in response:
            return jsonrpc.result(message["id"], response["result"])
        return {
            "jsonrpc": "2.0",
            "id": message["id"],
            "error": response.get("error", {"code": INTERNAL_ERROR, "message": "backend error"}),
        }

    def surface_changed(self) -> bool:
        """True if the set of available backends changed since the last check.

        A serve loop calls this after health transitions and emits
        :meth:`list_changed_notification` when it returns True (SEP §8.1).
        """
        current = self._available_ids()
        changed = current != self._last_available
        self._last_available = current
        return changed

    @staticmethod
    def list_changed_notification() -> jsonrpc.Message:
        return jsonrpc.notification("notifications/tools/list_changed")

    def _index_of(self, backend_id: str) -> int | None:
        for index, backend in enumerate(self._backends):
            if backend.id == backend_id:
                return index
        return None
