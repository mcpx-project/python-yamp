"""Worker-pool substrate for server-originated calls (σ2).

A server that originates responses must bound its own concurrency, cancel a call
the client abandoned, and not kill a call that is still making progress. This
module holds the deterministic core of that: the admission decision, the idle
deadline arithmetic, the two message extractors (a cancellation's target id, a
progress notification's token), and an in-flight registry that is a pure state
machine over an injected clock.

The concurrent execution itself (spawning the bounded set of tasks, racing them
against the client reader) is timing, not byte-matchable, and lives in the
router. Only the pieces below are deterministic, so only they are pinned in the
differential corpus; the registry is a pure state machine tested identically in
both arms.
"""

from __future__ import annotations

from . import jsonrpc


def admit(in_flight: int, cap: int) -> bool:
    """Whether a new call may start given the number already ``in_flight`` and the
    per-connection ``cap``. A cap of zero (or less) is unbounded, so it always
    admits; otherwise a call is admitted only while strictly under the cap."""
    if cap <= 0:
        return True
    return in_flight < cap


def deadline(now_ms: int, idle_ms: int) -> int:
    """The idle deadline for a call starting (or making progress) at ``now_ms``
    with an idle budget of ``idle_ms``. A budget of zero (or less) means no
    deadline, represented as ``0``; otherwise the wall-clock instant the call
    goes idle-expired absent further progress."""
    if idle_ms <= 0:
        return 0
    return now_ms + idle_ms


def expired(deadline_ms: int, now_ms: int) -> bool:
    """Whether a call whose idle deadline is ``deadline_ms`` has expired at
    ``now_ms``. A deadline of zero (or less) never expires (no idle budget)."""
    if deadline_ms <= 0:
        return False
    return now_ms >= deadline_ms


def cancel_request_id(message: jsonrpc.Message):
    """The ``requestId`` a ``notifications/cancelled`` targets, or ``None`` for
    any other message (or a cancellation that names nothing). The id is returned
    verbatim (a client's own request id may be a string or a number)."""
    if jsonrpc.method_of(message) != "notifications/cancelled":
        return None
    params = message.get("params")
    return params.get("requestId") if isinstance(params, dict) else None


def progress_token(message: jsonrpc.Message):
    """The ``progressToken`` a ``notifications/progress`` carries, or ``None`` for
    any other message. A progress notification for a tracked token resets that
    call's idle deadline."""
    if jsonrpc.method_of(message) != "notifications/progress":
        return None
    params = message.get("params")
    return params.get("progressToken") if isinstance(params, dict) else None


class InFlight:
    """The in-flight set of server-originated calls, keyed by client request id,
    each carrying its current idle deadline. A pure state machine: it takes the
    clock as an argument and never reads real time, so both arms and the tests are
    deterministic. Cancellation of the actual task and the concurrency semaphore
    live in the router; this is the bookkeeping they agree on."""

    def __init__(self) -> None:
        self._deadlines: dict = {}

    def register(self, id, deadline_ms: int) -> None:
        """Record a call as in-flight with its initial idle deadline."""
        self._deadlines[id] = deadline_ms

    def contains(self, id) -> bool:
        return id in self._deadlines

    def touch(self, id, deadline_ms: int) -> bool:
        """Reset a call's idle deadline (a progress notification arrived). Returns
        whether the call was in-flight; an unknown id is a no-op."""
        if id not in self._deadlines:
            return False
        self._deadlines[id] = deadline_ms
        return True

    def remove(self, id) -> bool:
        """Drop a call (it finished or was cancelled). Returns whether it was
        present, so a caller can tell a real cancellation from a stray id."""
        return self._deadlines.pop(id, None) is not None

    def count(self) -> int:
        return len(self._deadlines)

    def at_capacity(self, cap: int) -> bool:
        """Whether a new call must wait, i.e. the cap is reached."""
        return not admit(self.count(), cap)

    def expired_ids(self, now_ms: int) -> list:
        """The in-flight ids whose idle deadline has passed at ``now_ms``, sorted
        for a deterministic reap order."""
        return sorted((id for id, d in self._deadlines.items() if expired(d, now_ms)), key=repr)
