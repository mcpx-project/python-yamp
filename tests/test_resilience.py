import asyncio

from yamp import jsonrpc
from yamp.resilience import (
    PROXY_PARTIAL_KEY,
    SERVER_NOT_AVAILABLE,
    CircuitBreaker,
    CircuitState,
    ManagedBackend,
    ResilientRouter,
)


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class ScriptedChannel:
    """A backend whose per-request behavior follows a fixed script.

    ``script`` is a list of "ok" / "fail" markers, one per request; requests
    beyond the script default to "ok". Deterministic by construction.
    """

    def __init__(self, id: str, tools: list[str], script: list[str] | None = None) -> None:
        self.id = id
        self._tools = tools
        self._script = script or []
        self.calls = 0

    async def request(self, method: str, params: jsonrpc.Message) -> jsonrpc.Message:
        behavior = self._script[self.calls] if self.calls < len(self._script) else "ok"
        self.calls += 1
        if behavior == "fail":
            raise ConnectionError(f"{self.id} transport failure")
        if behavior == "err":
            return {"error": {"code": -32000, "message": "tool error"}}
        if method == "tools/list":
            return {"result": {"tools": [{"name": t} for t in self._tools]}}
        if method == "tools/call":
            return {"result": {"content": [{"type": "text", "text": f"{self.id}:{params['name']}"}]}}
        return {"result": {}}


# ---- unit: circuit breaker state machine ----

def test_breaker_opens_after_threshold():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=3, reset_timeout=10.0, now=clock)
    assert breaker.state is CircuitState.CLOSED
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.allow()  # still closed under threshold
    breaker.record_failure()  # third failure trips it
    assert breaker.state is CircuitState.OPEN
    assert not breaker.allow()


def test_breaker_half_open_recovers_on_success():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=10.0, now=clock)
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    clock.advance(10.0)  # reset timeout elapses
    assert breaker.state is CircuitState.HALF_OPEN
    assert breaker.allow()  # a trial is permitted
    breaker.record_success()
    assert breaker.state is CircuitState.CLOSED


def test_breaker_half_open_reopens_on_failure():
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=10.0, now=clock)
    breaker.record_failure()
    clock.advance(10.0)
    assert breaker.state is CircuitState.HALF_OPEN
    breaker.record_failure()  # trial fails
    assert breaker.state is CircuitState.OPEN
    clock.advance(5.0)
    assert not breaker.allow()  # still open before the next reset window


def test_breaker_success_resets_failure_count():
    breaker = CircuitBreaker(failure_threshold=3, now=FakeClock())
    breaker.record_failure()
    breaker.record_failure()
    breaker.record_success()  # clears the count
    breaker.record_failure()
    breaker.record_failure()
    assert breaker.allow()  # two failures after reset, still under threshold


# ---- chaos: resilient router under scripted faults ----

def _router(specs, clock, threshold=2, reset=10.0):
    backends = []
    channels = {}
    for name, tools, script in specs:
        channel = ScriptedChannel(name, tools, script)
        channels[name] = channel
        backends.append(
            ManagedBackend(channel, CircuitBreaker(threshold, reset, now=clock))
        )
    return ResilientRouter(backends), channels


def test_chaos_partial_fanout_reports_unavailable():
    clock = FakeClock()
    router, _ = _router(
        [("gh", ["a"], []), ("bad", ["b"], ["fail"]), ("gl", ["c"], [])],
        clock,
    )

    result = asyncio.run(router.tools_list("l"))["result"]
    names = {t["name"] for t in result["tools"]}
    assert names == {"gh__a", "gl__c"}  # healthy backends' tools returned
    partial = result["_meta"][PROXY_PARTIAL_KEY]
    assert partial["unavailable_backends"] == ["bad"]
    assert partial["reason"] == "circuit_breaker_open"


def test_chaos_open_breaker_removes_tools_and_blocks_calls():
    clock = FakeClock()
    # 'bad' fails on its first two list attempts, tripping its breaker (threshold 2).
    router, channels = _router([("bad", ["b"], ["fail", "fail"])], clock, threshold=2)

    async def scenario():
        await router.tools_list("1")  # failure 1
        await router.tools_list("2")  # failure 2 -> breaker opens
        # Now the breaker is open: tools removed, calls rejected with -32003.
        listed = await router.tools_list("3")
        call = await router.tools_call(
            {"jsonrpc": "2.0", "id": "c", "method": "tools/call", "params": {"name": "bad__b"}}
        )
        return listed, call

    listed, call = asyncio.run(scenario())
    assert listed["result"]["tools"] == []  # tools removed while open
    assert "bad" in listed["result"]["_meta"][PROXY_PARTIAL_KEY]["unavailable_backends"]
    assert call["error"]["code"] == SERVER_NOT_AVAILABLE
    # The open breaker short-circuited: no extra transport call was made for it.
    assert channels["bad"].calls == 2


def test_chaos_recovery_after_reset_timeout():
    clock = FakeClock()
    # fail twice (opens at threshold 2), then succeed forever.
    router, channels = _router([("b", ["t"], ["fail", "fail"])], clock, threshold=2, reset=10.0)

    async def scenario():
        await router.tools_list("1")
        await router.tools_list("2")  # breaker opens
        opened = await router.tools_call(
            {"jsonrpc": "2.0", "id": "a", "method": "tools/call", "params": {"name": "b__t"}}
        )
        clock.advance(10.0)  # reset window elapses -> half-open
        recovered = await router.tools_call(
            {"jsonrpc": "2.0", "id": "b", "method": "tools/call", "params": {"name": "b__t"}}
        )
        return opened, recovered

    opened, recovered = asyncio.run(scenario())
    assert opened["error"]["code"] == SERVER_NOT_AVAILABLE  # rejected while open
    assert recovered["result"]["content"][0]["text"] == "b:t"  # half-open trial succeeded


def test_chaos_no_retry_on_tools_call_failure():
    clock = FakeClock()
    # tools/call fails once; must be attempted exactly once (no retry).
    router, channels = _router([("b", ["t"], ["fail"])], clock, threshold=5)

    result = asyncio.run(
        router.tools_call(
            {"jsonrpc": "2.0", "id": "a", "method": "tools/call", "params": {"name": "b__t"}}
        )
    )
    assert result["error"]["code"] == SERVER_NOT_AVAILABLE
    assert channels["b"].calls == 1  # single attempt, side-effecting call not retried


def test_chaos_unknown_tool_rejected():
    clock = FakeClock()
    router, _ = _router([("b", ["t"], [])], clock)
    result = asyncio.run(
        router.tools_call(
            {"jsonrpc": "2.0", "id": "a", "method": "tools/call", "params": {"name": "nope"}}
        )
    )
    assert result["error"]["code"] != SERVER_NOT_AVAILABLE  # it is an unknown-tool error


def test_chaos_backend_application_error_propagated():
    clock = FakeClock()
    router, _ = _router([("b", ["t"], ["err"])], clock)
    result = asyncio.run(
        router.tools_call(
            {"jsonrpc": "2.0", "id": "a", "method": "tools/call", "params": {"name": "b__t"}}
        )
    )
    # An application error from the backend is a valid response, not a breaker
    # failure: it is forwarded to the client unchanged.
    assert result["error"]["code"] == -32000


def test_unknown_backend_after_valid_split_rejected():
    clock = FakeClock()
    router, _ = _router([("b", ["t"], [])], clock)
    result = asyncio.run(
        router.tools_call(
            {"jsonrpc": "2.0", "id": "a", "method": "tools/call", "params": {"name": "zz__x"}}
        )
    )
    assert result["error"]["code"] != SERVER_NOT_AVAILABLE  # unknown-tool, not unavailable


def test_surface_change_detection_and_notification():
    clock = FakeClock()
    router, _ = _router([("b", ["t"], ["fail", "fail"])], clock, threshold=2)

    async def scenario():
        assert router.surface_changed() is False  # initial snapshot, no change
        await router.tools_list("1")
        await router.tools_list("2")  # breaker opens -> surface shrinks
        return router.surface_changed()

    changed = asyncio.run(scenario())
    assert changed is True
    note = ResilientRouter.list_changed_notification()
    assert note["method"] == "notifications/tools/list_changed"


def test_resilience_latency_within_budget():
    import statistics
    import time

    from yamp.instrument import within_budget

    clock = FakeClock()
    router, _ = _router([("b", ["t"], [])], clock)

    async def scenario():
        message = {"jsonrpc": "2.0", "id": "t", "method": "tools/call", "params": {"name": "b__t"}}
        for _ in range(50):
            await router.tools_call(message)
        latencies = []
        for _ in range(300):
            start = time.perf_counter()
            await router.tools_call(message)
            latencies.append((time.perf_counter() - start) * 1000.0)
        return latencies

    latencies = asyncio.run(scenario())
    median = statistics.median(latencies)
    under = sum(1 for x in latencies if within_budget(x)) / len(latencies)
    print(f"\n[latency δ6 resilience] median={median:.4f}ms within={under:.3%}")
    assert within_budget(median)
    assert under >= 0.99
