"""σ2 worker-pool substrate unit tests (Python arm). Mirrors the Rust arm.

The pure helpers are also pinned in the differential corpus; this covers the
InFlight state machine, which is deterministic (injected clock) but stateful, so
it is tested identically in both arms rather than corpus-pinned.
"""

from yamp import pool


def test_admit_bounds():
    assert pool.admit(0, 0) is True  # cap 0 is unbounded
    assert pool.admit(99, 0) is True
    assert pool.admit(0, 1) is True
    assert pool.admit(1, 1) is False  # at cap
    assert pool.admit(3, 4) is True


def test_deadline_and_expired():
    assert pool.deadline(1000, 0) == 0  # no idle budget
    assert pool.deadline(1000, 5000) == 6000
    assert pool.expired(0, 10 ** 9) is False  # no deadline never expires
    assert pool.expired(6000, 5999) is False
    assert pool.expired(6000, 6000) is True


def test_cancel_and_progress_extraction():
    assert pool.cancel_request_id({"method": "notifications/cancelled", "params": {"requestId": "c-7"}}) == "c-7"
    assert pool.cancel_request_id({"method": "notifications/cancelled", "params": {}}) is None
    assert pool.cancel_request_id({"method": "tools/call", "params": {"requestId": "c-7"}}) is None
    assert pool.cancel_request_id({"method": "notifications/cancelled", "params": None}) is None
    assert pool.progress_token({"method": "notifications/progress", "params": {"progressToken": "t"}}) == "t"
    assert pool.progress_token({"method": "notifications/progress", "params": None}) is None
    assert pool.progress_token({"method": "notifications/cancelled", "params": {"progressToken": "t"}}) is None


def test_inflight_state_machine():
    f = pool.InFlight()
    assert f.count() == 0
    f.register("a", pool.deadline(1000, 5000))  # deadline 6000
    f.register("b", 0)  # no deadline
    assert f.count() == 2
    assert f.contains("a") and not f.contains("z")

    # At cap 2 a third call must wait; cap 3 admits.
    assert f.at_capacity(2) is True
    assert f.at_capacity(3) is False

    # Progress on `a` resets its deadline; an unknown id is a no-op.
    assert f.touch("a", pool.deadline(4000, 5000)) is True  # now 9000
    assert f.touch("missing", 1) is False

    # Only `a` had a deadline; at now=8000 nothing is expired (a moved to 9000).
    assert f.expired_ids(8000) == []
    # At now=9000 `a` expires; `b` has no deadline so never does.
    assert f.expired_ids(9000) == ["a"]

    assert f.remove("a") is True
    assert f.remove("a") is False  # already gone
    assert f.count() == 1


def test_expired_ids_sorted_deterministically():
    f = pool.InFlight()
    for id in ["m", "a", "z"]:
        f.register(id, 100)
    assert f.expired_ids(200) == ["a", "m", "z"]
