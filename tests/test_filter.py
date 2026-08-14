"""Extension filter chain (ε0): verdict set, failure policy, chain outcomes."""

from yamp import errors, filters


def test_resolve_failure_by_policy():
    assert filters.resolve_failure(filters.FAIL_CLOSED) == filters.DENY
    assert filters.resolve_failure(filters.FAIL_OPEN) == filters.ALLOW
    # An unknown policy is treated as advisory (allow), never a silent deny loop.
    assert filters.resolve_failure("nonsense") == filters.ALLOW


def test_deny_response_is_policy_error():
    response = filters.deny_response(9, "infected")
    assert response == {
        "jsonrpc": "2.0",
        "id": 9,
        "error": {"code": errors.POLICY_DENIED, "message": "infected"},
    }


def test_allow_forwards_unchanged():
    req = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "t"}}
    out = filters.chain_outcome([{"kind": "allow"}], req)
    assert out == {"action": "forward", "message": req}


def test_mutate_substitutes_arguments():
    req = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "t", "arguments": {"a": 1}}}
    out = filters.chain_outcome([{"kind": "mutate", "arguments": {"a": 2}}], req)
    assert out["action"] == "forward"
    assert out["message"]["params"]["arguments"] == {"a": 2}
    assert req["params"]["arguments"] == {"a": 1}, "input must not be mutated in place"


def test_annotate_merges_provenance_into_meta():
    req = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "t", "_meta": {"trace": "x"}}}
    out = filters.chain_outcome([{"kind": "annotate", "provenance": {"scanner": "clean"}}], req)
    assert out["message"]["params"]["_meta"] == {"trace": "x", "scanner": "clean"}


def test_deny_and_quarantine_block():
    req = {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {}}
    denied = filters.chain_outcome([{"kind": "deny", "reason": "no"}], req)
    assert denied["action"] == "block" and denied["quarantined"] is False
    assert denied["response"]["error"]["code"] == errors.POLICY_DENIED
    held = filters.chain_outcome([{"kind": "quarantine", "reason": "hold"}], req)
    assert held["action"] == "block" and held["quarantined"] is True


def test_deny_short_circuits_later_verdicts():
    req = {"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"arguments": {"a": 1}}}
    out = filters.chain_outcome(
        [{"kind": "deny", "reason": "stop"}, {"kind": "mutate", "arguments": {"a": 99}}], req
    )
    assert out["action"] == "block"


class _Stub(filters.Filter):
    def __init__(self, verdict, policy=filters.FAIL_CLOSED, raises=False):
        self._verdict = verdict
        self.failure_policy = policy
        self._raises = raises

    def evaluate(self, hook, message):
        if self._raises:
            raise RuntimeError("scanner down")
        return self._verdict


def test_chain_runs_filters_in_order():
    req = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"arguments": {"a": 0}}}
    chain = filters.FilterChain([
        _Stub({"kind": "annotate", "provenance": {"seen": True}}),
        _Stub({"kind": "mutate", "arguments": {"a": 1}}),
    ])
    out = chain.run(filters.REQUEST, req)
    assert out["message"]["params"]["arguments"] == {"a": 1}
    assert out["message"]["params"]["_meta"] == {"seen": True}


def test_failing_filter_is_host_resolved_by_policy():
    req = {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {}}
    closed = filters.FilterChain([_Stub(None, policy=filters.FAIL_CLOSED, raises=True)])
    assert closed.run(filters.REQUEST, req)["action"] == "block"
    open_ = filters.FilterChain([_Stub(None, policy=filters.FAIL_OPEN, raises=True)])
    assert open_.run(filters.REQUEST, req)["action"] == "forward"


def test_hook_points_and_verdicts_are_closed_sets():
    assert filters.REQUEST in filters.HOOK_POINTS
    assert filters.VERDICTS == {"allow", "deny", "mutate", "annotate", "quarantine"}


# ---- ε2: interest declaration and preview ----


def test_interest_matching():
    ctx = {"method": "tools/call", "tool": "gh__x", "direction": "c2u", "content_types": ["image/png"]}
    assert filters.interested({}, ctx)  # empty interest matches all
    assert filters.interested({"methods": ["tools/call"]}, ctx)
    assert not filters.interested({"methods": ["resources/read"]}, ctx)
    assert filters.interested({"methods": ["*"]}, ctx)
    assert filters.interested({"tools": ["gh__x"]}, ctx)
    assert not filters.interested({"tools": ["other"]}, ctx)
    assert filters.interested({"content_types": ["image/*"]}, ctx)
    assert not filters.interested({"content_types": ["application/pdf"]}, ctx)
    # A content-scoped filter skips a message that carries no matching content.
    assert not filters.interested({"content_types": ["image/*"]}, {"method": "tools/call", "content_types": []})


def test_message_context_extracts_dimensions():
    message = {
        "method": "tools/call",
        "params": {"name": "gh__x"},
        "result": {"content": [{"type": "image", "data": "", "mimeType": "image/png"}]},
    }
    ctx = filters.message_context(message, "u2c")
    assert ctx == {"method": "tools/call", "tool": "gh__x", "direction": "u2c", "content_types": ["image/png"]}


def test_preview_slice_and_resolve():
    assert filters.preview(b"abcdef", 3) == {"preview": b"abc".hex(), "ieof": False}
    assert filters.preview(b"abc", 3) == {"preview": b"abc".hex(), "ieof": True}
    assert filters.preview(b"abc", 9) == {"preview": b"abc".hex(), "ieof": True}
    assert filters.preview_resolve("deny", False) == {"action": "verdict", "kind": "deny"}
    assert filters.preview_resolve("continue", True) == {"action": "scan_full"}
    assert filters.preview_resolve("continue", False) == {"action": "need_more"}


class _Scoped(filters.Filter):
    def __init__(self, verdict, interest):
        self._verdict = verdict
        self._interest = interest

    def interest(self):
        return self._interest

    def evaluate(self, hook, message):
        return self._verdict


def test_chain_skips_uninterested_filter():
    req = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "gh__x", "arguments": {}}}
    # Scoped to resources/read: skipped on a tools/call, so its deny never fires.
    skipped = filters.FilterChain([_Scoped({"kind": "deny", "reason": "no"}, {"methods": ["resources/read"]})])
    assert skipped.run(filters.REQUEST, req)["action"] == "forward"
    # Scoped to tools/call: it runs and blocks.
    hit = filters.FilterChain([_Scoped({"kind": "deny", "reason": "no"}, {"methods": ["tools/call"]})])
    assert hit.run(filters.REQUEST, req)["action"] == "block"
