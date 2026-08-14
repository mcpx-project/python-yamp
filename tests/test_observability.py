from yamp.observability import (
    BAGGAGE,
    PROXY_HOPS_KEY,
    TRACEPARENT,
    TRACESTATE,
    append_hop,
    ensure_trace_context,
    make_traceparent,
    proxy_hop,
)


def _fixed_ids():
    return "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"


def test_existing_traceparent_and_state_preserved():
    meta = {
        TRACEPARENT: "00-aaaa-bbbb-01",
        TRACESTATE: "vendor=1",
        BAGGAGE: "k=v",
    }
    out = ensure_trace_context(meta, new_ids=_fixed_ids)
    assert out[TRACEPARENT] == "00-aaaa-bbbb-01"  # not regenerated
    assert out[TRACESTATE] == "vendor=1"
    assert out[BAGGAGE] == "k=v"


def test_traceparent_generated_when_absent():
    out = ensure_trace_context({}, new_ids=_fixed_ids)
    assert out[TRACEPARENT] == make_traceparent(*_fixed_ids())
    assert out[TRACEPARENT] == "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


def test_default_ids_produce_valid_shape():
    out = ensure_trace_context({})
    version, trace_id, span_id, flags = out[TRACEPARENT].split("-")
    assert version == "00"
    assert len(trace_id) == 32
    assert len(span_id) == 16
    assert flags == "01"


def test_hop_append_reused_from_observability():
    once = append_hop({})
    twice = append_hop(once)
    assert once[PROXY_HOPS_KEY] == [proxy_hop()]
    assert twice[PROXY_HOPS_KEY] == [proxy_hop(), proxy_hop()]
