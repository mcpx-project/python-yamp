from yamp.instrument import (
    LATENCY_BUDGET_MS,
    SessionRecord,
    within_budget,
)


def test_session_record_round_trips(tmp_path):
    record = SessionRecord(tmp_path / "session" / "arm.jsonl", arm="python")
    record.attempt(gate_before=1, gate_after=2, kind="advance")
    record.attempt(gate_before=2, gate_after=1, kind="regression")
    rows = record.read_all()
    assert [r["kind"] for r in rows] == ["advance", "regression"]
    assert rows[0]["arm"] == "python"
    assert rows[1]["gate_before"] == 2 and rows[1]["gate_after"] == 1


def test_read_all_missing_file_is_empty(tmp_path):
    record = SessionRecord(tmp_path / "absent.jsonl", arm="python")
    assert record.read_all() == []


def test_budget_boundary():
    assert LATENCY_BUDGET_MS == 10.0
    assert within_budget(0.0)
    assert within_budget(LATENCY_BUDGET_MS)
    assert not within_budget(LATENCY_BUDGET_MS + 0.1)
