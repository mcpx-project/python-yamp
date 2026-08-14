"""Golden-flow corpus (M-T1): the Python arm reproduces every recorded flow.

The Rust arm replays the same `conformance/flow-corpus.json` (rust/tests/flow.rs)
and must produce the identical client-facing message sequence, so the two data
planes are pinned to behave identically across whole exchanges, not just per pure
function. This test also guards against a stale corpus.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))

from gen_flow_corpus import SCENARIOS, drive  # noqa: E402

CORPUS = json.loads((Path(__file__).resolve().parents[1] / "conformance" / "flow-corpus.json").read_text())


def test_flow_corpus_matches_python_arm():
    by_name = {scenario["name"]: scenario for scenario in SCENARIOS}
    assert CORPUS["flows"], "flow corpus is empty"
    for flow in CORPUS["flows"]:
        got = drive(by_name[flow["name"]])
        assert got == flow["out"], f"{flow['name']} diverged"


def test_flow_corpus_covers_every_scenario():
    assert {f["name"] for f in CORPUS["flows"]} == {s["name"] for s in SCENARIOS}
