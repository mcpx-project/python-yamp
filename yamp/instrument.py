"""Session record and the proxy latency budget.

The session record is the append-only log the experiment protocol consumes
(EXPERIMENT.md). Every field is an index or a count: no source, prompt text, or
payload content is written, so the record is privacy-compatible by
construction.

``LATENCY_BUDGET_MS`` is the hard proxy-overhead budget: the added latency of a
relay hop must stay at or below it, measured against a direct baseline and
tracked across increments so regressions are visible.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

LATENCY_BUDGET_MS = 10.0


class SessionRecord:
    def __init__(self, path: str | os.PathLike[str], arm: str) -> None:
        self._path = Path(path)
        self._arm = arm
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def attempt(self, gate_before: int, gate_after: int, kind: str) -> None:
        """Log one repair attempt: first-failing gate before and after."""
        record = {
            "arm": self._arm,
            "gate_before": gate_before,
            "gate_after": gate_after,
            "kind": kind,
        }
        with self._path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")

    def read_all(self) -> list[dict]:
        if not self._path.exists():
            return []
        with self._path.open(encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]


def within_budget(added_latency_ms: float) -> bool:
    """True when a measured added latency respects the proxy budget."""
    return added_latency_ms <= LATENCY_BUDGET_MS
