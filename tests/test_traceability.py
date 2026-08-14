"""Spec-traceability checker (the SEP-2484 pattern).

Loads the shared clause->test matrix at conformance/sep-0000-traceability.json
and verifies it is well-formed and that every Python test it names actually
exists in this suite, so a renamed or deleted test breaks the matrix rather than
silently dropping conformance evidence.

The two arms ship as separate repositories and each carries an identical copy of
the matrix, so each arm validates its own half of the references: the Rust
references are checked by the parallel checker in that repository
(tests/traceability.rs), which is the only place that can see those tests.
"""

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MATRIX = json.loads((ROOT / "conformance" / "sep-0000-traceability.json").read_text())


def _defined(paths, pattern):
    names: set[str] = set()
    for path in paths:
        names.update(re.findall(pattern, path.read_text()))
    return names


PY_TESTS = _defined((ROOT / "tests").glob("*.py"), r"def (test_[A-Za-z0-9_]+)\(")


def test_matrix_is_well_formed():
    clauses = MATRIX["clauses"]
    assert clauses, "matrix has no clauses"
    ids = [c["id"] for c in clauses]
    assert len(ids) == len(set(ids)), "duplicate clause id"
    for clause in clauses:
        assert clause["level"] in {"MUST", "SHOULD"}, clause["id"]
        assert clause["section"] and clause["text"], clause["id"]
        assert clause["tests"]["python"], clause["id"]
        assert clause["tests"]["rust"], clause["id"]


def test_every_referenced_python_test_exists():
    missing = {t for c in MATRIX["clauses"] for t in c["tests"]["python"] if t not in PY_TESTS}
    assert not missing, f"traceability references unknown Python tests: {sorted(missing)}"
