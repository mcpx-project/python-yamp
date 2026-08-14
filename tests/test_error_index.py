"""Track U: the generated error index (ERRORS.md) cannot drift from the registry.

The doc is rendered from ``yamp.errors.REGISTRY`` by ``gen_error_index.py``. This
gate regenerates it and asserts the committed file matches, so an added, removed, or
reworded error forces ERRORS.md to be regenerated in the same change.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from gen_error_index import render, render_config_errors  # noqa: E402


def test_error_index_is_not_stale():
    committed = (ROOT / "ERRORS.md").read_text()
    assert committed == render(), "ERRORS.md is stale; run python/tools/gen_error_index.py"


def test_config_error_index_is_not_stale():
    committed = (ROOT / "CONFIG_ERRORS.md").read_text()
    assert committed == render_config_errors(), "CONFIG_ERRORS.md is stale; run python/tools/gen_error_index.py"
