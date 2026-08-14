"""Executable-docs gate (M-T1): every config example in the docs actually loads.

Extracts fenced ```json blocks from the README and, when it is present, the user
guide; any block in the loader's format (a JSON object with a ``listen`` key)
must pass config validation, so a config snippet cannot rot without CI catching
it. Illustrative blocks with placeholders (not valid JSON) and non-config JSON
(message examples) are skipped. This is the docs-as-code discipline: if a snippet
can rot, CI catches it rotting.

The guide ships in the separate docs repository, so it is absent from a bare
clone of this one. The gate then covers this repository's own README, and the
floor drops to match what is actually present rather than failing on a file it
cannot see. A checkout that has the guide alongside gets the full floor back.
"""

import json
import re
from pathlib import Path

from yamp import config

ROOT = Path(__file__).resolve().parents[1]
GUIDE = ROOT / "user-guide"
DOC_FILES = sorted(GUIDE.rglob("*.md")) + [ROOT / "README.md"]
MIN_CONFIGS = 4 if GUIDE.is_dir() else 1


def _json_blocks(text: str) -> list[str]:
    return re.findall(r"```json\n(.*?)```", text, re.DOTALL)


def test_config_snippets_validate():
    configs = 0
    for path in DOC_FILES:
        for block in _json_blocks(path.read_text()):
            try:
                data = json.loads(block)
            except json.JSONDecodeError:
                continue  # an illustrative block with placeholders, not a config
            if isinstance(data, dict) and "listen" in data:
                config.from_dict(data)  # raises ValueError on an invalid config
                configs += 1
    assert configs >= MIN_CONFIGS, f"expected at least {MIN_CONFIGS} config examples, got {configs}"
