"""Generate the error index (ERRORS.md) from the single-source errors registry.

The registry in ``yamp.errors`` (mirrored byte-for-byte in the Rust arm, pinned by
the differential corpus ``error_describe``) is the one place error ids, reasons,
causes, and hints live. This script renders them to the reference index so the doc
cannot drift from the code. A gate test (``test_error_index.py``) regenerates and
compares, failing CI when ERRORS.md is stale.

Run from anywhere:  python python/tools/gen_error_index.py
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from yamp import config, errors  # noqa: E402


def render_config_errors() -> str:
    """Render the config-error index (CONFIG_ERRORS.md) from the config catalog."""
    lines = [
        "# yamp Config Error Index",
        "",
        "When a config document fails validation, `yamp-config validate` and `yamp-doctor`",
        "report one of these field-failure causes with a stable slug, a fix hint, and a",
        "link here. This file is generated from the single-source catalog in the `config`",
        "module (both arms); regenerate it with `python/tools/gen_error_index.py`.",
        "",
        "| Slug | Description | Fix |",
        "| --- | --- | --- |",
    ]
    for entry in config.error_catalog():
        lines.append(f"| [{entry['slug']}](#{entry['slug']}) | {entry['description']} | {entry['hint']} |")
    lines.append("")
    for entry in config.error_catalog():
        lines.append(f"### {entry['slug']}")
        lines.append("")
        lines.append(f"{entry['description']}.")
        lines.append("")
        lines.append(f"Fix. {entry['hint']}")
        lines.append("")
    return "\n".join(lines).rstrip("\n") + "\n"


def render() -> str:
    """Render the whole error index to markdown, deterministically."""
    lines = [
        "# yamp Error Index",
        "",
        "Every error yamp emits carries a stable error id in `data.errorId`, and that",
        "id is the key to this index. The leading digit is the error class: `E4xxx` is a",
        "client-caused error and `E5xxx` is a server-side one. The remaining digits echo",
        "the nearest HTTP status where one exists. Backend errors pass through unnamed, so",
        "they are not listed here.",
        "",
        "This file is generated from the single-source registry in the `errors` module",
        "(both arms). Do not edit it by hand; regenerate it with",
        "`python/tools/gen_error_index.py`.",
        "",
        "| Error ID | JSON-RPC code | Reason |",
        "| --- | --- | --- |",
    ]
    for code, eid, reason, _cause, _hint in errors.REGISTRY:
        lines.append(f"| [{eid}](#{eid.lower()}) | `{code}` | {reason} |")
    lines.append("")
    for code, eid, reason, cause, hint in errors.REGISTRY:
        lines.append(f"### {eid}")
        lines.append("")
        lines.append(f"**{reason}** (JSON-RPC code `{code}`)")
        lines.append("")
        lines.append(f"Cause. {cause}")
        lines.append("")
        lines.append(f"Fix. {hint}")
        lines.append("")
    return "\n".join(lines).rstrip("\n") + "\n"


def main() -> None:
    (ROOT / "ERRORS.md").write_text(render())
    (ROOT / "CONFIG_ERRORS.md").write_text(render_config_errors())
    print(f"wrote {len(errors.REGISTRY)} errors to ERRORS.md and {len(config.CONFIG_ERRORS)} to CONFIG_ERRORS.md")


if __name__ == "__main__":
    main()
