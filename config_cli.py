"""yamp config: inspect a --config file's effective values and their provenance (Track U).

Five read-only subcommands over config documents:

  config_cli.py validate --config file.json [--json]
  config_cli.py explain --config file.json <key> [--json]
  config_cli.py effective --config file.json [--json]
  config_cli.py diff --config a.json --to b.json [--json]
  config_cli.py adapt --config human.json

``validate`` is the ``nginx -t`` for the config document: it reports whether the file
loads and satisfies the schema (a narrower check than ``yamp-doctor``, which also runs
the server-surface preflight on top of loading). ``explain`` reports one key's
effective value and whether it came from the config document (``config``), a built-in
default (``default``), or is unrecognized (``unknown``). ``effective`` reports every
known key in that form, the fully resolved view. ``diff`` reports every key whose
effective value differs between two documents. ``adapt`` normalizes a human-friendly
document to canonical SEP JSON (which then re-validates and yields an identical
effective config). Exit code is 0 on success (``diff``: no differences), 1 when
``validate`` finds an invalid config or ``diff`` finds a difference, and 2 when a
config cannot be read or parsed (or ``explain`` of an unknown key).
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from yamp import config as cfg


def _load_raw(path: str) -> dict:
    with open(path) as handle:
        return json.load(handle)


def _dump(value, as_json: bool, lines) -> None:
    if as_json:
        print(json.dumps(value, sort_keys=True, separators=(",", ":")))
    else:
        print("\n".join(lines))


def _invalid(finding, as_json) -> int:
    if as_json:
        print(json.dumps({"valid": False, "error": finding}, sort_keys=True, separators=(",", ":")))
    else:
        where = f" (line {finding['line']}, column {finding['column']})" if "line" in finding else ""
        print(f"config invalid: {finding['message']}{where}")
        print(f"  fix: {finding['hint']}")
        print(f"  docs: {finding['docsUrl']}")
    return 1


def run(args) -> int:
    if args.command == "validate":
        # Config-document conformance only (the nginx -t analog): does it load and
        # satisfy the schema, with a line/column, fix hint, and docs URL per error
        # (U4). The server-surface preflight is yamp-doctor's job.
        try:
            text = Path(args.config).read_text()
        except OSError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        try:
            raw = json.loads(text)
        except json.JSONDecodeError as exc:
            return _invalid(cfg.parse_error_finding(exc.msg, exc.lineno, exc.colno), args.json)
        finding = cfg.diagnose(raw)
        if finding is not None:
            return _invalid(finding, args.json)
        print(json.dumps({"valid": True}, separators=(",", ":")) if args.json else "config valid")
        return 0
    try:
        raw = _load_raw(args.config)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.command == "explain":
        entry = cfg.explain(raw, args.key)
        _dump(entry, args.json, [cfg.explain_line(entry)])
        return 2 if entry["source"] == cfg.SOURCE_UNKNOWN else 0
    if args.command == "diff":
        try:
            other = _load_raw(args.to)
        except (OSError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        changes = cfg.diff(raw, other)
        _dump(changes, args.json, [cfg.diff_line(entry) for entry in changes] or ["no differences"])
        return 1 if changes else 0
    if args.command == "adapt":
        print(json.dumps(cfg.adapt(raw), sort_keys=True, separators=(",", ":")))
        return 0
    entries = cfg.effective(raw)
    _dump(entries, args.json, [cfg.explain_line(entry) for entry in entries])
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="yamp-config")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate_parser = subparsers.add_parser("validate", help="check the config document loads and conforms to the schema")
    validate_parser.add_argument("--config", required=True)
    validate_parser.add_argument("--json", action="store_true")
    explain_parser = subparsers.add_parser("explain", help="explain one config key's value and provenance")
    explain_parser.add_argument("--config", required=True)
    explain_parser.add_argument("key")
    explain_parser.add_argument("--json", action="store_true")
    effective_parser = subparsers.add_parser("effective", help="show every key's resolved value and provenance")
    effective_parser.add_argument("--config", required=True)
    effective_parser.add_argument("--json", action="store_true")
    diff_parser = subparsers.add_parser("diff", help="diff two config documents' resolved values")
    diff_parser.add_argument("--config", required=True)
    diff_parser.add_argument("--to", required=True)
    diff_parser.add_argument("--json", action="store_true")
    adapt_parser = subparsers.add_parser("adapt", help="normalize a human-friendly document to canonical JSON")
    adapt_parser.add_argument("--config", required=True)
    sys.exit(run(parser.parse_args()))


if __name__ == "__main__":
    main()
