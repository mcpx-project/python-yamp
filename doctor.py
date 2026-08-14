"""yamp doctor: server-role config preflight over a --config file (Track U).

The ``nginx -t`` analog. Loads the config, builds the local handler surface it
would serve, and runs the σ6 doctor check over that surface and the advertised
protocol version. Every finding is printed at once.

The exit code follows a selectable strictness mode: ``default`` (warnings advisory,
only an error blocks, exit 1), ``--strict`` (any finding blocks), or ``--lenient``
(surface findings never block). All three still exit 2 when the config could not be
loaded at all, since an unparseable file cannot be preflighted.

Usage:
  python doctor.py --config file.json [--json] [--strict | --lenient]
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from yamp import config as cfg
from yamp import doctor, version
from yamp.handler import build_registry


def _emit(findings: list[dict], as_json: bool, mode: str) -> None:
    if as_json:
        # Sort keys so the JSON bytes match the Rust arm, whose serde_json map
        # serializes keys sorted; the two arms' --json output stays byte-identical.
        print(json.dumps(doctor.report(findings, mode), separators=(",", ":"), sort_keys=True))
    else:
        print(doctor.render_text(findings, mode))


def _config_finding(finding: dict) -> dict:
    # Map a config diagnosis (U4/U8) to a doctor finding: the specific field-failure
    # slug as the code, and the message plus fix hint. So doctor identifies the exact
    # cause rather than a generic config-load error.
    where = f" (line {finding['line']}, column {finding['column']})" if "line" in finding else ""
    return doctor.finding(doctor.LEVEL_ERROR, finding["slug"], f"{finding['message']}{where}; {finding['hint']}")


def run(config_path: str, as_json: bool = False, mode: str = doctor.MODE_DEFAULT) -> int:
    """Load and preflight ``config_path``, emit the report, return the exit code."""
    # A config that will not load is a fatal config-file problem (exit 2) in every
    # mode: an unparseable file cannot be preflighted. It is rendered under the default
    # mode so the verdict reads "config invalid", and the specific field-failure cause
    # is identified (U8) with a fix hint (U4).
    try:
        text = Path(config_path).read_text()
    except OSError as exc:
        _emit([doctor.finding(doctor.LEVEL_ERROR, "config-load", str(exc))], as_json, doctor.MODE_DEFAULT)
        return 2
    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        _emit([_config_finding(cfg.parse_error_finding(exc.msg, exc.lineno, exc.colno))], as_json, doctor.MODE_DEFAULT)
        return 2
    diagnosis = cfg.diagnose(raw)
    if diagnosis is not None:
        _emit([_config_finding(diagnosis)], as_json, doctor.MODE_DEFAULT)
        return 2
    config = cfg.from_dict(raw)
    # The server-role preflight inspects the *local* handler surface (Conversion
    # handlers, meta-tools). Live backend tools are not consulted, so the check
    # needs no backend connections, matching the pure σ6 doctor.
    registry = build_registry(
        config.handlers, backends_provider=lambda: [{"id": b.id} for b in config.backends]
    )
    findings = doctor.check_registry(registry, version.STATEFUL_PROTOCOL_VERSION)
    _emit(findings, as_json, mode)
    return doctor.exit_code(findings, mode)


def main() -> None:
    parser = argparse.ArgumentParser(prog="yamp-doctor")
    parser.add_argument("--config", required=True)
    parser.add_argument("--json", action="store_true")
    strictness = parser.add_mutually_exclusive_group()
    strictness.add_argument("--strict", action="store_true", help="any finding blocks (exit 1)")
    strictness.add_argument("--lenient", action="store_true", help="surface findings never block")
    args = parser.parse_args()
    mode = doctor.MODE_STRICT if args.strict else doctor.MODE_LENIENT if args.lenient else doctor.MODE_DEFAULT
    sys.exit(run(args.config, args.json, mode))


if __name__ == "__main__":
    main()
