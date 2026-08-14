"""Track U (U6): every CLI command supports --json and exits with documented codes.

Documented exit codes are 0 (success), 1 (a difference / invalid config), and 2 (a
config that cannot be read or an unknown key). Each command's machine output parses
as JSON. This is the U6 release gate.
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # python/ for the entrypoints

import config_cli
import doctor as doctor_cli

DOCUMENTED_EXITS = {0, 1, 2}


def _cfg(tmp_path, name, data):
    path = tmp_path / name
    path.write_text(json.dumps(data))
    return str(path)


def test_config_commands_json_and_exit_codes(tmp_path, capsys):
    good = _cfg(tmp_path, "good.json", {"listen": "127.0.0.1:9100", "backends": {"b0": {"address": "127.0.0.1:9101"}}})
    other = _cfg(tmp_path, "other.json", {"listen": "127.0.0.1:9200"})
    cases = [
        argparse.Namespace(command="validate", config=good, json=True, key=None, to=None),
        argparse.Namespace(command="explain", config=good, json=True, key="listen", to=None),
        argparse.Namespace(command="effective", config=good, json=True, key=None, to=None),
        argparse.Namespace(command="diff", config=good, json=True, key=None, to=other),
        argparse.Namespace(command="adapt", config=good, json=False, key=None, to=None),  # adapt always emits JSON
    ]
    for args in cases:
        code = config_cli.run(args)
        out = capsys.readouterr().out
        assert code in DOCUMENTED_EXITS, f"{args.command} returned undocumented exit {code}"
        json.loads(out)  # every command's stdout is valid JSON


def test_doctor_json_and_exit_codes(tmp_path, capsys):
    good = _cfg(tmp_path, "good.json", {"listen": "127.0.0.1:9100"})
    code = doctor_cli.run(good, as_json=True)
    assert code in DOCUMENTED_EXITS
    json.loads(capsys.readouterr().out)
