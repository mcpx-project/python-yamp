"""Track U (U5): hot reload of the Streamable HTTP server via SIGHUP.

Spawns the real serve_streamable.py, observes GET /status reflecting the current
config, then rewrites the config file and sends SIGHUP: a valid reload is applied to
new connections (the server never restarts, so in-flight sessions are not dropped),
and an invalid reload is rejected with the running config kept.
"""

import json
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _status(port):
    conn = socket.create_connection(("127.0.0.1", port), timeout=2)
    conn.settimeout(2)
    conn.sendall(b"GET /status HTTP/1.1\r\nHost: x\r\n\r\n")
    data = b""
    while b"\r\n\r\n" not in data:
        data += conn.recv(4096)
    head, _, rest = data.partition(b"\r\n\r\n")
    clen = next((int(line.split(b":")[1]) for line in head.split(b"\r\n") if line.lower().startswith(b"content-length:")), 0)
    while len(rest) < clen:
        rest += conn.recv(4096)
    conn.close()
    return json.loads(rest[:clen])


def _backends(port):
    return [b["id"] for b in _status(port)["backends"]]


def _await(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except OSError:
            pass
        time.sleep(0.05)
    return False


def test_sighup_reload_swaps_config_and_rejects_bad(tmp_path):
    port = _free_port()
    cfg = tmp_path / "c.json"
    one = {"listen": f"127.0.0.1:{port}", "backends": {"b0": {"address": "127.0.0.1:1"}}}
    cfg.write_text(json.dumps(one))
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "serve_streamable.py"), "--config", str(cfg)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        assert _await(lambda: _backends(port) == ["b0"]), "server did not come up with b0"

        # A valid reload adds b1; new connections see it (U5).
        two = {"listen": f"127.0.0.1:{port}", "backends": {"b0": {"address": "127.0.0.1:1"}, "b1": {"address": "127.0.0.1:2"}}}
        cfg.write_text(json.dumps(two))
        proc.send_signal(signal.SIGHUP)
        assert _await(lambda: _backends(port) == ["b0", "b1"]), "reload did not add b1"

        # A bad reload is rejected and the running config kept: the server stays up and
        # /status still reports the previous (b0, b1) surface.
        cfg.write_text(json.dumps({"listen": f"127.0.0.1:{port}", "namespacing": {"strategy": "nope"}, **two}))
        proc.send_signal(signal.SIGHUP)
        time.sleep(0.4)
        assert _backends(port) == ["b0", "b1"], "bad reload changed the config"
    finally:
        proc.terminate()
        proc.wait(timeout=5)
