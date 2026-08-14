"""Track U (U7): secure zero-config defaults, the loopback/auth bind gate.

The pure classifier and gate are pinned across arms in the differential corpus; this
covers the entrypoint adapter (guard_bind) and confirms a server refuses to expose a
non-loopback listener without client auth.
"""

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # python/ for the entrypoint

import serve_streamable
from yamp import security
from yamp.config import ProxyConfig


def test_guard_bind_refuses_public_without_auth():
    msg = security.guard_bind("0.0.0.0:9100", has_client_auth=False, insecure=False)
    assert msg is not None
    assert "0.0.0.0" in msg and "--insecure" in msg


def test_guard_bind_allows_loopback():
    assert security.guard_bind("127.0.0.1:9100", False, False) is None


def test_guard_bind_allows_public_with_auth():
    assert security.guard_bind("0.0.0.0:9100", True, False) is None


def test_guard_bind_insecure_override_allows_public():
    assert security.guard_bind("0.0.0.0:9100", False, True) is None


def test_streamable_serve_refuses_public_bind_without_auth():
    config = ProxyConfig(listen="0.0.0.0:0", backends=[])
    with pytest.raises(SystemExit) as exc:
        asyncio.run(serve_streamable.serve(config, insecure=False))
    assert exc.value.code == 2
