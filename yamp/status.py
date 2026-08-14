"""Read-only status snapshot (Track U, the ``stub_status`` analog).

A single pure function composes the proxy's operational status from its identity
(the Server Card, :func:`yamp.routing.server_card`) plus live operational inputs:
the configured backends and the number of active sessions. It is served read-only
at ``GET /status`` by the Streamable HTTP entrypoint, and being pure in its inputs it
is pinned in the differential corpus so both arms compute the identical object.

Per-backend health (circuit-breaker state) is not aggregated here: breakers are
per-session in the served path, so a cross-session health view is a fleet concern
(Track F). This snapshot reports the proxy's own liveness and its configuration.
"""

from __future__ import annotations

from . import routing


def snapshot(backend_ids: list[str], sessions: int) -> dict:
    """The proxy's read-only status: its self-description plus operational counts.

    ``backend_ids`` are the configured backends and ``sessions`` the number of live
    sessions. Pure in its inputs, so both arms compute the identical object."""
    card = routing.server_card()
    return {
        "status": "ok",
        "name": card["name"],
        "version": card["version"],
        "role": card["role"],
        "protocolVersions": card["protocolVersions"],
        "backends": [{"id": backend_id} for backend_id in backend_ids],
        "sessions": sessions,
    }
