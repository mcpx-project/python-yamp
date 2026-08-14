"""Namespace management (SEP §3, draft §8).

Tool and prompt names are namespaced with the ``__`` delimiter as
``{backend_id}__{original_name}``. The backend identifier is assigned by the
operator (never self-declared by a backend) and must be drawn from
``[A-Za-z0-9_-]``. Splitting uses the first delimiter only, so original names
that themselves contain ``__`` round-trip unchanged.
"""

from __future__ import annotations

import re

DELIMITER = "__"
_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


def valid_backend_id(backend_id: str) -> bool:
    # The charset allows single underscores, but an id containing the ``__``
    # delimiter would break reverse resolution (it splits on the first ``__``),
    # so ids carrying the delimiter are rejected.
    return bool(_ID_PATTERN.fullmatch(backend_id)) and DELIMITER not in backend_id


def prefix(backend_id: str, name: str) -> str:
    return f"{backend_id}{DELIMITER}{name}"


def split(name: str) -> tuple[str, str] | None:
    """Reverse a namespaced name into ``(backend_id, original)``.

    Returns ``None`` when the name carries no resolvable prefix.
    """
    backend_id, delimiter, original = name.partition(DELIMITER)
    if not delimiter or not backend_id or not original:
        return None
    return backend_id, original


def prefix_uri(backend_id: str, uri: str) -> str:
    """Namespace a resource URI by inserting the backend id as the first path
    segment (SEP §3.2): ``file:///reports/q3.md`` becomes
    ``file:///docs/reports/q3.md`` for backend ``docs``."""
    scheme, sep, rest = uri.partition("://")
    if not sep:
        return uri
    authority, slash, path = rest.partition("/")
    if not slash:
        return f"{scheme}://{authority}/{backend_id}"
    return f"{scheme}://{authority}/{backend_id}/{path}"


def split_uri(uri: str) -> tuple[str, str] | None:
    """Reverse a namespaced resource URI into ``(backend_id, original_uri)``."""
    scheme, sep, rest = uri.partition("://")
    if not sep:
        return None
    authority, slash, path = rest.partition("/")
    if not slash:
        return None
    backend_id, remainder_sep, remainder = path.partition("/")
    if not backend_id:
        return None
    if not remainder_sep:
        return backend_id, f"{scheme}://{authority}"
    return backend_id, f"{scheme}://{authority}/{remainder}"
