"""Namespace collision resolution strategies (SEP §3.4).

The drafts require an intermediary to implement at least one strategy and to
declare the active one. ``prefix`` (the required strategy) namespaces every tool
as ``backend__tool`` so distinct backends never collide, and is applied by the
router's labeling. This module adds the other three:

- ``priority``: names stay prefixed, but when the same original tool name is
  offered by several backends, only the highest-priority backend's version is
  kept; the rest are discarded and logged.
- ``manual``: explicit per-tool renames; two tools that would map to the same
  exposed name are an unresolved collision and are rejected at startup.
- ``passthrough``: no resolution, original names, collisions kept as duplicates.
  NOT RECOMMENDED.
"""

from __future__ import annotations

from typing import Callable

PREFIX = "prefix"
PRIORITY = "priority"
MANUAL = "manual"
PASSTHROUGH = "passthrough"
STRATEGIES = frozenset({PREFIX, PRIORITY, MANUAL, PASSTHROUGH})


class UnresolvedCollision(Exception):
    """Raised when a ``manual`` strategy leaves two tools sharing an exposed name."""


def apply_priority(
    tools: list[dict],
    split: Callable[[str], tuple[str, str] | None],
    priority: list[str],
    on_discard: Callable[[dict], None] | None = None,
    field: str = "name",
) -> list[dict]:
    """Keep the highest-priority backend's copy of each original name.

    ``priority`` lists backend ids highest-first; a backend not listed ranks
    below every listed one. Discarded tools are passed to ``on_discard``. Input
    order is otherwise preserved.
    """
    rank = {backend_id: index for index, backend_id in enumerate(priority)}

    def backend_of(value: str) -> str | None:
        resolved = split(value)
        return resolved[0] if resolved else None

    best: dict[str, int] = {}  # original name -> rank of the kept copy
    kept: dict[str, dict] = {}
    order: list[str] = []
    for tool in tools:
        resolved = split(tool[field])
        original = resolved[1] if resolved else tool[field]
        this_rank = rank.get(backend_of(tool[field]), len(priority))
        if original not in best:
            best[original] = this_rank
            kept[original] = tool
            order.append(original)
        elif this_rank < best[original]:
            if on_discard is not None:
                on_discard(kept[original])
            best[original] = this_rank
            kept[original] = tool
        elif on_discard is not None:
            on_discard(tool)
    return [kept[original] for original in order]


def resolve_manual(names: list[str], overrides: dict[str, str]) -> dict[str, str]:
    """Map each namespaced name to its exposed name, applying ``overrides``.

    Returns ``{namespaced: exposed}``. Raises :class:`UnresolvedCollision` if two
    names map to the same exposed name (SEP §3.4: manual MUST reject startup on
    unresolved collisions).
    """
    exposed: dict[str, str] = {}
    seen: dict[str, str] = {}  # exposed name -> the namespaced name that claimed it
    for name in names:
        target = overrides.get(name, name)
        if target in seen and seen[target] != name:
            raise UnresolvedCollision(
                f"{name!r} and {seen[target]!r} both map to {target!r}"
            )
        seen[target] = name
        exposed[name] = target
    return exposed
