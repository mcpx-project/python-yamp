import pytest

from yamp import collision, namespace


def test_strategy_constants():
    assert collision.STRATEGIES == {"prefix", "priority", "manual", "passthrough"}


def test_apply_priority_keeps_highest_and_discards_rest():
    tools = [
        {"name": "gh__search"},
        {"name": "gl__search"},
        {"name": "gh__only_gh"},
        {"name": "gl__only_gl"},
    ]
    discarded = []
    kept = collision.apply_priority(
        tools, namespace.split, priority=["gh", "gl"], on_discard=discarded.append
    )
    names = {t["name"] for t in kept}
    # 'search' collides: gh (higher priority) wins, gl is dropped.
    assert names == {"gh__search", "gh__only_gh", "gl__only_gl"}
    assert discarded == [{"name": "gl__search"}]


def test_apply_priority_lower_priority_first_still_keeps_higher():
    # Encounter the lower-priority copy first; the higher one must still win and
    # the earlier one is discarded.
    tools = [{"name": "gl__search"}, {"name": "gh__search"}]
    discarded = []
    kept = collision.apply_priority(
        tools, namespace.split, priority=["gh", "gl"], on_discard=discarded.append
    )
    assert [t["name"] for t in kept] == ["gh__search"]
    assert discarded == [{"name": "gl__search"}]


def test_apply_priority_unlisted_backend_ranks_lowest():
    tools = [{"name": "unlisted__t"}, {"name": "gh__t"}]
    kept = collision.apply_priority(tools, namespace.split, priority=["gh"])
    assert [t["name"] for t in kept] == ["gh__t"]


def test_resolve_manual_applies_overrides():
    mapping = collision.resolve_manual(
        ["github__create_issue", "gh__search"],
        overrides={"github__create_issue": "gh_new_issue"},
    )
    assert mapping == {"github__create_issue": "gh_new_issue", "gh__search": "gh__search"}


def test_resolve_manual_rejects_unresolved_collision():
    # Two distinct namespaced names renamed to the same exposed name.
    with pytest.raises(collision.UnresolvedCollision):
        collision.resolve_manual(
            ["gh__x", "gl__y"],
            overrides={"gh__x": "shared", "gl__y": "shared"},
        )
