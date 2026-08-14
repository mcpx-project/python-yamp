"""Unit tests for the server-variants module (SEP-2053).

Mirrors the Rust arm's tests/variants.rs.
"""

from yamp import variants

EXT = variants.EXTENSION_ID
KEY = variants.SERVER_VARIANT_META_KEY


def _caps(*ids):
    return {"extensions": {EXT: {"availableVariants": [{"id": i, "description": f"{i} variant"} for i in ids]}}}


def test_selected_variant_reads_meta_key():
    assert variants.selected_variant({"_meta": {KEY: "compact"}}) == "compact"


def test_selected_variant_absent_forms():
    assert variants.selected_variant(None) is None
    assert variants.selected_variant({}) is None
    assert variants.selected_variant({"_meta": {}}) is None
    assert variants.selected_variant({"_meta": {KEY: 7}}) is None  # non-string ignored


def test_available_variants_declared_order():
    assert variants.available_variants(_caps("a", "b", "c")) == ["a", "b", "c"]


def test_available_variants_missing_or_malformed():
    assert variants.available_variants(None) == []
    assert variants.available_variants({}) == []
    assert variants.available_variants({"extensions": {EXT: {}}}) == []  # no availableVariants
    assert variants.available_variants({"extensions": {EXT: {"availableVariants": "x"}}}) == []
    assert variants.available_variants({"extensions": {EXT: {"availableVariants": [{"noid": 1}]}}}) == []


def test_compose_variants_intersects_across_supporters():
    # b1 offers a,b,c; b2 offers b,c,d; a variant-agnostic backend imposes nothing.
    composed = variants.compose_variants([_caps("a", "b", "c"), _caps("b", "c", "d"), {}])
    assert [v["id"] for v in composed] == ["b", "c"]  # order from the first supporter


def test_compose_variants_none_supported():
    assert variants.compose_variants([{}, {"capabilities": {}}]) == []


def test_compose_variants_empty_intersection():
    assert variants.compose_variants([_caps("a"), _caps("b")]) == []


def test_compose_variants_single_supporter_keeps_default_first():
    composed = variants.compose_variants([_caps("claude", "compact"), {}])
    assert [v["id"] for v in composed] == ["claude", "compact"]


def test_cursor_round_trips_variant_and_backends():
    cursor = variants.bind_cursor("compact", {"b0": "p2", "b1": "p5"})
    assert cursor.startswith(variants.CURSOR_PREFIX)
    variant, cursors = variants.resolve_cursor(cursor)
    assert variant == "compact"
    assert cursors == {"b0": "p2", "b1": "p5"}


def test_cursor_binds_default_none_variant():
    variant, cursors = variants.resolve_cursor(variants.bind_cursor(None, {"b0": "p1"}))
    assert variant is None
    assert cursors == {"b0": "p1"}


def test_resolve_cursor_rejects_non_proxy_values():
    assert variants.resolve_cursor(None) is None
    assert variants.resolve_cursor(42) is None
    assert variants.resolve_cursor("raw-backend-cursor") is None  # no proxy prefix


def test_resolve_cursor_rejects_malformed_payloads():
    assert variants.resolve_cursor(variants.CURSOR_PREFIX + "zz") is None  # bad hex
    assert variants.resolve_cursor(variants.CURSOR_PREFIX + "6e6f74206a736f6e") is None  # "not json"
    assert variants.resolve_cursor(variants.CURSOR_PREFIX + b'{"c":"x"}'.hex()) is None  # c not a dict
    assert variants.resolve_cursor(variants.CURSOR_PREFIX + b'[1,2]'.hex()) is None  # not an object


def test_resolve_cursor_filters_non_string_entries_and_variant():
    raw = b'{"v":9,"c":{"b0":"p1","b1":5}}'.hex()
    variant, cursors = variants.resolve_cursor(variants.CURSOR_PREFIX + raw)
    assert variant is None  # non-string variant normalized to None
    assert cursors == {"b0": "p1"}  # non-string cursor dropped


def test_mismatch_data_shape():
    assert variants.mismatch_data("claude", "compact") == {"cursorVariant": "claude", "requestedVariant": "compact"}
