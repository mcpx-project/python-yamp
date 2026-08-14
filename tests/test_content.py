"""Typed content-block iterator (ε1): traversal, decode, write-back."""

import base64

from yamp import content


def _tool_result():
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "content": [
                {"type": "text", "text": "hello"},
                {"type": "image", "data": base64.b64encode(b"\x89PNG").decode(), "mimeType": "image/png"},
                {"type": "resource_link", "uri": "file:///a.txt", "mimeType": "text/plain"},
                {"type": "resource", "resource": {"uri": "file:///b.bin", "mimeType": "application/octet-stream", "blob": base64.b64encode(bytes([0, 1, 2])).decode()}},
                {"type": "widget", "foo": 1},
            ]
        },
    }


def test_traversal_normalizes_every_block():
    blocks = content.blocks(_tool_result())
    assert len(blocks) == 5

    assert blocks[0]["kind"] == "text" and blocks[0]["text"] == "hello"
    assert blocks[0]["path"] == ["result", "content", 0, "text"]

    assert blocks[1]["kind"] == "image" and blocks[1]["mime"] == "image/png"
    assert blocks[1]["bytes"] == b"\x89PNG".hex()
    assert blocks[1]["path"] == ["result", "content", 1, "data"]

    assert blocks[2]["kind"] == "resource_link" and blocks[2]["uri"] == "file:///a.txt"

    assert blocks[3]["kind"] == "resource" and blocks[3]["bytes"] == bytes([0, 1, 2]).hex()
    assert blocks[3]["path"] == ["result", "content", 3, "resource", "blob"]

    assert blocks[4]["kind"] == "unknown"


def test_resources_read_contents_are_covered():
    message = {
        "jsonrpc": "2.0",
        "id": 2,
        "result": {
            "contents": [
                {"uri": "file:///c.txt", "mimeType": "text/plain", "text": "doc"},
                {"uri": "file:///d.bin", "mimeType": "application/octet-stream", "blob": base64.b64encode(bytes([9, 9])).decode()},
            ]
        },
    }
    blocks = content.blocks(message)
    assert blocks[0]["kind"] == "resource" and blocks[0]["text"] == "doc"
    assert blocks[1]["bytes"] == bytes([9, 9]).hex()
    assert blocks[1]["path"] == ["result", "contents", 1, "blob"]


def test_no_content_yields_empty():
    assert content.blocks({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "t"}}) == []


def test_invalid_base64_surfaces_null_bytes():
    message = {"result": {"content": [{"type": "image", "data": "!!!not base64!!!", "mimeType": "image/png"}]}}
    assert content.blocks(message)[0]["bytes"] is None


def test_set_text_writes_back_at_path():
    message = _tool_result()
    out = content.set_text(message, ["result", "content", 0, "text"], "[redacted]")
    assert out["result"]["content"][0]["text"] == "[redacted]"
    assert message["result"]["content"][0]["text"] == "hello", "input untouched"


def test_set_bytes_reencodes_at_path():
    message = _tool_result()
    out = content.set_bytes(message, ["result", "content", 1, "data"], b"NEW")
    encoded = out["result"]["content"][1]["data"]
    assert base64.b64decode(encoded) == b"NEW"


def test_navigate_missing_path_is_a_noop():
    message = _tool_result()
    out = content.set_text(message, ["result", "content", 99, "text"], "x")
    assert out == message


def test_embedded_resource_text_and_nonstring_blob():
    message = {
        "result": {
            "content": [
                {"type": "resource", "resource": {"uri": "file:///t", "mimeType": "text/plain", "text": "inline"}},
                {"type": "image", "data": 123, "mimeType": "image/png"},
            ]
        }
    }
    blocks = content.blocks(message)
    assert blocks[0]["kind"] == "resource" and blocks[0]["text"] == "inline"
    assert blocks[0]["path"] == ["result", "content", 0, "resource", "text"]
    assert blocks[1]["bytes"] is None  # non-string data decodes to nothing


def test_navigate_leaf_int_index_and_missing_leaf():
    out = content.set_text({"items": ["a", "b", "c"]}, ["items", 1], "B")
    assert out["items"][1] == "B"
    # A leaf key that is not present writes nothing.
    assert content.set_text({"d": {}}, ["d", "missing"], "x") == {"d": {}}
