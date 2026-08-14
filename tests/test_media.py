from yamp import media


def test_prefers_mcp_json_when_accepted_explicitly():
    assert media.response_content_type("application/mcp+json") == media.MCP_JSON
    assert media.response_content_type("application/json, application/mcp+json") == media.MCP_JSON


def test_wildcards_get_mcp_json():
    assert media.response_content_type("*/*") == media.MCP_JSON
    assert media.response_content_type("application/*") == media.MCP_JSON
    assert media.response_content_type("text/html, */*;q=0.8") == media.MCP_JSON


def test_falls_back_to_json():
    assert media.response_content_type("application/json") == media.JSON
    assert media.response_content_type("text/plain") == media.JSON
    assert media.response_content_type(None) == media.JSON
    assert media.response_content_type("") == media.JSON


def test_ignores_accept_parameters():
    assert media.response_content_type("application/mcp+json;q=0.9") == media.MCP_JSON


def test_is_mcp_json():
    assert media.is_mcp_json("application/mcp+json")
    assert media.is_mcp_json("application/mcp+json; charset=utf-8")
    assert not media.is_mcp_json("application/json")
    assert not media.is_mcp_json(None)
    assert not media.is_mcp_json("")
