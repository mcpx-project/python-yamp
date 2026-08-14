from yamp import jsonrpc


def test_request_round_trip():
    msg = jsonrpc.request(7, "tools/list", {"cursor": "x"})
    assert jsonrpc.decode(jsonrpc.encode(msg)) == msg
    assert jsonrpc.method_of(msg) == "tools/list"


def test_request_without_params_omits_key():
    assert "params" not in jsonrpc.request(1, "ping")


def test_notification_and_method_of():
    note = jsonrpc.notification("notifications/initialized")
    assert "id" not in note
    assert jsonrpc.method_of(note) == "notifications/initialized"
    assert jsonrpc.method_of({"id": 1, "result": {}}) is None


def test_notification_with_params():
    note = jsonrpc.notification("notifications/progress", {"progress": 1})
    assert note["params"] == {"progress": 1}


def test_result_and_error_shapes():
    assert jsonrpc.result(1, {"ok": True}) == {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {"ok": True},
    }
    err = jsonrpc.error(2, -32600, "bad")
    assert err["error"] == {"code": -32600, "message": "bad"}
