from yamp import signing


def test_canonical_is_key_order_independent():
    a = signing.canonical({"b": 2, "a": 1, "nested": {"y": 1, "x": 2}})
    b = signing.canonical({"a": 1, "nested": {"x": 2, "y": 1}, "b": 2})
    assert a == b  # sorted keys at every level
    assert a == b'{"a":1,"b":2,"nested":{"x":2,"y":1}}'  # compact, deterministic


def test_signature_verifies_and_detects_tamper():
    record = signing.outcome_record("tools/call", "gh__x", True)
    sig = signing.sign("secret", record)
    assert signing.verify("secret", record, sig)
    assert not signing.verify("secret", {**record, "ok": False}, sig)  # tampered record
    assert not signing.verify("other-secret", record, sig)  # wrong key


def test_hash_chain_links_records():
    log = signing.AuditLog("k")
    first = log.append(signing.attestation_record("alice", "tools/call", "gh__x"))
    second = log.append(signing.outcome_record("tools/call", "gh__x", True))
    assert first["prev"] == signing.GENESIS
    assert second["prev"] == first["hash"]  # each record links to the previous
    assert log.verify()


def test_verify_fails_on_broken_chain():
    log = signing.AuditLog("k")
    log.append(signing.outcome_record("tools/call", "a", True))
    log.append(signing.outcome_record("tools/call", "b", True))
    assert log.verify()
    log.records[1]["record"]["ok"] = False  # tamper after the fact
    assert not log.verify()
    log.records[1]["record"]["ok"] = True
    log.records[1]["prev"] = "deadbeefdeadbeef"  # break the chain link
    assert not log.verify()
    # Signature and prev intact, but the stored hash is wrong.
    log2 = signing.AuditLog("k")
    log2.append(signing.outcome_record("tools/call", "a", True))
    log2.records[0]["hash"] = "ffffffffffffffff"
    assert not log2.verify()
