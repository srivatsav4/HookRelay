from app.security import signing

SECRET, BODY, TS = "whsec_unit_test_secret_value", b'{"id":"e1","data":{"amount":10}}', 1_700_000_000


def test_sign_is_deterministic_and_versioned():
    s = signing.sign(SECRET, TS, BODY)
    assert s == signing.sign(SECRET, TS, BODY)
    assert s.startswith("v1=") and len(s) == 3 + 64


def test_signature_covers_timestamp_and_body():
    base = signing.sign(SECRET, TS, BODY)
    assert signing.sign(SECRET, TS + 1, BODY) != base
    assert signing.sign(SECRET, TS, BODY + b" ") != base
    assert signing.sign("other-secret-value-xx", TS, BODY) != base


def test_verify_accepts_valid():
    assert signing.verify(SECRET, TS, BODY, signing.sign(SECRET, TS, BODY), now=TS + 10)


def test_verify_rejects_tampered_body_wrong_secret_bad_format():
    sig = signing.sign(SECRET, TS, BODY)
    assert not signing.verify(SECRET, TS, BODY.replace(b"10", b"99"), sig, now=TS)
    assert not signing.verify("wrong-secret-wrong-secret", TS, BODY, sig, now=TS)
    assert not signing.verify(SECRET, TS, BODY, "", now=TS)
    assert not signing.verify(SECRET, "not-a-number", BODY, sig, now=TS)


def test_verify_rejects_stale_and_future_timestamps():
    sig = signing.sign(SECRET, TS, BODY)
    assert not signing.verify(SECRET, TS, BODY, sig, tolerance_s=300, now=TS + 301)
    assert not signing.verify(SECRET, TS, BODY, sig, tolerance_s=300, now=TS - 301)
    assert signing.verify(SECRET, TS, BODY, sig, tolerance_s=300, now=TS + 300)


def test_body_is_canonical():
    a = signing.build_body("e", "t", "2026-01-01T00:00:00+00:00", {"b": 1, "a": 2})
    b = signing.build_body("e", "t", "2026-01-01T00:00:00+00:00", {"a": 2, "b": 1})
    assert a == b
