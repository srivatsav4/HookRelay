from app.security.hashing import (generate_api_key, hash_api_key, hash_password, parse_api_key, verify_api_key,
                                  verify_password)

PEPPER = "unit-test-pepper-0123456789"


def test_api_key_roundtrip_and_no_plaintext():
    raw, prefix = generate_api_key()
    assert parse_api_key(raw) == prefix
    h = hash_api_key(raw, PEPPER)
    assert raw not in h and len(h) == 64
    assert verify_api_key(raw, h, PEPPER)
    assert not verify_api_key(raw + "x", h, PEPPER)
    assert not verify_api_key(raw, h, "different-pepper-0123456789")


def test_keys_are_unique_and_high_entropy():
    assert len({generate_api_key()[0] for _ in range(200)}) == 200


def test_parse_rejects_garbage():
    for bad in ("", "abc", "hr_short_x", "xx_12345678_secret", "hr_12345678_"):
        assert parse_api_key(bad) is None


def test_password_hash_salted_and_verified():
    a, b = hash_password("correct horse"), hash_password("correct horse")
    assert a != b and verify_password("correct horse", a)
    assert not verify_password("wrong", a) and not verify_password("x", "garbage")
