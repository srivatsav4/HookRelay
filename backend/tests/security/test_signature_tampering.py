from app.security import signing


def test_tampered_payload_fails_verification():
    secret, ts = "whsec_tamper_test_secret_xx", 1_800_000_000
    body = signing.build_body("evt_1", "payment.created", "2026-01-01T00:00:00+00:00", {"amount": 100, "to": "alice"})
    sig = signing.sign(secret, ts, body)
    assert signing.verify(secret, ts, body, sig, now=ts)
    for tampered in (body.replace(b"100", b"999"), body.replace(b"alice", b"mallory"), body + b"\n", body[:-1]):
        assert not signing.verify(secret, ts, tampered, sig, now=ts)


def test_replayed_old_message_rejected():
    secret, body = "whsec_replay_test_secret_xx", b"{}"
    sig = signing.sign(secret, 1000, body)
    assert not signing.verify(secret, 1000, body, sig, now=1000 + 3600)
