"""Reference receiver-side verification + deduplication for HookRelay webhooks (stdlib only).

    python examples/verify_signature.py        # runs a self-test

Rules: (1) recompute HMAC-SHA256 over f"{timestamp}.{raw_body}" with your signing secret,
(2) compare in constant time, (3) reject timestamps outside a tolerance window (replay),
(4) deduplicate on the event id (delivery is AT-LEAST-ONCE, so duplicates will happen).
"""
import hashlib
import hmac
import json
import time

SEEN: set[str] = set()  # use a durable store with a unique constraint in production


def verify(secret: str, headers: dict, raw_body: bytes, tolerance_s: int = 300) -> bool:
    ts, sig = headers.get("X-HookRelay-Timestamp", ""), headers.get("X-HookRelay-Signature", "")
    if not ts.isdigit() or abs(time.time() - int(ts)) > tolerance_s:
        return False
    expected = "v1=" + hmac.new(secret.encode(), f"{ts}.".encode() + raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig)


def handle(secret: str, headers: dict, raw_body: bytes) -> tuple[int, str]:
    if not verify(secret, headers, raw_body):
        return 401, "bad signature"
    event_id = json.loads(raw_body)["id"]
    if event_id in SEEN:
        return 200, "duplicate ignored"  # still 2xx so HookRelay stops retrying
    SEEN.add(event_id)
    return 200, "processed"


if __name__ == "__main__":
    secret, body, ts = "whsec_example_only", b'{"id":"evt_1","type":"t","data":{}}', str(int(time.time()))
    hdr = {"X-HookRelay-Timestamp": ts, "X-HookRelay-Signature": "v1=" + hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()}
    print(handle(secret, hdr, body), handle(secret, hdr, body), handle(secret, hdr, body + b" "))
