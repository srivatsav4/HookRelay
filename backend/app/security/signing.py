"""HMAC-SHA256 webhook signing: signature = HMAC(secret, f"{timestamp}.{body}"). Pure stdlib."""
from __future__ import annotations

import hashlib
import hmac
import json
import time

TIMESTAMP_HEADER = "X-HookRelay-Timestamp"
SIGNATURE_HEADER = "X-HookRelay-Signature"
SIGNATURE_VERSION = "v1"
DEFAULT_TOLERANCE_S = 300


def sign(secret: str, timestamp: int, body: bytes) -> str:
    mac = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    return f"{SIGNATURE_VERSION}={mac}"


def verify(secret: str, timestamp: str | int, body: bytes, signature: str,
           tolerance_s: int = DEFAULT_TOLERANCE_S, now: float | None = None) -> bool:
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return False
    if abs((time.time() if now is None else now) - ts) > tolerance_s:
        return False  # replay protection
    return hmac.compare_digest(sign(secret, ts, body), signature or "")


def build_body(event_id: str, event_type: str, created_at_iso: str, data: dict) -> bytes:
    """Canonical, attempt-independent body. Receivers dedupe on `id` (at-least-once delivery)."""
    return json.dumps({"id": event_id, "type": event_type, "created_at": created_at_iso, "data": data},
                      separators=(",", ":"), sort_keys=True).encode()
