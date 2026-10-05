"""Configurable mock webhook receiver for demos, tests and the Reliability Lab.

Per-bucket behaviour (POST /config/{bucket}):
  behavior: ok | status | slow | timeout | flaky     status: code for 'status'/'flaky'
  delay_ms, fail_rate, fail_for_s (fault auto-heals after N seconds), retry_after, verify_secret
Webhook endpoint: POST /hook/{bucket}. Stats: GET /stats/{bucket}.
"""
import asyncio
import hashlib
import hmac
import random
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="HookRelay mock receiver")
CFG: dict[str, dict] = {}
STATS: dict[str, dict] = {}


def _stats(b: str) -> dict:
    return STATS.setdefault(b, {"received": 0, "ok": 0, "failed": 0, "signature_valid": 0, "signature_invalid": 0,
                                "unique_event_ids": set(), "duplicate_event_ids": 0})


def _verify(secret: str, ts: str, body: bytes, sig: str, tol: int = 300) -> bool:
    try:
        t = int(ts)
    except (TypeError, ValueError):
        return False
    if abs(time.time() - t) > tol:
        return False
    mac = hmac.new(secret.encode(), f"{t}.".encode() + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(f"v1={mac}", sig or "")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/config/{bucket}")
async def configure(bucket: str, request: Request):
    cfg = await request.json()
    cfg.setdefault("behavior", "ok")
    cfg["since"] = time.monotonic()
    CFG[bucket] = cfg
    STATS[bucket] = {k: (set() if isinstance(v, set) else 0) for k, v in _stats(bucket).items()}
    return {"bucket": bucket, "config": cfg}


@app.get("/stats/{bucket}")
async def stats(bucket: str):
    s = dict(_stats(bucket))
    s["unique_event_ids"] = len(s["unique_event_ids"])
    return s


@app.post("/hook/{bucket}")
async def hook(bucket: str, request: Request):
    cfg, st = CFG.get(bucket, {"behavior": "ok", "since": time.monotonic()}), _stats(bucket)
    body = await request.body()
    st["received"] += 1
    if cfg.get("verify_secret"):
        h = request.headers
        if not _verify(cfg["verify_secret"], h.get("x-hookrelay-timestamp", ""), body, h.get("x-hookrelay-signature", "")):
            st["signature_invalid"] += 1
            st["failed"] += 1
            return JSONResponse({"error": "invalid signature"}, status_code=401)
        st["signature_valid"] += 1
    eid = request.headers.get("x-hookrelay-event-id")
    if eid:
        if eid in st["unique_event_ids"]:
            st["duplicate_event_ids"] += 1  # at-least-once: receivers must dedupe on event id
        st["unique_event_ids"].add(eid)
    faulty = cfg["behavior"] != "ok" and (cfg.get("fail_for_s") is None or time.monotonic() - cfg["since"] < cfg["fail_for_s"])
    if faulty:
        b = cfg["behavior"]
        if b == "slow":
            await asyncio.sleep(cfg.get("delay_ms", 1000) / 1000)
        elif b == "timeout":
            await asyncio.sleep(60)
        elif b == "status" or (b == "flaky" and random.random() < cfg.get("fail_rate", 0.3)):
            st["failed"] += 1
            hdr = {"Retry-After": str(cfg["retry_after"])} if cfg.get("retry_after") else {}
            return JSONResponse({"error": "injected"}, status_code=int(cfg.get("status", 500)), headers=hdr)
    st["ok"] += 1
    return {"received": True}
