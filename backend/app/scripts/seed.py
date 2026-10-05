"""Idempotent demo seed. Creates a demo account/user/API key/endpoints and pushes sample events through the REAL
pipeline, so every delivery attempt shown in the dashboard was produced by the actual worker."""
from __future__ import annotations

import asyncio
import os
import uuid

from sqlalchemy import select

from app.core.config import get_settings
from app.db.session import make_engine, make_sessionmaker
from app.models import Account, ApiKey, Endpoint, User
from app.security.hashing import generate_api_key, hash_api_key, hash_password, parse_api_key
from app.services.ingestion import ingest_event
from app.services.queue import Queue

DEMO_EMAIL = os.environ.get("DEMO_USER_EMAIL", "demo@hookrelay.local")
DEMO_PASSWORD = os.environ.get("DEMO_USER_PASSWORD", "demo-password")


async def main() -> None:
    st = get_settings()
    engine = make_engine(st.database_url)
    sf, queue = make_sessionmaker(engine), Queue(st.redis_url)
    base = st.mock_receiver_url.rstrip("/")
    async with sf() as s, s.begin():
        acc = (await s.execute(select(Account).where(Account.name == "demo"))).scalar_one_or_none()
        if acc is None:
            acc = Account(id=uuid.uuid4(), name="demo")
            s.add(acc)
            await s.flush()
            s.add(User(account_id=acc.id, email=DEMO_EMAIL, password_hash=hash_password(DEMO_PASSWORD), is_admin=True))
            raw = os.environ.get("DEMO_API_KEY") or generate_api_key()[0]
            prefix = parse_api_key(raw)
            if not prefix:
                raise SystemExit("DEMO_API_KEY must look like hr_<8 hex>_<secret>")
            s.add(ApiKey(account_id=acc.id, prefix=prefix, key_hash=hash_api_key(raw, st.api_key_pepper), name="demo"))
            for name, bucket, extra in (("healthy receiver", "demo-ok", {}), ("flaky receiver (30% 500s)", "demo-flaky", {}),
                                        ("rate-limited receiver (2/s)", "demo-limited", {"rate_limit_per_sec": 2.0})):
                s.add(Endpoint(account_id=acc.id, url=f"{base}/hook/{bucket}", description=name,
                               signing_secret="whsec_demo_" + uuid.uuid4().hex, event_types=["*"],
                               retry_schedule=[2, 5, 10, 20, 40], timeout_s=3, **extra))
            print(f"[seed] created demo account. login: {DEMO_EMAIL} / {DEMO_PASSWORD}")
            print(f"[seed] API key (shown once; only its hash is stored): {raw}")
            fresh = True
        else:
            print("[seed] demo account already exists; leaving it as is")
            fresh = False
    if fresh:
        import httpx
        async with httpx.AsyncClient(timeout=5) as c:  # configure demo receiver buckets
            await c.post(f"{base}/config/demo-ok", json={"behavior": "ok"})
            await c.post(f"{base}/config/demo-flaky", json={"behavior": "flaky", "status": 500, "fail_rate": 0.3})
            await c.post(f"{base}/config/demo-limited", json={"behavior": "ok"})
        for i in range(15):
            await ingest_event(sf, queue, st, account_id=acc.id, idempotency_key=f"seed-{i}", event_type="order.created",
                               payload={"order_id": 1000 + i, "amount": 10 + i})
        print("[seed] ingested 15 sample events through the real pipeline")
    await queue.close()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
