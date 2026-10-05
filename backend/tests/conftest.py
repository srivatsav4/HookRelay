"""Shared fixtures. Integration/concurrency/reliability tests use REAL PostgreSQL and Redis.

They are skipped automatically unless DATABASE_URL and REDIS_URL are set (see `make test-integration`).
"""
from __future__ import annotations

import os
import uuid
from dataclasses import replace

import httpx
import pytest
import pytest_asyncio

os.environ.setdefault("JWT_SECRET", "test-jwt-secret-0123456789")
os.environ.setdefault("API_KEY_PEPPER", "test-pepper-0123456789")
HAVE_SERVICES = bool(os.environ.get("DATABASE_URL") and os.environ.get("REDIS_URL"))

if HAVE_SERVICES:
    import redis.asyncio as aioredis
    from sqlalchemy import text

    from app.core.config import load_settings
    from app.db.base import Base
    from app.db.session import make_engine, make_sessionmaker
    import app.models  # noqa: F401
    from app.models import Account, ApiKey, Endpoint
    from app.reliability.chaos import chaos
    from app.security.hashing import generate_api_key, hash_api_key
    from app.services.delivery import Runtime
    from app.services.queue import Queue


def pytest_collection_modifyitems(config, items):
    if HAVE_SERVICES:
        return
    skip = pytest.mark.skip(reason="needs DATABASE_URL and REDIS_URL (real PostgreSQL + Redis)")
    for it in items:
        if any(d in str(it.fspath) for d in ("integration", "concurrency", "reliability")):
            it.add_marker(skip)


@pytest_asyncio.fixture(scope="session")
async def engine():
    if not HAVE_SERVICES:
        pytest.skip("no services")
    eng = make_engine(os.environ["DATABASE_URL"], pool_size=20, max_overflow=20)
    async with eng.begin() as c:
        await c.run_sync(Base.metadata.drop_all)
        await c.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def env(engine):
    """Clean DB + Redis per test; settings tuned for fast, deterministic tests."""
    async with engine.begin() as c:
        await c.execute(text("TRUNCATE accounts RESTART IDENTITY CASCADE"))
    rds = aioredis.from_url(os.environ["REDIS_URL"])
    await rds.flushdb()
    chaos.unblock_redis()
    settings = replace(load_settings(), allow_private_destinations=True, retry_schedule=(1, 1, 1), retry_jitter=0.0,
                       cb_failure_threshold=3, cb_cooldown_s=2, recovery_pending_grace_s=0, recovery_queued_grace_s=0,
                       visibility_timeout_s=1, tenant_max_concurrency=50)
    queue = Queue(os.environ["REDIS_URL"])
    yield Env(make_sessionmaker(engine), queue, rds, settings)
    chaos.unblock_redis()
    await queue.close()
    await rds.aclose()


class Env:
    def __init__(self, sf, queue, rds, settings):
        self.sf, self.queue, self.rds, self.settings = sf, queue, rds, settings
        self.calls: list[httpx.Request] = []
        self.handler = lambda req: httpx.Response(200)

    def runtime(self, worker_id="w-test", handler=None) -> "Runtime":
        h = handler or self._dispatch
        return Runtime(self.sf, self.queue, self.rds, httpx.AsyncClient(transport=httpx.MockTransport(h)), self.settings, worker_id)

    async def _dispatch(self, req: httpx.Request) -> httpx.Response:
        self.calls.append(req)
        r = self.handler(req)
        return await r if hasattr(r, "__await__") else r

    async def account(self, name=None):
        raw, prefix = generate_api_key()
        acc = Account(id=uuid.uuid4(), name=name or f"acc-{uuid.uuid4().hex[:6]}")
        async with self.sf() as s, s.begin():
            s.add(acc)
            await s.flush()
            s.add(ApiKey(account_id=acc.id, prefix=prefix, key_hash=hash_api_key(raw, self.settings.api_key_pepper)))
        return acc.id, raw

    async def endpoint(self, account_id, url="http://receiver.test/hook", **kw):
        ep = Endpoint(id=uuid.uuid4(), account_id=account_id, url=url, signing_secret="whsec_test_secret_0123456789",
                      event_types=kw.pop("event_types", ["*"]), retry_schedule=kw.pop("retry_schedule", [1, 1, 1]), **kw)
        async with self.sf() as s, s.begin():
            s.add(ep)
        return ep


async def rewind(env_, ids=None):
    """Make every RETRYING delivery due now so tests don't sleep through backoff."""
    from sqlalchemy import update
    from app.models import Delivery, utcnow
    async with env_.sf() as s, s.begin():
        q = update(Delivery).where(Delivery.status == "RETRYING").values(next_attempt_at=utcnow())
        if ids:
            q = q.where(Delivery.id.in_(ids))
        await s.execute(q)


async def get_delivery(env_, did):
    from app.models import Delivery
    async with env_.sf() as s:
        return await s.get(Delivery, did)


async def attempts_of(env_, did):
    from sqlalchemy import select
    from app.models import DeliveryAttempt
    async with env_.sf() as s:
        return (await s.execute(select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == did)
                                .order_by(DeliveryAttempt.attempt_number))).scalars().all()
