import asyncio
from datetime import timedelta

import httpx
import pytest
from sqlalchemy import select, update

from app.models import AuditLog, Delivery, utcnow
from app.reliability.chaos import chaos
from app.services.delivery import process_delivery
from app.services.ingestion import ingest_event
from app.services.recovery import run_recovery
from tests.conftest import get_delivery

pytestmark = pytest.mark.asyncio


async def test_invariant_E_redis_outage_pending_work_is_recovered_from_postgres(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    chaos.block_redis(600)
    res = [await ingest_event(env.sf, env.queue, env.settings, account_id=acc, idempotency_key=f"k{i}", event_type="t", payload={}) for i in range(5)]
    ids = [r.delivery_ids[0] for r in res]
    assert all((await get_delivery(env, i)).status == "PENDING" for i in ids) and await env.rds.zcard("arq:queue") == 0
    chaos.unblock_redis()
    rep = await run_recovery(env.sf, env.queue, env.settings)
    assert rep.pending_reenqueued == 5
    assert await env.rds.zcard("arq:queue") == 5
    assert all((await get_delivery(env, i)).status == "QUEUED" for i in ids)
    rt = env.runtime()
    assert [await process_delivery(rt, i) for i in ids] == ["success"] * 5
    async with env.sf() as s:
        n = (await s.execute(select(AuditLog).where(AuditLog.action == "RECOVERY_REENQUEUE"))).scalars().all()
    assert len(n) == 5


async def test_recovery_is_idempotent_no_duplicate_jobs(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    chaos.block_redis(600)
    await ingest_event(env.sf, env.queue, env.settings, account_id=acc, idempotency_key="k", event_type="t", payload={})
    chaos.unblock_redis()
    await run_recovery(env.sf, env.queue, env.settings)
    again = await run_recovery(env.sf, env.queue, env.settings)
    assert again.pending_reenqueued == 0 and await env.rds.zcard("arq:queue") == 1


async def test_redis_flushed_while_queued_work_is_rebuilt(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    for i in range(3):
        await ingest_event(env.sf, env.queue, env.settings, account_id=acc, idempotency_key=f"k{i}", event_type="t", payload={})
    assert await env.rds.zcard("arq:queue") == 3
    await env.rds.flushdb()  # Redis lost everything
    await asyncio.sleep(0.05)
    rep = await run_recovery(env.sf, env.queue, env.settings)
    assert rep.queued_reenqueued == 3 and await env.rds.zcard("arq:queue") == 3


async def test_worker_crash_stale_processing_is_reclaimed_and_delivered(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    did = (await ingest_event(env.sf, env.queue, env.settings, account_id=acc, idempotency_key="k", event_type="t", payload={})).delivery_ids[0]
    async with env.sf() as s, s.begin():  # a worker claimed it and died
        await s.execute(update(Delivery).where(Delivery.id == did).values(
            status="PROCESSING", locked_by="dead-worker", locked_at=utcnow() - timedelta(seconds=30), attempt_count=1))
    await env.rds.flushdb()
    rep = await run_recovery(env.sf, env.queue, env.settings)
    assert rep.stale_processing_reset == 1 and rep.retrying_reenqueued == 1
    assert (await get_delivery(env, did)).status == "RETRYING"
    env.handler = lambda req: httpx.Response(200)
    assert await process_delivery(env.runtime(), did) == "success"
    assert (await get_delivery(env, did)).attempt_count == 2


async def test_stale_processing_with_exhausted_attempts_goes_dead(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    did = (await ingest_event(env.sf, env.queue, env.settings, account_id=acc, idempotency_key="k", event_type="t", payload={})).delivery_ids[0]
    async with env.sf() as s, s.begin():
        d = await s.get(Delivery, did)
        d.status, d.locked_at, d.attempt_count = "PROCESSING", utcnow() - timedelta(seconds=30), d.max_attempts
    rep = await run_recovery(env.sf, env.queue, env.settings)
    assert rep.stale_processing_dead == 1 and (await get_delivery(env, did)).status == "DEAD"
