"""End-to-end failure scenarios driven through the real service layer against real PostgreSQL + Redis."""
import asyncio

import httpx
import pytest

from app.reliability.chaos import chaos
from app.services.delivery import process_delivery
from app.services.ingestion import ingest_event
from app.services.recovery import run_recovery
from tests.conftest import attempts_of, get_delivery, rewind

pytestmark = pytest.mark.asyncio


async def _send(env, acc, n, prefix="k"):
    return [(await ingest_event(env.sf, env.queue, env.settings, account_id=acc, idempotency_key=f"{prefix}{i}", event_type="t", payload={})).delivery_ids[0] for i in range(n)]


async def test_receiver_outage_then_recovery_no_event_lost(env):
    acc, _ = await env.account()
    await env.endpoint(acc, retry_schedule=[1] * 6)
    state = {"up": False}
    env.handler = lambda req: httpx.Response(200 if state["up"] else 503)
    ids = await _send(env, acc, 10)
    rt = env.runtime()
    for i in ids:
        await process_delivery(rt, i)
    assert all((await get_delivery(env, i)).status in ("RETRYING",) for i in ids[:3])
    state["up"] = True  # receiver recovers
    await asyncio.sleep(env.settings.cb_cooldown_s + 0.2)
    for _ in range(3):  # a few scheduler sweeps
        await rewind(env)
        for i in ids:
            await process_delivery(rt, i)
    final = [(await get_delivery(env, i)).status for i in ids]
    assert final == ["DELIVERED"] * 10


async def test_slow_receiver_times_out_then_succeeds_with_longer_window(env):
    acc, _ = await env.account()
    await env.endpoint(acc, timeout_s=0.1)

    def handler(req):
        raise httpx.ReadTimeout("slow")

    ids = await _send(env, acc, 1)
    await process_delivery(env.runtime(handler=handler), ids[0])
    a = (await attempts_of(env, ids[0]))[0]
    assert a.error_detail == "timeout" and a.will_retry and a.timeout_s == 0.1
    await rewind(env)
    env.handler = lambda req: httpx.Response(200)
    await process_delivery(env.runtime(), ids[0])
    assert (await get_delivery(env, ids[0])).status == "DELIVERED"


async def test_combined_redis_outage_and_worker_crash(env):
    from datetime import timedelta
    from sqlalchemy import update
    from app.models import Delivery, utcnow
    acc, _ = await env.account()
    await env.endpoint(acc)
    chaos.block_redis(600)
    ids = await _send(env, acc, 4)
    async with env.sf() as s, s.begin():  # half of them were claimed by a worker that then died
        await s.execute(update(Delivery).where(Delivery.id.in_(ids[:2])).values(status="PROCESSING", locked_at=utcnow() - timedelta(seconds=30), attempt_count=1))
    chaos.unblock_redis()
    await run_recovery(env.sf, env.queue, env.settings)
    await run_recovery(env.sf, env.queue, env.settings)
    rt = env.runtime()
    for i in ids:
        await process_delivery(rt, i)
    assert [(await get_delivery(env, i)).status for i in ids] == ["DELIVERED"] * 4


async def test_receiver_sees_stable_event_id_across_retries_for_dedup(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    codes = iter([500, 200])
    env.handler = lambda req: httpx.Response(next(codes))
    did = (await _send(env, acc, 1))[0]
    rt = env.runtime()
    await process_delivery(rt, did)
    await rewind(env)
    await process_delivery(rt, did)
    ids = {c.headers["x-hookrelay-event-id"] for c in env.calls}
    attempts = [c.headers["x-hookrelay-attempt"] for c in env.calls]
    assert len(ids) == 1 and attempts == ["1", "2"]  # at-least-once: same event id lets receivers dedupe
