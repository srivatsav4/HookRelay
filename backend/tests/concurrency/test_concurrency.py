import asyncio

import httpx
import pytest
from sqlalchemy import func, select

from app.models import Delivery, DeliveryAttempt, Event
from app.services.delivery import claim_delivery, process_delivery
from app.services.ingestion import ingest_event

pytestmark = pytest.mark.asyncio


async def test_invariant_B_100_concurrent_requests_one_logical_event(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    res = await asyncio.gather(*(ingest_event(env.sf, env.queue, env.settings, account_id=acc, idempotency_key="same-key",
                                              event_type="t", payload={"i": i}) for i in range(100)))
    assert sum(r.created for r in res) == 1 and len({r.event_id for r in res}) == 1
    async with env.sf() as s:
        assert (await s.execute(select(func.count()).select_from(Event))).scalar_one() == 1
        assert (await s.execute(select(func.count()).select_from(Delivery))).scalar_one() == 1  # one fan-out only
    assert await env.rds.zcard("arq:queue") == 1


async def test_same_key_different_accounts_are_independent(env):
    a, _ = await env.account()
    b, _ = await env.account()
    r = await asyncio.gather(*(ingest_event(env.sf, env.queue, env.settings, account_id=x, idempotency_key="k", event_type="t", payload={}) for x in (a, b)))
    assert all(x.created for x in r) and r[0].event_id != r[1].event_id


async def test_distinct_keys_all_accepted_under_concurrency(env):
    acc, _ = await env.account()
    res = await asyncio.gather(*(ingest_event(env.sf, env.queue, env.settings, account_id=acc, idempotency_key=f"k{i}", event_type="t", payload={}) for i in range(60)))
    assert all(r.created for r in res)


async def test_invariant_D_only_one_worker_can_claim_a_delivery(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    did = (await ingest_event(env.sf, env.queue, env.settings, account_id=acc, idempotency_key="k", event_type="t", payload={})).delivery_ids[0]
    claims = await asyncio.gather(*(claim_delivery(env.sf, did, f"w{i}") for i in range(25)))
    assert sum(c is not None for c in claims) == 1


async def test_multiple_workers_processing_same_job_deliver_once(env):
    acc, _ = await env.account()
    await env.endpoint(acc)

    async def slow_ok(req):
        await asyncio.sleep(0.2)
        return httpx.Response(200)

    env.handler = slow_ok
    did = (await ingest_event(env.sf, env.queue, env.settings, account_id=acc, idempotency_key="k", event_type="t", payload={})).delivery_ids[0]
    outs = await asyncio.gather(*(process_delivery(env.runtime(f"w{i}"), did) for i in range(10)))
    assert outs.count("success") == 1 and outs.count("skipped") == 9
    assert len(env.calls) == 1
    async with env.sf() as s:
        assert (await s.execute(select(func.count()).select_from(DeliveryAttempt))).scalar_one() == 1


async def test_tenant_fairness_cap_throttles_noisy_tenant_without_consuming_attempts(env):
    from dataclasses import replace
    env.settings = replace(env.settings, tenant_max_concurrency=2)
    noisy, _ = await env.account()
    await env.endpoint(noisy)

    async def slow_ok(req):
        await asyncio.sleep(0.3)
        return httpx.Response(200)

    env.handler = slow_ok
    ids = [(await ingest_event(env.sf, env.queue, env.settings, account_id=noisy, idempotency_key=f"k{i}", event_type="t", payload={})).delivery_ids[0] for i in range(6)]
    rt = env.runtime()
    outs = await asyncio.gather(*(process_delivery(rt, i) for i in ids))
    assert outs.count("success") == 2 and outs.count("tenant_throttled") == 4
    assert int(await env.rds.get(f"hr:tenant:{noisy}:throttled")) == 4
    assert int(await env.rds.get(f"hr:tenant:{noisy}:active") or 0) == 0  # released
