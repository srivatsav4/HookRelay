import httpx
import pytest

from app.security import signing
from app.services.delivery import process_delivery
from app.services.ingestion import ingest_event
from tests.conftest import attempts_of, get_delivery, rewind

pytestmark = pytest.mark.asyncio


async def _ingest(env, acc, key="k", typ="t"):
    return await ingest_event(env.sf, env.queue, env.settings, account_id=acc, idempotency_key=key, event_type=typ, payload={"n": 1})


async def test_success_records_attempt_and_signs_request(env):
    acc, _ = await env.account()
    ep = await env.endpoint(acc)
    seen = {}

    def handler(req: httpx.Request):
        seen["ok"] = signing.verify(ep.signing_secret, req.headers[signing.TIMESTAMP_HEADER], req.content, req.headers[signing.SIGNATURE_HEADER])
        seen["event_id"], seen["delivery_id"], seen["attempt"] = (req.headers["x-hookrelay-event-id"], req.headers["x-hookrelay-delivery-id"], req.headers["x-hookrelay-attempt"])
        return httpx.Response(200)

    env.handler = handler
    r = await _ingest(env, acc)
    did = r.delivery_ids[0]
    assert await process_delivery(env.runtime(), did) == "success"
    d = await get_delivery(env, did)
    assert d.status == "DELIVERED" and d.attempt_count == 1 and d.delivered_at
    a = (await attempts_of(env, did))[0]
    assert a.http_status == 200 and a.error_class == "SUCCESS" and a.duration_ms >= 0 and not a.will_retry
    assert seen["ok"] and seen["event_id"] == str(r.event_id) and seen["delivery_id"] == str(did) and seen["attempt"] == "1"


async def test_invariant_C_transient_failures_retried_until_success(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    codes = iter([503, 500, 200])
    env.handler = lambda req: httpx.Response(next(codes))
    did = (await _ingest(env, acc)).delivery_ids[0]
    rt = env.runtime()
    for expected in ("transient", "transient", "success"):
        await rewind(env)
        assert await process_delivery(rt, did) == expected
    d = await get_delivery(env, did)
    atts = await attempts_of(env, did)
    assert d.status == "DELIVERED" and [a.http_status for a in atts] == [503, 500, 200]
    assert atts[0].will_retry and atts[0].retry_delay_s == 1 and atts[0].retry_decision == "transient_failure_retry_scheduled"


async def test_invariant_F_permanent_4xx_not_retried(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    env.handler = lambda req: httpx.Response(404)
    did = (await _ingest(env, acc)).delivery_ids[0]
    await process_delivery(env.runtime(), did)
    d = await get_delivery(env, did)
    assert d.status == "DEAD" and d.attempt_count == 1 and (await attempts_of(env, did))[0].retry_decision == "permanent_failure_not_retried"


async def test_429_is_transient_and_honours_retry_after(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    env.handler = lambda req: httpx.Response(429, headers={"Retry-After": "7"})
    did = (await _ingest(env, acc)).delivery_ids[0]
    await process_delivery(env.runtime(), did)
    a = (await attempts_of(env, did))[0]
    assert (await get_delivery(env, did)).status == "RETRYING" and a.retry_delay_s == 7


async def test_timeout_and_connection_errors_are_transient(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    seq = iter([httpx.ReadTimeout("t"), httpx.ConnectError("c")])

    def handler(req):
        raise next(seq)

    did = (await _ingest(env, acc)).delivery_ids[0]
    rt = env.runtime(handler=handler)
    await process_delivery(rt, did)
    await rewind(env)
    await process_delivery(rt, did)
    atts = await attempts_of(env, did)
    assert [a.error_detail for a in atts] == ["timeout", "connection"] and all(a.will_retry for a in atts)


async def test_exhausted_retries_go_dead_then_replay_is_a_new_audited_delivery(env):
    from sqlalchemy import select
    from app.models import AuditLog, Delivery
    acc, _ = await env.account()
    await env.endpoint(acc)  # schedule [1,1,1] => 4 attempts
    env.handler = lambda req: httpx.Response(500)
    did = (await _ingest(env, acc)).delivery_ids[0]
    rt = env.runtime()
    for _ in range(4):
        await rewind(env)
        await process_delivery(rt, did)
    d = await get_delivery(env, did)
    atts = await attempts_of(env, did)
    assert d.status == "DEAD" and d.attempt_count == 4 and atts[-1].retry_decision == "max_attempts_exhausted"
    # Invariant H: replay (same code path as POST /deliveries/{id}/replay) never mutates history
    import httpx as hx
    from app.main import app
    from app.models import User
    from app.security.hashing import hash_password
    app.state.settings, app.state.sf, app.state.queue, app.state.lab_tasks = env.settings, env.sf, env.queue, {}
    async with env.sf() as s, s.begin():
        s.add(User(account_id=acc, email="r@b.c", password_hash=hash_password("pw-pw-pw")))
    async with hx.AsyncClient(transport=hx.ASGITransport(app=app), base_url="http://t") as c:
        tok = (await c.post("/auth/login", json={"email": "r@b.c", "password": "pw-pw-pw"})).json()["access_token"]
        h = {"Authorization": f"Bearer {tok}"}
        r = await c.post(f"/deliveries/{did}/replay", headers=h)
        assert r.status_code == 202
        new_id = __import__("uuid").UUID(r.json()["new_delivery_id"])
        assert (await c.post(f"/deliveries/{new_id}/replay", headers=h)).status_code == 409  # only DEAD can be replayed
        detail = (await c.get(f"/deliveries/{did}", headers=h)).json()
    assert [a.attempt_number for a in await attempts_of(env, did)] == [1, 2, 3, 4]
    assert (await get_delivery(env, did)).status == "DEAD"
    assert detail["replays"][0]["id"] == str(new_id) and detail["audit"][0]["action"] == "DELIVERY_REPLAYED"
    env.handler = lambda req: httpx.Response(200)
    await process_delivery(rt, new_id)
    nd = await get_delivery(env, new_id)
    assert nd.status == "DELIVERED" and nd.replay_of == did and nd.event_id == d.event_id
    async with env.sf() as s:
        assert (await s.execute(select(AuditLog).where(AuditLog.action == "DELIVERY_REPLAYED"))).scalars().one()


async def test_redirects_are_not_followed_and_are_permanent(env):
    acc, _ = await env.account()
    await env.endpoint(acc)
    env.handler = lambda req: httpx.Response(302, headers={"Location": "http://169.254.169.254/"})
    did = (await _ingest(env, acc)).delivery_ids[0]
    await process_delivery(env.runtime(), did)
    assert (await get_delivery(env, did)).status == "DEAD" and len(env.calls) == 1


async def test_inactive_endpoint_goes_dead_without_sending(env):
    from sqlalchemy import update
    from app.models import Endpoint
    acc, _ = await env.account()
    ep = await env.endpoint(acc)
    did = (await _ingest(env, acc)).delivery_ids[0]
    async with env.sf() as s, s.begin():
        await s.execute(update(Endpoint).where(Endpoint.id == ep.id).values(active=False))
    await process_delivery(env.runtime(), did)
    assert (await get_delivery(env, did)).status == "DEAD" and env.calls == []


async def test_endpoint_rate_limit_defers_without_consuming_attempt(env):
    acc, _ = await env.account()
    await env.endpoint(acc, rate_limit_per_sec=1.0)
    ids = [(await _ingest(env, acc, key=f"k{i}")).delivery_ids[0] for i in range(3)]
    rt = env.runtime()
    outs = [await process_delivery(rt, i) for i in ids]
    assert outs.count("success") == 1 and outs.count("rate_limited") == 2
    deferred = [await get_delivery(env, i) for i in ids if (await get_delivery(env, i)).status == "RETRYING"]
    assert all(d.attempt_count == 0 and d.release_count == 1 for d in deferred)


async def test_circuit_opens_blocks_traffic_then_half_open_probe_closes(env):
    import asyncio
    from sqlalchemy import select
    from app.models import Endpoint
    acc, _ = await env.account()
    ep = await env.endpoint(acc)
    env.handler = lambda req: httpx.Response(500)
    ids = [(await _ingest(env, acc, key=f"k{i}")).delivery_ids[0] for i in range(6)]
    rt = env.runtime()
    outs = [await process_delivery(rt, i) for i in ids]  # threshold = 3
    assert outs[:3] == ["transient"] * 3 and outs[3:] == ["circuit_open"] * 3
    assert len(env.calls) == 3  # the open circuit stopped traffic to the dead endpoint
    async def state():
        async with env.sf() as s:
            return (await s.execute(select(Endpoint.cb_state).where(Endpoint.id == ep.id))).scalar_one()
    assert await state() == "OPEN"
    await asyncio.sleep(env.settings.cb_cooldown_s + 0.2)
    env.handler = lambda req: httpx.Response(200)
    await rewind(env)
    assert await process_delivery(rt, ids[3]) == "success"  # HALF_OPEN probe
    assert await state() == "CLOSED"
