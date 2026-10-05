import httpx
import pytest
from sqlalchemy import select, text

from app.main import app
from app.models import ApiKey, Delivery, Event
from app.reliability.chaos import chaos
from tests.conftest import get_delivery

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def client(env):
    app.state.settings, app.state.sf, app.state.queue, app.state.lab_tasks = env.settings, env.sf, env.queue, {}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        yield c


async def test_auth_required_and_idempotency_header_required(env, client):
    acc, key = await env.account()
    body = {"type": "order.created", "payload": {"a": 1}}
    assert (await client.post("/events", json=body, headers={"Idempotency-Key": "k"})).status_code == 401
    assert (await client.post("/events", json=body, headers={"X-API-Key": "hr_bad00000_x", "Idempotency-Key": "k"})).status_code == 401
    r = await client.post("/events", json=body, headers={"X-API-Key": key})
    assert r.status_code == 400 and r.json()["error"]["code"] == "idempotency_key_required"


async def test_accept_persists_event_and_fans_out(env, client):
    acc, key = await env.account()
    ep = await env.endpoint(acc)
    await env.endpoint(acc, event_types=["other.type"])
    r = await client.post("/events", json={"type": "order.created", "payload": {"a": 1}},
                          headers={"X-API-Key": key, "Idempotency-Key": "k1"})
    assert r.status_code == 202 and r.json()["deliveries"] == 1 and not r.json()["duplicate"]
    assert r.headers["X-Request-ID"]
    d = await get_delivery(env, __import__("uuid").UUID(r.json()["delivery_ids"][0]))
    assert d.endpoint_id == ep.id and d.status == "QUEUED"
    assert await env.queue.depth() == 1


async def test_same_key_returns_same_event(env, client):
    acc, key = await env.account()
    await env.endpoint(acc)
    h = {"X-API-Key": key, "Idempotency-Key": "same"}
    a = (await client.post("/events", json={"type": "t", "payload": {}}, headers=h)).json()
    b = (await client.post("/events", json={"type": "t", "payload": {"different": True}}, headers=h)).json()
    assert a["event_id"] == b["event_id"] and b["duplicate"] and not a["duplicate"]


async def test_invariant_A_event_durable_even_when_redis_enqueue_fails(env, client):
    acc, key = await env.account()
    await env.endpoint(acc)
    chaos.block_redis(60)
    r = await client.post("/events", json={"type": "t", "payload": {"x": 1}}, headers={"X-API-Key": key, "Idempotency-Key": "durable"})
    assert r.status_code == 202
    async with env.sf() as s:
        assert (await s.get(Event, __import__("uuid").UUID(r.json()["event_id"]))) is not None
    d = await get_delivery(env, __import__("uuid").UUID(r.json()["delivery_ids"][0]))
    assert d.status == "PENDING"  # recoverable from PostgreSQL
    assert await env.rds.zcard("arq:queue") == 0  # nothing was enqueued
    chaos.unblock_redis()


async def test_api_key_is_never_stored_in_plaintext(env, client):
    acc, key = await env.account()
    async with env.sf() as s:
        row = (await s.execute(select(ApiKey))).scalar_one()
        assert key not in {row.key_hash, row.prefix, row.name}
        dump = (await s.execute(text("SELECT to_jsonb(api_keys)::text FROM api_keys"))).scalar_one()
    assert key not in dump and key.split("_", 2)[2] not in dump


async def test_validation_error_shape_and_payload_limit(env, client):
    acc, key = await env.account()
    h = {"X-API-Key": key, "Idempotency-Key": "v"}
    r = await client.post("/events", json={"type": "bad type!", "payload": {}}, headers=h)
    assert r.status_code == 422 and r.json()["error"]["code"] == "validation_error"
    big = {"type": "t", "payload": {"blob": "x" * (env.settings.max_payload_bytes + 10)}}
    assert (await client.post("/events", json=big, headers=h)).status_code == 413


async def test_login_and_management_requires_jwt(env, client):
    from app.models import User
    from app.security.hashing import hash_password
    acc, _ = await env.account()
    async with env.sf() as s, s.begin():
        s.add(User(account_id=acc, email="a@b.c", password_hash=hash_password("pw-pw-pw"), is_admin=True))
    assert (await client.get("/endpoints")).status_code == 401
    assert (await client.post("/auth/login", json={"email": "a@b.c", "password": "nope"})).status_code == 401
    tok = (await client.post("/auth/login", json={"email": "a@b.c", "password": "pw-pw-pw"})).json()["access_token"]
    assert (await client.get("/endpoints", headers={"Authorization": f"Bearer {tok}"})).status_code == 200


async def test_endpoint_creation_blocks_ssrf_in_strict_mode(env, client):
    from dataclasses import replace
    from app.models import User
    from app.security.hashing import hash_password
    app.state.settings = replace(env.settings, allow_private_destinations=False)
    acc, _ = await env.account()
    async with env.sf() as s, s.begin():
        s.add(User(account_id=acc, email="s@b.c", password_hash=hash_password("pw-pw-pw")))
    tok = (await client.post("/auth/login", json={"email": "s@b.c", "password": "pw-pw-pw"})).json()["access_token"]
    h = {"Authorization": f"Bearer {tok}"}
    for url in ("http://127.0.0.1/x", "http://169.254.169.254/", "http://10.1.2.3/", "file:///etc/passwd"):
        r = await client.post("/endpoints", json={"url": url}, headers=h)
        assert r.status_code == 422 and r.json()["error"]["code"] == "ssrf_blocked", url
    r = await client.post("/endpoints", json={"url": "http://127.0.0.1/x"}, headers=h)
    assert "signing_secret" not in r.text


async def test_ready_reports_dependencies(env, client):
    r = await client.get("/ready")
    assert r.status_code == 200 and r.json()["checks"] == {"postgres": "ok", "redis": "ok"}
    assert (await client.get("/health")).json() == {"status": "ok"}
