"""Delivery engine: claim (row lock) -> gates (tenant cap, circuit, rate limit) -> signed send -> finalize."""
from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core import metrics
from app.core.config import Settings
from app.core.logging import bind, log
from app.models import Delivery, DeliveryAttempt, Endpoint, Event, utcnow
from app.security import signing
from app.security.ssrf import SSRFError, validate_destination_url_async
from app.services import circuit_breaker as cb
from app.services import circuit_store, ratelimit, state_machine as sm
from app.services.queue import Queue
from app.services.retry import ErrorClass, RetryPolicy, classify, decide

logger = logging.getLogger("hookrelay.delivery")


@dataclass
class Runtime:
    sf: async_sessionmaker[AsyncSession]
    queue: Queue
    rds: object            # redis.asyncio.Redis
    http: httpx.AsyncClient
    settings: Settings
    worker_id: str


@dataclass
class Claim:
    delivery_id: uuid.UUID
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    account_id: uuid.UUID
    attempt_number: int
    max_attempts: int
    release_count: int
    created_at: datetime


def policy_for(ep_schedule: list | None, settings: Settings) -> RetryPolicy:
    sched = tuple(float(x) for x in ep_schedule) if ep_schedule else settings.retry_schedule
    return RetryPolicy(schedule=sched, jitter=settings.retry_jitter, budget_s=settings.retry_budget_s)


async def claim_delivery(sf, delivery_id: uuid.UUID, worker_id: str) -> Claim | None:
    """Invariant D: SELECT ... FOR UPDATE SKIP LOCKED + status check => at most one worker owns a delivery."""
    async with sf() as s, s.begin():
        d = (await s.execute(select(Delivery).where(Delivery.id == delivery_id).with_for_update(skip_locked=True))
             ).scalar_one_or_none()
        if d is None or sm.DeliveryStatus(d.status) not in sm.CLAIMABLE:
            return None
        now = utcnow()
        if d.status == "RETRYING" and d.next_attempt_at and d.next_attempt_at > now + timedelta(seconds=1):
            return None  # early/duplicate job; the properly deferred job (or recovery) will pick it up
        d.status = sm.check(d.status, "PROCESSING").value
        d.attempt_count += 1
        d.locked_by, d.locked_at = worker_id, now
        return Claim(d.id, d.event_id, d.endpoint_id, d.account_id, d.attempt_count, d.max_attempts,
                     d.release_count, d.created_at)


async def release(rt: Runtime, c: Claim, defer_s: float, reason: str) -> str:
    """Give the delivery back without consuming an attempt (circuit open, rate limited, tenant throttled)."""
    defer_s = max(defer_s, 0.2)
    async with rt.sf() as s, s.begin():
        d = (await s.execute(select(Delivery).where(Delivery.id == c.delivery_id).with_for_update())).scalar_one()
        d.status = sm.check(d.status, "RETRYING").value
        d.attempt_count -= 1
        d.release_count += 1
        d.locked_by = None
        d.next_attempt_at = utcnow() + timedelta(seconds=defer_s)
        d.last_error = reason
        ac, rc = d.attempt_count, d.release_count
    try:
        await rt.queue.enqueue_delivery(c.delivery_id, ac, rc, defer_s)
    except Exception:
        log(logger, logging.WARNING, "re-enqueue after release failed; recovery will pick it up", reason=reason)
    return reason


@dataclass
class Outcome:
    status: int | None
    error_kind: str | None
    retry_after: float | None
    started_at: datetime
    completed_at: datetime
    duration_ms: float


async def _send(rt: Runtime, c: Claim, ep: dict, ev: dict, timeout_s: float) -> Outcome:
    started = utcnow()
    t0 = time.monotonic()
    status = kind = None
    retry_after = None
    try:
        await validate_destination_url_async(ep["url"], allow_private=rt.settings.allow_private_destinations,
                                             require_https=rt.settings.require_https)  # re-check per attempt
        body = signing.build_body(str(ev["id"]), ev["type"], ev["created_at"].isoformat(), ev["payload"])
        ts = int(time.time())
        headers = {
            "Content-Type": "application/json", "User-Agent": "HookRelay/1.0",
            "X-HookRelay-Event-Id": str(ev["id"]), "X-HookRelay-Delivery-Id": str(c.delivery_id),
            "X-HookRelay-Attempt": str(c.attempt_number), "X-HookRelay-Event-Type": ev["type"],
            signing.TIMESTAMP_HEADER: str(ts), signing.SIGNATURE_HEADER: signing.sign(ep["secret"], ts, body),
        }
        async with rt.http.stream("POST", ep["url"], content=body, headers=headers,
                                  timeout=httpx.Timeout(timeout_s), follow_redirects=False) as r:
            status = r.status_code
            ra = r.headers.get("retry-after")
            if ra and ra.isdigit():
                retry_after = float(ra)
    except SSRFError:
        kind = "blocked_destination"
    except httpx.TimeoutException:
        kind = "timeout"
    except httpx.TransportError:
        kind = "connection"
    except Exception:  # never let a delivery bug kill the worker
        logger.exception("unexpected error while sending")
        kind = "unexpected"
    return Outcome(status, kind, retry_after, started, utcnow(), (time.monotonic() - t0) * 1000)


async def process_delivery(rt: Runtime, delivery_id: uuid.UUID) -> str:
    s = rt.settings
    c = await claim_delivery(rt.sf, delivery_id, rt.worker_id)
    if c is None:
        return "skipped"
    bind(delivery_id=c.delivery_id, event_id=c.event_id, attempt=c.attempt_number)

    async with rt.sf() as sess:
        e = await sess.get(Endpoint, c.endpoint_id)
        v = await sess.get(Event, c.event_id)
        ep = dict(id=e.id, url=e.url, secret=e.signing_secret, active=e.active and e.deleted_at is None,
                  rate=e.rate_limit_per_sec, schedule=e.retry_schedule, timeout=e.timeout_s)
        ev = dict(id=v.id, type=v.event_type, payload=v.payload, created_at=v.created_at)

    if not ep["active"]:
        await _finalize(rt, c, None, ErrorClass.PERMANENT, "endpoint_inactive", None, ep, final="DEAD",
                        reason="endpoint_inactive_or_deleted")
        return "dead:endpoint_inactive"

    if not await ratelimit.tenant_acquire(rt.rds, c.account_id, s.tenant_max_concurrency):
        metrics.TENANT_THROTTLED.inc()
        return await release(rt, c, 1.0, "tenant_throttled")
    try:
        cfg = cb.CBConfig(s.cb_failure_threshold, s.cb_cooldown_s)
        allowed, wait = await circuit_store.check(rt.sf, c.endpoint_id, cfg, utcnow())
        if not allowed:
            return await release(rt, c, wait + 0.5, "circuit_open")
        if ep["rate"]:
            ok, wait = await ratelimit.take_token(rt.rds, c.endpoint_id, ep["rate"])
            if not ok:
                metrics.RATE_LIMITED.inc()
                return await release(rt, c, wait, "rate_limited")

        timeout_s = ep["timeout"] or s.delivery_timeout_s
        out = await _send(rt, c, ep, ev, timeout_s)
        ec, label = classify(out.status, out.error_kind)
        counts = ec is ErrorClass.TRANSIENT and out.status != 429
        await circuit_store.record(rt.sf, c.endpoint_id, ec is ErrorClass.SUCCESS, counts, cfg, utcnow())
        await _finalize(rt, c, out, ec, label, timeout_s, ep)
        return ec.value.lower()
    finally:
        await ratelimit.tenant_release(rt.rds, c.account_id)


async def _finalize(rt: Runtime, c: Claim, out: Outcome | None, ec: ErrorClass, label: str, timeout_s: float | None,
                    ep: dict, final: str | None = None, reason: str | None = None) -> None:
    policy = policy_for(ep["schedule"], rt.settings)
    now = utcnow()
    decision = decide(c.attempt_number, ec, policy, elapsed_s=(now - c.created_at).total_seconds(),
                      retry_after_s=out.retry_after if out else None)
    # a delivery may carry a smaller max_attempts than the policy default
    if decision.retry and c.attempt_number >= c.max_attempts:
        decision = type(decision)(False, 0.0, "max_attempts_exhausted")
    status = final or ("DELIVERED" if ec is ErrorClass.SUCCESS else "RETRYING" if decision.retry else "DEAD")
    next_at = now + timedelta(seconds=decision.delay_s) if status == "RETRYING" else None

    async with rt.sf() as s, s.begin():
        d = (await s.execute(select(Delivery).where(Delivery.id == c.delivery_id).with_for_update())).scalar_one()
        if out is not None:
            s.add(DeliveryAttempt(
                delivery_id=c.delivery_id, attempt_number=c.attempt_number, started_at=out.started_at,
                completed_at=out.completed_at, http_status=out.status, timeout_s=timeout_s or 0,
                duration_ms=out.duration_ms, error_class=ec.value, error_detail=label,
                will_retry=status == "RETRYING", retry_decision=decision.reason,
                retry_delay_s=decision.delay_s if status == "RETRYING" else None))
        d.status = sm.check(d.status, status).value
        d.locked_by = None
        d.last_status_code = out.status if out else None
        d.last_error = None if status == "DELIVERED" else (reason or label)
        d.next_attempt_at = next_at
        if status == "DELIVERED":
            d.delivered_at = now
        ac, rc = d.attempt_count, d.release_count

    if out is not None:
        metrics.ATTEMPTS.labels(ec.value.lower()).inc()
        metrics.ATTEMPT_SECONDS.observe(out.duration_ms / 1000)
    if status == "RETRYING":
        metrics.RETRIES.inc()
        try:
            await rt.queue.enqueue_delivery(c.delivery_id, ac, rc, decision.delay_s)
        except Exception:
            log(logger, logging.WARNING, "retry enqueue failed; recovery will pick it up")
    elif status == "DEAD":
        metrics.DEAD.inc()
    log(logger, logging.INFO, "attempt finished", outcome=label, status=status, decision=decision.reason,
        retry_delay_s=round(decision.delay_s, 2))
