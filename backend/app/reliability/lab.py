"""Reliability Laboratory: scoped, controlled failure experiments with invariant verification.

Every number in an experiment result is read back from PostgreSQL / the mock receiver after the run.
Fault injection is limited to: (1) the mock receiver's behaviour API, (2) an in-process gate on the Redis
*enqueue path*, (3) a DB-state simulation of a worker that died after claiming a delivery. No shell, no
arbitrary commands. Real process-level chaos lives in scripts/chaos_*.sh and is run by an operator.
"""
from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
import uuid
from datetime import timedelta
from typing import Any

import httpx
from sqlalchemy import func, select, update

from app.core.config import Settings
from app.models import (Account, AuditLog, Delivery, DeliveryAttempt, Endpoint, Event, Experiment, ExperimentEvent,
                        utcnow)
from app.reliability.chaos import chaos
from app.security import signing
from app.security.ssrf import SSRFError, validate_destination_url
from app.services.ingestion import ingest_event

logger = logging.getLogger("hookrelay.lab")

EXPERIMENT_TYPES = ["REDIS_OUTAGE", "WORKER_CRASH", "RECEIVER_500", "RECEIVER_429", "RECEIVER_TIMEOUT",
                    "SLOW_RECEIVER", "INTERMITTENT_FAILURE", "DUPLICATE_REQUEST", "CIRCUIT_BREAKER",
                    "SIGNATURE_TAMPERING", "SSRF_ATTEMPT", "TENANT_FLOOD"]
DESCRIPTIONS = {
    "REDIS_OUTAGE": "Enqueue path to Redis fails while traffic continues; recovery must re-derive work from PostgreSQL.",
    "WORKER_CRASH": "A worker 'dies' after claiming deliveries (simulated DB state); recovery must reclaim them.",
    "RECEIVER_500": "Receiver returns HTTP 500 for a window, then recovers; transient failures must be retried.",
    "RECEIVER_429": "Receiver throttles with 429 + Retry-After for a window; retries must honour it and succeed.",
    "RECEIVER_TIMEOUT": "Receiver hangs past the delivery timeout for a window; attempts time out and retry.",
    "SLOW_RECEIVER": "Receiver is slow but within the timeout; deliveries must succeed without retries.",
    "INTERMITTENT_FAILURE": "Receiver fails ~30% of requests indefinitely; retries must still deliver everything.",
    "DUPLICATE_REQUEST": "Many concurrent requests share one Idempotency-Key; exactly one logical event may exist.",
    "CIRCUIT_BREAKER": "Receiver is hard-down; the breaker must open, stop hammering it, then close after recovery.",
    "SIGNATURE_TAMPERING": "Tampered body, wrong secret and stale timestamp must all be rejected by a verifying receiver.",
    "SSRF_ATTEMPT": "Loopback/private/metadata/odd-scheme destinations must be rejected at registration.",
    "TENANT_FLOOD": "A noisy tenant floods a slow receiver; a quiet tenant must still complete before the flood does.",
}
RETRY_SCHEDULE = [1, 2, 3, 5, 8, 13, 20]  # lab-only fast schedule (7 retries => 8 attempts)


def _num(cfg: dict, key: str, default: float, lo: float, hi: float) -> float:
    try:
        v = float(cfg.get(key, default))
    except (TypeError, ValueError):
        v = default
    return max(lo, min(hi, v))


def pct(vals: list[float], q: float) -> float | None:
    if not vals:
        return None
    v = sorted(vals)
    return round(v[min(len(v) - 1, int(round(q * (len(v) - 1))))], 1)


class Receiver:
    def __init__(self, base: str, bucket: str) -> None:
        self.base, self.bucket = base.rstrip("/"), bucket

    async def configure(self, **cfg: Any) -> None:
        async with httpx.AsyncClient(timeout=5) as c:
            (await c.post(f"{self.base}/config/{self.bucket}", json=cfg)).raise_for_status()

    async def stats(self) -> dict:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{self.base}/stats/{self.bucket}")
            r.raise_for_status()
            return r.json()

    @property
    def url(self) -> str:
        return f"{self.base}/hook/{self.bucket}"


class Lab:
    def __init__(self, sf, queue, settings: Settings, exp_id: uuid.UUID) -> None:
        self.sf, self.queue, self.settings, self.exp_id = sf, queue, settings, exp_id
        self.accounts: list[uuid.UUID] = []
        self.accepted: list[uuid.UUID] = []
        self.generated = 0
        self.duplicates = 0
        self.t0 = time.monotonic()
        self.extra: dict = {}

    async def log(self, kind: str, msg: str, **data: Any) -> None:
        async with self.sf() as s, s.begin():
            s.add(ExperimentEvent(experiment_id=self.exp_id, kind=kind, message=msg, data=data or None))

    # ---- provisioning / traffic ---------------------------------------------------------------
    async def provision(self, label: str, *, timeout_s: float = 3, schedule: list[float] | None = None,
                        bucket: str | None = None, active_types=("*",)) -> tuple[uuid.UUID, uuid.UUID, str, Receiver]:
        bucket = bucket or f"{self.exp_id.hex[:12]}-{label}"
        acc = Account(id=uuid.uuid4(), name=f"lab-{self.exp_id.hex[:8]}-{label}", is_lab=True)
        secret = "whsec_" + secrets.token_urlsafe(24)
        rcv = Receiver(self.settings.mock_receiver_url, bucket)
        ep = Endpoint(id=uuid.uuid4(), account_id=acc.id, url=rcv.url, event_types=list(active_types),
                      signing_secret=secret, retry_schedule=schedule or RETRY_SCHEDULE, timeout_s=timeout_s)
        async with self.sf() as s, s.begin():
            s.add(acc)
            await s.flush()
            s.add(ep)
        self.accounts.append(acc.id)
        return acc.id, ep.id, secret, rcv

    async def send(self, acc: uuid.UUID, n: int, tag: str, concurrency: int = 10, interval_s: float = 0.0) -> list:
        sem = asyncio.Semaphore(concurrency)
        results: list = []

        async def one(i: int) -> None:
            async with sem:
                r = await ingest_event(self.sf, self.queue, self.settings, account_id=acc, idempotency_key=f"{self.exp_id}:{tag}:{i}",
                                       event_type="lab.event", payload={"i": i, "tag": tag})
                results.append(r)
                if interval_s:
                    await asyncio.sleep(interval_s)

        self.generated += n
        await asyncio.gather(*(one(i) for i in range(n)))
        self.accepted += [r.event_id for r in results if r.created]
        self.duplicates += sum(1 for r in results if not r.created)
        return results

    async def counts(self, accs: list[uuid.UUID] | None = None) -> dict[str, int]:
        async with self.sf() as s:
            rows = (await s.execute(select(Delivery.status, func.count()).where(Delivery.account_id.in_(accs or self.accounts))
                                    .group_by(Delivery.status))).all()
        return dict(rows)

    async def wait_terminal(self, accs: list[uuid.UUID] | None = None, timeout: float = 90) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            c = await self.counts(accs)
            if c and not any(k not in ("DELIVERED", "DEAD") for k in c):
                return True
            await asyncio.sleep(0.5)
        return False

    async def wait_for(self, predicate, timeout: float, every: float = 0.4) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if await predicate():
                return True
            await asyncio.sleep(every)
        return False

    # ---- measurement --------------------------------------------------------------------------
    async def measure(self) -> dict:
        async with self.sf() as s:
            ds = (await s.execute(select(Delivery).where(Delivery.account_id.in_(self.accounts)))).scalars().all()
            attempts = (await s.execute(select(func.count()).select_from(DeliveryAttempt).join(
                Delivery, Delivery.id == DeliveryAttempt.delivery_id).where(Delivery.account_id.in_(self.accounts)))).scalar_one()
            rec = (await s.execute(select(func.count()).select_from(AuditLog).where(
                AuditLog.account_id.in_(self.accounts), AuditLog.action.like("RECOVERY_%")))).scalar_one()
            n_events = (await s.execute(select(func.count()).select_from(Event).where(Event.account_id.in_(self.accounts)))).scalar_one()
        lat = [(d.delivered_at - d.created_at).total_seconds() * 1000 for d in ds if d.delivered_at]
        delivered = sum(1 for d in ds if d.status == "DELIVERED")
        return {"events_generated": self.generated, "events_accepted": len(self.accepted), "events_in_db": n_events,
                "deliveries_total": len(ds), "deliveries_delivered": delivered,
                "deliveries_dead": sum(1 for d in ds if d.status == "DEAD"), "attempts_total": attempts,
                "retries": max(0, attempts - len(ds)), "recovery_actions": rec, "duplicate_requests": self.duplicates,
                "latency_ms": {"p50": pct(lat, .5), "p95": pct(lat, .95), "max": pct(lat, 1.0)}}

    async def durable(self) -> dict:
        """Invariant A: every accepted event id is present in PostgreSQL."""
        async with self.sf() as s:
            n = (await s.execute(select(func.count()).select_from(Event).where(Event.id.in_(self.accepted)))).scalar_one() if self.accepted else 0
        return inv("A", "Durable acceptance", n == len(self.accepted), f"{n}/{len(self.accepted)} accepted events exist in PostgreSQL")

    # ---- scenarios ----------------------------------------------------------------------------
    async def run(self, typ: str, cfg: dict) -> dict:
        fn = getattr(self, f"_x_{typ.lower()}")
        started = utcnow()
        await self.log("start", f"experiment {typ} started", config=cfg)
        injected, invs = await fn(cfg)
        metrics = await self.measure()
        metrics.update(self.extra)
        ended = utcnow()
        await self.log("end", "experiment finished", **{k: v for k, v in metrics.items() if k != "latency_ms"})
        verdict = "PASS" if invs and all(i["passed"] for i in invs) else "FAIL"
        return {"experiment_id": str(self.exp_id), "type": typ, "config": cfg, "started_at": started.isoformat(),
                "ended_at": ended.isoformat(), "duration_s": round(time.monotonic() - self.t0, 1),
                "injected_failure": injected, "metrics": metrics, "invariants": invs, "verdict": verdict}

    async def _receiver_fault(self, cfg: dict, label: str, behavior_cfg: dict, expect_retries: bool, injected: str):
        n = int(_num(cfg, "events", 20, 1, 200))
        acc, _, _, rcv = await self.provision(label, timeout_s=_num(cfg, "timeout_s", 2, 1, 10))
        await rcv.configure(behavior="ok")
        await self.log("traffic", f"traffic started: {n} events")
        await rcv.configure(**behavior_cfg)
        await self.log("fault", f"fault injected: {injected}")
        await self.send(acc, n, label, interval_s=0.05)
        await self.log("traffic", f"ingestion finished; {len(self.accepted)} events accepted")
        done = await self.wait_terminal(timeout=_num(cfg, "timeout", 90, 10, 240))
        await self.log("recovery" if done else "timeout", "all deliveries reached a terminal state" if done else "timed out waiting for terminal states")
        m = await self.measure()
        invs = [await self.durable(),
                inv("C", "At-least-once delivery", done and m["deliveries_delivered"] == m["deliveries_total"] > 0,
                    f"{m['deliveries_delivered']}/{m['deliveries_total']} delivered, {m['deliveries_dead']} dead"),
                inv("F", "Transient failures are retried", (m["retries"] > 0) == expect_retries,
                    f"{m['retries']} retries observed (expected {'>0' if expect_retries else '0'})")]
        self.extra = {"receiver": await rcv.stats()}
        return injected, invs

    async def _x_receiver_500(self, cfg):
        s = _num(cfg, "fail_for_s", 8, 2, 60)
        return await self._receiver_fault(cfg, "r500", dict(behavior="status", status=500, fail_for_s=s), True, f"HTTP 500 for {s:.0f}s")

    async def _x_receiver_429(self, cfg):
        s = _num(cfg, "fail_for_s", 8, 2, 60)
        return await self._receiver_fault(cfg, "r429", dict(behavior="status", status=429, retry_after=2, fail_for_s=s), True,
                                          f"HTTP 429 (Retry-After: 2) for {s:.0f}s")

    async def _x_receiver_timeout(self, cfg):
        s = _num(cfg, "fail_for_s", 6, 2, 60)
        return await self._receiver_fault(cfg, "rto", dict(behavior="timeout", fail_for_s=s), True, f"hang beyond timeout for {s:.0f}s")

    async def _x_intermittent_failure(self, cfg):
        return await self._receiver_fault(cfg, "flaky", dict(behavior="flaky", status=500, fail_rate=_num(cfg, "fail_rate", 0.3, 0.05, 0.6)),
                                          True, "random 500s at the configured rate")

    async def _x_slow_receiver(self, cfg):
        d = int(_num(cfg, "delay_ms", 1200, 100, 5000))
        cfg = {**cfg, "timeout_s": max(_num(cfg, "timeout_s", 5, 1, 10), d / 1000 + 2)}
        inj, invs = await self._receiver_fault(cfg, "slow", dict(behavior="slow", delay_ms=d), False, f"{d}ms response delay")
        async with self.sf() as s:
            mx = (await s.execute(select(func.max(DeliveryAttempt.duration_ms)).join(Delivery, Delivery.id == DeliveryAttempt.delivery_id)
                                  .where(Delivery.account_id.in_(self.accounts)))).scalar_one()
        invs.append(inv("-", "Slowness was real", bool(mx and mx >= d * 0.9), f"max attempt duration {mx and round(mx)}ms >= {d}ms"))
        return inj, invs

    async def _x_redis_outage(self, cfg):
        out_s, n = _num(cfg, "outage_s", 10, 3, 60), int(_num(cfg, "events", 20, 1, 200))
        acc, _, _, rcv = await self.provision("redis")
        await rcv.configure(behavior="ok")
        await self.log("traffic", "traffic started")
        chaos.block_redis(out_s + 600)
        await self.log("fault", f"Redis enqueue path blocked (injected for ~{out_s:.0f}s)")
        await self.send(acc, n, "outage", interval_s=0.05)
        c = await self.counts()
        await self.log("ingestion", f"ingestion continued during outage: {len(self.accepted)} accepted, {c.get('PENDING', 0)} deliveries PENDING (no job enqueued)")
        pending_during = c.get("PENDING", 0)
        await asyncio.sleep(out_s)
        chaos.unblock_redis()
        await self.log("recovered", "enqueue-path gate lifted (the worker-side recovery scanner may already have acted)")
        done = await self.wait_terminal(timeout=_num(cfg, "timeout", 90, 20, 240))
        await self._timeline_from_audit()
        m = await self.measure()
        invs = [await self.durable(),
                inv("-", "Fault was real: no job was enqueued during the outage", pending_during == m["deliveries_total"] > 0,
                    f"{pending_during}/{m['deliveries_total']} deliveries were PENDING before recovery"),
                inv("E", "Recovery: PostgreSQL-authoritative re-enqueue", done and m["recovery_actions"] > 0,
                    f"{m['recovery_actions']} recovery re-enqueue actions recorded"),
                inv("C", "At-least-once delivery", done and m["deliveries_delivered"] == m["deliveries_total"] > 0,
                    f"{m['deliveries_delivered']}/{m['deliveries_total']} delivered")]
        return "Redis unavailable on the enqueue path", invs

    async def _x_worker_crash(self, cfg):
        n = int(_num(cfg, "events", 10, 1, 100))
        acc, _, _, rcv = await self.provision("crash")
        await rcv.configure(behavior="ok")
        chaos.block_redis(600)
        await self.send(acc, n, "crash")
        stale = utcnow() - timedelta(seconds=self.settings.visibility_timeout_s * 2)
        async with self.sf() as s, s.begin():  # simulate: worker claimed the delivery, then died before finishing
            await s.execute(update(Delivery).where(Delivery.account_id == acc).values(
                status="PROCESSING", locked_by="simulated-crashed-worker", locked_at=stale, attempt_count=1))
        await self.log("fault", f"simulated worker crash: {n} deliveries left PROCESSING with an expired lock")
        chaos.unblock_redis()
        done = await self.wait_terminal(timeout=_num(cfg, "timeout", 90, 20, 240))
        await self._timeline_from_audit()
        m = await self.measure()
        async with self.sf() as s:
            first = (await s.execute(select(func.min(DeliveryAttempt.attempt_number)).join(Delivery, Delivery.id == DeliveryAttempt.delivery_id)
                                     .where(Delivery.account_id == acc))).scalar_one()
        return "worker died after claiming deliveries (simulated state)", [
            await self.durable(),
            inv("E", "Stale PROCESSING work is reclaimed", done and m["recovery_actions"] >= n, f"{m['recovery_actions']} recovery actions for {n} deliveries"),
            inv("C", "At-least-once delivery", done and m["deliveries_delivered"] == m["deliveries_total"] > 0, f"{m['deliveries_delivered']}/{m['deliveries_total']} delivered"),
            inv("D", "Crashed claim consumed attempt 1; next owner was attempt 2", first == 2, f"first recorded attempt number = {first}")]

    async def _x_duplicate_request(self, cfg):
        n = int(_num(cfg, "requests", 100, 2, 300))
        acc, _, _, rcv = await self.provision("dup")
        await rcv.configure(behavior="ok")
        key = f"{self.exp_id}:same-key"
        await self.log("traffic", f"{n} concurrent requests with one Idempotency-Key")
        res = await asyncio.gather(*(ingest_event(self.sf, self.queue, self.settings, account_id=acc, idempotency_key=key,
                                                  event_type="lab.event", payload={"n": i}) for i in range(n)))
        self.generated += n
        created = [r for r in res if r.created]
        self.accepted += [r.event_id for r in created]
        self.duplicates += n - len(created)
        async with self.sf() as s:
            rows = (await s.execute(select(func.count()).select_from(Event).where(Event.account_id == acc, Event.idempotency_key == key))).scalar_one()
        done = await self.wait_terminal(timeout=60)
        m = await self.measure()
        return f"{n} simultaneous submissions", [
            await self.durable(),
            inv("B", "Idempotent ingestion: exactly one logical event", rows == 1 and len(created) == 1, f"{rows} event row(s); {len(created)} request(s) created it; {n - len(created)} deduplicated"),
            inv("B", "All callers got the same event id", len({r.event_id for r in res}) == 1, f"{len({r.event_id for r in res})} distinct event id(s) returned"),
            inv("C", "Exactly one delivery fan-out", done and m["deliveries_total"] == 1 and m["deliveries_delivered"] == 1, f"{m['deliveries_total']} delivery, {m['deliveries_delivered']} delivered")]

    async def _x_circuit_breaker(self, cfg):
        n = int(_num(cfg, "events", 12, 6, 60))
        s_ = self.settings
        acc, ep_id, _, rcv = await self.provision("cb", timeout_s=2)
        await rcv.configure(behavior="status", status=500)
        await self.log("fault", "receiver hard-down (HTTP 500 on every request)")
        await self.send(acc, n, "cb", interval_s=0.2)

        async def state() -> str:
            async with self.sf() as s:
                return (await s.execute(select(Endpoint.cb_state).where(Endpoint.id == ep_id))).scalar_one()

        async def is_open() -> bool:
            return await state() == "OPEN"

        opened = await self.wait_for(is_open, 60)
        await self.log("circuit", "circuit OPEN" if opened else "circuit never opened")
        await asyncio.sleep(2.5)  # let requests already in flight when the circuit opened drain (timeout is 2s)
        hits1 = (await rcv.stats())["received"]
        await asyncio.sleep(min(4.0, max(1.0, s_.cb_cooldown_s / 3)))
        hits2 = (await rcv.stats())["received"]
        await self.log("circuit", f"while OPEN receiver saw {hits2 - hits1} request(s) in the hold window")
        await rcv.configure(behavior="ok")
        await self.log("recovered", "receiver recovered; waiting for HALF_OPEN probe")

        async def closed() -> bool:
            return await state() == "CLOSED"

        reclosed = await self.wait_for(closed, s_.cb_cooldown_s + 60)
        await self.log("circuit", "circuit CLOSED after successful probe" if reclosed else "circuit did not close")
        done = await self.wait_terminal(timeout=90)
        m = await self.measure()
        return "receiver hard-down", [
            await self.durable(),
            inv("-", f"Breaker opened after >= {s_.cb_failure_threshold} consecutive failures", opened, f"state reached OPEN: {opened}"),
            inv("-", "Open breaker stops traffic to the unhealthy endpoint", hits2 - hits1 <= 1, f"{hits2 - hits1} request(s) reached the receiver while OPEN (allowed <= 1)"),
            inv("-", "Breaker re-closed after recovery (HALF_OPEN probe)", reclosed, f"final state CLOSED: {reclosed}"),
            inv("C", "All deliveries eventually delivered", done and m["deliveries_delivered"] == m["deliveries_total"] > 0, f"{m['deliveries_delivered']}/{m['deliveries_total']} delivered, {m['deliveries_dead']} dead")]

    async def _x_signature_tampering(self, cfg):
        acc, _, secret, rcv = await self.provision("sig")
        await rcv.configure(behavior="ok", verify_secret=secret)
        await self.send(acc, 1, "sig")
        done = await self.wait_terminal(timeout=30)
        body = signing.build_body("evt-test", "lab.event", "2026-01-01T00:00:00+00:00", {"amount": 10})
        ts = int(time.time())
        good = signing.sign(secret, ts, body)

        async def post(b: bytes, sig: str, t: int) -> int:
            async with httpx.AsyncClient(timeout=5) as c:
                return (await c.post(rcv.url, content=b, headers={signing.TIMESTAMP_HEADER: str(t), signing.SIGNATURE_HEADER: sig})).status_code

        tampered = body.replace(b"10", b"99")
        r_ok, r_tamper = await post(body, good, ts), await post(tampered, good, ts)
        r_wrong = await post(body, signing.sign("whsec_wrong", ts, body), ts)
        r_stale = await post(body, signing.sign(secret, ts - 3600, body), ts - 3600)
        st = await rcv.stats()
        await self.log("verify", f"receiver verdicts: valid={r_ok} tampered={r_tamper} wrong_secret={r_wrong} stale={r_stale}")
        self.extra = {"receiver": st}
        return "tampered body / wrong secret / replayed timestamp", [
            inv("G", "HookRelay-signed delivery verifies at the receiver", done and st["signature_valid"] >= 2 and st["signature_invalid"] == 3, f"valid={st['signature_valid']} invalid={st['signature_invalid']}"),
            inv("G", "Valid signature accepted", r_ok == 200, f"HTTP {r_ok}"),
            inv("G", "Tampered payload rejected", r_tamper == 401, f"HTTP {r_tamper}"),
            inv("G", "Wrong secret rejected", r_wrong == 401, f"HTTP {r_wrong}"),
            inv("G", "Stale timestamp (replay) rejected", r_stale == 401, f"HTTP {r_stale}")]

    async def _x_ssrf_attempt(self, cfg):
        bad = ["http://127.0.0.1/", "http://localhost:8000/", "http://[::1]/", "http://169.254.169.254/latest/meta-data/",
               "http://10.0.0.5/", "http://192.168.1.10/", "http://172.16.0.1/", "http://0.0.0.0/", "http://2130706433/",
               "http://100.64.0.1/", "file:///etc/passwd", "ftp://example.com/", "gopher://example.com/",
               "http://user:pass@example.com/", "http://metadata.google.internal/"]
        rejected = []
        for u in bad:
            try:
                validate_destination_url(u, allow_private=False)
                rejected.append((u, False))
            except SSRFError:
                rejected.append((u, True))
        for u, ok in rejected:
            await self.log("ssrf", f"{'REJECTED' if ok else 'ACCEPTED (BUG)'}: {u}")
        leaked = [u for u, ok in rejected if not ok]
        self.generated = len(bad)
        self.extra = {"urls_tested": len(bad), "urls_rejected": len(bad) - len(leaked)}
        return "dangerous destination URLs", [inv("G", "All dangerous destinations rejected", not leaked, f"{len(bad) - len(leaked)}/{len(bad)} rejected" + (f"; ACCEPTED: {leaked}" if leaked else ""))]

    async def _x_tenant_flood(self, cfg):
        flood, quiet = int(_num(cfg, "flood_events", 60, 10, 200)), int(_num(cfg, "quiet_events", 5, 1, 30))
        delay = int(_num(cfg, "delay_ms", 1500, 200, 4000))
        a, _, _, ra = await self.provision("noisy", timeout_s=delay / 1000 + 3)
        b, _, _, rb = await self.provision("quiet", timeout_s=delay / 1000 + 3)
        await ra.configure(behavior="slow", delay_ms=delay)
        await rb.configure(behavior="slow", delay_ms=delay)
        await self.log("traffic", f"noisy tenant floods {flood} events; quiet tenant sends {quiet}")
        await self.send(a, flood, "flood", concurrency=20)
        await asyncio.sleep(0.3)
        await self.send(b, quiet, "quiet")
        t0 = time.monotonic()
        quiet_done = await self.wait_terminal([b], 120)
        quiet_t = time.monotonic() - t0
        await self.log("tenant", f"quiet tenant finished in {quiet_t:.1f}s")
        flood_done = await self.wait_terminal([a], 180)
        flood_t = time.monotonic() - t0
        await self.log("tenant", f"noisy tenant finished at {flood_t:.1f}s")
        rds = await self.queue.pool()
        thr = int(await rds.get(f"hr:tenant:{a}:throttled") or 0)
        m = await self.measure()
        self.extra = {"quiet_tenant_seconds": round(quiet_t, 1), "noisy_tenant_seconds": round(flood_t, 1), "noisy_tenant_throttle_events": thr}
        return "one tenant saturating a slow receiver", [
            await self.durable(),
            inv("-", "Noisy tenant was throttled by the per-tenant cap", thr > 0, f"{thr} throttle events (cap={self.settings.tenant_max_concurrency} concurrent)"),
            inv("-", "Quiet tenant completed before the noisy tenant", quiet_done and quiet_t < flood_t, f"quiet {quiet_t:.1f}s vs noisy {flood_t:.1f}s"),
            inv("C", "All deliveries delivered for both tenants", quiet_done and flood_done and m["deliveries_delivered"] == m["deliveries_total"], f"{m['deliveries_delivered']}/{m['deliveries_total']} delivered")]

    async def _timeline_from_audit(self) -> None:
        async with self.sf() as s:
            rows = (await s.execute(select(AuditLog.action, func.min(AuditLog.created_at), func.count()).where(
                AuditLog.account_id.in_(self.accounts), AuditLog.action.like("RECOVERY_%")).group_by(AuditLog.action))).all()
        async with self.sf() as s, s.begin():
            for action, ts, n in rows:
                s.add(ExperimentEvent(experiment_id=self.exp_id, ts=ts, kind="recovery",
                                      message=f"recovery scanner: {action} x{n}", data={"action": action, "count": n}))


def inv(code: str, name: str, passed: bool, detail: str) -> dict:
    return {"invariant": code, "name": name, "passed": bool(passed), "detail": detail}


async def run_experiment(app_state, exp_id: uuid.UUID) -> None:
    sf = app_state.sf
    async with sf() as s, s.begin():
        e = await s.get(Experiment, exp_id)
        e.status, e.started_at = "RUNNING", utcnow()
        typ, cfg = e.type, dict(e.config or {})
    lab = Lab(sf, app_state.queue, app_state.settings, exp_id)
    try:
        result = await asyncio.wait_for(lab.run(typ, cfg), timeout=420)
        status, verdict = "COMPLETED", result["verdict"]
    except Exception as ex:
        logger.exception("experiment crashed")
        chaos.unblock_redis()
        result, status, verdict = {"error": type(ex).__name__, "message": str(ex)[:300]}, "FAILED", "FAIL"
    async with sf() as s, s.begin():
        e = await s.get(Experiment, exp_id)
        e.status, e.verdict, e.result = status, verdict, json.loads(json.dumps(result, default=str))
        e.finished_at, e.lab_account_id = utcnow(), (lab.accounts[0] if lab.accounts else None)
