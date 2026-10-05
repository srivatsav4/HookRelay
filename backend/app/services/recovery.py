"""Recovery scanner: PostgreSQL is authoritative, so work that Redis lost is re-derived from it."""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import timedelta

from sqlalchemy import case, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core import metrics
from app.core.config import Settings
from app.core.logging import log
from app.models import AuditLog, Delivery, utcnow
from app.services.queue import Queue

logger = logging.getLogger("hookrelay.recovery")


@dataclass
class RecoveryReport:
    stale_processing_reset: int = 0
    stale_processing_dead: int = 0
    pending_reenqueued: int = 0
    queued_reenqueued: int = 0
    retrying_reenqueued: int = 0
    redis_errors: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


async def run_recovery(sf: async_sessionmaker[AsyncSession], queue: Queue, settings: Settings) -> RecoveryReport:
    rep = RecoveryReport()
    now = utcnow()

    # 1. Stale PROCESSING = worker died after claiming. The attempt was consumed (it may have reached the
    #    receiver), so retrying is at-least-once behaviour, never a silent loss.
    async with sf() as s, s.begin():
        res = await s.execute(
            update(Delivery)
            .where(Delivery.status == "PROCESSING",
                   Delivery.locked_at < now - timedelta(seconds=settings.visibility_timeout_s))
            .values(status=case((Delivery.attempt_count >= Delivery.max_attempts, "DEAD"), else_="RETRYING"),
                    locked_by=None, next_attempt_at=now, last_error="stale_processing_recovered")
            .returning(Delivery.id, Delivery.status, Delivery.account_id))
        for did, st, acc in res.all():
            s.add(AuditLog(account_id=acc, action="RECOVERY_STALE_PROCESSING", entity="delivery", entity_id=str(did),
                           data={"new_status": st}))
            if st == "DEAD":
                rep.stale_processing_dead += 1
            else:
                rep.stale_processing_reset += 1

    # 2. Find deliveries whose job may be missing from Redis.
    conds = [
        ("pending", Delivery.status == "PENDING",
         Delivery.created_at < now - timedelta(seconds=settings.recovery_pending_grace_s)),
        ("queued", Delivery.status == "QUEUED",
         Delivery.updated_at < now - timedelta(seconds=settings.recovery_queued_grace_s)),
        ("retrying", Delivery.status == "RETRYING",
         Delivery.next_attempt_at <= now - timedelta(seconds=settings.recovery_pending_grace_s)),
    ]
    for reason, a, b in conds:
        async with sf() as s:
            rows = (await s.execute(select(Delivery.id, Delivery.attempt_count, Delivery.release_count,
                                           Delivery.account_id).where(a, b).order_by(Delivery.created_at)
                                    .limit(settings.recovery_batch))).all()
        done: list = []
        recovered: list = []
        for did, ac, rc, acc in rows:
            try:
                new_job = await queue.enqueue_delivery(did, ac, rc)  # deterministic job id => idempotent
            except Exception:
                rep.redis_errors += 1
                break
            done.append((did, acc))
            if new_job:
                recovered.append((did, acc))
                metrics.RECOVERED.labels(reason).inc()
                setattr(rep, f"{reason}_reenqueued", getattr(rep, f"{reason}_reenqueued") + 1)
        if done:
            ids = [d for d, _ in done]
            async with sf() as s, s.begin():
                # touch updated_at so QUEUED rows are not rescanned every cycle; PENDING becomes QUEUED
                if reason == "pending":
                    await s.execute(update(Delivery).where(Delivery.id.in_(ids), Delivery.status == "PENDING")
                                    .values(status="QUEUED"))
                elif reason == "queued":
                    await s.execute(update(Delivery).where(Delivery.id.in_(ids), Delivery.status == "QUEUED")
                                    .values(updated_at=now))
                for did, acc in recovered:  # audit only jobs that were actually missing from Redis
                    s.add(AuditLog(account_id=acc, action="RECOVERY_REENQUEUE", entity="delivery",
                                   entity_id=str(did), data={"reason": reason}))
    if any(v for k, v in rep.to_dict().items() if k != "redis_errors"):
        log(logger, logging.WARNING, "recovery acted", **rep.to_dict())
    return rep
