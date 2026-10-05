"""Event ingestion: durable acceptance in PostgreSQL first, Redis enqueue second."""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core import metrics
from app.core.config import Settings
from app.core.logging import bind, log
from app.models import Delivery, Endpoint, Event, utcnow
from app.services.queue import Queue

logger = logging.getLogger("hookrelay.ingestion")


@dataclass
class IngestResult:
    event_id: uuid.UUID
    created: bool
    delivery_ids: list[uuid.UUID]


def endpoint_matches(ep: Endpoint, event_type: str) -> bool:
    types = ep.event_types or ["*"]
    return "*" in types or event_type in types


def max_attempts_for(ep: Endpoint, settings: Settings) -> int:
    return len(ep.retry_schedule) + 1 if ep.retry_schedule else settings.retry_max_attempts


async def ingest_event(sf: async_sessionmaker[AsyncSession], queue: Queue, settings: Settings, *,
                       account_id: uuid.UUID, idempotency_key: str, event_type: str, payload: dict,
                       occurred_at: datetime | None = None) -> IngestResult:
    new_id = uuid.uuid4()
    async with sf() as s, s.begin():
        # The unique constraint (account_id, idempotency_key) is the arbiter, not an application-level check.
        stmt = (pg_insert(Event).values(id=new_id, account_id=account_id, idempotency_key=idempotency_key,
                                        event_type=event_type, payload=payload, occurred_at=occurred_at or utcnow())
                .on_conflict_do_nothing(constraint="uq_events_account_idem").returning(Event.id))
        inserted = (await s.execute(stmt)).scalar_one_or_none()
        if inserted is None:
            existing = (await s.execute(select(Event.id).where(
                Event.account_id == account_id, Event.idempotency_key == idempotency_key))).scalar_one()
            metrics.EVENTS.labels("duplicate").inc()
            return IngestResult(existing, False, [])
        eps = (await s.execute(select(Endpoint).where(
            Endpoint.account_id == account_id, Endpoint.active.is_(True), Endpoint.deleted_at.is_(None)))).scalars().all()
        deliveries = [Delivery(id=uuid.uuid4(), event_id=new_id, endpoint_id=ep.id, account_id=account_id,
                               status="PENDING", max_attempts=max_attempts_for(ep, settings))
                      for ep in eps if endpoint_matches(ep, event_type)]
        s.add_all(deliveries)
    # --- committed: the event is now durable. Anything below may fail without losing it. ---
    bind(event_id=new_id)
    metrics.EVENTS.labels("accepted").inc()
    ids = [d.id for d in deliveries]
    queued: list[uuid.UUID] = []
    for d in deliveries:
        try:
            await queue.enqueue_delivery(d.id, 0, 0)
            queued.append(d.id)
        except Exception as e:  # Redis down: leave PENDING; the recovery scanner re-enqueues from PostgreSQL
            log(logger, logging.WARNING, "enqueue failed; delivery stays PENDING for recovery",
                delivery_id=str(d.id), error=type(e).__name__)
            break
    if queued:
        async with sf() as s, s.begin():
            await s.execute(update(Delivery).where(Delivery.id.in_(queued), Delivery.status == "PENDING")
                            .values(status="QUEUED"))
    return IngestResult(new_id, True, ids)
