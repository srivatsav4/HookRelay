from __future__ import annotations

import json
import uuid

from fastapi import APIRouter, Depends, Header, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import Principal, api_key_account, current_user, get_session
from app.core.errors import AppError, Conflict, NotFound
from app.models import AuditLog, Delivery, DeliveryAttempt, Event
from app.schemas.api import EventAccepted, EventIn
from app.services.ingestion import ingest_event

router = APIRouter()


@router.post("/events", status_code=202, response_model=EventAccepted, tags=["events"])
async def post_event(body: EventIn, request: Request, account_id: uuid.UUID = Depends(api_key_account),
                     idempotency_key: str | None = Header(default=None, alias="Idempotency-Key")):
    cfg = request.app.state.settings
    if not idempotency_key or not (1 <= len(idempotency_key) <= 255):
        raise AppError("Idempotency-Key header (1-255 chars) is required", status=400, code="idempotency_key_required")
    if len(json.dumps(body.payload)) > cfg.max_payload_bytes:
        raise AppError("payload too large", status=413, code="payload_too_large")
    r = await ingest_event(request.app.state.sf, request.app.state.queue, cfg, account_id=account_id,
                           idempotency_key=idempotency_key, event_type=body.type, payload=body.payload,
                           occurred_at=body.timestamp)
    return EventAccepted(event_id=r.event_id, duplicate=not r.created, deliveries=len(r.delivery_ids),
                         delivery_ids=r.delivery_ids)


def _event(e: Event) -> dict:
    return {"id": str(e.id), "type": e.event_type, "idempotency_key": e.idempotency_key,
            "occurred_at": e.occurred_at, "created_at": e.created_at}


@router.get("/events", tags=["events"])
async def list_events(limit: int = 50, offset: int = 0, p: Principal = Depends(current_user),
                      s: AsyncSession = Depends(get_session)):
    limit = max(1, min(limit, 100))
    rows = (await s.execute(select(Event).where(Event.account_id == p.account_id)
                            .order_by(Event.created_at.desc()).limit(limit).offset(offset))).scalars().all()
    ids = [r.id for r in rows]
    counts: dict = {}
    if ids:  # one grouped query instead of N queries
        for eid, st, n in (await s.execute(select(Delivery.event_id, Delivery.status, func.count())
                                           .where(Delivery.event_id.in_(ids)).group_by(Delivery.event_id, Delivery.status))):
            counts.setdefault(eid, {})[st] = n
    return {"items": [{**_event(r), "deliveries": counts.get(r.id, {})} for r in rows], "limit": limit, "offset": offset}


@router.get("/events/{eid}", tags=["events"])
async def get_event(eid: uuid.UUID, p: Principal = Depends(current_user), s: AsyncSession = Depends(get_session)):
    e = (await s.execute(select(Event).where(Event.id == eid, Event.account_id == p.account_id))).scalar_one_or_none()
    if e is None:
        raise NotFound("event not found")
    ds = (await s.execute(select(Delivery).where(Delivery.event_id == eid).order_by(Delivery.created_at))).scalars().all()
    return {**_event(e), "payload": e.payload, "deliveries": [_delivery(d) for d in ds]}


def _delivery(d: Delivery) -> dict:
    return {"id": str(d.id), "event_id": str(d.event_id), "endpoint_id": str(d.endpoint_id), "status": d.status,
            "attempt_count": d.attempt_count, "max_attempts": d.max_attempts, "next_attempt_at": d.next_attempt_at,
            "last_error": d.last_error, "last_status_code": d.last_status_code, "replay_of": str(d.replay_of) if d.replay_of else None,
            "created_at": d.created_at, "delivered_at": d.delivered_at}


@router.get("/deliveries", tags=["deliveries"])
async def list_deliveries(status: str | None = None, endpoint_id: uuid.UUID | None = None,
                          event_id: uuid.UUID | None = None, limit: int = 50, offset: int = 0,
                          p: Principal = Depends(current_user), s: AsyncSession = Depends(get_session)):
    limit = max(1, min(limit, 100))
    q = select(Delivery).where(Delivery.account_id == p.account_id)
    if status:
        q = q.where(Delivery.status == status.upper())
    if endpoint_id:
        q = q.where(Delivery.endpoint_id == endpoint_id)
    if event_id:
        q = q.where(Delivery.event_id == event_id)
    rows = (await s.execute(q.order_by(Delivery.created_at.desc()).limit(limit).offset(offset))).scalars().all()
    return {"items": [_delivery(d) for d in rows], "limit": limit, "offset": offset}


async def _owned(s: AsyncSession, p: Principal, did: uuid.UUID) -> Delivery:
    d = (await s.execute(select(Delivery).where(Delivery.id == did, Delivery.account_id == p.account_id))).scalar_one_or_none()
    if d is None:
        raise NotFound("delivery not found")
    return d


@router.get("/deliveries/{did}", tags=["deliveries"])
async def get_delivery(did: uuid.UUID, p: Principal = Depends(current_user), s: AsyncSession = Depends(get_session)):
    d = await _owned(s, p, did)
    atts = (await s.execute(select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == did)
                            .order_by(DeliveryAttempt.attempt_number))).scalars().all()
    replays = (await s.execute(select(Delivery.id, Delivery.status).where(Delivery.replay_of == did))).all()
    audit = (await s.execute(select(AuditLog).where(AuditLog.entity == "delivery", AuditLog.entity_id == str(did))
                             .order_by(AuditLog.created_at))).scalars().all()
    ev = await s.get(Event, d.event_id)
    return {**_delivery(d), "event": _event(ev),
            "attempts": [{"attempt_number": a.attempt_number, "started_at": a.started_at, "completed_at": a.completed_at,
                          "http_status": a.http_status, "timeout_s": a.timeout_s, "duration_ms": round(a.duration_ms, 1),
                          "error_class": a.error_class, "error_detail": a.error_detail, "will_retry": a.will_retry,
                          "retry_decision": a.retry_decision, "retry_delay_s": a.retry_delay_s} for a in atts],
            "replays": [{"id": str(i), "status": st} for i, st in replays],
            "audit": [{"action": a.action, "at": a.created_at, "data": a.data} for a in audit]}


@router.post("/deliveries/{did}/replay", status_code=202, tags=["deliveries"])
async def replay(did: uuid.UUID, request: Request, p: Principal = Depends(current_user),
                 s: AsyncSession = Depends(get_session)):
    """Invariant H: replay creates a NEW delivery linked via replay_of; the DEAD one and its attempts are untouched."""
    d = await _owned(s, p, did)
    if d.status != "DEAD":
        raise Conflict(f"only DEAD deliveries can be replayed (status is {d.status})")
    new = Delivery(id=uuid.uuid4(), event_id=d.event_id, endpoint_id=d.endpoint_id, account_id=d.account_id,
                   status="PENDING", max_attempts=d.max_attempts, replay_of=d.id)
    s.add(new)
    s.add(AuditLog(account_id=p.account_id, action="DELIVERY_REPLAYED", entity="delivery", entity_id=str(did),
                   data={"new_delivery_id": str(new.id), "by_user": str(p.user_id)}))
    await s.commit()
    try:
        await request.app.state.queue.enqueue_delivery(new.id, 0, 0)
        from sqlalchemy import update
        await s.execute(update(Delivery).where(Delivery.id == new.id, Delivery.status == "PENDING").values(status="QUEUED"))
        await s.commit()
    except Exception:
        pass  # stays PENDING; recovery scanner will enqueue it
    return {"replayed_from": str(did), "new_delivery_id": str(new.id)}
