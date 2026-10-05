"""Dashboard stats, tenants, health/readiness, Prometheus metrics, manual recovery."""
from __future__ import annotations

import uuid
from datetime import timedelta

from fastapi import APIRouter, Depends, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import case, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import Principal, admin_user, current_user, get_session
from app.api.routes_auth_endpoints import endpoint_out
from app.core import metrics
from app.core.errors import NotFound
from app.models import Account, Delivery, DeliveryAttempt, Endpoint, Event, utcnow
from app.services.recovery import run_recovery

router = APIRouter()


@router.get("/health", tags=["ops"])
async def health():
    return {"status": "ok"}  # process liveness only


@router.get("/ready", tags=["ops"])
async def ready(request: Request):
    checks: dict[str, str] = {}
    try:
        async with request.app.state.sf() as s:
            await s.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = f"unavailable ({type(e).__name__})"
    try:
        await request.app.state.queue.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"unavailable ({type(e).__name__})"
    ok = all(v == "ok" for v in checks.values())
    return JSONResponse({"status": "ready" if ok else "not_ready", "checks": checks}, status_code=200 if ok else 503)


@router.get("/metrics", tags=["ops"])
async def prom_metrics(request: Request):
    try:
        async with request.app.state.sf() as s:
            for st, n in (await s.execute(select(Delivery.status, func.count()).group_by(Delivery.status))):
                metrics.BACKLOG.labels(st).set(n)
        metrics.QUEUE_DEPTH.set(await request.app.state.queue.depth())
    except Exception:
        pass  # metrics endpoint must stay available when a dependency is down
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/stats/overview", tags=["stats"])
async def overview(request: Request, p: Principal = Depends(current_user), s: AsyncSession = Depends(get_session)):
    now = utcnow()
    h1, m1 = now - timedelta(hours=1), now - timedelta(minutes=1)
    acc = p.account_id
    by_status = dict((await s.execute(select(Delivery.status, func.count()).where(Delivery.account_id == acc)
                                      .group_by(Delivery.status))).all())
    ev_1h = (await s.execute(select(func.count()).select_from(Event).where(Event.account_id == acc, Event.created_at > h1))).scalar_one()
    ev_1m = (await s.execute(select(func.count()).select_from(Event).where(Event.account_id == acc, Event.created_at > m1))).scalar_one()
    A = DeliveryAttempt
    base = (select(A).join(Delivery, Delivery.id == A.delivery_id).where(Delivery.account_id == acc, A.completed_at > h1))
    sub = base.subquery()
    agg = (await s.execute(select(
        func.count(), func.count().filter(sub.c.error_class == "SUCCESS"), func.count().filter(sub.c.will_retry.is_(True)),
        func.percentile_cont(0.5).within_group(sub.c.duration_ms), func.percentile_cont(0.95).within_group(sub.c.duration_ms),
        func.percentile_cont(0.99).within_group(sub.c.duration_ms)).select_from(sub))).one()
    attempts, ok, retried = agg[0], agg[1], agg[2]
    eps = (await s.execute(select(Endpoint).where(Endpoint.account_id == acc, Endpoint.deleted_at.is_(None)))).scalars().all()
    failures = (await s.execute(select(A.delivery_id, A.attempt_number, A.error_detail, A.http_status, A.completed_at,
                                       Delivery.endpoint_id).join(Delivery, Delivery.id == A.delivery_id)
                                .where(Delivery.account_id == acc, A.error_class != "SUCCESS")
                                .order_by(A.completed_at.desc()).limit(10))).all()
    workers = 0
    queue_depth = None
    try:
        pool = await request.app.state.queue.pool()
        workers = len([k async for k in pool.scan_iter(match="hr:worker:*", count=100)])
        queue_depth = await request.app.state.queue.depth()
    except Exception:
        pass
    return {
        "events_last_hour": ev_1h, "events_per_sec": round(ev_1m / 60, 3), "deliveries_by_status": by_status,
        "attempts_last_hour": attempts, "delivery_success_rate": round(ok / attempts, 4) if attempts else None,
        "retry_rate": round(retried / attempts, 4) if attempts else None,
        "latency_ms": {"p50": agg[3], "p95": agg[4], "p99": agg[5]},
        "dead": by_status.get("DEAD", 0), "queue_depth": queue_depth,
        "backlog_in_postgres": sum(by_status.get(k, 0) for k in ("PENDING", "QUEUED", "RETRYING", "PROCESSING")),
        "active_workers": workers,
        "endpoints": [{"id": str(e.id), "url": e.url, **{k: v for k, v in endpoint_out(e).items() if k in ("health", "circuit")}} for e in eps],
        "recent_failures": [{"delivery_id": str(d), "attempt": n, "detail": det, "http_status": hs, "at": at,
                             "endpoint_id": str(ep)} for d, n, det, hs, at, ep in failures],
    }


@router.get("/stats/timeseries", tags=["stats"])
async def timeseries(p: Principal = Depends(current_user), s: AsyncSession = Depends(get_session)):
    since = utcnow() - timedelta(minutes=30)
    em = func.date_trunc("minute", Event.created_at).label("m")
    ev = dict((await s.execute(select(em, func.count()).where(Event.account_id == p.account_id, Event.created_at > since)
                               .group_by(em))).all())
    A = DeliveryAttempt
    am = func.date_trunc("minute", A.completed_at).label("m")
    at = (await s.execute(select(am, func.count().filter(A.error_class == "SUCCESS"),
                                 func.count().filter(A.error_class != "SUCCESS")).join(Delivery, Delivery.id == A.delivery_id)
                          .where(Delivery.account_id == p.account_id, A.completed_at > since).group_by(am))).all()
    okf = {k: (o, f) for k, o, f in at}
    mins = sorted(set(ev) | set(okf))
    return [{"minute": m, "events": ev.get(m, 0), "attempts_ok": okf.get(m, (0, 0))[0],
             "attempts_failed": okf.get(m, (0, 0))[1]} for m in mins]


@router.get("/tenants", tags=["tenants"])
async def tenants(request: Request, p: Principal = Depends(current_user), s: AsyncSession = Depends(get_session)):
    return {"items": await _tenant_rows(request, s, None if p.is_admin else [p.account_id])}


@router.get("/tenants/{tid}", tags=["tenants"])
async def tenant(tid: uuid.UUID, request: Request, p: Principal = Depends(current_user),
                 s: AsyncSession = Depends(get_session)):
    if not p.is_admin and tid != p.account_id:
        raise NotFound("tenant not found")
    rows = await _tenant_rows(request, s, [tid])
    if not rows:
        raise NotFound("tenant not found")
    return rows[0]


async def _tenant_rows(request: Request, s: AsyncSession, only: list | None) -> list[dict]:
    now = utcnow()
    accs = (await s.execute(select(Account).where(Account.id.in_(only)) if only else select(Account)
                            .order_by(Account.created_at.desc()).limit(100))).scalars().all()
    ids = [a.id for a in accs]
    D = Delivery
    rows = (await s.execute(select(
        D.account_id,
        func.count().filter(D.status.in_(("PENDING", "QUEUED", "RETRYING"))),
        func.count().filter(D.status == "PROCESSING"),
        func.count().filter(D.status == "DEAD"),
        func.count().filter((D.status == "DELIVERED") & (D.delivered_at > now - timedelta(hours=1))))
        .where(D.account_id.in_(ids)).group_by(D.account_id))).all()
    d = {r[0]: r[1:] for r in rows}
    ev = dict((await s.execute(select(Event.account_id, func.count()).where(
        Event.account_id.in_(ids), Event.created_at > now - timedelta(minutes=1)).group_by(Event.account_id))).all())
    out = []
    redis = None
    try:
        redis = await request.app.state.queue.pool()
    except Exception:
        pass
    for a in accs:
        q, act, dead, dlv = d.get(a.id, (0, 0, 0, 0))
        throttled = active_now = None
        if redis is not None:
            try:
                throttled = int(await redis.get(f"hr:tenant:{a.id}:throttled") or 0)
                active_now = int(await redis.get(f"hr:tenant:{a.id}:active") or 0)
            except Exception:
                pass
        out.append({"id": str(a.id), "name": a.name, "is_lab": a.is_lab, "queue_depth": q, "processing": act,
                    "dead": dead, "delivered_last_hour": dlv, "events_per_sec": round(ev.get(a.id, 0) / 60, 3),
                    "throttled_total": throttled, "active_now": active_now})
    return out


@router.post("/admin/recovery", tags=["ops"])
async def manual_recovery(request: Request, _: Principal = Depends(admin_user)):
    rep = await run_recovery(request.app.state.sf, request.app.state.queue, request.app.state.settings)
    return rep.to_dict()
