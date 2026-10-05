from __future__ import annotations

import secrets
import time
import uuid

import jwt
from fastapi import APIRouter, Depends, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import Principal, current_user, get_session
from app.core.errors import AppError, NotFound, Unauthorized
from app.models import AuditLog, Endpoint, User
from app.schemas.api import EndpointIn, EndpointPatch, LoginIn, TokenOut
from app.security.hashing import verify_password
from app.security.ssrf import SSRFError, validate_destination_url_async
from app.services import circuit_breaker as cb
from app.services.circuit_store import state_of

router = APIRouter()
_DUMMY_HASH = "scrypt$16384$8$1$" + "00" * 16 + "$" + "00" * 64


@router.post("/auth/login", response_model=TokenOut, tags=["auth"])
async def login(body: LoginIn, request: Request, s: AsyncSession = Depends(get_session)):
    user = (await s.execute(select(User).where(User.email == body.email.lower()))).scalar_one_or_none()
    ok = verify_password(body.password, user.password_hash if user else _DUMMY_HASH)  # constant-ish time
    if not user or not ok:
        raise Unauthorized("invalid credentials")
    cfg = request.app.state.settings
    token = jwt.encode({"sub": str(user.id), "acc": str(user.account_id), "adm": user.is_admin,
                        "exp": int(time.time()) + cfg.jwt_ttl_s}, cfg.jwt_secret, algorithm="HS256")
    return TokenOut(access_token=token, expires_in=cfg.jwt_ttl_s)


def endpoint_out(ep: Endpoint, with_secret: bool = False) -> dict:
    st = state_of(ep)
    out = {"id": str(ep.id), "url": ep.url, "description": ep.description, "event_types": ep.event_types,
           "active": ep.active, "rate_limit_per_sec": ep.rate_limit_per_sec, "retry_schedule": ep.retry_schedule,
           "timeout_s": ep.timeout_s, "circuit": {"state": st.state, "failures": ep.cb_failures,
                                                  "opened_at": ep.cb_opened_at},
           "health": cb.health(st), "created_at": ep.created_at}
    if with_secret:
        out["signing_secret"] = ep.signing_secret  # returned exactly once, at creation
    return out


async def _check_url(request: Request, url: str) -> None:
    cfg = request.app.state.settings
    try:
        await validate_destination_url_async(url, allow_private=cfg.allow_private_destinations,
                                             require_https=cfg.require_https)
    except SSRFError as e:
        raise AppError(f"destination rejected: {e}", status=422, code="ssrf_blocked") from e


@router.post("/endpoints", status_code=201, tags=["endpoints"])
async def create_endpoint(body: EndpointIn, request: Request, p: Principal = Depends(current_user),
                          s: AsyncSession = Depends(get_session)):
    await _check_url(request, body.url)
    ep = Endpoint(id=uuid.uuid4(), account_id=p.account_id, url=body.url, description=body.description,
                  event_types=body.event_types or ["*"], signing_secret="whsec_" + secrets.token_urlsafe(32),
                  rate_limit_per_sec=body.rate_limit_per_sec, retry_schedule=body.retry_schedule,
                  timeout_s=body.timeout_s)
    s.add(ep)
    s.add(AuditLog(account_id=p.account_id, action="ENDPOINT_CREATED", entity="endpoint", entity_id=str(ep.id)))
    await s.commit()
    await s.refresh(ep)
    return endpoint_out(ep, with_secret=True)


@router.get("/endpoints", tags=["endpoints"])
async def list_endpoints(limit: int = 50, offset: int = 0, p: Principal = Depends(current_user),
                         s: AsyncSession = Depends(get_session)):
    limit = max(1, min(limit, 100))
    q = select(Endpoint).where(Endpoint.account_id == p.account_id, Endpoint.deleted_at.is_(None))
    total = (await s.execute(select(func.count()).select_from(q.subquery()))).scalar_one()
    rows = (await s.execute(q.order_by(Endpoint.created_at.desc()).limit(limit).offset(offset))).scalars().all()
    return {"items": [endpoint_out(r) for r in rows], "total": total, "limit": limit, "offset": offset}


async def _get(s: AsyncSession, p: Principal, eid: uuid.UUID) -> Endpoint:
    ep = (await s.execute(select(Endpoint).where(Endpoint.id == eid, Endpoint.account_id == p.account_id,
                                                 Endpoint.deleted_at.is_(None)))).scalar_one_or_none()
    if ep is None:
        raise NotFound("endpoint not found")
    return ep


@router.get("/endpoints/{eid}", tags=["endpoints"])
async def get_endpoint(eid: uuid.UUID, p: Principal = Depends(current_user), s: AsyncSession = Depends(get_session)):
    return endpoint_out(await _get(s, p, eid))


@router.patch("/endpoints/{eid}", tags=["endpoints"])
async def patch_endpoint(eid: uuid.UUID, body: EndpointPatch, request: Request, p: Principal = Depends(current_user),
                         s: AsyncSession = Depends(get_session)):
    ep = await _get(s, p, eid)
    if body.url is not None:
        await _check_url(request, body.url)
        ep.url = body.url
    for f in ("description", "event_types", "active", "rate_limit_per_sec"):
        v = getattr(body, f)
        if v is not None:
            setattr(ep, f, v)
    if body.reset_circuit:
        ep.cb_state, ep.cb_failures, ep.cb_opened_at, ep.cb_probe_at = "CLOSED", 0, None, None
    s.add(AuditLog(account_id=p.account_id, action="ENDPOINT_UPDATED", entity="endpoint", entity_id=str(ep.id),
                   data=body.model_dump(exclude_none=True)))
    await s.commit()
    return endpoint_out(ep)


@router.delete("/endpoints/{eid}", status_code=204, tags=["endpoints"])
async def delete_endpoint(eid: uuid.UUID, p: Principal = Depends(current_user), s: AsyncSession = Depends(get_session)):
    from app.models import utcnow
    ep = await _get(s, p, eid)
    ep.active, ep.deleted_at = False, utcnow()  # soft delete keeps delivery history intact
    s.add(AuditLog(account_id=p.account_id, action="ENDPOINT_DELETED", entity="endpoint", entity_id=str(ep.id)))
    await s.commit()
