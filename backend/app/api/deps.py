from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass

import jwt
from fastapi import Depends, Header, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.errors import Forbidden, Unauthorized
from app.models import ApiKey
from app.security.hashing import parse_api_key, verify_api_key


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.sf() as s:
        yield s


@dataclass
class Principal:
    user_id: uuid.UUID
    account_id: uuid.UUID
    is_admin: bool


def current_user(request: Request, authorization: str | None = Header(default=None)) -> Principal:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise Unauthorized("missing bearer token")
    try:
        claims = jwt.decode(authorization[7:], request.app.state.settings.jwt_secret, algorithms=["HS256"])
        return Principal(uuid.UUID(claims["sub"]), uuid.UUID(claims["acc"]), bool(claims.get("adm")))
    except (jwt.PyJWTError, KeyError, ValueError):
        raise Unauthorized("invalid or expired token") from None


def admin_user(p: Principal = Depends(current_user)) -> Principal:
    if not p.is_admin:
        raise Forbidden("admin only")
    return p


async def api_key_account(request: Request, x_api_key: str | None = Header(default=None),
                          s: AsyncSession = Depends(get_session)) -> uuid.UUID:
    prefix = parse_api_key(x_api_key or "")
    if not prefix:
        raise Unauthorized("missing or malformed X-API-Key")
    row = (await s.execute(select(ApiKey).where(ApiKey.prefix == prefix, ApiKey.revoked.is_(False)))).scalar_one_or_none()
    if row is None or not verify_api_key(x_api_key, row.key_hash, request.app.state.settings.api_key_pepper):
        raise Unauthorized("invalid API key")
    return row.account_id
