"""Persists circuit-breaker state on the endpoint row (row lock serialises concurrent workers)."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core import metrics
from app.core.logging import log
from app.models import Endpoint
from app.services import circuit_breaker as cb

logger = logging.getLogger("hookrelay.circuit")


def state_of(ep: Endpoint) -> cb.CBState:
    return cb.CBState(ep.cb_state, ep.cb_failures, ep.cb_opened_at, ep.cb_probe_at)


def _write(ep: Endpoint, st: cb.CBState) -> None:
    ep.cb_state, ep.cb_failures, ep.cb_opened_at, ep.cb_probe_at = st.state, st.failures, st.opened_at, st.probe_at


def _note(ep_id: uuid.UUID, transition: str | None) -> None:
    if transition:
        metrics.CB_TRANSITIONS.labels(transition).inc()
        log(logger, logging.WARNING, "circuit transition", endpoint_id=str(ep_id), transition=transition)


async def check(sf: async_sessionmaker[AsyncSession], endpoint_id: uuid.UUID, cfg: cb.CBConfig,
                now: datetime) -> tuple[bool, float]:
    async with sf() as s, s.begin():
        ep = (await s.execute(select(Endpoint).where(Endpoint.id == endpoint_id).with_for_update())).scalar_one()
        allowed, wait, st, tr = cb.allow(state_of(ep), now, cfg)
        _write(ep, st)
        _note(endpoint_id, tr)
        return allowed, wait


async def record(sf: async_sessionmaker[AsyncSession], endpoint_id: uuid.UUID, ok: bool, counts_as_failure: bool,
                 cfg: cb.CBConfig, now: datetime) -> None:
    async with sf() as s, s.begin():
        ep = (await s.execute(select(Endpoint).where(Endpoint.id == endpoint_id).with_for_update())).scalar_one()
        st = state_of(ep)
        if ok:
            st, tr = cb.on_success(st, cfg)
        elif counts_as_failure:
            st, tr = cb.on_failure(st, now, cfg)
        else:
            # reachable endpoint answering 4xx/429: not a breaker failure, but a HALF_OPEN probe proved liveness
            st, tr = cb.on_success(st, cfg) if st.state == cb.HALF_OPEN else (st, None)
        _write(ep, st)
        _note(endpoint_id, tr)
