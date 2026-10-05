from __future__ import annotations

import asyncio
import uuid

from fastapi import APIRouter, Depends, Request
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import Principal, current_user, get_session
from app.core.errors import AppError, Conflict, NotFound
from app.models import AuditLog, Experiment, ExperimentEvent
from app.reliability.lab import DESCRIPTIONS, EXPERIMENT_TYPES, run_experiment
from app.schemas.api import ExperimentIn

router = APIRouter(prefix="/experiments", tags=["reliability-lab"])


def _out(e: Experiment) -> dict:
    return {"id": str(e.id), "type": e.type, "description": DESCRIPTIONS.get(e.type), "config": e.config,
            "status": e.status, "verdict": e.verdict, "created_at": e.created_at, "started_at": e.started_at,
            "finished_at": e.finished_at}


@router.get("/types")
async def types(_: Principal = Depends(current_user)):
    return [{"type": t, "description": DESCRIPTIONS[t]} for t in EXPERIMENT_TYPES]


@router.post("", status_code=201)
async def create(body: ExperimentIn, p: Principal = Depends(current_user), s: AsyncSession = Depends(get_session)):
    if body.type not in EXPERIMENT_TYPES:
        raise AppError(f"unknown experiment type; choose one of {EXPERIMENT_TYPES}", status=422, code="invalid_experiment_type")
    e = Experiment(id=uuid.uuid4(), account_id=p.account_id, type=body.type, config=body.config)
    s.add(e)
    await s.commit()
    return _out(e)


@router.get("")
async def list_(limit: int = 50, offset: int = 0, p: Principal = Depends(current_user), s: AsyncSession = Depends(get_session)):
    limit = max(1, min(limit, 100))
    rows = (await s.execute(select(Experiment).where(Experiment.account_id == p.account_id)
                            .order_by(Experiment.created_at.desc()).limit(limit).offset(offset))).scalars().all()
    return {"items": [_out(e) for e in rows], "limit": limit, "offset": offset}


@router.get("/{eid}")
async def get(eid: uuid.UUID, p: Principal = Depends(current_user), s: AsyncSession = Depends(get_session)):
    e = (await s.execute(select(Experiment).where(Experiment.id == eid, Experiment.account_id == p.account_id))).scalar_one_or_none()
    if e is None:
        raise NotFound("experiment not found")
    tl = (await s.execute(select(ExperimentEvent).where(ExperimentEvent.experiment_id == eid)
                          .order_by(ExperimentEvent.ts, ExperimentEvent.id))).scalars().all()
    return {**_out(e), "result": e.result, "timeline": [{"ts": t.ts, "kind": t.kind, "message": t.message} for t in tl]}


@router.post("/{eid}/start", status_code=202)
async def start(eid: uuid.UUID, request: Request, p: Principal = Depends(current_user), s: AsyncSession = Depends(get_session)):
    e = (await s.execute(select(Experiment).where(Experiment.id == eid, Experiment.account_id == p.account_id))).scalar_one_or_none()
    if e is None:
        raise NotFound("experiment not found")
    if e.status != "CREATED":
        raise Conflict(f"experiment is {e.status}; create a new one to run again")
    running = request.app.state.lab_tasks
    if any(not t.done() for t in running.values()):
        raise Conflict("another experiment is running; the lab runs one at a time")
    e.status = "RUNNING"
    s.add(AuditLog(account_id=p.account_id, action="EXPERIMENT_STARTED", entity="experiment", entity_id=str(eid), data={"type": e.type}))
    await s.commit()
    running[eid] = asyncio.create_task(run_experiment(request.app.state, eid))
    return _out(e)
