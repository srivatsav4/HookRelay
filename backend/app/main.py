from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import update

from app.api import routes_auth_endpoints, routes_events_deliveries, routes_experiments, routes_ops
from app.core.config import get_settings
from app.core.errors import AppError
from app.core.logging import bind, clear, log, setup_logging
from app.db.session import make_engine, make_sessionmaker
from app.models import Experiment, utcnow
from app.services.queue import Queue

logger = logging.getLogger("hookrelay.api")


def _err(status: int, code: str, message: str, request: Request, extra=None) -> JSONResponse:
    body = {"error": {"code": code, "message": message, "request_id": getattr(request.state, "request_id", None)}}
    if extra:
        body["error"]["details"] = extra
    return JSONResponse(body, status_code=status, headers={"X-Request-ID": body["error"]["request_id"] or ""})


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    setup_logging()
    engine = make_engine(settings.database_url)
    app.state.settings, app.state.engine = settings, engine
    app.state.sf, app.state.queue, app.state.lab_tasks = make_sessionmaker(engine), Queue(settings.redis_url), {}
    try:  # experiments left RUNNING by a previous process can never finish
        async with app.state.sf() as s, s.begin():
            await s.execute(update(Experiment).where(Experiment.status == "RUNNING").values(
                status="FAILED", verdict="FAIL", finished_at=utcnow(), result={"error": "api_restarted_during_run"}))
    except Exception:
        log(logger, logging.WARNING, "startup experiment cleanup skipped (database not ready?)")
    yield
    await app.state.queue.close()
    await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="HookRelay", version="1.0.0", lifespan=lifespan,
                  description="Fault-tolerant event delivery & reliability platform")
    s = get_settings()
    app.add_middleware(CORSMiddleware, allow_origins=list(s.cors_origins), allow_methods=["*"],
                       allow_headers=["*"], expose_headers=["X-Request-ID"])

    @app.middleware("http")
    async def request_id(request: Request, call_next):
        clear()
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex
        request.state.request_id = rid[:64]
        bind(request_id=request.state.request_id)
        resp = await call_next(request)
        resp.headers["X-Request-ID"] = request.state.request_id
        return resp

    @app.exception_handler(AppError)
    async def _app_error(request: Request, exc: AppError):
        return _err(exc.status, exc.code, exc.message, request)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError):
        details = [{"loc": list(e["loc"]), "msg": e["msg"]} for e in exc.errors()]
        return _err(422, "validation_error", "request validation failed", request, details)

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception):
        logger.exception("unhandled error")
        return _err(500, "internal_error", "internal server error", request)

    for r in (routes_ops.router, routes_auth_endpoints.router, routes_events_deliveries.router, routes_experiments.router):
        app.include_router(r)
    return app


app = create_app()
