"""arq worker: bounded concurrency, crash-safe tasks, recovery cron, heartbeat, Prometheus on :9101."""
from __future__ import annotations

import asyncio
import logging
import uuid

import httpx
import redis.asyncio as aioredis
from arq import cron
from arq.connections import RedisSettings
from prometheus_client import start_http_server

from app.core.config import get_settings
from app.core.logging import bind, clear, log, setup_logging
from app.db.session import make_engine, make_sessionmaker
from app.services.delivery import Runtime, process_delivery
from app.services.queue import Queue
from app.services.recovery import run_recovery

logger = logging.getLogger("hookrelay.worker")
settings = get_settings()


async def deliver(ctx: dict, delivery_id: str) -> str:
    clear()
    try:
        return await process_delivery(ctx["rt"], uuid.UUID(delivery_id))
    except Exception:  # a bad job must never take the worker down; recovery handles a stuck PROCESSING row
        logger.exception("delivery task crashed", extra={"fields": {"delivery_id": delivery_id}})
        return "error"


async def recovery_job(ctx: dict) -> dict:
    rep = await run_recovery(ctx["rt"].sf, ctx["rt"].queue, settings)
    return rep.to_dict()


async def _heartbeat(rds, worker_id: str) -> None:
    while True:
        try:
            await rds.set(f"hr:worker:{worker_id}", "1", ex=15)
        except Exception:
            pass
        await asyncio.sleep(5)


async def startup(ctx: dict) -> None:
    setup_logging()
    engine = make_engine(settings.database_url, pool_size=settings.worker_max_jobs + 5, max_overflow=5)
    rds = aioredis.from_url(settings.redis_url)
    http = httpx.AsyncClient(limits=httpx.Limits(max_connections=settings.worker_max_jobs * 2))
    wid = f"w-{uuid.uuid4().hex[:8]}"
    ctx["rt"] = Runtime(make_sessionmaker(engine), Queue(settings.redis_url), rds, http, settings, wid)
    ctx["engine"], ctx["hb"] = engine, asyncio.create_task(_heartbeat(rds, wid))
    try:
        start_http_server(settings.worker_metrics_port)
    except OSError:
        pass
    bind(worker_id=wid)
    log(logger, logging.INFO, "worker started", max_jobs=settings.worker_max_jobs)


async def shutdown(ctx: dict) -> None:
    ctx["hb"].cancel()
    rt: Runtime = ctx["rt"]
    await rt.http.aclose()
    await rt.rds.aclose()
    await rt.queue.close()
    await ctx["engine"].dispose()


class WorkerSettings:
    functions = [deliver]
    cron_jobs = [cron(recovery_job, second=set(range(0, 60, max(1, settings.recovery_interval_s))),
                      run_at_startup=True, unique=True)]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    max_jobs = settings.worker_max_jobs
    job_timeout = int(settings.delivery_timeout_s * 3 + 30)
    keep_result = 0
    max_tries = 1
    health_check_interval = 10
