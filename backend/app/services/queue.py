"""Redis (arq) job transport. Redis is a *transport*; PostgreSQL is authoritative."""
from __future__ import annotations

import uuid
from datetime import timedelta

from arq import create_pool
from arq.connections import ArqRedis, RedisSettings

from app.reliability.chaos import chaos


def job_id(delivery_id: uuid.UUID, attempt_count: int, release_count: int) -> str:
    """Deterministic id => re-enqueueing the same logical job is a no-op (dedupe)."""
    return f"{delivery_id}:{attempt_count}:{release_count}"


class Queue:
    def __init__(self, redis_url: str) -> None:
        self._settings = RedisSettings.from_dsn(redis_url)
        self._pool: ArqRedis | None = None

    async def pool(self) -> ArqRedis:
        if self._pool is None:
            self._pool = await create_pool(self._settings)
        return self._pool

    async def enqueue_delivery(self, delivery_id: uuid.UUID, attempt_count: int, release_count: int,
                               defer_s: float = 0.0) -> bool:
        """True if a new job was created, False if an identical job already exists. Raises on Redis failure."""
        if chaos.redis_blocked():
            raise ConnectionError("redis unavailable (injected by Reliability Lab)")
        pool = await self.pool()
        job = await pool.enqueue_job(
            "deliver", str(delivery_id), _job_id=job_id(delivery_id, attempt_count, release_count),
            _defer_by=timedelta(seconds=defer_s) if defer_s > 0 else None)
        return job is not None

    async def depth(self) -> int:
        pool = await self.pool()
        return int(await pool.zcard("arq:queue"))

    async def ping(self) -> bool:
        pool = await self.pool()
        return bool(await pool.ping())

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.aclose()
            self._pool = None
