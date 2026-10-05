"""Scoped fault injection. Only toggles in-process gates used by the demo Reliability Lab; no shell access,
no arbitrary commands. The gate lives in the API process and affects the enqueue path only."""
from __future__ import annotations

import time


class Chaos:
    def __init__(self) -> None:
        self._redis_blocked_until = 0.0

    def block_redis(self, seconds: float) -> None:
        self._redis_blocked_until = time.monotonic() + seconds

    def unblock_redis(self) -> None:
        self._redis_blocked_until = 0.0

    def redis_blocked(self) -> bool:
        return time.monotonic() < self._redis_blocked_until


chaos = Chaos()
