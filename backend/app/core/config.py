"""Environment-driven settings (no hidden defaults for secrets)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def _bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    return default if v is None else v.strip().lower() in {"1", "true", "yes", "on"}


def _num(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _schedule(name: str, default: str) -> tuple[float, ...]:
    raw = os.environ.get(name, default)
    return tuple(float(x) for x in raw.split(",") if x.strip())


@dataclass(frozen=True)
class Settings:
    env: str
    database_url: str
    redis_url: str
    jwt_secret: str
    jwt_ttl_s: int
    api_key_pepper: str
    cors_origins: tuple[str, ...]
    allow_private_destinations: bool
    require_https: bool
    max_payload_bytes: int
    delivery_timeout_s: float
    retry_schedule: tuple[float, ...]
    retry_jitter: float
    retry_budget_s: float
    cb_failure_threshold: int
    cb_cooldown_s: float
    worker_max_jobs: int
    tenant_max_concurrency: int
    visibility_timeout_s: float
    recovery_interval_s: int
    recovery_pending_grace_s: float
    recovery_queued_grace_s: float
    recovery_batch: int
    mock_receiver_url: str
    worker_metrics_port: int

    @property
    def retry_max_attempts(self) -> int:
        return len(self.retry_schedule) + 1


def load_settings() -> Settings:
    jwt_secret = os.environ.get("JWT_SECRET", "")
    pepper = os.environ.get("API_KEY_PEPPER", "")
    if len(jwt_secret) < 16 or len(pepper) < 16:
        raise RuntimeError("JWT_SECRET and API_KEY_PEPPER must be set (>=16 chars). See .env.example")
    return Settings(
        env=os.environ.get("APP_ENV", "dev"),
        database_url=os.environ["DATABASE_URL"],
        redis_url=os.environ["REDIS_URL"],
        jwt_secret=jwt_secret,
        jwt_ttl_s=int(_num("JWT_TTL_S", 3600)),
        api_key_pepper=pepper,
        cors_origins=tuple(o for o in os.environ.get("CORS_ORIGINS", "http://localhost:3000").split(",") if o),
        allow_private_destinations=_bool("ALLOW_PRIVATE_DESTINATIONS", False),
        require_https=_bool("REQUIRE_HTTPS", False),
        max_payload_bytes=int(_num("MAX_PAYLOAD_BYTES", 256 * 1024)),
        delivery_timeout_s=_num("DELIVERY_TIMEOUT_S", 10),
        retry_schedule=_schedule("RETRY_SCHEDULE", "10,30,120,600,3600"),
        retry_jitter=_num("RETRY_JITTER", 0.2),
        retry_budget_s=_num("RETRY_BUDGET_S", 86400),
        cb_failure_threshold=int(_num("CB_FAILURE_THRESHOLD", 5)),
        cb_cooldown_s=_num("CB_COOLDOWN_S", 30),
        worker_max_jobs=int(_num("WORKER_MAX_JOBS", 20)),
        tenant_max_concurrency=int(_num("TENANT_MAX_CONCURRENCY", 5)),
        visibility_timeout_s=_num("VISIBILITY_TIMEOUT_S", 60),
        recovery_interval_s=int(_num("RECOVERY_INTERVAL_S", 10)),
        recovery_pending_grace_s=_num("RECOVERY_PENDING_GRACE_S", 5),
        recovery_queued_grace_s=_num("RECOVERY_QUEUED_GRACE_S", 30),
        recovery_batch=int(_num("RECOVERY_BATCH", 500)),
        mock_receiver_url=os.environ.get("MOCK_RECEIVER_URL", "http://mock-receiver:9000"),
        worker_metrics_port=int(_num("WORKER_METRICS_PORT", 9101)),
    )


@lru_cache
def get_settings() -> Settings:
    return load_settings()
