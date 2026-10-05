from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (BigInteger, Boolean, CheckConstraint, DateTime, Float, ForeignKey, Index, Integer,
                        String, Text, UniqueConstraint, func)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ts(**kw):
    return mapped_column(DateTime(timezone=True), **kw)


class Account(Base):
    __tablename__ = "accounts"
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    is_lab: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = _ts(server_default=func.now())


class User(Base):
    __tablename__ = "users"
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"), index=True)
    email: Mapped[str] = mapped_column(String(255), unique=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = _ts(server_default=func.now())


class ApiKey(Base):
    __tablename__ = "api_keys"
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"), index=True)
    prefix: Mapped[str] = mapped_column(String(16), unique=True)  # lookup handle, not secret
    key_hash: Mapped[str] = mapped_column(String(128))  # HMAC-SHA256(pepper, raw key); raw key never stored
    name: Mapped[str] = mapped_column(String(120), default="default")
    revoked: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = _ts(server_default=func.now())


class Endpoint(Base):
    __tablename__ = "endpoints"
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"), index=True)
    url: Mapped[str] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(String(255), nullable=True)
    event_types: Mapped[list] = mapped_column(JSONB, default=lambda: ["*"])
    signing_secret: Mapped[str] = mapped_column(String(128))  # needed to sign; never logged/returned after creation
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    deleted_at: Mapped[datetime | None] = _ts(nullable=True)
    rate_limit_per_sec: Mapped[float | None] = mapped_column(Float, nullable=True)
    retry_schedule: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    timeout_s: Mapped[float | None] = mapped_column(Float, nullable=True)
    cb_state: Mapped[str] = mapped_column(String(16), default="CLOSED")
    cb_failures: Mapped[int] = mapped_column(Integer, default=0)
    cb_opened_at: Mapped[datetime | None] = _ts(nullable=True)
    cb_probe_at: Mapped[datetime | None] = _ts(nullable=True)
    created_at: Mapped[datetime] = _ts(server_default=func.now())
    __table_args__ = (CheckConstraint("cb_state in ('CLOSED','OPEN','HALF_OPEN')", name="ck_endpoints_cb_state"),)


class Event(Base):
    __tablename__ = "events"
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"))
    idempotency_key: Mapped[str] = mapped_column(String(255))
    event_type: Mapped[str] = mapped_column(String(120))
    payload: Mapped[dict] = mapped_column(JSONB)
    occurred_at: Mapped[datetime] = _ts()
    created_at: Mapped[datetime] = _ts(server_default=func.now())
    __table_args__ = (
        UniqueConstraint("account_id", "idempotency_key", name="uq_events_account_idem"),
        Index("ix_events_account_created", "account_id", "created_at"),
    )


class Delivery(Base):
    __tablename__ = "deliveries"
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("events.id", ondelete="CASCADE"), index=True)
    endpoint_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("endpoints.id", ondelete="CASCADE"), index=True)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"))
    status: Mapped[str] = mapped_column(String(16), default="PENDING")
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    release_count: Mapped[int] = mapped_column(Integer, default=0)  # job-id nonce, see docs/design-decisions.md
    max_attempts: Mapped[int] = mapped_column(Integer, default=6)
    next_attempt_at: Mapped[datetime | None] = _ts(nullable=True)
    locked_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    locked_at: Mapped[datetime | None] = _ts(nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    replay_of: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("deliveries.id"), nullable=True)
    created_at: Mapped[datetime] = _ts(server_default=func.now())
    updated_at: Mapped[datetime] = _ts(server_default=func.now(), onupdate=func.now())
    delivered_at: Mapped[datetime | None] = _ts(nullable=True)
    __table_args__ = (
        CheckConstraint("status in ('PENDING','QUEUED','PROCESSING','RETRYING','DELIVERED','DEAD')", name="ck_deliveries_status"),
        Index("ix_deliveries_status_next", "status", "next_attempt_at"),
        Index("ix_deliveries_account_status", "account_id", "status"),
    )


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    delivery_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("deliveries.id", ondelete="CASCADE"))
    attempt_number: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime] = _ts()
    completed_at: Mapped[datetime] = _ts()
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    timeout_s: Mapped[float] = mapped_column(Float)
    duration_ms: Mapped[float] = mapped_column(Float)
    error_class: Mapped[str] = mapped_column(String(16))  # SUCCESS | TRANSIENT | PERMANENT
    error_detail: Mapped[str] = mapped_column(String(64))  # http_503 | timeout | connection | ...
    will_retry: Mapped[bool] = mapped_column(Boolean)
    retry_decision: Mapped[str] = mapped_column(String(64))
    retry_delay_s: Mapped[float | None] = mapped_column(Float, nullable=True)
    __table_args__ = (UniqueConstraint("delivery_id", "attempt_number", name="uq_attempt_delivery_number"),)


class Experiment(Base):
    __tablename__ = "experiments"
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"), index=True)
    type: Mapped[str] = mapped_column(String(40))
    config: Mapped[dict] = mapped_column(JSONB, default=dict)
    status: Mapped[str] = mapped_column(String(16), default="CREATED")  # CREATED|RUNNING|COMPLETED|FAILED
    verdict: Mapped[str | None] = mapped_column(String(8), nullable=True)
    lab_account_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True)
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = _ts(server_default=func.now())
    started_at: Mapped[datetime | None] = _ts(nullable=True)
    finished_at: Mapped[datetime | None] = _ts(nullable=True)


class ExperimentEvent(Base):
    """Timeline entries for an experiment (traffic started, fault injected, recovered, ...)."""
    __tablename__ = "experiment_events"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    experiment_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("experiments.id", ondelete="CASCADE"), index=True)
    ts: Mapped[datetime] = _ts(default=utcnow)
    kind: Mapped[str] = mapped_column(String(32))
    message: Mapped[str] = mapped_column(String(500))
    data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)


class AuditLog(Base):
    __tablename__ = "audit_logs"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account_id: Mapped[uuid.UUID | None] = mapped_column(nullable=True, index=True)
    action: Mapped[str] = mapped_column(String(48), index=True)
    entity: Mapped[str] = mapped_column(String(32))
    entity_id: Mapped[str] = mapped_column(String(64))
    data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = _ts(server_default=func.now())
