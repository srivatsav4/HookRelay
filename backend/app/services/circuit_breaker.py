"""Endpoint circuit breaker as pure functions over a small state record.

CLOSED --(failures >= threshold)--> OPEN --(cooldown elapsed, one probe admitted)--> HALF_OPEN
HALF_OPEN --probe success--> CLOSED        HALF_OPEN --probe failure--> OPEN (cooldown restarts)
"DEGRADED" is a derived health label: CLOSED with failures > 0.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime

CLOSED, OPEN, HALF_OPEN = "CLOSED", "OPEN", "HALF_OPEN"


@dataclass(frozen=True)
class CBConfig:
    failure_threshold: int = 5
    cooldown_s: float = 30.0


@dataclass(frozen=True)
class CBState:
    state: str = CLOSED
    failures: int = 0
    opened_at: datetime | None = None
    probe_at: datetime | None = None


def allow(st: CBState, now: datetime, cfg: CBConfig) -> tuple[bool, float, CBState, str | None]:
    """Returns (allowed, wait_seconds_if_denied, new_state, transition_label)."""
    if st.state == CLOSED:
        return True, 0.0, st, None
    if st.state == OPEN:
        elapsed = (now - st.opened_at).total_seconds() if st.opened_at else cfg.cooldown_s
        if elapsed < cfg.cooldown_s:
            return False, cfg.cooldown_s - elapsed, st, None
        return True, 0.0, replace(st, state=HALF_OPEN, probe_at=now), "OPEN->HALF_OPEN"
    # HALF_OPEN: exactly one probe in flight; a stale probe (crashed worker) is replaced after the cooldown
    if st.probe_at is not None:
        age = (now - st.probe_at).total_seconds()
        if age < cfg.cooldown_s:
            return False, max(cfg.cooldown_s - age, 1.0), st, None
    return True, 0.0, replace(st, probe_at=now), None


def on_success(st: CBState, cfg: CBConfig) -> tuple[CBState, str | None]:
    if st.state == HALF_OPEN:
        return CBState(CLOSED, 0, None, None), "HALF_OPEN->CLOSED"
    if st.state == CLOSED:
        return replace(st, failures=0), None
    return st, None  # late success from a request started before the circuit opened: ignore


def on_failure(st: CBState, now: datetime, cfg: CBConfig) -> tuple[CBState, str | None]:
    if st.state == HALF_OPEN:
        return CBState(OPEN, st.failures + 1, now, None), "HALF_OPEN->OPEN"
    if st.state == CLOSED:
        failures = st.failures + 1
        if failures >= cfg.failure_threshold:
            return CBState(OPEN, failures, now, None), "CLOSED->OPEN"
        return replace(st, failures=failures), None
    return st, None


def health(st: CBState) -> str:
    if st.state == OPEN:
        return "UNHEALTHY"
    if st.state == HALF_OPEN:
        return "RECOVERING"
    return "DEGRADED" if st.failures > 0 else "HEALTHY"
