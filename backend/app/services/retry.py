"""Retry classification and exponential backoff with jitter. Pure stdlib, fully unit-tested."""
from __future__ import annotations

import random
from dataclasses import dataclass
from enum import Enum


class ErrorClass(str, Enum):
    SUCCESS = "SUCCESS"
    TRANSIENT = "TRANSIENT"
    PERMANENT = "PERMANENT"


TRANSIENT_STATUS = {408, 425, 429}


def classify(status_code: int | None, error_kind: str | None) -> tuple[ErrorClass, str]:
    """Returns (class, short label). error_kind: timeout | connection | blocked_destination | unexpected."""
    if error_kind:
        if error_kind == "blocked_destination":
            return ErrorClass.PERMANENT, error_kind
        return ErrorClass.TRANSIENT, error_kind
    assert status_code is not None
    label = f"http_{status_code}"
    if 200 <= status_code < 300:
        return ErrorClass.SUCCESS, label
    if status_code in TRANSIENT_STATUS or status_code >= 500:
        return ErrorClass.TRANSIENT, label
    return ErrorClass.PERMANENT, label  # 3xx (redirects are not followed) and other 4xx


@dataclass(frozen=True)
class RetryPolicy:
    schedule: tuple[float, ...] = (10, 30, 120, 600, 3600)
    jitter: float = 0.2            # +/- fraction of the base delay
    budget_s: float = 86400        # max total time from creation to last retry
    max_retry_after_s: float = 3600

    @property
    def max_attempts(self) -> int:
        return len(self.schedule) + 1


@dataclass(frozen=True)
class RetryDecision:
    retry: bool
    delay_s: float
    reason: str


def backoff_delay(attempt_number: int, policy: RetryPolicy, rng: random.Random | None = None) -> float:
    """Delay before attempt (attempt_number + 1). Jitter avoids synchronized retry storms."""
    r = rng or random
    base = policy.schedule[min(max(attempt_number, 1) - 1, len(policy.schedule) - 1)]
    return max(0.0, base * (1 + r.uniform(-policy.jitter, policy.jitter)))


def decide(attempt_number: int, error_class: ErrorClass, policy: RetryPolicy, *, elapsed_s: float = 0.0,
           retry_after_s: float | None = None, rng: random.Random | None = None) -> RetryDecision:
    if error_class is ErrorClass.SUCCESS:
        return RetryDecision(False, 0.0, "delivered")
    if error_class is ErrorClass.PERMANENT:
        return RetryDecision(False, 0.0, "permanent_failure_not_retried")
    if attempt_number >= policy.max_attempts:
        return RetryDecision(False, 0.0, "max_attempts_exhausted")
    delay = backoff_delay(attempt_number, policy, rng)
    if retry_after_s is not None and retry_after_s > 0:
        delay = max(delay, min(retry_after_s, policy.max_retry_after_s))  # honor 429 Retry-After
    if elapsed_s + delay > policy.budget_s:
        return RetryDecision(False, 0.0, "retry_budget_exhausted")
    return RetryDecision(True, delay, "transient_failure_retry_scheduled")
