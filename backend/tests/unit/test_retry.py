import random

from app.services.retry import ErrorClass, RetryPolicy, backoff_delay, classify, decide

P = RetryPolicy(schedule=(10, 30, 120, 600, 3600), jitter=0.2)


def test_classification():
    assert classify(200, None)[0] is ErrorClass.SUCCESS and classify(204, None)[0] is ErrorClass.SUCCESS
    for code in (408, 425, 429, 500, 502, 503, 504):
        assert classify(code, None)[0] is ErrorClass.TRANSIENT, code
    for code in (301, 302, 400, 401, 403, 404, 410, 422):
        assert classify(code, None)[0] is ErrorClass.PERMANENT, code
    assert classify(None, "timeout")[0] is ErrorClass.TRANSIENT
    assert classify(None, "connection")[0] is ErrorClass.TRANSIENT
    assert classify(None, "blocked_destination")[0] is ErrorClass.PERMANENT


def test_schedule_follows_policy_when_jitter_is_zero():
    p = RetryPolicy(schedule=(10, 30, 120, 600, 3600), jitter=0.0)
    assert [backoff_delay(n, p) for n in range(1, 6)] == [10, 30, 120, 600, 3600]


def test_jitter_stays_within_bounds():
    rng = random.Random(1234)
    for attempt in range(1, 6):
        base = P.schedule[attempt - 1]
        for _ in range(500):
            d = backoff_delay(attempt, P, rng)
            assert base * 0.8 - 1e-9 <= d <= base * 1.2 + 1e-9


def test_jitter_actually_varies():
    rng = random.Random(7)
    assert len({round(backoff_delay(3, P, rng), 3) for _ in range(50)}) > 25


def test_max_attempts_is_schedule_plus_one():
    assert P.max_attempts == 6
    assert decide(5, ErrorClass.TRANSIENT, P).retry
    d = decide(6, ErrorClass.TRANSIENT, P)
    assert not d.retry and d.reason == "max_attempts_exhausted"


def test_permanent_not_retried_success_terminal():
    assert not decide(1, ErrorClass.PERMANENT, P).retry
    assert decide(1, ErrorClass.PERMANENT, P).reason == "permanent_failure_not_retried"
    assert not decide(1, ErrorClass.SUCCESS, P).retry


def test_retry_after_is_honoured_and_capped():
    p = RetryPolicy(schedule=(1, 2), jitter=0.0, max_retry_after_s=60)
    assert decide(1, ErrorClass.TRANSIENT, p, retry_after_s=30).delay_s == 30
    assert decide(1, ErrorClass.TRANSIENT, p, retry_after_s=9999).delay_s == 60
    assert decide(1, ErrorClass.TRANSIENT, p, retry_after_s=0.1).delay_s == 1


def test_retry_budget():
    p = RetryPolicy(schedule=(100,), jitter=0.0, budget_s=150)
    assert decide(1, ErrorClass.TRANSIENT, p, elapsed_s=10).retry
    d = decide(1, ErrorClass.TRANSIENT, p, elapsed_s=100)
    assert not d.retry and d.reason == "retry_budget_exhausted"
