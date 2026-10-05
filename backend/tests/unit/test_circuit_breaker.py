from datetime import datetime, timedelta, timezone

from app.services import circuit_breaker as cb

CFG = cb.CBConfig(failure_threshold=3, cooldown_s=30)
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_opens_only_at_threshold_and_reports_degraded():
    st = cb.CBState()
    for i in range(2):
        st, tr = cb.on_failure(st, T0, CFG)
        assert st.state == cb.CLOSED and tr is None
    assert cb.health(st) == "DEGRADED"
    st, tr = cb.on_failure(st, T0, CFG)
    assert st.state == cb.OPEN and tr == "CLOSED->OPEN" and cb.health(st) == "UNHEALTHY"


def test_success_resets_failure_count():
    st, _ = cb.on_failure(cb.CBState(), T0, CFG)
    st, _ = cb.on_success(st, CFG)
    assert st.failures == 0 and cb.health(st) == "HEALTHY"


def _open():
    st = cb.CBState()
    for _ in range(3):
        st, _ = cb.on_failure(st, T0, CFG)
    return st


def test_open_denies_until_cooldown_then_admits_single_probe():
    st = _open()
    ok, wait, st2, _ = cb.allow(st, T0 + timedelta(seconds=10), CFG)
    assert not ok and abs(wait - 20) < 1e-6 and st2.state == cb.OPEN
    ok, _, st3, tr = cb.allow(st, T0 + timedelta(seconds=31), CFG)
    assert ok and st3.state == cb.HALF_OPEN and tr == "OPEN->HALF_OPEN"
    ok2, wait2, _, _ = cb.allow(st3, T0 + timedelta(seconds=32), CFG)  # second concurrent probe denied
    assert not ok2 and wait2 > 0


def test_half_open_success_closes():
    _, _, half, _ = cb.allow(_open(), T0 + timedelta(seconds=31), CFG)
    st, tr = cb.on_success(half, CFG)
    assert st.state == cb.CLOSED and st.failures == 0 and tr == "HALF_OPEN->CLOSED"


def test_half_open_failure_reopens_and_restarts_cooldown():
    _, _, half, _ = cb.allow(_open(), T0 + timedelta(seconds=31), CFG)
    now = T0 + timedelta(seconds=32)
    st, tr = cb.on_failure(half, now, CFG)
    assert st.state == cb.OPEN and tr == "HALF_OPEN->OPEN" and st.opened_at == now
    ok, wait, _, _ = cb.allow(st, now + timedelta(seconds=5), CFG)
    assert not ok and abs(wait - 25) < 1e-6


def test_stale_probe_is_replaced_after_cooldown():
    _, _, half, _ = cb.allow(_open(), T0 + timedelta(seconds=31), CFG)
    ok, _, st, _ = cb.allow(half, T0 + timedelta(seconds=31 + 31), CFG)
    assert ok and st.probe_at == T0 + timedelta(seconds=62)


def test_late_success_while_open_is_ignored():
    st, tr = cb.on_success(_open(), CFG)
    assert st.state == cb.OPEN and tr is None
