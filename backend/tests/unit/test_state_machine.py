import pytest

from app.services.state_machine import ALLOWED, TERMINAL, DeliveryStatus as S, InvalidTransition, check


@pytest.mark.parametrize("a,b", [(S.PENDING, S.QUEUED), (S.QUEUED, S.PROCESSING), (S.PROCESSING, S.DELIVERED),
                                 (S.PROCESSING, S.RETRYING), (S.PROCESSING, S.DEAD), (S.RETRYING, S.PROCESSING),
                                 (S.PENDING, S.PROCESSING)])
def test_valid(a, b):
    assert check(a, b) is b


@pytest.mark.parametrize("a,b", [(S.DELIVERED, S.PENDING), (S.DELIVERED, S.RETRYING), (S.DEAD, S.PENDING),
                                 (S.DEAD, S.QUEUED), (S.DEAD, S.PROCESSING), (S.PENDING, S.DELIVERED),
                                 (S.QUEUED, S.DEAD), (S.RETRYING, S.DELIVERED), (S.RETRYING, S.DEAD)])
def test_invalid(a, b):
    with pytest.raises(InvalidTransition):
        check(a, b)


def test_terminal_states_have_no_exits():
    assert all(not ALLOWED[s] for s in TERMINAL)


def test_accepts_plain_strings():
    assert check("PROCESSING", "DEAD") is S.DEAD
