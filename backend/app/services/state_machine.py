"""Delivery state machine. Invalid transitions raise InvalidTransition."""
from __future__ import annotations

from enum import Enum


class DeliveryStatus(str, Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    RETRYING = "RETRYING"
    DELIVERED = "DELIVERED"
    DEAD = "DEAD"


S = DeliveryStatus
ALLOWED: dict[DeliveryStatus, frozenset[DeliveryStatus]] = {
    S.PENDING: frozenset({S.QUEUED, S.PROCESSING}),            # PROCESSING: worker claimed a job recovery enqueued
    S.QUEUED: frozenset({S.PROCESSING}),
    S.PROCESSING: frozenset({S.DELIVERED, S.RETRYING, S.DEAD}),  # RETRYING also covers release & stale recovery
    S.RETRYING: frozenset({S.PROCESSING}),
    S.DELIVERED: frozenset(),                                    # terminal
    S.DEAD: frozenset(),                                         # terminal; replay creates a NEW delivery
}
CLAIMABLE = frozenset({S.PENDING, S.QUEUED, S.RETRYING})
TERMINAL = frozenset({S.DELIVERED, S.DEAD})


class InvalidTransition(Exception):
    pass


def check(current: str | DeliveryStatus, new: str | DeliveryStatus) -> DeliveryStatus:
    cur, nxt = DeliveryStatus(current), DeliveryStatus(new)
    if nxt not in ALLOWED[cur]:
        raise InvalidTransition(f"{cur.value} -> {nxt.value} is not allowed")
    return nxt
