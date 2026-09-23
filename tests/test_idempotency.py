"""Duplicate applications are idempotent; illegal transitions are rejected
with distinct, stable reasons instead of one generic failure.
"""

from __future__ import annotations

import pytest

from controlplane.errors import (
    EnvCanceledError,
    EnvDestroyedError,
    EnvExpiredError,
    IllegalTransitionError,
    InvalidTokenError,
    TokenExpiredError,
)
from controlplane.models import FakeClock
from controlplane.service import ControlPlane

from ._logging import log, section


def _plane():
    return ControlPlane(capacity=2, queue_limit=2, default_lease_seconds=100,
                        clock=FakeClock())


def test_identical_apply_is_idempotent():
    section("[idempotency] repeated identical apply returns same record")
    plane = _plane()
    first = plane.apply("alice", lease_seconds=50, idempotency_key="k", fingerprint="f")
    again = plane.apply("alice", lease_seconds=50, idempotency_key="k", fingerprint="f")
    third = plane.apply("alice", lease_seconds=50, idempotency_key="k", fingerprint="f")

    assert first.deduplicated is False
    assert again.deduplicated and third.deduplicated
    ids = {first.environment.env_id, again.environment.env_id, third.environment.env_id}
    assert ids == {first.environment.env_id}
    assert plane.capacity_view().used_slots == 1
    log(f"3 identical applies -> 1 record {first.environment.env_id}, 1 slot held")


def test_same_key_different_body_is_conflict():
    section("[idempotency] same key with a different body is a conflict")
    plane = _plane()
    plane.apply("alice", lease_seconds=50, idempotency_key="k", fingerprint="f")
    with pytest.raises(IllegalTransitionError) as ei:
        plane.apply("alice", lease_seconds=99, idempotency_key="k", fingerprint="f")
    assert ei.value.reason == "idempotency key reused with a different request"
    log(f"conflict rejected with distinct reason: {ei.value.reason}")
    with pytest.raises(IllegalTransitionError) as ei:
        plane.apply("bob", lease_seconds=50, idempotency_key="k", fingerprint="f")
    log("different owner with same key also rejected as conflict")
    assert plane.capacity_view().used_slots == 1


def test_terminal_state_operations_are_distinct():
    section("[idempotency] illegal ops on terminal states use distinct codes")
    clock = FakeClock()
    plane = ControlPlane(capacity=1, default_lease_seconds=10, clock=clock)

    env = plane.apply("alice").environment
    token = env.token

    # Destroyed
    plane.destroy(env.env_id, token)
    with pytest.raises(EnvDestroyedError):
        plane.start(env.env_id, token)
    with pytest.raises(EnvDestroyedError):
        plane.destroy(env.env_id, token)
    log("start/destroy on DESTROYED -> env_destroyed")

    # Expired (via boundary credential use)
    env2 = plane.apply("bob").environment
    plane.start(env2.env_id, env2.token)
    clock.advance(11)
    with pytest.raises(TokenExpiredError):
        plane.status(env2.env_id, env2.token)
    with pytest.raises(EnvExpiredError):
        plane.start(env2.env_id, env2.token)
    log("start after expiry -> env_expired; first overdue use -> token_expired")

    # Canceled while queued
    plane.capacity  # capacity 1 still free after expiry -> apply queues? no
    env3 = plane.apply("carol")  # slot free -> admitted, not queued
    # Force a queued one: occupy the only slot first
    env4 = plane.apply("dave")  # queued
    assert env4.disposition == "queued"
    plane.cancel(env4.environment.env_id, env4.environment.token)
    with pytest.raises(EnvCanceledError):
        plane.start(env4.environment.env_id, env4.environment.token)
    log("start on CANCELED -> env_canceled")


def test_non_terminal_illegal_transitions_have_reasons():
    section("[idempotency] stop/cancel in wrong phases explain why")
    plane = _plane()
    # occupy capacity, create a queued env
    e1 = plane.apply("a").environment
    e2 = plane.apply("b").environment
    q = plane.apply("c")
    assert q.disposition == "queued"

    with pytest.raises(IllegalTransitionError) as ei:
        plane.stop(q.environment.env_id, q.environment.token)
    assert ei.value.reason == "use_cancel_while_queued"
    log(f"stop on QUEUED -> {ei.value.reason}")

    plane.start(e1.env_id, e1.token)
    plane.start(e2.env_id, e2.token)
    # RUNNING -> cancel is illegal (only queued can cancel)
    with pytest.raises(IllegalTransitionError) as ei:
        plane.cancel(e1.env_id, e1.token)
    assert ei.value.reason == "only_queued_requests_can_cancel"
    log(f"cancel on RUNNING -> {ei.value.reason}")
