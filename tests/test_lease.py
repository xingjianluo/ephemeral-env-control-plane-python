"""Lease expiry: environments are reaped, slots released, credentials die,
and queued waiters are promoted in FIFO order.
"""

from __future__ import annotations

import time

import pytest

from controlplane.errors import EnvExpiredError
from controlplane.models import EnvState, FakeClock
from controlplane.service import ControlPlane

from ._logging import log, section


def test_expiry_releases_slot_and_promotes_waiter():
    section("[lease] expiry frees a slot and promotes the head waiter")
    clock = FakeClock()
    plane = ControlPlane(capacity=1, queue_limit=3, default_lease_seconds=10,
                         clock=clock)
    first = plane.apply("alice").environment
    waiter = plane.apply("bob")
    assert waiter.disposition == "queued"
    plane.start(first.env_id, first.token)
    assert plane.capacity_view().used_slots == 1

    clock.advance(10)  # lease boundary
    expired = plane.reap_expired()
    assert expired == [first.env_id]
    promoted = plane.status(waiter.environment.env_id, waiter.environment.token)
    assert promoted.state is EnvState.READY
    log(f"reaped {expired}; waiter promoted to {promoted.state.value}")
    view = plane.capacity_view()
    assert view.used_slots == 1 and view.queued == 0
    log(f"capacity after reap: {view.as_dict()}")

    # expired credential no longer works
    with pytest.raises(EnvExpiredError):
        plane.start(first.env_id, first.token)
    log("expired credential rejected with env_expired")


def test_lease_does_not_run_while_stopped():
    section("[lease] stopping pauses the lease; restart gets a fresh lease")
    clock = FakeClock()
    plane = ControlPlane(capacity=1, default_lease_seconds=10, clock=clock)
    env = plane.apply("alice").environment
    plane.start(env.env_id, env.token)
    clock.advance(9)
    stopped = plane.stop(env.env_id, env.token)
    assert stopped.state is EnvState.STOPPED and stopped.expires_at is None
    clock.advance(100)  # would have expired if lease kept running
    env_fresh = plane.status(env.env_id, env.token)
    assert env_fresh.state is EnvState.STOPPED
    log("stayed STOPPED after 100s: lease does not run while stopped")
    out = plane.start(env.env_id, env.token)
    assert out.environment.state is EnvState.RUNNING
    assert out.environment.expires_at == clock.now() + 10
    log(f"restarted, fresh expiry at {out.environment.expires_at}")


def test_background_reaper_recycles_automatically():
    section("[lease] background reaper reclaims expired envs without polling")
    plane = ControlPlane(capacity=1, default_lease_seconds=1, clock=_MonoClock())
    env = plane.apply("alice").environment
    plane.start(env.env_id, env.token)
    reaper = plane.start_reaper(interval=0.02)
    try:
        deadline = time.time() + 2
        while time.time() < deadline:
            cur = plane.explain(env.env_id)
            if cur["state"] == "EXPIRED":
                break
            time.sleep(0.02)
        final = plane.explain(env.env_id)
        assert final["state"] == "EXPIRED", final["state"]
        assert plane.capacity_view().used_slots == 0
        log("background reaper transitioned RUNNING -> EXPIRED and freed slot")
    finally:
        plane.stop_reaper()


class _MonoClock:
    def now(self) -> float:
        return time.monotonic()
