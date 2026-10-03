"""Startup failures and mid-start interruptions must not leak slots or leave
state/occupancy inconsistent. A failed env is FAILED, keeps no slot, and the
next waiter is promoted; retry is possible once provisioning recovers.
"""

from __future__ import annotations

import threading

import pytest

from controlplane.errors import ProvisioningError
from controlplane.models import EnvState, FakeClock
from controlplane.service import ControlPlane

from ._logging import log, section


def test_failed_start_releases_slot_and_promotes_waiter():
    section("[failure] provisioning failure leaves no slot and promotes waiter")
    clock = FakeClock()

    def boom(env):
        raise RuntimeError("simulated image pull failure")

    plane = ControlPlane(capacity=1, queue_limit=2, provisioner=boom,
                         clock=clock)
    target = plane.apply("alice")
    waiter = plane.apply("bob")
    assert target.disposition == "admitted"
    assert waiter.disposition == "queued"

    with pytest.raises(ProvisioningError) as ei:
        plane.start(target.environment.env_id, target.environment.token)
    failed = ei.value.env
    assert failed.state is EnvState.FAILED
    assert failed.fail_reason == "simulated image pull failure"
    assert failed.failure_count == 1
    log(f"start failed: state={failed.state.value} reason={failed.fail_reason}")

    view = plane.capacity_view()
    # The failed target holds no slot; the freed slot is immediately taken
    # by the promoted waiter (capacity 1), so occupancy is still consistent.
    assert view.used_slots == 1
    assert view.queued == 0
    assert plane.store.get_locked(target.environment.env_id).state is EnvState.FAILED
    log(f"capacity right after failure: {view.as_dict()} (failed target holds none)")

    # Failure released the reservation, so the waiter was promoted to READY.
    promoted = plane.status(waiter.environment.env_id, waiter.environment.token)
    assert promoted.state is EnvState.READY
    log(f"waiter promoted to {promoted.state.value} (no stuck occupancy)")


def test_retry_after_failure_succeeds_when_provisioning_recovers():
    section("[failure] FAILED env retries; success ends RUNNING")
    clock = FakeClock()
    attempts = {"n": 0}

    def flaky(env):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("transient network blip")

    plane = ControlPlane(capacity=1, provisioner=flaky, clock=clock)
    out = plane.apply("alice")
    with pytest.raises(ProvisioningError):
        plane.start(out.environment.env_id, out.environment.token)
    assert plane.capacity_view().used_slots == 0

    result = plane.start(out.environment.env_id, out.environment.token)
    assert result.environment.state is EnvState.RUNNING
    assert plane.capacity_view().used_slots == 1
    log(f"recovered after {attempts['n']} attempt(s); occupancy consistent")


def test_interrupted_provisioning_rolls_back_under_concurrency():
    section("[failure] concurrent start interrupted by exception stays consistent")
    clock = FakeClock()
    entered = threading.Event()
    release = threading.Event()

    def failing(env):
        entered.set()
        release.wait(timeout=5)
        raise RuntimeError("provisioning interrupted mid-flight")

    plane = ControlPlane(
        capacity=1, start_timeout_seconds=100,
        provisioner=failing, clock=clock,
    )
    out = plane.apply("alice")

    # A second requester waits behind the single slot.
    waiter = plane.apply("bob")
    assert waiter.disposition == "queued"

    def do_start():
        with pytest.raises(ProvisioningError):
            plane.start(out.environment.env_id, out.environment.token)

    t = threading.Thread(target=do_start)
    t.start()
    assert entered.wait(timeout=1)
    # While provisioning is in flight, the slot must count as occupied AND
    # control-plane reads must return immediately (provisioning holds no lock).
    assert plane.capacity_view().used_slots == 1
    target = plane.status(out.environment.env_id, out.environment.token)
    assert target.state is EnvState.STARTING
    log("during interrupted provisioning slot counts as held (STARTING), "
        "reads not blocked")
    release.set()
    t.join(timeout=2)
    assert not t.is_alive()

    # After interruption: no leak, no stuck STARTING, waiter promoted.
    view = plane.capacity_view()
    assert view.used_slots == 1  # promoted waiter occupies it
    assert view.queued == 0
    final_target = plane.store.get_locked(out.environment.env_id)
    assert final_target.state is EnvState.FAILED
    promoted = plane.store.get_locked(waiter.environment.env_id)
    assert promoted.state is EnvState.READY
    log(f"after interrupt: target={final_target.state.value}, "
        f"waiter={promoted.state.value}, view={view.as_dict()}")

    # Repeated failure cycles never accumulate held slots.
    def always_fails(env):
        raise RuntimeError("always fails")

    plane2 = ControlPlane(capacity=3, provisioner=always_fails, clock=FakeClock())
    outs = [plane2.apply(f"u{i}") for i in range(3)]
    for o in outs:
        with pytest.raises(ProvisioningError):
            plane2.start(o.environment.env_id, o.environment.token)
    assert plane2.capacity_view().used_slots == 0
    assert all(
        plane2.store.get_locked(o.environment.env_id).state is EnvState.FAILED
        for o in outs
    )
    log("three failing starts -> three FAILED, zero slots held")
