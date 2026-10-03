"""Slow starts must not freeze the plane; a start past its time limit is
failed as *timeout* (slot released, head waiter promoted, restartable);
an owner can actively abort a STARTING env (cause distinct from timeout and
from a provisioning error), and every other abort attempt is rejected with
the existing state/credential semantics.

These scenarios use real wall-clock waits with short limits plus explicit
worker synchronisation events, so criteria are deterministic yet exercise
the genuinely asynchronous provisioner thread.
"""

from __future__ import annotations

import threading
import time

import pytest

from controlplane.errors import (
    EnvAccessDeniedError,
    EnvDestroyedError,
    IllegalTransitionError,
    InvalidTokenError,
    StartupAbortedError,
    StartupTimeoutError,
)
from controlplane.models import EnvState, FakeClock
from controlplane.service import ControlPlane

from ._logging import log, section


def _wait_state(plane, env_id, state, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cur = plane.store.get_locked(env_id)
        if cur is not None and cur.state is state:
            return cur
        time.sleep(0.005)
    raise AssertionError(
        f"env {env_id} never reached {state}; "
        f"now {plane.store.get_locked(env_id).state if plane.store.get_locked(env_id) else 'gone'}"
    )


def test_plane_stays_responsive_during_slow_start_and_slot_stays_held():
    section("[slow-start] control plane stays fast while a start is in flight")
    entered = threading.Event()
    release = threading.Event()

    def slow(env, cancel_event=None):
        entered.set()
        release.wait(timeout=5)

    plane = ControlPlane(capacity=1, queue_limit=10, provisioner=slow)
    target = plane.apply("alice").environment
    waiter = plane.apply("bob")
    assert waiter.disposition == "queued"

    def do_start():
        return plane.start(target.env_id, target.token)

    t = threading.Thread(target=do_start, name="slow-start-caller")
    t.start()
    try:
        assert entered.wait(timeout=2), "provisioner never entered"

        # Other clients must keep getting fast, consistent answers.
        timings = []
        for _ in range(5):
            t0 = time.monotonic()
            view = plane.capacity_view()
            other = plane.apply("carol")
            status = plane.status(target.env_id, target.token)
            explain = plane.explain(target.env_id)
            timings.append(time.monotonic() - t0)

            assert view.used_slots == 1
            assert view.free_slots == 0
            assert status.state is EnvState.STARTING
            assert other.disposition == "queued"
            assert explain["state"] == "STARTING"
            assert explain["reason"] == "provisioning_in_progress"
        worst = max(timings)
        log(f"5 rounds of capacity/apply/status/explain during STARTING; "
            f"worst round {worst*1000:.2f}ms (must be milliseconds)")
        assert worst < 0.1, "plane must not block on the in-flight provisioner"

        view = plane.capacity_view()
        log(f"capacity during slow start: {view.as_dict()} "
            f"(STARTING honestly holds its slot)")
        assert view.used_slots == 1  # not free, not double-booked
        assert view.queued == 6  # bob + five carols, all still waiting

        # The queued waiter must not have been promoted into the held slot.
        bob = plane.status(waiter.environment.env_id, waiter.environment.token)
        assert bob.state is EnvState.QUEUED
        log(f"waiter remains {bob.state.value}: STARTING slot is never reused")
    finally:
        release.set()
        t.join(timeout=2)
    assert not t.is_alive()

    final = plane.store.get_locked(target.env_id)
    assert final.state is EnvState.RUNNING
    log(f"after provisioner releases, target -> {final.state.value}")


def test_start_timeout_releases_slot_and_promotes_head_waiter():
    section("[timeout] slow start past limit fails as startup_timeout")
    release = threading.Event()
    plane = ControlPlane(
        capacity=1,
        queue_limit=2,
        default_start_timeout_seconds=0.3,
        provisioner=lambda env, cancel_event=None: release.wait(timeout=5),
    )
    target = plane.apply("alice").environment
    waiter = plane.apply("bob")
    assert waiter.disposition == "queued"

    t0 = time.monotonic()
    with pytest.raises(StartupTimeoutError) as ei:
        plane.start(target.env_id, target.token)
    elapsed = time.monotonic() - t0
    failed = ei.value.env
    assert failed.state is EnvState.FAILED
    assert failed.fail_reason == "startup_timeout"
    assert ei.value.code == "startup_timeout"
    log(f"start caller got {ei.value.code} after {elapsed:.2f}s, "
        f"fail_reason={failed.fail_reason}")
    assert 0.25 < elapsed < 1.0

    view = plane.capacity_view()
    assert view.used_slots == 1 and view.queued == 0
    promoted = plane.status(waiter.environment.env_id, waiter.environment.token)
    assert promoted.state is EnvState.READY
    log(f"freed slot taken by head waiter -> {promoted.state.value}; "
        f"view={view.as_dict()}")

    explain = plane.explain(target.env_id)
    assert explain["reason"].startswith("startup_timeout:")
    log(f"explain distinguishes the cause: {explain['reason']}")

    # Timed-out env is restartable once the slot situation allows.
    release.set()  # let the orphaned hung provisioner return (ignored, stale)
    plane.stop(promoted.env_id, promoted.token)
    out = plane.start(target.env_id, target.token)
    assert out.environment.state is EnvState.RUNNING
    log("timed-out env restarted into RUNNING (not stuck)")


def test_timeout_swept_without_waiting_caller_via_housekeeping():
    section("[timeout] background housekeeping reclaims a hung start")
    clock = FakeClock()
    entered = threading.Event()
    release = threading.Event()

    def hung(env, cancel_event=None):
        entered.set()
        release.wait(timeout=5)

    plane = ControlPlane(
        capacity=1,
        queue_limit=2,
        default_start_timeout_seconds=10,
        provisioner=hung,
        clock=clock,
    )
    target = plane.apply("alice").environment
    waiter = plane.apply("bob").environment

    def hung_start():
        # The blocked caller surfaces the swept timeout on its next wake-up.
        try:
            plane.start(target.env_id, target.token)
        except StartupTimeoutError:
            pass

    t = threading.Thread(target=hung_start, daemon=True)
    t.start()
    assert entered.wait(timeout=2)

    # A client whose credential is otherwise idle advances the fake clock;
    # the next mutating call sweeps the overdue STARTING attempt.
    clock.advance(10.1)
    reaped = plane.reap_expired()
    assert isinstance(reaped, list)
    swept = _wait_state(plane, target.env_id, EnvState.FAILED, timeout=1.0)
    assert swept.fail_reason == "startup_timeout"
    promoted = plane.status(waiter.env_id, waiter.token)
    assert promoted.state is EnvState.READY
    log(f"housekeeping swept hung start ({swept.fail_reason}); "
        f"head waiter -> {promoted.state.value}")
    release.set()

    view = plane.capacity_view()
    assert view.used_slots == 1 and view.queued == 0
    log(f"final view={view.as_dict()} (no leaked occupancy)")


def test_owner_aborts_start_no_residual_and_distinct_cause():
    section("[abort] owner aborts an in-flight start cleanly")
    entered = threading.Event()
    release = threading.Event()

    def slow(env, cancel_event=None):
        entered.set()
        release.wait(timeout=5)

    plane = ControlPlane(
        capacity=1,
        queue_limit=2,
        provisioner=slow,
        default_start_timeout_seconds=30,
    )
    target = plane.apply("alice").environment
    waiter = plane.apply("bob").environment

    def do_start():
        with pytest.raises(StartupAbortedError) as ei:
            plane.start(target.env_id, target.token)
        return ei.value

    start_error: dict = {}

    def runner():
        start_error["err"] = do_start()

    t = threading.Thread(target=runner)
    t.start()
    assert entered.wait(timeout=2)

    aborted = plane.abort_start(target.env_id, target.token)
    t.join(timeout=2)
    assert not t.is_alive()
    assert aborted.state is EnvState.FAILED
    assert aborted.fail_reason == "aborted_by_owner"
    log(f"abort -> {aborted.state.value} fail_reason={aborted.fail_reason}")

    err = start_error["err"]
    assert err.code == "startup_aborted"
    assert err.env.fail_reason == "aborted_by_owner"
    log(f"waiting start caller received {err.code} (distinct from timeout/error)")

    view = plane.capacity_view()
    promoted = plane.status(waiter.env_id, waiter.token)
    assert view.used_slots == 1 and view.queued == 0
    assert promoted.state is EnvState.READY
    log(f"no residual occupancy: view={view.as_dict()}, "
        f"head waiter -> {promoted.state.value}")

    explain = plane.explain(target.env_id)
    assert explain["reason"].startswith("startup_aborted:")
    log(f"explain: {explain['reason']}")

    # restartable after abort
    release.set()
    plane.stop(waiter.env_id, waiter.token)
    out = plane.start(target.env_id, target.token)
    assert out.environment.state is EnvState.RUNNING
    log("aborted env restarted into RUNNING")


def test_abort_rejections_keep_existing_semantics():
    section("[abort] non-starting / unauthorized / stale / destroyed rejected")
    clock = FakeClock()
    plane = ControlPlane(
        capacity=2,
        default_lease_seconds=10,
        default_start_timeout_seconds=30,
        provisioner=lambda env, cancel_event=None: None,
        clock=clock,
    )
    a = plane.apply("alice").environment
    b = plane.apply("bob").environment
    queued = plane.apply("zoe").environment
    assert plane.explain(queued.env_id)["state"] == "QUEUED"

    # READY (not yet starting): abort refused with a specific reason.
    with pytest.raises(IllegalTransitionError) as ei:
        plane.abort_start(a.env_id, a.token)
    assert ei.value.reason == "not_starting_use_stop"
    log("READY abort refused (not_starting_use_stop)")

    # RUNNING
    plane.start(a.env_id, a.token)
    with pytest.raises(IllegalTransitionError) as ei:
        plane.abort_start(a.env_id, a.token)
    assert ei.value.reason == "already_running"
    log("RUNNING abort refused (already_running)")

    # FAILED caused by a provisioning error -> already_failed (illegal_transition)
    bad = ControlPlane(capacity=1,
                       provisioner=lambda env: (_ for _ in ()).throw(RuntimeError("x")),
                       clock=clock)
    fe = bad.apply("frank").environment
    from controlplane.errors import ProvisioningError
    with pytest.raises(ProvisioningError):
        bad.start(fe.env_id, fe.token)
    with pytest.raises(IllegalTransitionError) as ei:
        bad.abort_start(fe.env_id, fe.token)
    assert ei.value.reason == "already_failed"
    log("provisioning-failed FAILED abort -> illegal_transition (already_failed)")

    # STOPPED / FAILED
    plane.stop(a.env_id, a.token)
    with pytest.raises(IllegalTransitionError) as ei:
        plane.abort_start(a.env_id, a.token)
    assert ei.value.reason == "not_starting"
    log("STOPPED abort refused (not_starting)")

    # Stopping freed a slot, promoting the old waiter; fill both slots again
    # before enqueuing a fresh request (b holds one, fill the other).
    plane.start(queued.env_id, queued.token)  # promoted zoe -> RUNNING
    queued2 = plane.apply("roy").environment
    assert plane.explain(queued2.env_id)["state"] == "QUEUED"

    # QUEUED: must use cancel
    with pytest.raises(IllegalTransitionError) as ei:
        plane.abort_start(queued2.env_id, queued2.token)
    assert ei.value.reason == "use_cancel_while_queued"
    log("QUEUED abort refused (use_cancel_while_queued)")

    # credential checks happen first: cross-owner / bad token / unknown
    with pytest.raises(EnvAccessDeniedError):
        plane.abort_start(a.env_id, b.token)
    with pytest.raises(InvalidTokenError):
        plane.abort_start(a.env_id, "tok_forged")
    log("cross-owner -> env_access_denied; forged token -> invalid_token")

    # expired credential -> token_expired; destroyed -> env_destroyed
    plane.cancel(queued2.env_id, queued2.token)  # FIFO: clear head waiter
    plane.stop(queued.env_id, queued.token)  # free a slot for carol2
    c = plane.apply("carol2").environment
    assert plane.explain(c.env_id)["state"] != "QUEUED"
    plane.start(c.env_id, c.token)
    clock.advance(10)
    from controlplane.errors import TokenExpiredError

    with pytest.raises(TokenExpiredError):
        plane.abort_start(c.env_id, c.token)
    log("expired credential abort -> token_expired (not a success)")

    plane.destroy(a.env_id, a.token)
    with pytest.raises(EnvDestroyedError):
        plane.abort_start(a.env_id, a.token)
    log("destroyed env abort -> env_destroyed (not a success)")
    log("every rejected abort left occupancy unchanged: "
        f"{plane.capacity_view().as_dict()}")


def test_abort_distinguishes_from_provisioning_error_and_timeout():
    section("[abort] three start failure causes stay machine-distinguishable")
    from controlplane.errors import ProvisioningError

    # provisioning error
    p1 = ControlPlane(capacity=1, provisioner=lambda env: (_ for _ in ()).throw(RuntimeError("boom")))
    e1 = p1.apply("u").environment
    with pytest.raises(ProvisioningError) as ei:
        p1.start(e1.env_id, e1.token)
    assert ei.value.code == "provisioning_failed"
    assert ei.value.env.fail_reason == "boom"

    # timeout
    ev = threading.Event()
    p2 = ControlPlane(capacity=1, default_start_timeout_seconds=0.1,
                      provisioner=lambda env, c=None: ev.wait(3))
    e2 = p2.apply("u").environment
    with pytest.raises(StartupTimeoutError) as ei:
        p2.start(e2.env_id, e2.token)
    assert ei.value.code == "startup_timeout"
    assert ei.value.env.fail_reason == "startup_timeout"
    ev.set()

    # abort
    entered = threading.Event(); release = threading.Event()
    p3 = ControlPlane(capacity=1, provisioner=lambda env, c=None: (entered.set(), release.wait(3)))
    e3 = p3.apply("u").environment

    def do_aborted_start():
        try:
            p3.start(e3.env_id, e3.token)
        except StartupAbortedError:
            pass  # expected: the waiting caller is told the start was aborted

    t = threading.Thread(target=do_aborted_start, daemon=True)
    t.start(); entered.wait(2)
    env = p3.abort_start(e3.env_id, e3.token)
    assert env.fail_reason == "aborted_by_owner"
    release.set(); t.join(1)

    log("causes: provisioning_failed -> 'boom' | startup_timeout -> "
        "'startup_timeout' | abort -> 'aborted_by_owner'")
    assert p1.capacity_view().used_slots == 0
    assert p3.capacity_view().used_slots == 0


def test_destroy_while_starting_releases_slot_and_wakes_caller():
    section("[slow-start] destroy during STARTING wakes the blocked start call")
    from controlplane.errors import EnvDestroyedError

    entered = threading.Event()
    release = threading.Event()
    plane = ControlPlane(
        capacity=1,
        provisioner=lambda env, cancel_event=None: (entered.set(), release.wait(5)),
    )
    target = plane.apply("alice").environment
    waiter = plane.apply("bob").environment

    seen: dict = {}

    def do_start():
        try:
            plane.start(target.env_id, target.token)
        except EnvDestroyedError:
            seen["err"] = "env_destroyed"

    t = threading.Thread(target=do_start)
    t.start()
    assert entered.wait(2)

    plane.destroy(target.env_id, target.token)
    t.join(timeout=2)
    assert not t.is_alive()
    assert seen.get("err") == "env_destroyed"
    log("waiting start caller woke up with env_destroyed")

    view = plane.capacity_view()
    promoted = plane.status(waiter.env_id, waiter.token)
    assert view.used_slots == 1 and view.queued == 0
    assert promoted.state is EnvState.READY
    release.set()
    log(f"destroy released the STARTING slot; waiter -> {promoted.state.value}, "
        f"view={view.as_dict()}")


def test_lease_expiry_while_starting_releases_slot():
    section("[slow-start] lease expiry during a slow start reclaims the slot")
    from controlplane.errors import EnvExpiredError

    clock = FakeClock()
    entered = threading.Event()
    release = threading.Event()

    def slow(env, cancel_event=None):
        entered.set()
        release.wait(5)

    plane = ControlPlane(
        capacity=1,
        default_lease_seconds=10,
        default_start_timeout_seconds=100,
        provisioner=slow,
        clock=clock,
    )
    target = plane.apply("alice").environment
    waiter = plane.apply("bob").environment

    seen: dict = {}

    def do_start():
        try:
            plane.start(target.env_id, target.token)
        except EnvExpiredError:
            seen["err"] = "env_expired"

    t = threading.Thread(target=do_start, daemon=True)
    t.start()
    assert entered.wait(2)

    clock.advance(10)
    plane.reap_expired()  # sweeps lease expiry
    swept = _wait_state(plane, target.env_id, EnvState.EXPIRED, timeout=1.0)
    promoted = plane.status(waiter.env_id, waiter.token)
    assert swept.end_reason == "expired"
    assert promoted.state is EnvState.READY
    log(f"slow-start env expired at lease boundary; waiter -> "
        f"{promoted.state.value}")
    release.set()
    t.join(2)
    assert seen.get("err") == "env_expired"
    log("waiting start caller woke up with env_expired; no occupancy leaked")


def test_abort_after_timeout_deadline_reports_timeout_not_abort():
    section("[abort] abort landing after the deadline is rejected as timeout")
    clock = FakeClock()
    entered = threading.Event()
    release = threading.Event()

    def hung(env, cancel_event=None):
        entered.set()
        release.wait(5)

    plane = ControlPlane(
        capacity=1,
        default_start_timeout_seconds=10,
        provisioner=hung,
        clock=clock,
    )
    target = plane.apply("alice").environment

    def do_start():
        try:
            plane.start(target.env_id, target.token)
        except StartupTimeoutError:
            pass

    t = threading.Thread(target=do_start, daemon=True)
    t.start()
    assert entered.wait(2)

    clock.advance(10.1)
    with pytest.raises(StartupTimeoutError) as ei:
        plane.abort_start(target.env_id, target.token)
    assert ei.value.code == "startup_timeout"
    assert ei.value.env.fail_reason == "startup_timeout"
    log("abort past the start deadline -> startup_timeout (not a successful abort)")
    release.set()
    t.join(1)
    assert plane.capacity_view().used_slots == 0
