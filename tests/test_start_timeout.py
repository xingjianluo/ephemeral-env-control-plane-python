"""Startup time limit guarantees, exercised with the real (wall) clock.

These scenarios mirror the local reproduction: a single start, no other
requests in flight, no reaper running. The guarantees must hold anyway:

* during a slow start, unrelated apply/capacity/status requests stay fast
  and the STARTING env keeps holding its slot;
* a start that overruns the limit is ruled a timeout *by the start call
  itself* -- env FAILED with a timeout reason, slot released immediately,
  FIFO queue head promoted -- and the env can be restarted afterwards;
* a hung start returns ``start_timeout`` to its caller at the deadline
  instead of blocking forever, and never slows the rest of the plane;
* an owner abort leaves no residual occupancy and stays distinguishable
  from a timeout and from a provisioning crash; invalid aborts are
  rejected with the existing state/credential semantics.
"""

from __future__ import annotations

import threading
import time

import pytest

from controlplane.errors import (
    EnvAccessDeniedError,
    EnvDestroyedError,
    IllegalTransitionError,
    StartAbortedError,
    StartTimeoutError,
    TokenExpiredError,
)
from controlplane.models import (
    FAIL_KIND_ABORTED,
    FAIL_KIND_PROVISIONING,
    FAIL_KIND_TIMEOUT,
    EnvState,
    FakeClock,
)
from controlplane.service import ControlPlane

from ._logging import log, section

# Wall-clock margins: the limit is 0.2s, so anything well below 1s proves
# the caller was not dragged by the slow/hung provisioner.
LIMIT = 0.2


def _start_async(plane, env_id, token) -> dict:
    box: dict = {}

    def run():
        started = time.monotonic()
        try:
            box["outcome"] = plane.start(env_id, token)
        except Exception as exc:  # noqa: BLE001 - captured for main-thread asserts
            box["error"] = exc
        box["elapsed"] = time.monotonic() - started

    t = threading.Thread(target=run, name="start-caller", daemon=True)
    t.start()
    box["thread"] = t
    return box


def test_slow_start_others_stay_fast_and_slot_held():
    section("[start-timeout] slow start: others fast, STARTING slot held")
    entered = threading.Event()
    finish = threading.Event()

    def slow(env):
        entered.set()
        finish.wait(timeout=5)

    plane = ControlPlane(capacity=2, queue_limit=2, start_timeout_seconds=5)
    plane.provisioner = slow
    a = plane.apply("alice")
    b = plane.apply("bob")
    assert a.disposition == "admitted" and b.disposition == "admitted"

    box = _start_async(plane, a.environment.env_id, a.environment.token)
    assert entered.wait(timeout=1)
    log("alice STARTING with a blocked provisioner; probing other requests ...")

    t0 = time.monotonic()
    view = plane.capacity_view()
    fast = time.monotonic() - t0
    assert view.used_slots == 2 and view.free_slots == 0
    assert fast < 0.5
    log(f"capacity_view() in {fast*1000:.1f}ms -> {view.as_dict()} (slot held)")

    t0 = time.monotonic()
    c = plane.apply("carol")
    fast = time.monotonic() - t0
    assert c.disposition == "queued" and fast < 0.5
    log(f"carol apply() in {fast*1000:.1f}ms -> queued (no slot stealing)")

    t0 = time.monotonic()
    bob = plane.status(b.environment.env_id, b.environment.token)
    fast = time.monotonic() - t0
    assert bob.state is EnvState.READY and fast < 0.5
    log(f"bob status() in {fast*1000:.1f}ms -> {bob.state.value}")

    alice = plane.status(a.environment.env_id, a.environment.token)
    assert alice.state is EnvState.STARTING
    log(f"alice still {alice.state.value}, used={plane.capacity_view().used_slots}")

    finish.set()
    box["thread"].join(timeout=2)
    assert not box["thread"].is_alive()
    assert "error" not in box
    assert box["outcome"].environment.state is EnvState.RUNNING
    log(f"slow start completed -> {box['outcome'].environment.state.value}")


def test_overrun_without_other_activity_times_out_and_fifo_promotes():
    section("[start-timeout] lone start past the limit -> timeout, slot freed, FIFO head promoted")
    plane = ControlPlane(capacity=1, queue_limit=2, start_timeout_seconds=LIMIT)
    # The start takes 0.6s: 3x the 0.2s limit. No reaper, no other requests.
    plane.provisioner = lambda env: time.sleep(0.6)

    a = plane.apply("alice")
    w = plane.apply("bob")
    assert a.disposition == "admitted" and w.disposition == "queued"

    t0 = time.monotonic()
    with pytest.raises(StartTimeoutError):
        plane.start(a.environment.env_id, a.environment.token)
    elapsed = time.monotonic() - t0
    # Ruled at the deadline (~0.2s), not when the provisioner returns (0.6s).
    assert elapsed < 0.5
    log(f"start() raised start_timeout after {elapsed*1000:.0f}ms "
        f"(limit {LIMIT*1000:.0f}ms, provision would take 600ms)")

    env = plane.store.get_locked(a.environment.env_id)
    assert env.state is EnvState.FAILED
    assert env.fail_kind == FAIL_KIND_TIMEOUT
    assert env.fail_reason.startswith("startup_timeout:")
    log(f"alice -> {env.state.value} fail_kind={env.fail_kind} "
        f"reason={env.fail_reason!r}")

    # Slot released immediately; the FIFO head was promoted in order.
    view = plane.capacity_view()
    assert view.used_slots == 1  # bob now holds the single slot
    bob = plane.store.get_locked(w.environment.env_id)
    assert bob.state is EnvState.READY
    log(f"slot handed to queue head in FIFO order: bob -> {bob.state.value}")

    # The failed env can be restarted successfully once a slot frees.
    plane.destroy(w.environment.env_id, w.environment.token)
    plane.provisioner = lambda env: None
    out = plane.start(a.environment.env_id, a.environment.token)
    assert out.environment.state is EnvState.RUNNING
    log(f"alice restarted after timeout -> {out.environment.state.value}")


def test_hung_start_returns_timeout_at_deadline_and_plane_stays_fast():
    section("[start-timeout] hung start: caller unblocked at the limit, plane stays fast")
    hang = threading.Event()  # never set until the test allows it
    entered = threading.Event()

    def stuck(env):
        entered.set()
        hang.wait(timeout=30)

    plane = ControlPlane(capacity=1, queue_limit=2, start_timeout_seconds=LIMIT)
    plane.provisioner = stuck
    a = plane.apply("alice")

    box = _start_async(plane, a.environment.env_id, a.environment.token)
    assert entered.wait(timeout=1)

    # While the provisioner is stuck (before the deadline), the rest of the
    # control plane is still fast.
    t0 = time.monotonic()
    view = plane.capacity_view()
    w = plane.apply("bob")
    fast = time.monotonic() - t0
    assert view.used_slots == 1 and w.disposition == "queued"
    assert fast < 0.5
    log(f"during hang: capacity_view+apply returned in {fast*1000:.1f}ms, "
        f"used={view.used_slots}, bob queued")

    # The caller is released at the deadline with start_timeout, not stuck
    # behind the hung provisioner.
    box["thread"].join(timeout=5)
    assert not box["thread"].is_alive(), "start() must not hang with the provisioner"
    assert isinstance(box.get("error"), StartTimeoutError)
    assert box["elapsed"] < 1.0
    log(f"caller got start_timeout after {box['elapsed']*1000:.0f}ms "
        f"while provisioner still hung")

    env = plane.store.get_locked(a.environment.env_id)
    assert env.state is EnvState.FAILED and env.fail_kind == FAIL_KIND_TIMEOUT
    assert plane.capacity_view().used_slots == 1  # bob promoted, alice freed
    log(f"alice FAILED({env.fail_kind}), slot freed, bob promoted")

    # The stuck provisioner's late return must not corrupt anything.
    hang.set()
    time.sleep(0.1)
    env = plane.store.get_locked(a.environment.env_id)
    assert env.state is EnvState.FAILED and env.failure_count == 1
    log("late return of the stuck provisioner discarded (stale attempt)")

    # And the env restarts fine afterwards.
    plane.destroy(w.environment.env_id, w.environment.token)
    plane.provisioner = lambda env: None
    out = plane.start(a.environment.env_id, a.environment.token)
    assert out.environment.state is EnvState.RUNNING
    log(f"alice restarted after hung-start timeout -> {out.environment.state.value}")


def test_abort_leaves_no_occupancy_and_reasons_stay_distinct():
    section("[start-timeout] abort: no residual occupancy, three failure kinds distinct")
    release = threading.Event()
    entered = threading.Event()

    def slow(env):
        entered.set()
        release.wait(timeout=5)

    plane = ControlPlane(capacity=1, queue_limit=2, start_timeout_seconds=30)
    plane.provisioner = slow
    a = plane.apply("alice")
    w = plane.apply("bob")
    assert w.disposition == "queued"

    box = _start_async(plane, a.environment.env_id, a.environment.token)
    assert entered.wait(timeout=1)

    aborted = plane.abort_start(a.environment.env_id, a.environment.token)
    assert aborted.state is EnvState.FAILED
    assert aborted.fail_kind == FAIL_KIND_ABORTED
    assert aborted.fail_reason.startswith("startup_aborted:")
    log(f"abort -> FAILED({aborted.fail_kind}) reason={aborted.fail_reason!r}")

    # No residual occupancy: alice's slot went to the FIFO head.
    view = plane.capacity_view()
    assert view.used_slots == 1  # only bob, promoted in order
    assert plane.store.get_locked(w.environment.env_id).state is EnvState.READY
    log(f"no residual occupancy: used={view.used_slots} is bob's promoted slot")

    # The in-flight start() caller is told it was aborted (not a timeout).
    box["thread"].join(timeout=2)
    assert isinstance(box.get("error"), StartAbortedError)
    log(f"start() caller got {type(box['error']).__name__}")
    release.set()

    # The three failure kinds remain distinguishable on the same plane.
    assert FAIL_KIND_ABORTED != FAIL_KIND_TIMEOUT != FAIL_KIND_PROVISIONING
    kinds = {FAIL_KIND_PROVISIONING, FAIL_KIND_TIMEOUT, FAIL_KIND_ABORTED}
    assert aborted.fail_kind in kinds
    log(f"fail_kind stays distinct from timeout/provisioning: {sorted(kinds)}")

    # Restartable after an abort.
    plane.destroy(w.environment.env_id, w.environment.token)
    plane.provisioner = lambda env: None
    out = plane.start(a.environment.env_id, a.environment.token)
    assert out.environment.state is EnvState.RUNNING
    log(f"alice restarted after abort -> {out.environment.state.value}")


def test_abort_rejections_keep_existing_semantics():
    section("[start-timeout] abort rejections: non-starting / foreign / expired / destroyed")
    clock = FakeClock()
    plane = ControlPlane(
        capacity=2, queue_limit=2, start_timeout_seconds=100, clock=clock,
    )
    ready = plane.apply("alice", lease_seconds=10)
    other = plane.apply("bob")

    # Not STARTING -> illegal transition, no slot change, not a success.
    with pytest.raises(IllegalTransitionError):
        plane.abort_start(ready.environment.env_id, ready.environment.token)
    log("abort on READY -> illegal_transition (only_starting_requests_can_abort)")

    # Someone else's credential -> access denied.
    with pytest.raises(EnvAccessDeniedError):
        plane.abort_start(ready.environment.env_id, other.environment.token)
    log("abort with bob's token on alice's env -> env_access_denied")

    # Credential whose lease just elapsed -> token_expired on this call.
    clock.advance(11)
    with pytest.raises(TokenExpiredError):
        plane.abort_start(ready.environment.env_id, ready.environment.token)
    log("abort after lease expiry -> token_expired (reaped as EXPIRED)")

    # Destroyed env -> env_destroyed.
    plane.destroy(other.environment.env_id, other.environment.token)
    with pytest.raises(EnvDestroyedError):
        plane.abort_start(other.environment.env_id, other.environment.token)
    log("abort on destroyed env -> env_destroyed")

    # No abort left any occupancy behind.
    assert plane.capacity_view().used_slots == 0
    log(f"no residual occupancy after rejected aborts: "
        f"used={plane.capacity_view().used_slots}")
