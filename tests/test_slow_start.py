"""Slow startups, the startup time limit and owner-initiated abort.

Scenarios:

* a multi-second startup never blocks the rest of the control plane; other
  apply/status/capacity requests return quickly while the STARTING env keeps
  holding its slot;
* a startup past the time limit is reaped as a *timeout* failure, its slot is
  released and the FIFO queue head is promoted; the env can restart;
* the owner can actively abort a STARTING env, leaving no occupancy, with a
  reason distinct from timeout and from a provisioning crash; non-STARTING /
  cross-credential / expired / destroyed aborts are rejected distinctly.
"""

from __future__ import annotations

import threading
import time

import pytest

from controlplane.errors import (
    EnvAccessDeniedError,
    EnvDestroyedError,
    EnvExpiredError,
    IllegalTransitionError,
    InvalidTokenError,
    ProvisioningError,
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


def _start_async(plane, env_id, token) -> dict:
    """Call start() in a thread; capture the outcome or raised error."""
    box: dict = {}

    def run():
        try:
            box["outcome"] = plane.start(env_id, token)
        except Exception as exc:  # noqa: BLE001 - captured for main-thread asserts
            box["error"] = exc

    t = threading.Thread(target=run, name="slow-start", daemon=True)
    t.start()
    box["thread"] = t
    return box


def test_slow_start_keeps_control_plane_responsive_and_slot_held():
    section("[slow-start] control plane stays responsive during a slow start")
    clock = FakeClock()
    entered = threading.Event()
    finish = threading.Event()

    def slow(env):
        entered.set()
        # Simulate a multi-second real-world startup.
        finish.wait(timeout=5)

    plane = ControlPlane(
        capacity=2, queue_limit=4, start_timeout_seconds=100,
        provisioner=slow, clock=clock,
    )

    a = plane.apply("alice")
    b = plane.apply("bob")  # takes the second slot
    assert a.disposition == "admitted" and b.disposition == "admitted"

    box = _start_async(plane, a.environment.env_id, a.environment.token)
    assert entered.wait(timeout=1), "slow provisioner should have started"
    log("alice's provisioner is blocking; issuing unrelated requests ...")

    # --- while STARTING, other requests are fast and never block ---
    started = time.perf_counter()

    # 1) the starting env's slot counts as occupied (never handed to others)
    view = plane.capacity_view()
    elapsed = time.perf_counter() - started
    assert view.used_slots == 2
    assert view.free_slots == 0
    assert elapsed < 0.5
    log(f"capacity_view() returned in {elapsed*1000:.1f}ms: {view.as_dict()}")

    # 2) a third applicant is queued, not admitted into the held slot
    started = time.perf_counter()
    c = plane.apply("carol")
    elapsed = time.perf_counter() - started
    assert c.disposition == "queued"
    assert elapsed < 0.5
    log(f"carol apply() returned in {elapsed*1000:.1f}ms -> queued (no slot stealing)")

    # 3) someone else's status query is fast and unaffected
    started = time.perf_counter()
    bob_env = plane.status(b.environment.env_id, b.environment.token)
    elapsed = time.perf_counter() - started
    assert bob_env.state is EnvState.READY
    assert elapsed < 0.5
    log(f"bob status() returned in {elapsed*1000:.1f}ms -> {bob_env.state.value}")

    # 4) the starting env itself reports STARTING and still holds the slot
    alice_now = plane.status(a.environment.env_id, a.environment.token)
    assert alice_now.state is EnvState.STARTING
    assert plane.capacity_view().used_slots == 2
    log(f"alice is {alice_now.state.value} and still occupies her slot")

    # --- let the slow startup complete; caller observes RUNNING ---
    finish.set()
    box["thread"].join(timeout=2)
    assert not box["thread"].is_alive()
    assert "error" not in box
    assert box["outcome"].environment.state is EnvState.RUNNING
    log(f"slow start finished -> {box['outcome'].environment.state.value}; "
        f"carol still queued, used={plane.capacity_view().used_slots}")


def test_start_timeout_reclaims_slot_and_promotes_queue_head():
    section("[slow-start] startup time limit reclaims slot, head promotes, restartable")
    clock = FakeClock()
    entered = threading.Event()
    release = threading.Event()

    def hanging(env):
        entered.set()
        # Pretend the startup is stuck; do not release until told.
        release.wait(timeout=5)

    plane = ControlPlane(
        capacity=1, queue_limit=2, start_timeout_seconds=5,
        provisioner=hanging, clock=clock,
    )
    a = plane.apply("alice")
    w = plane.apply("bob")
    assert a.disposition == "admitted" and w.disposition == "queued"

    box = _start_async(plane, a.environment.env_id, a.environment.token)
    assert entered.wait(timeout=1)
    assert plane.status(a.environment.env_id, a.environment.token).state is EnvState.STARTING

    # Before the deadline nothing is reaped.
    clock.advance(4)
    assert plane.capacity_view().used_slots == 1
    plane.maintain()
    assert plane.status(a.environment.env_id, a.environment.token).state is EnvState.STARTING
    log("after 4s (< limit 5s) startup still STARTING, slot still held")

    # Cross the time limit: maintenance (also runs lazily on any request)
    # reaps the attempt as a timeout and promotes the FIFO head.
    clock.advance(2)  # now 6s >= 5s deadline
    expired, timed_out = plane.maintain()
    assert expired == []
    assert timed_out == [a.environment.env_id]
    log(f"maintenance reaped timed-out start: {timed_out}")

    failed = plane.store.get_locked(a.environment.env_id)
    assert failed.state is EnvState.FAILED
    assert failed.fail_kind == FAIL_KIND_TIMEOUT
    assert "startup_timeout" in failed.fail_reason
    assert failed.failure_count == 1

    view = plane.capacity_view()
    assert view.used_slots == 1  # promoted waiter holds it
    assert view.queued == 0
    promoted = plane.status(w.environment.env_id, w.environment.token)
    assert promoted.state is EnvState.READY
    log(f"alice FAILED({failed.fail_kind}); bob promoted to "
        f"{promoted.state.value}; view={view.as_dict()}")

    # The hanging start() caller learns it timed out (distinct error code).
    release.set()
    box["thread"].join(timeout=2)
    assert isinstance(box.get("error"), StartTimeoutError)
    log(f"hanging start() caller got: {type(box['error']).__name__}")

    # The timed-out env can restart once provisioning recovers and a slot frees.
    plane.stop(w.environment.env_id, w.environment.token)
    assert plane.capacity_view().used_slots == 0

    def instant(env):
        return None

    plane.provisioner = instant
    out = plane.start(a.environment.env_id, a.environment.token)
    assert out.environment.state is EnvState.RUNNING
    assert plane.capacity_view().used_slots == 1
    log("alice restarted successfully after timeout -> RUNNING, slot held again")


def test_overdue_start_times_out_without_any_other_activity():
    section("[slow-start] start past the limit is a timeout on its own "
            "(no other request, no reaper, real clock)")
    # Real clock on purpose: the limit must be enforced by the start() call
    # itself. The provisioner is slow but finite (0.25s > 0.2s limit) and
    # nothing else touches the plane: no concurrent requests, no maintain(),
    # no reaper thread.
    def slow(env):
        time.sleep(0.25)

    plane = ControlPlane(
        capacity=1, queue_limit=2, start_timeout_seconds=0.2,
        provisioner=slow,
    )
    a = plane.apply("alice")
    w = plane.apply("bob")
    assert a.disposition == "admitted" and w.disposition == "queued"

    t0 = time.perf_counter()
    with pytest.raises(StartTimeoutError) as ei:
        plane.start(a.environment.env_id, a.environment.token)
    elapsed = time.perf_counter() - t0
    assert elapsed < 1.0, "caller must return at the limit, not when the provisioner ends"
    log(f"single 0.25s start vs 0.2s limit -> StartTimeoutError "
        f"after {elapsed*1000:.0f}ms (code={ei.value.code})")

    failed = plane.store.get_locked(a.environment.env_id)
    assert failed.state is EnvState.FAILED
    assert failed.fail_kind == FAIL_KIND_TIMEOUT
    assert "startup_timeout" in failed.fail_reason
    assert failed.failure_count == 1

    # The slot was released and the FIFO queue head took it, all within the
    # timed-out start() call itself.
    view = plane.capacity_view()
    assert view.used_slots == 1
    assert view.queued == 0
    promoted = plane.status(w.environment.env_id, w.environment.token)
    assert promoted.state is EnvState.READY
    log(f"alice FAILED({failed.fail_kind}); slot released, bob promoted to "
        f"{promoted.state.value}; view={view.as_dict()}")

    # The timed-out env restarts successfully once a slot frees.
    plane.stop(w.environment.env_id, w.environment.token)
    assert plane.capacity_view().used_slots == 0
    plane.provisioner = lambda env: None
    out = plane.start(a.environment.env_id, a.environment.token)
    assert out.environment.state is EnvState.RUNNING
    assert plane.capacity_view().used_slots == 1
    log("alice restarted after the timeout -> RUNNING")


def test_hung_start_returns_timeout_at_deadline_and_stays_responsive():
    section("[slow-start] hung start: caller times out at the limit, "
            "plane stays fast, no residual occupancy")
    hang = threading.Event()  # released only at the very end: a stuck startup

    plane = ControlPlane(
        capacity=1, queue_limit=2, start_timeout_seconds=0.2,
        provisioner=lambda env: hang.wait(),
    )
    a = plane.apply("alice")
    w = plane.apply("bob")
    assert w.disposition == "queued"

    box = _start_async(plane, a.environment.env_id, a.environment.token)
    t0 = time.perf_counter()
    box["thread"].join(timeout=5)
    elapsed = time.perf_counter() - t0
    assert not box["thread"].is_alive(), "caller must not be held by the hung start"
    assert isinstance(box.get("error"), StartTimeoutError)
    assert elapsed < 2.0
    log(f"hung start -> caller got StartTimeoutError after {elapsed*1000:.0f}ms "
        f"(limit 200ms) while the provisioner is still stuck")

    # The attempt was reaped by the caller itself: slot freed, head promoted.
    failed = plane.store.get_locked(a.environment.env_id)
    assert failed.state is EnvState.FAILED
    assert failed.fail_kind == FAIL_KIND_TIMEOUT
    view = plane.capacity_view()
    assert view.used_slots == 1  # the promoted waiter, not the stuck env
    assert view.queued == 0
    assert plane.status(w.environment.env_id, w.environment.token).state is EnvState.READY
    log(f"slot reclaimed despite the stuck provisioner; bob promoted; "
        f"view={view.as_dict()}")

    # The provisioner is STILL hung, yet unrelated requests return fast.
    started = time.perf_counter()
    plane.capacity_view()
    c = plane.apply("carol")
    other = plane.status(w.environment.env_id, w.environment.token)
    elapsed = time.perf_counter() - started
    assert c.disposition == "queued"
    assert other.state is EnvState.READY
    assert elapsed < 0.5
    log(f"with the provisioner still hung: capacity/apply/status all returned "
        f"in {elapsed*1000:.1f}ms (carol queued, no slot stealing)")

    # When the stuck provisioner finally returns, its late result is
    # discarded: nothing changes, no slot is resurrected.
    hang.set()
    time.sleep(0.1)
    still = plane.store.get_locked(a.environment.env_id)
    assert still.state is EnvState.FAILED
    assert still.fail_kind == FAIL_KIND_TIMEOUT
    assert plane.capacity_view().used_slots == 1
    log("late return of the stuck provisioner discarded; state unchanged")

    # And the env can still be restarted successfully afterwards.
    plane.cancel(c.environment.env_id, c.environment.token)
    plane.stop(w.environment.env_id, w.environment.token)
    assert plane.capacity_view().used_slots == 0
    plane.provisioner = lambda env: None
    out = plane.start(a.environment.env_id, a.environment.token)
    assert out.environment.state is EnvState.RUNNING
    log("alice restarted after the hung attempt -> RUNNING")


def test_owner_abort_releases_slot_with_distinct_reason():
    section("[slow-start] owner aborts STARTING env; no occupancy remains")
    clock = FakeClock()
    entered = threading.Event()
    release = threading.Event()

    def slow(env):
        entered.set()
        release.wait(timeout=5)

    plane = ControlPlane(
        capacity=1, queue_limit=2, start_timeout_seconds=100,
        provisioner=slow, clock=clock,
    )
    a = plane.apply("alice")
    w = plane.apply("bob")
    assert w.disposition == "queued"

    box = _start_async(plane, a.environment.env_id, a.environment.token)
    assert entered.wait(timeout=1)

    aborted = plane.abort_start(a.environment.env_id, a.environment.token)
    assert aborted.state is EnvState.FAILED
    assert aborted.fail_kind == FAIL_KIND_ABORTED
    assert "startup_aborted" in aborted.fail_reason
    log(f"abort -> FAILED({aborted.fail_kind}) reason={aborted.fail_reason}")

    view = plane.capacity_view()
    assert view.used_slots == 1  # promoted waiter
    assert view.queued == 0
    promoted = plane.status(w.environment.env_id, w.environment.token)
    assert promoted.state is EnvState.READY
    log(f"no residual occupancy; bob promoted to {promoted.state.value}")

    # The in-flight start() caller receives an abort error, not timeout/502.
    release.set()
    box["thread"].join(timeout=2)
    err = box.get("error")
    assert isinstance(err, StartAbortedError)
    log(f"aborted start() caller got: {type(err).__name__} (code={err.code})")

    # Restartable after abort.
    plane.stop(w.environment.env_id, w.environment.token)
    plane.provisioner = lambda env: None
    out = plane.start(a.environment.env_id, a.environment.token)
    assert out.environment.state is EnvState.RUNNING
    log("alice restarted after abort -> RUNNING")


def test_abort_rejections_are_distinct():
    section("[slow-start] abort on non-STARTING / bad credentials is rejected")
    clock = FakeClock()
    plane = ControlPlane(
        capacity=2, queue_limit=2, start_timeout_seconds=100,
        provisioner=lambda env: None, clock=clock,
    )

    ready = plane.apply("alice")                 # READY
    other = plane.apply("bob")                   # READY
    queued = plane.apply("carol")
    assert queued.disposition == "queued"

    # not STARTING -> illegal_transition with a state-specific reason
    with pytest.raises(IllegalTransitionError) as ei:
        plane.abort_start(ready.environment.env_id, ready.environment.token)
    assert ei.value.from_state == "READY"
    assert ei.value.reason == "only_starting_requests_can_abort"
    log("abort on READY -> illegal_transition(only_starting_requests_can_abort)")

    with pytest.raises(IllegalTransitionError) as ei:
        plane.abort_start(queued.environment.env_id, queued.environment.token)
    assert ei.value.from_state == "QUEUED"
    log("abort on QUEUED -> illegal_transition from QUEUED (use cancel instead)")

    # cross-credential access denied
    with pytest.raises(EnvAccessDeniedError):
        plane.abort_start(ready.environment.env_id, other.environment.token)
    log("abort with another env's token -> env_access_denied")

    # unknown token
    with pytest.raises(InvalidTokenError):
        plane.abort_start(ready.environment.env_id, "tok_does_not_exist")
    log("abort with bogus token -> invalid_token")

    # destroyed env
    plane.destroy(ready.environment.env_id, ready.environment.token)
    with pytest.raises(EnvDestroyedError):
        plane.abort_start(ready.environment.env_id, ready.environment.token)
    log("abort after destroy -> env_destroyed")


def test_abort_with_expired_credential_is_token_expired():
    section("[slow-start] abort during STARTING with an expired lease rejected")
    clock = FakeClock()
    entered = threading.Event()
    release = threading.Event()

    def slow(env):
        entered.set()
        release.wait(timeout=5)

    plane = ControlPlane(
        capacity=1, start_timeout_seconds=1000,
        provisioner=slow, clock=clock,
    )
    a = plane.apply("alice", lease_seconds=10)
    box = _start_async(plane, a.environment.env_id, a.environment.token)
    assert entered.wait(timeout=1)
    assert plane.capacity_view().used_slots == 1

    # Lease (10s) elapses well before the generous startup limit (1000s);
    # the STARTING env is reaped on use as EXPIRED, not as an abort.
    clock.advance(11)
    with pytest.raises(TokenExpiredError):
        plane.abort_start(a.environment.env_id, a.environment.token)
    log("abort after lease expiry -> token_expired (env reaped as EXPIRED)")

    reaped = plane.store.get_locked(a.environment.env_id)
    assert reaped.state is EnvState.EXPIRED
    assert plane.capacity_view().used_slots == 0
    release.set()
    box["thread"].join(timeout=2)
    # The start caller likewise sees expiry, never a false success.
    assert "outcome" not in box
    assert isinstance(box.get("error"), EnvExpiredError)
    log(f"start() caller after expiry got: {type(box.get('error')).__name__}")


def test_three_failure_kinds_are_distinguishable():
    section("[slow-start] provisioning error vs timeout vs abort are distinct")
    clock = FakeClock()
    kinds: dict[str, str] = {}

    # 1) provisioning error
    def boom(env):
        raise RuntimeError("image pull failed")

    p1 = ControlPlane(capacity=1, provisioner=boom, clock=FakeClock())
    o1 = p1.apply("u1")
    with pytest.raises(ProvisioningError):
        p1.start(o1.environment.env_id, o1.environment.token)
    kinds["crash"] = p1.store.get_locked(o1.environment.env_id).fail_kind
    assert kinds["crash"] == FAIL_KIND_PROVISIONING

    # 2) timeout
    release = threading.Event()
    p2 = ControlPlane(
        capacity=1, start_timeout_seconds=3,
        provisioner=lambda env: release.wait(timeout=5), clock=clock,
    )
    o2 = p2.apply("u2")
    box = _start_async(p2, o2.environment.env_id, o2.environment.token)
    clock.advance(4)
    p2.maintain()
    kinds["timeout"] = p2.store.get_locked(o2.environment.env_id).fail_kind
    assert kinds["timeout"] == FAIL_KIND_TIMEOUT
    release.set()
    box["thread"].join(timeout=2)

    # 3) abort
    go = threading.Event()
    done = threading.Event()
    p3 = ControlPlane(
        capacity=1, start_timeout_seconds=100,
        provisioner=lambda env: (go.set(), done.wait(timeout=5)), clock=FakeClock(),
    )
    o3 = p3.apply("u3")
    box3 = _start_async(p3, o3.environment.env_id, o3.environment.token)
    assert go.wait(timeout=1)
    p3.abort_start(o3.environment.env_id, o3.environment.token)
    kinds["abort"] = p3.store.get_locked(o3.environment.env_id).fail_kind
    done.set()
    box3["thread"].join(timeout=2)
    assert kinds["abort"] == FAIL_KIND_ABORTED

    assert len(set(kinds.values())) == 3
    log(f"three distinct fail_kinds observed: {kinds}")
