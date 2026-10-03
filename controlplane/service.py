"""Control-plane service: lifecycle, capacity, lease and credential policy.

Everything mutating state runs inside the store lock, so concurrent
applications, the lease reaper and owner operations can never observe
half-applied changes. Provisioning is a local in-process simulation that can
take real time (or hang), so it runs in its own daemon thread **outside** the
store lock: while an environment is STARTING the plane keeps serving apply /
status / capacity requests instantly, and the STARTING reservation honestly
occupies its slot. The start caller waits on a per-attempt condition with the
configured time limit; the reaper and any mutating call also sweep STARTING
attempts past their deadline, so a hung start always gets reclaimed with a
"startup_timeout" cause and the head waiter is promoted. Owners can
additionally abort an in-flight start ("aborted_by_owner"); both causes are
kept distinct from a provisioning error.
"""

from __future__ import annotations

import inspect
import secrets
import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from .errors import (
    CapacityExhaustedError,
    EnvAccessDeniedError,
    EnvCanceledError,
    EnvDestroyedError,
    EnvExpiredError,
    EnvNotFoundError,
    IllegalTransitionError,
    InvalidTokenError,
    ProvisioningError,
    StartupAbortedError,
    StartupTimeoutError,
    TokenExpiredError,
)
from .models import (
    ApplyOutcome,
    CapacityView,
    Clock,
    Environment,
    EnvState,
    SLOT_HOLDING_STATES,
    StartOutcome,
    SystemClock,
    TERMINAL_STATES,
)
from .store import CapacityConfig, StateStore

# A provisioner simulates local environment startup. It either returns
# normally or raises; raising means startup failed (slot must be released).
# It may optionally accept a ``threading.Event`` as a second argument which is
# set when the owner aborts the start (best-effort cooperative cancellation).
Provisioner = Callable[..., None]

# Stable fail_reason values distinguishing the three start failure causes.
# A provisioning error stores the raw exception text instead.
FAIL_REASON_STARTUP_TIMEOUT = "startup_timeout"
FAIL_REASON_ABORTED_BY_OWNER = "aborted_by_owner"


def new_env_id() -> str:
    return "env_" + secrets.token_hex(6)


def new_token() -> str:
    return "tok_" + secrets.token_hex(16)


@dataclass
class StartAttempt:
    """Coordination for one asynchronous provisioning run.

    The provisioner runs on a worker daemon thread without holding the store
    lock. The thread that requested the start waits on ``cond``; abort and
    the timeout sweeper wake it up. All env state transitions still happen
    under the store lock; ``cond`` only wakes waiters.
    """

    env_id: str
    started_at: float
    timeout_seconds: float | None
    cond: threading.Condition = field(default_factory=threading.Condition)
    finished: bool = False  # set once the worker thread returned/raised
    success: bool = False
    error: BaseException | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event)


def env_to_dict(env: Environment) -> dict:
    return {
        "env_id": env.env_id,
        "owner": env.owner,
        "state": env.state.value,
        "token": env.token,
        "lease_seconds": env.lease_seconds,
        "created_at": env.created_at,
        "accepted_at": env.accepted_at,
        "started_at": env.started_at,
        "starting_at": env.starting_at,
        "expires_at": env.expires_at,
        "ended_at": env.ended_at,
        "end_reason": env.end_reason,
        "fail_reason": env.fail_reason,
        "reject_reason": env.reject_reason,
        "failure_count": env.failure_count,
        "idempotency_key": env.idempotency_key,
        "sequence": env.sequence,
    }


@dataclass
class ControlPlane:
    capacity: int = 2
    queue_limit: int = 4
    default_lease_seconds: int = 60
    # Upper bound for a single start; None means no time limit.
    default_start_timeout_seconds: float | None = None
    clock: Clock | None = None
    provisioner: Provisioner | None = None
    _store: StateStore | None = field(default=None, init=False, repr=False)
    _reaper: "LeaseReaper | None" = field(default=None, init=False, repr=False)
    # env_id -> StartAttempt while the environment is STARTING. Guarded by the
    # store lock; attempt.cond may additionally be taken while holding it.
    _start_attempts: dict[str, StartAttempt] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")
        if self.queue_limit < 0:
            raise ValueError("queue_limit must be >= 0")
        if self.default_lease_seconds <= 0:
            raise ValueError("default_lease_seconds must be positive")
        if (
            self.default_start_timeout_seconds is not None
            and self.default_start_timeout_seconds <= 0
        ):
            raise ValueError("default_start_timeout_seconds must be positive or None")
        self.clock = self.clock or SystemClock()
        self._store = StateStore(
            CapacityConfig(capacity=self.capacity, queue_limit=self.queue_limit),
            clock=self.clock,
        )

    @property
    def store(self) -> StateStore:
        assert self._store is not None
        return self._store

    # -- apply / admission ---------------------------------------------

    def apply(
        self,
        owner: str,
        lease_seconds: int | None = None,
        idempotency_key: str | None = None,
        fingerprint: str | None = None,
    ) -> ApplyOutcome:
        lease = lease_seconds if lease_seconds is not None else self.default_lease_seconds
        if lease <= 0:
            raise ValueError("lease_seconds must be positive")
        store = self.store
        with store.lock:
            now = store.clock.now()
            # Reclaim expired leases first so freed slots are usable.
            self._run_housekeeping_locked(now)

            if idempotency_key is not None:
                existing = store.resolve_idem_locked(idempotency_key)
                if existing is not None:
                    same = (
                        existing.owner == owner
                        and existing.spec_fingerprint == fingerprint
                        and existing.lease_seconds == lease
                    )
                    if not same:
                        raise IllegalTransitionError(
                            existing.state.value,
                            "apply(idempotency_replay)",
                            "idempotency key reused with a different request",
                        )
                    return ApplyOutcome(
                        environment=existing,
                        deduplicated=True,
                        disposition=self._disposition_locked(existing),
                        reason=self._reason_locked(existing),
                        capacity=store.capacity_view_locked(),
                    )

            env = Environment(
                env_id=new_env_id(),
                owner=owner,
                state=EnvState.QUEUED,
                token=new_token(),
                lease_seconds=lease,
                created_at=now,
                enqueued_at=now,
                idempotency_key=idempotency_key,
                spec_fingerprint=fingerprint,
                sequence=store.next_sequence_locked(),
            )
            store.token_index[env.token] = env.env_id
            store.put_locked(env)
            if idempotency_key is not None:
                store.idem_index[idempotency_key] = env.env_id
            store.enqueue_locked(env)

            view_before = store.capacity_view_locked()
            if view_before.free_slots > 0:
                admitted = self._admit_locked()
                target = next((e for e in admitted if e.env_id == env.env_id), env)
                return ApplyOutcome(
                    environment=target,
                    deduplicated=False,
                    disposition="admitted",
                    reason="slot_available",
                    capacity=store.capacity_view_locked(),
                )
            if view_before.queued <= store.config.queue_limit:
                # Still inside the queue budget (our enqueue is counted).
                refreshed = store.get_locked(env.env_id) or env
                return ApplyOutcome(
                    environment=refreshed,
                    deduplicated=False,
                    disposition="queued",
                    reason=self._reason_locked(refreshed),
                    capacity=store.capacity_view_locked(),
                )
            # Queue full: reject deterministically, leave a traceable record.
            self._reject_locked(
                env,
                f"all {store.config.capacity} slots in use and wait queue "
                f"full (limit {store.config.queue_limit})",
            )
            raise CapacityExhaustedError(
                env.reject_reason, env_id=env.env_id
            )

    # -- start / stop / destroy ----------------------------------------

    def start(
        self,
        env_id: str,
        token: str,
        timeout_seconds: float | None = -1,
    ) -> StartOutcome:
        """Start an environment.

        ``timeout_seconds`` overrides the plane-wide start time limit; pass
        ``None`` to disable it for this call, leave the default (-1) to use
        the configured limit. Provisioning runs outside the store lock, so
        this call blocks at most until provisioning finishes or the time
        limit is hit; other plane requests stay fast meanwhile.
        """
        if timeout_seconds == -1:
            timeout_seconds = self.default_start_timeout_seconds
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive or None")
        store = self.store
        with store.lock:
            now = store.clock.now()
            self._run_housekeeping_locked(now)
            env = self._authenticate_locked(env_id, token)

            if env.state in (EnvState.STARTING, EnvState.RUNNING):
                return StartOutcome(
                    environment=env,
                    disposition="running",
                    reason="already_running",
                    capacity=store.capacity_view_locked(),
                )
            if env.state is EnvState.READY:
                attempt = self._begin_provision_locked(env, now, timeout_seconds)
            elif env.state in (EnvState.STOPPED, EnvState.FAILED):
                view = store.capacity_view_locked()
                if view.free_slots <= 0:
                    env.state = EnvState.QUEUED
                    env.enqueued_at = now
                    env.expires_at = None
                    env.accepted_at = None
                    env.started_at = None
                    env.starting_at = None
                    store.enqueue_locked(env)
                    return StartOutcome(
                        environment=env,
                        disposition="queued",
                        reason=self._reason_locked(env),
                        capacity=store.capacity_view_locked(),
                    )
                self._reserve_locked(env, now)
                attempt = self._begin_provision_locked(env, now, timeout_seconds)
            elif env.state is EnvState.QUEUED:
                return StartOutcome(
                    environment=env,
                    disposition="queued",
                    reason=self._reason_locked(env),
                    capacity=store.capacity_view_locked(),
                )
            else:
                self._illegal_terminal(env, "start")

        # Provision without the store lock: a slow/hung provisioner must not
        # block apply/status/capacity requests from anyone else.
        try:
            self._launch_attempt(attempt)
            self._await_attempt(attempt)
        finally:
            # Never keep a stale attempt key around if the waiter left early.
            with store.lock:
                self._discard_attempt_locked(attempt)
        with store.lock:
            env = store.get_locked(env_id)
            assert env is not None
            if env.state is EnvState.RUNNING:
                return StartOutcome(
                    environment=env,
                    disposition="running",
                    reason="provisioned",
                    capacity=store.capacity_view_locked(),
                )
            self._raise_after_await_locked(env)

    def abort_start(self, env_id: str, token: str) -> Environment:
        """Abort an in-progress (STARTING) start.

        Only a STARTING environment can be aborted: the reservation is
        released, the head waiter is promoted, and the failure cause is
        recorded as ``aborted_by_owner`` (distinct from timeout and from a
        provisioning error). Any non-starting state is rejected with the
        existing state-specific semantics; it never counts as success.
        """
        store = self.store
        with store.lock:
            # Credential first (same rule as status): an expired credential
            # must surface as token_expired, not be hidden behind a pre-reap.
            now = store.clock.now()
            env = self._authenticate_locked(env_id, token)
            # Settle an overdue start before deciding this is a real abort:
            # a start-deadline-exceeded attempt must surface as a timeout,
            # never as a successful abort. (Lease expiry is left to the
            # normal terminal-state handling below.)
            self._sweep_start_timeouts_locked(now)
            attempt = self._start_attempts.get(env_id)
            if env.state is EnvState.STARTING and attempt is not None:
                # If the worker already finished the race, honor its outcome
                # instead of aborting an (about to be) finished start.
                self._finalize_finished_attempt_locked(attempt)
                if env.state is not EnvState.STARTING:
                    raise IllegalTransitionError(
                        env.state.value, "abort", "start_already_completed"
                    )
                attempt.cancel_event.set()
                self._fail_start_locked(
                    attempt,
                    env,
                    FAIL_REASON_ABORTED_BY_OWNER,
                    now,
                )
                self._discard_attempt_locked(attempt)
                self._admit_locked()
                return env
            # Not starting: reject with the existing, state-specific semantics.
            if env.state is EnvState.RUNNING:
                raise IllegalTransitionError(
                    env.state.value, "abort", "already_running"
                )
            if env.state is EnvState.READY:
                raise IllegalTransitionError(
                    env.state.value, "abort", "not_starting_use_stop"
                )
            if env.state is EnvState.QUEUED:
                raise IllegalTransitionError(
                    env.state.value, "abort", "use_cancel_while_queued"
                )
            if env.state is EnvState.STOPPED:
                raise IllegalTransitionError(
                    env.state.value, "abort", "not_starting"
                )
            if env.state is EnvState.FAILED:
                if (
                    env_id in self._start_attempts
                    and env.fail_reason == FAIL_REASON_STARTUP_TIMEOUT
                ):
                    # This very start crossed its deadline moments before the
                    # abort landed: report the timeout cause distinctly.
                    self._raise_after_await_locked(env)
                raise IllegalTransitionError(
                    env.state.value, "abort", "already_failed"
                )
            self._illegal_terminal(env, "abort")

    def stop(self, env_id: str, token: str) -> Environment:
        store = self.store
        with store.lock:
            now = store.clock.now()
            self._run_housekeeping_locked(now)
            env = self._authenticate_locked(env_id, token)
            if env.state is EnvState.STOPPED:
                return env
            if env.state in (EnvState.RUNNING, EnvState.READY):
                env.state = EnvState.STOPPED
                env.expires_at = None
                if env.env_id in store.queue:
                    store.dequeue_locked(env.env_id)
                self._admit_locked()
                return env
            if env.state is EnvState.STARTING:
                raise IllegalTransitionError(
                    env.state.value, "stop", "provisioning_in_progress"
                )
            if env.state is EnvState.QUEUED:
                raise IllegalTransitionError(
                    env.state.value, "stop", "use_cancel_while_queued"
                )
            if env.state is EnvState.FAILED:
                raise IllegalTransitionError(
                    env.state.value, "stop", "already_failed"
                )
            self._illegal_terminal(env, "stop")

    def destroy(self, env_id: str, token: str) -> Environment:
        store = self.store
        with store.lock:
            now = store.clock.now()
            self._run_housekeeping_locked(now)
            env = self._authenticate_locked(env_id, token)
            self._finish_locked(env, EnvState.DESTROYED, "destroyed", now)
            self._admit_locked()
            return env

    def cancel(self, env_id: str, token: str) -> Environment:
        store = self.store
        with store.lock:
            now = store.clock.now()
            self._run_housekeeping_locked(now)
            env = self._authenticate_locked(env_id, token)
            if env.state is EnvState.QUEUED:
                self._finish_locked(env, EnvState.CANCELED, "canceled", now)
                return env
            if env.state in TERMINAL_STATES:
                self._illegal_terminal(env, "cancel")
            raise IllegalTransitionError(
                env.state.value, "cancel", "only_queued_requests_can_cancel"
            )

    def status(self, env_id: str, token: str) -> Environment:
        store = self.store
        with store.lock:
            # Credential first: an expired credential must surface as
            # token_expired, not be hidden behind a global pre-reap.
            return self._authenticate_locked(env_id, token)

    # -- introspection -------------------------------------------------

    def capacity_view(self) -> CapacityView:
        store = self.store
        with store.lock:
            self._run_housekeeping_locked(store.clock.now())
            return store.capacity_view_locked()

    def explain(self, env_id: str) -> dict:
        store = self.store
        with store.lock:
            self._run_housekeeping_locked(store.clock.now())
            env = store.get_locked(env_id)
            if env is None:
                raise EnvNotFoundError()
            data = env_to_dict(env)
            data["disposition"] = self._disposition_locked(env)
            data["reason"] = self._reason_locked(env)
            if env.state is EnvState.QUEUED:
                data["queue_position"] = store.queue.index(env.env_id) + 1
            return data

    def list_envs(self) -> list[Environment]:
        store = self.store
        with store.lock:
            return store.list_envs_locked()

    def reap_expired(self) -> list[str]:
        store = self.store
        with store.lock:
            expired = self._run_housekeeping_locked(store.clock.now())
            # Freed slots may promote queued environments.
            if expired:
                self._admit_locked()
            return expired

    # -- internals: authentication ------------------------------------

    def _authenticate_locked(self, env_id: str, token: str) -> Environment:
        store = self.store
        by_token = store.resolve_token_locked(token)
        if by_token is None:
            # Unknown credential. Distinguish an unknown target so callers
            # cannot fish for env ids using a bad token.
            if store.get_locked(env_id) is None:
                raise EnvNotFoundError()
            raise InvalidTokenError()
        env = store.get_locked(env_id)
        if env is None or by_token.env_id != env_id:
            # Credential is valid but does not own the target environment.
            raise EnvAccessDeniedError(
                "credential does not belong to the target environment"
            )
        if (
            env.expires_at is not None
            and store.clock.now() >= env.expires_at
        ):
            # Expired credential: lease elapsed and environment was reaped.
            if env.state in SLOT_HOLDING_STATES:
                self._finish_locked(
                    env, EnvState.EXPIRED, "expired", store.clock.now()
                )
                self._admit_locked()
            raise TokenExpiredError()
        if env.state is EnvState.DESTROYED:
            raise EnvDestroyedError()
        if env.state is EnvState.EXPIRED:
            raise EnvExpiredError()
        if env.state is EnvState.CANCELED:
            raise EnvCanceledError()
        return env

    # -- internals: admission / reaping / transitions ------------------

    def _admit_locked(self) -> list[Environment]:
        """Promote queued environments in strict FIFO order while slots free."""
        store = self.store
        admitted: list[Environment] = []
        now = store.clock.now()
        while store.queue and store.used_slots_locked() < store.config.capacity:
            env_id = store.queue[0]
            env = store.get_locked(env_id)
            if env is None or env.state is not EnvState.QUEUED:
                store.dequeue_locked(env_id)
                continue
            store.queue.pop(0)
            self._reserve_locked(env, now)
            admitted.append(env)
        return admitted

    def _reserve_locked(self, env: Environment, now: float) -> None:
        env.state = EnvState.READY
        env.accepted_at = now
        env.enqueued_at = None
        env.started_at = None
        env.starting_at = None
        env.expires_at = now + env.lease_seconds
        env.fail_reason = None

    # -- internals: asynchronous provisioning --------------------------

    def _begin_provision_locked(
        self,
        env: Environment,
        now: float,
        timeout_seconds: float | None,
    ) -> StartAttempt:
        """Flip READY -> STARTING (slot stays held) and register an attempt."""
        attempt = StartAttempt(
            env_id=env.env_id,
            started_at=now,
            timeout_seconds=timeout_seconds,
        )
        env.state = EnvState.STARTING
        env.starting_at = now
        env.fail_reason = None
        self._start_attempts[env.env_id] = attempt
        return attempt

    def _launch_attempt(self, attempt: StartAttempt) -> None:
        """Run the provisioner on a daemon thread, never holding the lock."""
        provisioner = self.provisioner or self._default_provision
        thread = threading.Thread(
            target=self._run_provisioner,
            args=(attempt, provisioner),
            name=f"provision-{attempt.env_id}",
            daemon=True,
        )
        thread.start()

    @staticmethod
    def _invoke_provisioner(
        provisioner: Provisioner,
        env: Environment,
        cancel_event: threading.Event,
    ) -> None:
        params = len(inspect.signature(provisioner).parameters)
        if params >= 2:
            provisioner(env, cancel_event)
        else:
            provisioner(env)

    def _run_provisioner(
        self, attempt: StartAttempt, provisioner: Provisioner
    ) -> None:
        env = self.store.get_locked(attempt.env_id)
        if env is None:
            return
        try:
            self._invoke_provisioner(provisioner, env, attempt.cancel_event)
        except Exception as exc:  # surfaced to the waiter under the lock
            attempt.error = exc
            attempt.success = False
        else:
            attempt.success = True
        finally:
            with attempt.cond:
                attempt.finished = True
                attempt.cond.notify_all()

    def _await_attempt(self, attempt: StartAttempt) -> None:
        """Wait outside the store lock until done, aborted or timed out."""
        cond = attempt.cond
        while True:
            with self.store.lock:
                now = self.store.clock.now()
                # Any other mutating traffic (reaper/abort/destroy) settles
                # state here first; also enforce the start deadline.
                self._run_housekeeping_locked(now)
                env = self.store.get_locked(attempt.env_id)
                if env is None or env.state is not EnvState.STARTING:
                    return
                if self._finalize_finished_attempt_locked(attempt):
                    return
                remaining: float | None
                if attempt.timeout_seconds is None:
                    remaining = None
                else:
                    remaining = (
                        attempt.started_at + attempt.timeout_seconds - now
                    )
                    if remaining <= 0:
                        self._fail_start_locked(
                            attempt,
                            env,
                            FAIL_REASON_STARTUP_TIMEOUT,
                            now,
                        )
                        self._admit_locked()
                        return
            # Release the store lock while waiting so the plane stays
            # responsive; abort/destroy/reaper notify this condition.
            with cond:
                if remaining is None:
                    cond.wait()
                else:
                    cond.wait(timeout=remaining)

    def _finalize_finished_attempt_locked(self, attempt: StartAttempt) -> bool:
        """Apply the worker's outcome if it finished while still STARTING.

        If the attempt was already settled otherwise (timeout, abort,
        destroy, expiry) the worker is late and its outcome is ignored, so a
        terminal state can never be overwritten by a just-finishing worker.
        """
        if not attempt.finished:
            return False
        env = self.store.get_locked(attempt.env_id)
        if env is None or env.state is not EnvState.STARTING:
            self._discard_attempt_locked(attempt)
            return True
        if attempt.success:
            now = self.store.clock.now()
            env.state = EnvState.RUNNING
            env.started_at = now
            env.starting_at = None
            env.fail_reason = None
            if env.expires_at is None:
                env.expires_at = now + env.lease_seconds
        else:
            reason = (
                str(attempt.error)
                or (attempt.error.__class__.__name__ if attempt.error else "unknown")
            )
            self._fail_start_locked(attempt, env, reason, self.store.clock.now())
            self._admit_locked()
        self._discard_attempt_locked(attempt)
        return True

    def _fail_start_locked(
        self,
        attempt: StartAttempt | None,
        env: Environment,
        reason: str,
        now: float,
    ) -> None:
        """Roll a STARTING reservation back to FAILED (slot released)."""
        env.state = EnvState.FAILED
        env.failure_count += 1
        env.fail_reason = reason
        env.starting_at = None
        env.started_at = None
        env.expires_at = None
        env.ended_at = None
        env.end_reason = None
        if attempt is not None:
            attempt.cancel_event.set()
            with attempt.cond:
                attempt.cond.notify_all()

    def _discard_attempt_locked(self, attempt: StartAttempt) -> None:
        current = self._start_attempts.get(attempt.env_id)
        if current is attempt:
            del self._start_attempts[attempt.env_id]
            with attempt.cond:
                attempt.cond.notify_all()

    def _raise_after_await_locked(self, env: Environment) -> None:
        """Map the post-await env state to the error the start caller sees."""
        if env.state in TERMINAL_STATES:
            # The start was settled out from under the caller: keep the same
            # state-specific errors as synchronous terminal-state operations.
            self._illegal_terminal(env, "start")
        if env.fail_reason == FAIL_REASON_STARTUP_TIMEOUT:
            raise StartupTimeoutError(
                "provisioning timed out after the start time limit", env=env
            )
        if env.fail_reason == FAIL_REASON_ABORTED_BY_OWNER:
            raise StartupAbortedError("start aborted by owner", env=env)
        raise ProvisioningError(
            f"provisioning failed: {env.fail_reason}", env=env
        )

    # -- internals: lease reaping / start deadline sweeping ------------

    def _run_housekeeping_locked(self, now: float) -> list[str]:
        """Reap expired leases and fail STARTING attempts past their deadline.

        Returns the ids reaped by lease expiry (kept for reap_expired()).
        """
        expired = self._reap_expired_locked(now)
        self._sweep_start_timeouts_locked(now)
        return expired

    def _sweep_start_timeouts_locked(self, now: float) -> list[str]:
        store = self.store
        timed_out: list[str] = []
        for env in list(store.envs.values()):
            attempt = self._start_attempts.get(env.env_id)
            if (
                env.state is EnvState.STARTING
                and attempt is not None
                and attempt.timeout_seconds is not None
                and now - attempt.started_at >= attempt.timeout_seconds
            ):
                # Fail the env but leave the attempt registered: the blocked
                # start caller (and a racing abort) still needs it to learn
                # that *this* start ended via timeout. The waiter discards it.
                self._fail_start_locked(
                    attempt, env, FAIL_REASON_STARTUP_TIMEOUT, now
                )
                timed_out.append(env.env_id)
        if timed_out:
            self._admit_locked()
        return timed_out

    def _reap_expired_locked(self, now: float) -> list[str]:
        store = self.store
        expired: list[str] = []
        for env in store.list_envs_locked():
            if (
                env.state in SLOT_HOLDING_STATES
                and env.expires_at is not None
                and now >= env.expires_at
            ):
                self._finish_locked(env, EnvState.EXPIRED, "expired", now)
                expired.append(env.env_id)
        for env_id in expired:
            store.dequeue_locked(env_id)
        if expired:
            self._admit_locked()
        return expired

    def _finish_locked(
        self,
        env: Environment,
        state: EnvState,
        reason: str,
        now: float,
    ) -> None:
        store = self.store
        attempt = self._start_attempts.pop(env.env_id, None)
        env.state = state
        env.end_reason = reason
        env.ended_at = now
        env.starting_at = None
        env.expires_at = None
        store.dequeue_locked(env.env_id)
        if attempt is not None:
            attempt.cancel_event.set()
            with attempt.cond:
                attempt.cond.notify_all()

    def _reject_locked(self, env: Environment, reason: str) -> None:
        store = self.store
        store.dequeue_locked(env.env_id)
        env.state = EnvState.REJECTED
        env.reject_reason = reason
        env.ended_at = store.clock.now()
        env.end_reason = "rejected"
        env.expires_at = None

    def _illegal_terminal(self, env: Environment, action: str) -> None:
        if env.state is EnvState.DESTROYED:
            raise EnvDestroyedError()
        if env.state is EnvState.EXPIRED:
            raise EnvExpiredError()
        if env.state is EnvState.CANCELED:
            raise EnvCanceledError()
        raise IllegalTransitionError(
            env.state.value, action, "state_does_not_allow_action"
        )

    # -- internals: explanation ----------------------------------------

    def _disposition_locked(self, env: Environment) -> str:
        mapping = {
            EnvState.READY: "admitted",
            EnvState.STARTING: "admitted",
            EnvState.RUNNING: "admitted",
            EnvState.QUEUED: "queued",
            EnvState.REJECTED: "rejected",
            EnvState.STOPPED: "stopped",
            EnvState.FAILED: "failed",
            EnvState.EXPIRED: "expired",
            EnvState.CANCELED: "canceled",
            EnvState.DESTROYED: "destroyed",
        }
        return mapping[env.state]

    def _reason_locked(self, env: Environment) -> str:
        store = self.store
        if env.state is EnvState.QUEUED:
            position = (
                store.queue.index(env.env_id) + 1
                if env.env_id in store.queue
                else 0
            )
            return (
                f"waiting_for_slot (fifo position {position}, "
                f"{store.used_slots_locked()}/{store.config.capacity} slots in use)"
            )
        if env.state is EnvState.READY:
            return "slot_reserved_waiting_for_start"
        if env.state is EnvState.STARTING:
            return "provisioning_in_progress"
        if env.state is EnvState.RUNNING:
            return "running"
        if env.state is EnvState.STOPPED:
            return "stopped_by_owner"
        if env.state is EnvState.FAILED:
            if env.fail_reason == FAIL_REASON_STARTUP_TIMEOUT:
                return "startup_timeout: start exceeded the configured time limit"
            if env.fail_reason == FAIL_REASON_ABORTED_BY_OWNER:
                return "startup_aborted: start aborted by owner"
            return f"provisioning_failed: {env.fail_reason}"
        if env.state is EnvState.REJECTED:
            return env.reject_reason or "capacity_exhausted"
        if env.state is EnvState.EXPIRED:
            return "lease_expired_and_reaped"
        if env.state is EnvState.CANCELED:
            return "canceled_while_queued"
        if env.state is EnvState.DESTROYED:
            return "destroyed_by_owner"
        return "unknown"

    def _default_provision(self, env: Environment) -> None:
        # Local simulation: startup succeeds immediately.
        return None

    # -- reaper lifecycle ----------------------------------------------

    def start_reaper(self, interval: float = 0.05) -> "LeaseReaper":
        if self._reaper is not None and self._reaper.running:
            return self._reaper
        self._reaper = LeaseReaper(self, interval)
        self._reaper.start()
        return self._reaper

    def stop_reaper(self) -> None:
        if self._reaper is not None:
            self._reaper.stop()
            self._reaper = None


class LeaseReaper:
    """Background thread that periodically expires overdue environments."""

    def __init__(self, plane: ControlPlane, interval: float = 0.05) -> None:
        self.plane = plane
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="lease-reaper", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.plane.reap_expired()
            except Exception:
                # The reaper must never die silently from a transient error.
                pass
