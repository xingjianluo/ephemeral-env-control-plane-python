"""Control-plane service: lifecycle, capacity, lease and credential policy.

Everything mutating state runs inside the store lock, so concurrent
applications, the lease reaper and owner operations can never observe
half-applied changes. Provisioning is a local in-process simulation, so it
runs while the lock is held: if it raises, the slot reservation is rolled
back in the same critical section and no occupancy is leaked.
"""

from __future__ import annotations

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
Provisioner = Callable[[Environment], None]


def new_env_id() -> str:
    return "env_" + secrets.token_hex(6)


def new_token() -> str:
    return "tok_" + secrets.token_hex(16)


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
    clock: Clock | None = None
    provisioner: Provisioner | None = None
    _store: StateStore | None = field(default=None, init=False, repr=False)
    _reaper: "LeaseReaper | None" = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")
        if self.queue_limit < 0:
            raise ValueError("queue_limit must be >= 0")
        if self.default_lease_seconds <= 0:
            raise ValueError("default_lease_seconds must be positive")
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
            self._reap_expired_locked(now)

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

    def start(self, env_id: str, token: str) -> StartOutcome:
        store = self.store
        with store.lock:
            now = store.clock.now()
            self._reap_expired_locked(now)
            env = self._authenticate_locked(env_id, token)

            if env.state in (EnvState.STARTING, EnvState.RUNNING):
                return StartOutcome(
                    environment=env,
                    disposition="running",
                    reason="already_running",
                    capacity=store.capacity_view_locked(),
                )
            if env.state is EnvState.READY:
                self._provision_locked(env)
                return StartOutcome(
                    environment=env,
                    disposition="running",
                    reason="provisioned",
                    capacity=store.capacity_view_locked(),
                )
            if env.state in (EnvState.STOPPED, EnvState.FAILED):
                view = store.capacity_view_locked()
                if view.free_slots <= 0:
                    env.state = EnvState.QUEUED
                    env.enqueued_at = now
                    env.expires_at = None
                    env.accepted_at = None
                    env.started_at = None
                    store.enqueue_locked(env)
                    return StartOutcome(
                        environment=env,
                        disposition="queued",
                        reason=self._reason_locked(env),
                        capacity=store.capacity_view_locked(),
                    )
                self._reserve_locked(env, now)
                self._provision_locked(env)
                return StartOutcome(
                    environment=env,
                    disposition="running",
                    reason="provisioned",
                    capacity=store.capacity_view_locked(),
                )
            if env.state is EnvState.QUEUED:
                return StartOutcome(
                    environment=env,
                    disposition="queued",
                    reason=self._reason_locked(env),
                    capacity=store.capacity_view_locked(),
                )
            self._illegal_terminal(env, "start")

    def stop(self, env_id: str, token: str) -> Environment:
        store = self.store
        with store.lock:
            now = store.clock.now()
            self._reap_expired_locked(now)
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
            self._reap_expired_locked(now)
            env = self._authenticate_locked(env_id, token)
            self._finish_locked(env, EnvState.DESTROYED, "destroyed", now)
            self._admit_locked()
            return env

    def cancel(self, env_id: str, token: str) -> Environment:
        store = self.store
        with store.lock:
            now = store.clock.now()
            self._reap_expired_locked(now)
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
            self._reap_expired_locked(store.clock.now())
            return store.capacity_view_locked()

    def explain(self, env_id: str) -> dict:
        store = self.store
        with store.lock:
            self._reap_expired_locked(store.clock.now())
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
            expired = self._reap_expired_locked(store.clock.now())
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
        env.expires_at = now + env.lease_seconds
        env.fail_reason = None

    def _provision_locked(self, env: Environment) -> None:
        env.state = EnvState.STARTING
        provisioner = self.provisioner or self._default_provision
        try:
            provisioner(env)
        except Exception as exc:  # startup failure: roll back reservation
            store = self.store
            env.state = EnvState.FAILED
            env.failure_count += 1
            env.fail_reason = str(exc) or exc.__class__.__name__
            env.expires_at = None
            self._admit_locked()
            raise ProvisioningError(
                f"provisioning failed: {env.fail_reason}", env=env
            ) from exc
        now = self.store.clock.now()
        env.state = EnvState.RUNNING
        env.started_at = now
        if env.expires_at is None:
            env.expires_at = now + env.lease_seconds

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
        env.state = state
        env.end_reason = reason
        env.ended_at = now
        env.expires_at = None
        store.dequeue_locked(env.env_id)

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
            return "provisioning"
        if env.state is EnvState.RUNNING:
            return "running"
        if env.state is EnvState.STOPPED:
            return "stopped_by_owner"
        if env.state is EnvState.FAILED:
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
