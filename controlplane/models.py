"""Domain model: environment lifecycle states, entities and value objects."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol


class EnvState(str, Enum):
    # Waiting for a free slot (holds no slot).
    QUEUED = "QUEUED"
    # A slot is reserved, waiting for explicit start (holds a slot).
    READY = "READY"
    # Local provisioning in progress (holds a slot).
    STARTING = "STARTING"
    # Provisioned and usable (holds a slot).
    RUNNING = "RUNNING"
    # Stopped by the owner; compute released, restartable (holds no slot).
    STOPPED = "STOPPED"
    # Provisioning failed mid-start; slot released, restartable.
    FAILED = "FAILED"
    # Terminal: lease expired and reaped.
    EXPIRED = "EXPIRED"
    # Terminal: canceled while queued.
    CANCELED = "CANCELED"
    # Terminal: application rejected at admission (capacity exhausted).
    REJECTED = "REJECTED"
    # Terminal: explicitly destroyed.
    DESTROYED = "DESTROYED"


TERMINAL_STATES = frozenset(
    {
        EnvState.EXPIRED,
        EnvState.CANCELED,
        EnvState.REJECTED,
        EnvState.DESTROYED,
    }
)

# States that occupy a capacity slot.
SLOT_HOLDING_STATES = frozenset(
    {EnvState.READY, EnvState.STARTING, EnvState.RUNNING}
)

# Distinct ways a start attempt can end up in FAILED. Kept as stable string
# constants so callers can tell a provisioning crash apart from the startup
# time limit or an owner-initiated abort.
FAIL_KIND_PROVISIONING = "provisioning_error"
FAIL_KIND_TIMEOUT = "start_timeout"
FAIL_KIND_ABORTED = "start_aborted"


@dataclass
class Environment:
    env_id: str
    owner: str
    state: EnvState
    token: str
    lease_seconds: int
    created_at: float
    enqueued_at: float | None = None
    accepted_at: float | None = None  # last time a slot was acquired
    started_at: float | None = None
    expires_at: float | None = None
    ended_at: float | None = None
    end_reason: str | None = None  # "destroyed" | "expired" | "canceled"
    fail_reason: str | None = None
    fail_kind: str | None = None  # FAIL_KIND_* when state == FAILED
    reject_reason: str | None = None
    failure_count: int = 0
    idempotency_key: str | None = None
    spec_fingerprint: str | None = None
    sequence: int = 0  # stable FIFO ordering, assigned at application
    # Startup bookkeeping, populated while STARTING:
    # deadline for the startup time limit, and an attempt generation that
    # changes on every provision so a stale (timed-out/aborted) provisioner
    # can never overwrite a newer attempt's state.
    start_deadline: float | None = None
    start_attempt: int = 0


@dataclass
class CapacityView:
    capacity: int
    used_slots: int
    free_slots: int
    queued: int
    queue_limit: int

    def as_dict(self) -> dict:
        return {
            "capacity": self.capacity,
            "used_slots": self.used_slots,
            "free_slots": self.free_slots,
            "queued": self.queued,
            "queue_limit": self.queue_limit,
        }


@dataclass
class ApplyOutcome:
    environment: Environment
    deduplicated: bool
    disposition: str  # "admitted" | "queued" | "rejected"
    reason: str  # human/stable explanation for queued/rejected
    capacity: CapacityView


@dataclass
class StartOutcome:
    environment: Environment
    disposition: str  # "running" | "queued"
    reason: str
    capacity: CapacityView


class Clock(Protocol):
    def now(self) -> float: ...


@dataclass
class SystemClock:
    def now(self) -> float:
        return time.monotonic()


@dataclass
class FakeClock:
    """Deterministic clock for tests; advance manually."""

    current: float = 0.0

    def now(self) -> float:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += seconds
