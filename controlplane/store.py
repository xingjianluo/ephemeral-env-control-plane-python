"""In-memory, thread-safe state store.

Pure state container: owns the global lock, the environment records, the
credential/idempotency indexes and the FIFO wait queue. All policy lives in
``controlplane.service``. Methods named ``*_locked`` assume the caller holds
``self.lock``.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .models import (
    CapacityView,
    Clock,
    Environment,
    SLOT_HOLDING_STATES,
    SystemClock,
)


@dataclass
class CapacityConfig:
    capacity: int = 2
    queue_limit: int = 4


class StateStore:
    def __init__(
        self,
        config: CapacityConfig | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.config = config or CapacityConfig()
        self.clock = clock or SystemClock()
        self.lock = threading.RLock()
        self.envs: dict[str, Environment] = {}
        self.token_index: dict[str, str] = {}  # token -> env_id
        self.idem_index: dict[str, str] = {}  # idempotency_key -> env_id
        self.queue: list[str] = []  # env_ids in QUEUED, FIFO
        self._sequence = 0

    # -- helpers -------------------------------------------------------

    def next_sequence_locked(self) -> int:
        self._sequence += 1
        return self._sequence

    def used_slots_locked(self) -> int:
        return sum(
            1 for e in self.envs.values() if e.state in SLOT_HOLDING_STATES
        )

    def capacity_view_locked(self) -> CapacityView:
        used = self.used_slots_locked()
        return CapacityView(
            capacity=self.config.capacity,
            used_slots=used,
            free_slots=self.config.capacity - used,
            queued=len(self.queue),
            queue_limit=self.config.queue_limit,
        )

    def get_locked(self, env_id: str) -> Environment | None:
        return self.envs.get(env_id)

    def put_locked(self, env: Environment) -> None:
        self.envs[env.env_id] = env

    def resolve_token_locked(self, token: str) -> Environment | None:
        env_id = self.token_index.get(token)
        return self.envs.get(env_id) if env_id is not None else None

    def resolve_idem_locked(self, key: str) -> Environment | None:
        env_id = self.idem_index.get(key)
        return self.envs.get(env_id) if env_id is not None else None

    def enqueue_locked(self, env: Environment) -> None:
        if env.env_id not in self.queue:
            self.queue.append(env.env_id)

    def dequeue_locked(self, env_id: str) -> None:
        try:
            self.queue.remove(env_id)
        except ValueError:
            pass

    def list_envs_locked(self) -> list[Environment]:
        return list(self.envs.values())
