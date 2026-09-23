"""Concurrent application: capacity never exceeded, FIFO stable, rejections
explainable. No external services; state lives in the in-memory plane.
"""

from __future__ import annotations

import threading

import pytest

from controlplane.errors import CapacityExhaustedError
from controlplane.models import FakeClock
from controlplane.service import ControlPlane

from ._logging import log, section

CAPACITY = 3
QUEUE_LIMIT = 5
TOTAL_REQUESTS = 25


def test_concurrent_apply_respects_capacity_and_fifo():
    section("[concurrent] burst of 25 applies into 3 slots / queue 5")
    clock = FakeClock()
    plane = ControlPlane(
        capacity=CAPACITY,
        queue_limit=QUEUE_LIMIT,
        default_lease_seconds=1000,
        clock=clock,
    )

    outcomes: dict[int, str] = {}
    rejected_ids: list[str] = []
    barrier = threading.Barrier(TOTAL_REQUESTS)

    def worker(i: int):
        barrier.wait()  # maximize contention: all threads call apply together
        try:
            out = plane.apply(owner=f"owner-{i:02d}")
            outcomes[i] = out.disposition
        except CapacityExhaustedError as exc:
            outcomes[i] = "rejected"
            rejected_ids.append(exc.env_id or "?")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(TOTAL_REQUESTS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    admitted = [i for i, d in outcomes.items() if d == "admitted"]
    queued = [i for i, d in outcomes.items() if d == "queued"]
    rejected = [i for i, d in outcomes.items() if d == "rejected"]

    log(f"admitted={len(admitted)} queued={len(queued)} rejected={len(rejected)}")
    assert len(admitted) == CAPACITY
    assert len(queued) == QUEUE_LIMIT
    assert len(rejected) == TOTAL_REQUESTS - CAPACITY - QUEUE_LIMIT

    view = plane.capacity_view()
    log(f"final capacity view: {view.as_dict()}")
    assert view.used_slots == CAPACITY
    assert view.free_slots == 0
    assert view.queued == QUEUE_LIMIT

    # FIFO: queue ordered by application sequence, and explain tells why.
    with plane.store.lock:
        queued_envs = [plane.store.get_locked(eid) for eid in plane.store.queue]
    seqs = [e.sequence for e in queued_envs]
    log(f"queue sequence numbers: {seqs}")
    assert seqs == sorted(seqs), "queue must be strict FIFO by application order"
    for pos, env in enumerate(queued_envs, start=1):
        info = plane.explain(env.env_id)
        assert info["state"] == "QUEUED"
        assert info["queue_position"] == pos
        assert "waiting_for_slot" in info["reason"]
    log("every queued request reports a stable fifo position and reason")

    # Every rejected request left a traceable REJECTED record.
    for rid in rejected_ids:
        info = plane.explain(rid)
        assert info["state"] == "REJECTED"
        assert "queue full" in info["reason"]
    log(f"all {len(rejected_ids)} rejected requests explainable as REJECTED")


def test_concurrent_same_idempotency_key_is_deduped():
    section("[concurrent] N threads replay the same idempotency key")
    clock = FakeClock()
    plane = ControlPlane(capacity=2, queue_limit=2, clock=clock)
    n = 16
    env_ids: set[str] = set()
    flags: list[bool] = []
    barrier = threading.Barrier(n)
    lock = threading.Lock()

    def worker():
        barrier.wait()
        out = plane.apply(
            owner="alice", lease_seconds=100, idempotency_key="same-key",
            fingerprint="fp",
        )
        with lock:
            env_ids.add(out.environment.env_id)
            flags.append(out.deduplicated)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    log(f"distinct env records from {n} identical requests: {len(env_ids)}")
    assert len(env_ids) == 1, "idempotent replays must not create records"
    assert flags.count(False) == 1
    assert flags.count(True) == n - 1
    view = plane.capacity_view()
    log(f"occupancy after replay storm: {view.as_dict()}")
    assert view.used_slots == 1
    assert view.queued == 0
