"""Cancelling a queued request frees its queue slot immediately; subsequent
requests can be admitted/queued and every decision remains explainable.
"""

from __future__ import annotations

from controlplane.models import EnvState
from controlplane.service import ControlPlane
from controlplane.models import FakeClock

from ._logging import log, section


def test_cancel_frees_queue_slot():
    section("[queue] cancel while queued releases the waiting slot at once")
    clock = FakeClock()
    plane = ControlPlane(capacity=1, queue_limit=1, default_lease_seconds=100,
                         clock=clock)

    first = plane.apply("alice")
    queued = plane.apply("bob")
    assert first.disposition == "admitted"
    assert queued.disposition == "queued"
    assert plane.capacity_view().queued == 1

    # Queue is full -> third request rejected
    third = None
    try:
        plane.apply("carol")
        raise AssertionError("expected capacity_exhausted")
    except Exception as exc:
        assert exc.code == "capacity_exhausted"
        third = exc.env_id
    log(f"third request rejected (queue full); record {third} = REJECTED")

    canceled = plane.cancel(queued.environment.env_id, queued.environment.token)
    assert canceled.state is EnvState.CANCELED
    view = plane.capacity_view()
    assert view.queued == 0
    log(f"after cancel: {view.as_dict()}")

    # Now carol can queue deterministically.
    again = plane.apply("carol")
    assert again.disposition == "queued"
    info = plane.explain(again.environment.env_id)
    assert info["queue_position"] == 1
    log(f"new request queued at position {info['queue_position']}: {info['reason']}")

    # Destroying the head frees the running slot -> queued carol promoted.
    plane.destroy(first.environment.env_id, first.environment.token)
    promoted = plane.status(again.environment.env_id, again.environment.token)
    assert promoted.state is EnvState.READY
    log(f"carol promoted to {promoted.state.value} after head destroy")
