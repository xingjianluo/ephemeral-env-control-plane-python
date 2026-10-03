"""End-to-end HTTP checks through the FastAPI app (in-process TestClient)."""

from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from controlplane.api import create_app
from controlplane.models import FakeClock
from controlplane.service import ControlPlane

from ._logging import log, section


@pytest.fixture()
def client():
    clock = FakeClock()
    plane = ControlPlane(capacity=2, queue_limit=2, default_lease_seconds=10,
                         clock=clock)
    with TestClient(create_app(plane, reaper_interval=1000)) as c:
        yield c, plane, clock


def test_full_lifecycle_over_http(client):
    section("[http] apply -> start -> status -> stop -> destroy")
    c, plane, clock = client
    r = c.post("/environments", json={"owner": "alice", "lease_seconds": 10,
                                      "idempotency_key": "k", "fingerprint": "f"})
    assert r.status_code == 200
    env = r.json()["environment"]
    assert r.json()["disposition"] == "admitted"
    log(f"apply -> {env['state']} ({env['env_id']})")

    # duplicate POST is idempotent
    r2 = c.post("/environments", json={"owner": "alice", "lease_seconds": 10,
                                       "idempotency_key": "k", "fingerprint": "f"})
    assert r2.json()["deduplicated"] is True
    assert r2.json()["environment"]["env_id"] == env["env_id"]
    log("duplicate POST deduplicated")

    # no token -> 401 invalid_token
    assert c.post(f"/environments/{env['env_id']}/start").status_code == 401
    r = c.post(f"/environments/{env['env_id']}/start",
               headers={"X-Auth-Token": env["token"]})
    assert r.json()["environment"]["state"] == "RUNNING"
    log("start -> RUNNING")

    r = c.get(f"/environments/{env['env_id']}",
              headers={"X-Auth-Token": env["token"]})
    assert r.json()["environment"]["state"] == "RUNNING"

    r = c.post(f"/environments/{env['env_id']}/stop",
               headers={"X-Auth-Token": env["token"]})
    assert r.json()["environment"]["state"] == "STOPPED"
    log("stop -> STOPPED, slot released")
    assert c.get("/capacity").json()["used_slots"] == 0

    r = c.post(f"/environments/{env['env_id']}/destroy",
               headers={"X-Auth-Token": env["token"]})
    assert r.json()["environment"]["state"] == "DESTROYED"
    r = c.post(f"/environments/{env['env_id']}/destroy",
               headers={"X-Auth-Token": env["token"]})
    assert r.status_code == 410 and r.json()["error"]["code"] == "env_destroyed"
    log("destroy -> DESTROYED; second destroy 410 env_destroyed")


def test_capacity_reject_and_explain_over_http(client):
    section("[http] overload rejects 503 and explain reports the reason")
    c, plane, clock = client
    tokens = []
    for i, owner in enumerate(["a", "b", "q1", "q2"]):
        r = c.post("/environments", json={"owner": owner})
        tokens.append(r.json()["environment"])
    rejected = c.post("/environments", json={"owner": "overflow"})
    assert rejected.status_code == 503
    body = rejected.json()
    assert body["error"]["code"] == "capacity_exhausted"
    rid = body["error"]["details"]["env_id"]
    info = c.get(f"/environments/{rid}/explain").json()
    assert info["state"] == "REJECTED" and "queue full" in info["reason"]
    log(f"overflow -> 503; explain: {info['reason']}")

    # capacity snapshot is public and consistent
    cap = c.get("/capacity").json()
    assert cap == {"capacity": 2, "used_slots": 2, "free_slots": 0,
                   "queued": 2, "queue_limit": 2}
    log(f"capacity snapshot: {cap}")


def test_abort_slow_start_over_http():
    section("[http] abort a slow STARTING env; slot released, reason distinct")
    clock = FakeClock()
    entered = threading.Event()
    release = threading.Event()

    def slow(env):
        entered.set()
        release.wait(timeout=5)

    plane = ControlPlane(
        capacity=1, queue_limit=2, default_lease_seconds=100,
        start_timeout_seconds=100, provisioner=slow, clock=clock,
    )
    with TestClient(create_app(plane, reaper_interval=1000)) as c:
        env = c.post("/environments", json={"owner": "alice"}).json()["environment"]
        waiter = c.post("/environments", json={"owner": "bob"}).json()["environment"]

        def do_start():
            box["resp"] = c.post(
                f"/environments/{env['env_id']}/start",
                headers={"X-Auth-Token": env["token"]},
            )

        box: dict = {}
        t = threading.Thread(target=do_start, daemon=True)
        t.start()
        assert entered.wait(timeout=1)

        # abort while STARTING -> FAILED with fail_kind=start_aborted
        r = c.post(
            f"/environments/{env['env_id']}/abort",
            headers={"X-Auth-Token": env["token"]},
        )
        assert r.status_code == 200
        body = r.json()["environment"]
        assert body["state"] == "FAILED"
        assert body["fail_kind"] == "start_aborted"
        log(f"POST abort -> {body['state']} ({body['fail_kind']})")

        # slot released and queue head promoted
        cap = c.get("/capacity").json()
        assert cap["used_slots"] == 1 and cap["queued"] == 0
        w = c.get(f"/environments/{waiter['env_id']}",
                  headers={"X-Auth-Token": waiter["token"]}).json()["environment"]
        assert w["state"] == "READY"
        log(f"capacity after abort: {cap}; waiter -> {w['state']}")

        # abort on a non-STARTING env -> 409 illegal_transition
        r2 = c.post(
            f"/environments/{env['env_id']}/abort",
            headers={"X-Auth-Token": env["token"]},
        )
        assert r2.status_code == 409
        assert r2.json()["error"]["details"]["reason"] == \
            "only_starting_requests_can_abort"
        log("second abort -> 409 illegal_transition(only_starting_requests_can_abort)")

        release.set()
        t.join(timeout=2)
        assert not t.is_alive()
        sr = box["resp"]
        assert sr.status_code == 409
        assert sr.json()["error"]["code"] == "start_aborted"
        log(f"original slow start returned {sr.status_code} start_aborted")
