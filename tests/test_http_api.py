"""End-to-end HTTP checks through the FastAPI app (in-process TestClient)."""

from __future__ import annotations

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
