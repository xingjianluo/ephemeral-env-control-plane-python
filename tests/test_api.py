"""HTTP 端到端：走真实 FastAPI 路由，仅使用本地 TestClient。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from control_plane.api import create_app
from control_plane.manager import EnvManager, ManualClock, SimulatedProvisioner
from tests.helpers import banner, verdict


@pytest.fixture()
def client():
    clock = ManualClock()
    prov = SimulatedProvisioner()
    mgr = EnvManager(capacity=2, max_queue=2, lease_ttl_seconds=10,
                     clock=clock, provisioner=prov)
    app = create_app(manager=mgr)
    with TestClient(app) as c:
        c.mgr = mgr
        c.clock = clock
        yield c


def test_default_app_construction():
    banner("默认构造：不注入 manager 也能正常建服并申请")
    with TestClient(create_app(capacity=1, max_queue=1,
                               lease_ttl_seconds=5)) as c:
        r = c.post("/environments", json={"request_id": "d1", "owner": "alice"})
        verdict("默认 app 申请成功",
                r.status_code == 200 and r.json()["status"] == "ALLOCATED")


def test_full_lifecycle_over_http(client):
    banner("HTTP：申请->启动->查询->停止->销毁 全链路")
    resp = client.post("/environments", json={"request_id": "r1", "owner": "alice"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    cred = body["credential"]
    env_id = body["env_id"]
    print("申请返回:", {k: body[k] for k in ("status", "slot", "queued")})
    verdict("申请即分配槽位", body["status"] == "ALLOCATED" and body["slot"] == 0)

    r = client.post(f"/environments/{env_id}/start",
                    headers={"X-Credential": cred})
    verdict("启动成功 RUNNING", r.status_code == 200
            and r.json()["status"] == "RUNNING")
    lease = r.json()["lease"]
    verdict("返回租约", lease and lease["ttl_seconds"] == 10)

    g = client.get(f"/environments/{env_id}", headers={"X-Credential": cred})
    verdict("凭证查询成功", g.json()["status"] == "RUNNING")

    s = client.post(f"/environments/{env_id}/stop",
                    headers={"X-Credential": cred})
    verdict("停止成功", s.json()["status"] == "STOPPED")

    d = client.delete(f"/environments/{env_id}", headers={"X-Credential": cred})
    verdict("销毁成功", d.json()["status"] == "DESTROYED")


def test_http_idempotent_and_error_codes(client):
    banner("HTTP：幂等申请 + 差异化错误码")
    a = client.post("/environments", json={"request_id": "x", "owner": "alice"}).json()
    again = client.post("/environments", json={"request_id": "x", "owner": "alice"}).json()
    verdict("重复申请返回同一 env", a["env_id"] == again["env_id"])

    # 越权
    b = client.post("/environments", json={"request_id": "y", "owner": "bob"}).json()
    r = client.get(f"/environments/{a['env_id']}",
                   headers={"X-Credential": b["credential"]})
    verdict("越权 403 unauthorized",
            r.status_code == 403 and r.json()["error"]["code"] == "unauthorized")

    r = client.get(f"/environments/{a['env_id']}", headers={"X-Credential": "junk"})
    verdict("伪造凭证 401 invalid_credential",
            r.status_code == 401
            and r.json()["error"]["code"] == "invalid_credential")

    # 容量：capacity=2，再起 2 个排队，第 5 个拒绝
    client.post("/environments", json={"request_id": "c", "owner": "c"})
    client.post("/environments", json={"request_id": "d", "owner": "d"})
    client.post("/environments", json={"request_id": "e", "owner": "e"})
    rej = client.post("/environments", json={"request_id": "f", "owner": "f"})
    verdict("容量耗尽 503 capacity_exhausted",
            rej.status_code == 503
            and rej.json()["error"]["code"] == "capacity_exhausted")

    cap = client.get("/capacity").json()
    verdict("容量视图正确", cap["occupied_slots"] == 2 and cap["queue_length"] == 2,
            str(cap))

    expl = client.get("/requests/f").json()
    verdict("可解释被拒原因", expl["outcome"] == "rejected")


def test_http_lease_expiry_and_reap(client):
    banner("HTTP：租约到期后内部回收接口释放槽位")
    a = client.post("/environments",
                    json={"request_id": "a", "owner": "alice"}).json()
    client.post(f"/environments/{a['env_id']}/start",
                headers={"X-Credential": a["credential"]})
    client.clock.advance(11)
    reap = client.post("/internal/reap").json()
    verdict("回收 1 个环境", reap["reaped"] == [a["env_id"]], str(reap))
    g = client.get(f"/environments/{a['env_id']}",
                   headers={"X-Credential": a["credential"]})
    verdict("过期凭证 410 lease_expired",
            g.status_code == 410
            and g.json()["error"]["code"] == "lease_expired")
    verdict("槽位释放", client.get("/capacity").json()["occupied_slots"] == 0)


def test_http_start_failure_rollback(client):
    banner("HTTP：启动失败 502 且槽位释放")
    client.mgr.provisioner.fail_next_start = True
    a = client.post("/environments",
                    json={"request_id": "a", "owner": "alice"}).json()
    r = client.post(f"/environments/{a['env_id']}/start",
                    headers={"X-Credential": a["credential"]})
    verdict("启动失败返回 502 startup_failed",
            r.status_code == 502
            and r.json()["error"]["code"] == "startup_failed")
    g = client.get(f"/environments/{a['env_id']}").json()
    verdict("环境状态 FAILED 且槽位为 null",
            g["status"] == "FAILED" and g["slot"] is None, str(g))
    verdict("无槽位残留", client.get("/capacity").json()["occupied_slots"] == 0)
