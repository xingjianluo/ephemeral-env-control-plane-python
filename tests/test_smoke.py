"""HTTP 层并发申请压测：真实路由 + 多线程，验证占用不越上限。"""

from __future__ import annotations

import threading

from fastapi.testclient import TestClient

from control_plane.api import create_app
from control_plane.manager import EnvManager, ManualClock, SimulatedProvisioner
from tests.helpers import banner, verdict


def test_concurrent_applies_over_http_never_oversubscribe():
    banner("HTTP 并发：30 线程各申请 4 次，占用/排队/拒绝守恒")
    mgr = EnvManager(capacity=5, max_queue=20, clock=ManualClock(),
                     provisioner=SimulatedProvisioner())
    with TestClient(create_app(manager=mgr)) as client:
        accepted, queued, rejected = [], [], []
        lock = threading.Lock()

        def worker(tid):
            for j in range(4):
                r = client.post("/environments",
                                json={"request_id": f"t{tid}-{j}",
                                      "owner": f"o{tid}"})
                with lock:
                    if r.status_code == 503:
                        rejected.append(r.json()["error"]["code"])
                    elif r.json().get("queued"):
                        queued.append(1)
                    else:
                        accepted.append(1)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(30)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        cap = client.get("/capacity").json()
        print(f"直接占用={len(accepted)} 排队={len(queued)} 拒绝={len(rejected)}")
        print("容量视图:", cap)
        total = len(accepted) + len(queued) + len(rejected)
        verdict("占用槽位不超过上限", cap["occupied_slots"] <= 5)
        verdict("排队长度不超过队列上限", cap["queue_length"] <= 20)
        verdict("三类结果之和等于提交总数(120)", total == 120, f"total={total}")
        verdict("被拒原因全部是 capacity_exhausted",
                set(rejected) == {"capacity_exhausted"})
