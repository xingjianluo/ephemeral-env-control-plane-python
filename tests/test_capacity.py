"""容量、排队、并发申请与幂等测试。"""

from __future__ import annotations

import threading

import pytest

from control_plane.errors import (
    CapacityExhausted,
    IdempotencyConflict,
    InvalidStateTransition,
)
from tests.helpers import banner, verdict


def test_apply_fills_slots_then_queues_then_rejects(manager):
    banner("容量：先占槽、再排队、最后明确拒绝")
    results = [manager.apply(f"r{i}", f"u{i}") for i in range(6)]
    statuses = [(r.env.status.value, r.position) for r in results]
    print("申请序列(env状态,排队位置):", statuses)

    snap = manager.snapshot()
    print("第 6 个申请后容量快照:", snap.to_dict())
    verdict("前 3 个立即占用槽位", all(not r.queued for r in results[:3]))
    verdict("后 3 个进入 FIFO 队列",
            all(r.queued for r in results[3:6])
            and [r.position for r in results[3:6]] == [1, 2, 3])
    verdict("占用槽位等于上限", snap.occupied_slots == 3)

    with pytest.raises(CapacityExhausted) as ei:
        manager.apply("r6", "u6")
    verdict("第 7 个（槽满且队列满）被拒绝",
            ei.value.code == "capacity_exhausted", ei.value.message)
    assert snap.queue_length == 3


def test_capacity_exhausted_is_raised_and_explained(manager):
    banner("容量耗尽：抛 capacity_exhausted 且可解释")
    for i in range(6):
        manager.apply(f"r{i}", f"u{i}")
    with pytest.raises(CapacityExhausted) as ei:
        manager.apply("rX", "uX")
    print("拒绝异常 code:", ei.value.code, "| message:", ei.value.message)
    verdict("拒绝原因稳定可区分", ei.value.code == "capacity_exhausted",
            ei.value.message)
    explanation = manager.explain("rX")
    print("explain(rX) =", explanation)
    verdict("explain 说明被拒绝原因",
            explanation["outcome"] == "rejected" and "occupied" in explanation["reason"])

    # 重试同一被拒 request_id：幂等地得到同样拒绝
    with pytest.raises(CapacityExhausted):
        manager.apply("rX", "uX")
    verdict("被拒申请重试得到同样拒绝", True)


def test_idempotent_apply_does_not_double_allocate(manager):
    banner("幂等：重复申请不重复占用、不重复记录")
    first = manager.apply("dup", "alice")
    second = manager.apply("dup", "alice")
    snap = manager.snapshot()
    print("首次 env:", first.env.env_id, "再次 env:", second.env.env_id)
    print("容量快照:", snap.to_dict())
    verdict("返回同一环境", first.env.env_id == second.env.env_id)
    verdict("仅占用 1 个槽位", snap.occupied_slots == 1)
    verdict("仅 1 条环境记录",
            len([e for e in manager._envs.values()]) == 1)

    with pytest.raises(IdempotencyConflict):
        manager.apply("dup", "bob")
    verdict("同 request_id 不同 owner 被判定冲突", True)


def test_cancel_pending_releases_queue_position(manager):
    banner("排队取消：及时释放队列位置，FIFO 顺序保持")
    rs = [manager.apply(f"r{i}", f"u{i}") for i in range(6)]
    waiting = rs[3].env  # 队首
    next_one = rs[4].env  # 队中第 2
    print("取消前队列长度:", manager.snapshot().queue_length)
    manager.cancel_pending("r3", "u3")
    print("取消 r3 后队列长度:", manager.snapshot().queue_length)
    verdict("取消项进入 CANCELLED", waiting.status.value == "CANCELLED")
    verdict("取消项不占槽位", waiting.slot is None)
    # PENDING 不占槽，取消释放的是“队列位置”，槽位占用不变
    verdict("取消不改变槽位占用", manager.snapshot().occupied_slots == 3)
    moved = manager.get(next_one.env_id)
    verdict("后一位升到队首但仍 PENDING（无空槽不补位）",
            moved.status.value == "PENDING"
            and manager.explain("r4")["position"] == 1)

    # 队列腾出一个位置：新申请可入队，而不是被拒绝
    newcomer = manager.apply("r6", "u6")
    verdict("取消后新申请能够入队（队列位置被及时释放）",
            newcomer.queued and newcomer.position == 3,
            f"position={newcomer.position}")

    with pytest.raises(InvalidStateTransition):
        manager.cancel_pending("r0", "u0")  # 已分配的不能走取消
    verdict("非 PENDING 取消被拒绝", True)


def test_slot_release_promotes_queue_head_fifo(manager):
    banner("槽位释放：销毁/到期后队首按 FIFO 补位")
    rs = [manager.apply(f"r{i}", f"u{i}") for i in range(6)]
    head_pending = rs[3].env
    second_pending = rs[4].env
    # 运行首个环境再销毁，释放一个槽
    e0 = rs[0].env
    manager.start(e0.env_id, e0.credential)
    manager.destroy(e0.env_id, e0.credential)
    promoted = manager.get(head_pending.env_id)
    still_waiting = manager.get(second_pending.env_id)
    verdict("队首补位 ALLOCATED 拿到空槽",
            promoted.status.value == "ALLOCATED" and promoted.slot == 0,
            f"status={promoted.status.value} slot={promoted.slot}")
    verdict("第二位仍在队列且升到队首",
            still_waiting.status.value == "PENDING"
            and manager.explain("r4")["position"] == 1)
    verdict("占用槽位仍等于上限", manager.snapshot().occupied_slots == 3)


def test_concurrent_applies_never_exceed_capacity():
    banner("并发：多线程申请总量绝不超过槽位上限")
    from control_plane.manager import EnvManager, ManualClock, SimulatedProvisioner

    mgr = EnvManager(capacity=4, max_queue=100, clock=ManualClock(),
                     provisioner=SimulatedProvisioner(delay=0.0))
    n_threads, per_thread = 16, 5
    allocated = []
    rejected = []
    lock = threading.Lock()

    def worker(tid):
        for j in range(per_thread):
            rid = f"t{tid}-{j}"
            try:
                r = mgr.apply(rid, f"owner-{tid}")
                if not r.queued:
                    with lock:
                        allocated.append(rid)
            except CapacityExhausted:
                with lock:
                    rejected.append(rid)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = mgr.snapshot()
    print(f"提交 {n_threads * per_thread} 个并发申请")
    print("立即占用数:", len(allocated), "| 排队:", snap.queue_length,
          "| 被拒:", len(rejected))
    print("最终快照:", snap.to_dict())
    verdict("占用槽位 <= 上限", snap.occupied_slots <= 4,
            f"occupied={snap.occupied_slots}")
    verdict("占用 + 排队 = 已受理总数且槽位恰好占满(因申请足够多)",
            snap.occupied_slots == 4 and
            snap.occupied_slots + snap.queue_length + len(rejected)
            == n_threads * per_thread)
