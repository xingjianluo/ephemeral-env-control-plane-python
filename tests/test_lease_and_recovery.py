"""租约到期自动回收、槽位补位，以及启动失败/中断后的一致性测试。"""

from __future__ import annotations

import threading

import pytest

from control_plane.errors import (
    CapacityExhausted,
    LeaseExpired,
    StartupFailed,
)
from control_plane.manager import EnvManager, ManualClock, SimulatedProvisioner
from tests.helpers import banner, verdict


def _make(capacity=3, ttl=10.0, **kw):
    clock = kw.pop("clock", ManualClock())
    prov = kw.pop("provisioner", SimulatedProvisioner())
    return EnvManager(capacity=capacity, lease_ttl_seconds=ttl,
                      clock=clock, provisioner=prov, **kw), clock, prov


def test_lease_expiry_releases_slot_and_rejects_access():
    banner("租约：到期自动回收，释放槽位，访问被拒")
    m, clock, _ = _make(capacity=1, ttl=10.0)
    r = m.apply("r1", "alice")
    env = m.start(r.env.env_id, r.env.credential)
    assert m.snapshot().occupied_slots == 1

    clock.advance(9)
    assert m.reap_expired() == []
    verdict("未到期不回收", m.get(env.env_id, env.credential).status.value == "RUNNING")

    clock.advance(2)  # 11s > ttl
    reaped = m.reap_expired()
    verdict("到期被回收", len(reaped) == 1 and reaped[0].status.value == "EXPIRED")
    snap = m.snapshot()
    verdict("回收后槽位释放", snap.occupied_slots == 0 and snap.free_slots == 1,
            snap.to_dict().__str__())

    with pytest.raises(LeaseExpired) as e:
        m.get(env.env_id, env.credential)
    verdict("过期凭证访问 -> lease_expired", e.value.code == "lease_expired")
    with pytest.raises(LeaseExpired):
        m.start(env.env_id, env.credential)
    verdict("过期环境不能再启动", True)


def test_expiry_lazy_check_and_promotes_queue():
    banner("租约：惰性到期回收 + 队列补位")
    m, clock, _ = _make(capacity=1, ttl=10.0)
    a = m.apply("a", "alice")
    m.start(a.env.env_id, a.env.credential)
    b = m.apply("b", "bob")
    verdict("b 在排队", b.queued and b.position == 1)

    clock.advance(20)
    # 不调用 reap，直接发起对 a 的访问触发惰性回收
    try:
        m.get(a.env.env_id, a.env.credential)
    except LeaseExpired:
        pass
    promoted = m.get(b.env.env_id)
    verdict("惰性回收后队首 b 提升并拿到槽位",
            promoted.status.value == "ALLOCATED" and promoted.slot == 0,
            f"status={promoted.status.value} slot={promoted.slot}")
    verdict("槽位占用仍为 1", m.snapshot().occupied_slots == 1)


def test_stopped_lease_is_paused_then_refreshed_on_start():
    banner("租约：STOPPED 期间不计时，重新启动刷新租约")
    m, clock, _ = _make(capacity=1, ttl=10.0)
    r = m.apply("r", "alice")
    env = m.start(r.env.env_id, r.env.credential)
    first_expiry = env.lease.expires_at
    m.stop(env.env_id, env.credential)
    clock.advance(100)  # 停止很久
    verdict("STOPPED 不被回收（无租约）",
            m.get(env.env_id, env.credential).status.value == "STOPPED")
    env2 = m.start(env.env_id, env.credential)
    verdict("重启后新租约在未来到期",
            env2.lease.expires_at == clock.now() + 10.0
            and env2.lease.expires_at > first_expiry)


def test_start_failure_rolls_back_and_promotes():
    banner("启动失败：状态 FAILED、释放槽位、队首补位")
    clock = ManualClock()
    prov = SimulatedProvisioner(fail_next_start=True)
    m = EnvManager(capacity=1, lease_ttl_seconds=10,
                   clock=clock, provisioner=prov)
    a = m.apply("a", "alice")
    b = m.apply("b", "bob")
    verdict("b 排队中", b.queued)

    with pytest.raises(StartupFailed) as e:
        m.start(a.env.env_id, a.env.credential)
    verdict("启动失败抛出 startup_failed", e.value.code == "startup_failed")

    failed = m.get(a.env.env_id)
    verdict("a 进入 FAILED 且记录原因",
            failed.status.value == "FAILED" and bool(failed.fail_reason),
            failed.fail_reason or "")
    verdict("a 不残留槽位", failed.slot is None)
    # a 的槽位被释放后立即被队首 b 补走，因此占用仍为 1 且归属 b
    snap = m.snapshot()
    promoted = m.get(b.env.env_id)
    verdict("释放出的槽位已转移给队首 b（无丢失/无双占）",
            snap.occupied_slots == 1 and promoted.slot == 0
            and failed.slot is None, snap.to_dict().__str__())
    verdict("失败释放后 b 立即补位 ALLOCATED",
            promoted.status.value == "ALLOCATED" and promoted.slot == 0)


def test_interrupted_start_leaves_no_residue_and_retry_succeeds():
    banner("执行中断：异常回滚无残留，随后可成功重试")
    clock = ManualClock()
    prov = SimulatedProvisioner(interrupt_in_start=True)
    m = EnvManager(capacity=1, clock=clock, provisioner=prov)
    r = m.apply("r", "carol")

    with pytest.raises(StartupFailed):
        m.start(r.env.env_id, r.env.credential)
    snap = m.snapshot()
    verdict("中断后无槽位残留",
            m.get(r.env.env_id).status.value == "FAILED"
            and snap.occupied_slots == 0, snap.to_dict().__str__())

    prov.interrupt_in_start = False
    ok = m.start(r.env.env_id, r.env.credential)
    verdict("重试成功且重新占用同一容量池",
            ok.status.value == "RUNNING" and ok.slot == 0
            and m.snapshot().occupied_slots == 1)


def test_failed_retry_blocked_when_no_slot_then_succeeds_after_release():
    banner("FAILED 重试：无空槽被拒，释放后成功（占用始终一致）")
    clock = ManualClock()
    prov = SimulatedProvisioner(fail_next_start=True)
    m = EnvManager(capacity=1, clock=clock, provisioner=prov)
    a = m.apply("a", "alice")
    with pytest.raises(StartupFailed):
        m.start(a.env.env_id, a.env.credential)
    # 此时 b 占满唯一槽
    b = m.apply("b", "bob")
    m.start(b.env.env_id, b.env.credential)

    with pytest.raises(CapacityExhausted) as e:
        m.start(a.env.env_id, a.env.credential)
    verdict("无空槽时 FAILED 重试被 capacity_exhausted 拒绝",
            e.value.code == "capacity_exhausted")
    verdict("被拒后 a 仍 FAILED 且不占槽",
            m.get(a.env.env_id).status.value == "FAILED"
            and m.get(a.env.env_id).slot is None)

    m.destroy(b.env.env_id, b.env.credential)
    ok = m.start(a.env.env_id, a.env.credential)
    verdict("释放槽位后 a 重试成功",
            ok.status.value == "RUNNING" and ok.slot == 0
            and m.snapshot().occupied_slots == 1)


def test_concurrent_starts_hold_slot_at_most_once():
    banner("并发：同一环境重复启动不会重复占用/产生重复租约")
    clock = ManualClock()
    prov = SimulatedProvisioner(delay=0.05)
    m = EnvManager(capacity=2, clock=clock, provisioner=prov)
    r = m.apply("r", "alice")
    results = []
    errors = []

    def go():
        try:
            results.append(m.start(r.env.env_id, r.env.credential))
        except Exception as exc:  # noqa: BLE001
            errors.append(type(exc).__name__)

    threads = [threading.Thread(target=go) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = m.snapshot()
    print("成功数:", len(results), "异常:", errors, "快照:", snap.to_dict())
    verdict("恰好占用 1 个槽位", snap.occupied_slots == 1)
    verdict("所有成功返回同一租约到期时间",
            len({e.lease.expires_at for e in results}) == 1)
