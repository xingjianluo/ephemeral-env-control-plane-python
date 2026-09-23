"""状态机非法迁移、终态拒绝与可区分错误测试。"""

from __future__ import annotations

import pytest

from control_plane.errors import (
    InvalidCredential,
    InvalidStateTransition,
    Unauthorized,
)
from tests.helpers import banner, verdict


def _allocate_started(manager, rid="r1", owner="alice"):
    r = manager.apply(rid, owner)
    return manager.start(r.env.env_id, r.env.credential)


def test_legal_and_idempotent_transitions(manager):
    banner("状态机：合法迁移与重复操作幂等")
    env = _allocate_started(manager)
    verdict("申请->启动 为 RUNNING", env.status.value == "RUNNING")

    again = manager.start(env.env_id, env.credential)
    verdict("RUNNING 重复 start 幂等（同一租约）",
            again is env or again.lease.expires_at == env.lease.expires_at)

    stopped = manager.stop(env.env_id, env.credential)
    verdict("RUNNING->STOPPED", stopped.status.value == "STOPPED")
    stopped2 = manager.stop(env.env_id, env.credential)
    verdict("STOPPED 重复 stop 幂等", stopped2.status.value == "STOPPED")

    restarted = manager.start(env.env_id, env.credential)
    verdict("STOPPED->STARTING->RUNNING 可重启",
            restarted.status.value == "RUNNING" and restarted.slot is not None)

    destroyed = manager.destroy(env.env_id, env.credential)
    verdict("RUNNING->DESTROYED", destroyed.status.value == "DESTROYED")


def test_illegal_transitions_are_distinct(manager):
    banner("非法迁移：不同原因返回不同错误码")
    env = _allocate_started(manager)
    cred = env.credential

    # RUNNING 中再 destroy -> 合法；这里先验证 STARTING 中不能 destroy/stop
    from control_plane.manager import EnvManager, ManualClock, SimulatedProvisioner
    import threading

    blocking = SimulatedProvisioner(delay=0.3)
    m2 = EnvManager(capacity=1, clock=ManualClock(), provisioner=blocking)
    rr = m2.apply("x", "carol")
    t = threading.Thread(
        target=lambda: m2.start(rr.env.env_id, rr.env.credential)
    )
    t.start()
    import time

    time.sleep(0.05)  # 确保进入 STARTING
    with pytest.raises(InvalidStateTransition) as e_stop:
        m2.stop(rr.env.env_id, rr.env.credential)
    verdict("STARTING 中 stop -> invalid_state_transition",
            e_stop.value.code == "invalid_state_transition")
    with pytest.raises(InvalidStateTransition) as e_des:
        m2.destroy(rr.env.env_id, rr.env.credential)
    verdict("STARTING 中 destroy -> invalid_state_transition",
            e_des.value.code == "invalid_state_transition")
    t.join()

    # PENDING 不能 start：占满 3 槽后再申请
    for i in range(2):
        manager.apply(f"fill{i}", f"f{i}")  # 与 env 合计 3 个占用
    q = manager.apply("queued", "bob")
    assert q.queued
    with pytest.raises(InvalidStateTransition) as e2:
        manager.start(q.env.env_id, q.env.credential)
    verdict("PENDING start -> invalid_state_transition",
            e2.value.code == "invalid_state_transition")

    # 销毁后再操作：凭证先失效，其次状态为终态
    manager.destroy(env.env_id, cred)
    with pytest.raises(InvalidCredential) as e1:
        manager.start(env.env_id, cred)
    verdict("销毁后再 start -> invalid_credential", e1.value.code == "invalid_credential")

    # STOPPED 允许 destroy（独立 manager，避免受排队影响）
    m3 = EnvManager(capacity=1, clock=ManualClock(),
                    provisioner=SimulatedProvisioner())
    rs = m3.apply("s", "dave")
    env_s = m3.start(rs.env.env_id, rs.env.credential)
    m3.stop(env_s.env_id, env_s.credential)
    destroyed = m3.destroy(env_s.env_id, env_s.credential)
    verdict("STOPPED->DESTROYED 合法", destroyed.status.value == "DESTROYED")


def test_credential_binding_and_cross_access(manager):
    banner("凭证：仅对所属环境有效，越权/伪造/过期被区分拒绝")
    a = _allocate_started(manager, "a", "alice")
    b = manager.apply("b", "bob").env

    with pytest.raises(Unauthorized) as e:
        manager.get(a.env_id, b.credential)
    verdict("用 bob 凭证访问 alice 环境 -> unauthorized",
            e.value.code == "unauthorized")

    with pytest.raises(InvalidCredential) as e:
        manager.get(a.env_id, "cred_tampered")
    verdict("伪造凭证 -> invalid_credential", e.value.code == "invalid_credential")

    with pytest.raises(InvalidCredential) as e:
        manager.stop(a.env_id, "")
    verdict("缺少凭证 -> invalid_credential", e.value.code == "invalid_credential")

    manager.destroy(a.env_id, a.credential)
    with pytest.raises(InvalidCredential) as e:
        manager.get(a.env_id, a.credential)
    verdict("销毁后旧凭证 -> invalid_credential", e.value.code == "invalid_credential")

    with pytest.raises(Unauthorized) as e:
        manager.cancel_pending("b", "mallory")
    verdict("取消他人申请 -> unauthorized", e.value.code == "unauthorized")
