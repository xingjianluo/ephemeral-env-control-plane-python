"""环境控制面核心：槽位、队列、租约、凭证与状态机。

所有状态变更都在单一 ``threading.RLock`` 下完成，保证并发下
"状态—槽位"始终一致；启动外部副作用在锁外执行，失败时回滚占用。
"""

from __future__ import annotations

import secrets
import threading
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Protocol

from .errors import (
    AlreadyCancelled,
    CapacityExhausted,
    CredentialExpired,
    EnvironmentNotFound,
    IdempotencyConflict,
    InvalidCredential,
    InvalidStateTransition,
    LeaseExpired,
    StartupFailed,
    Unauthorized,
)
from .models import EnvStatus, Environment, Lease


class Clock(Protocol):
    def now(self) -> float: ...


class SystemClock:
    def now(self) -> float:
        import time

        return time.monotonic()


class ManualClock:
    """测试用可控时钟：时间只有显式 advance 才前进。"""

    def __init__(self, start: float = 1000.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> float:
        self._now += seconds
        return self._now


@dataclass
class _ProvisionResult:
    ok: bool
    detail: str = ""


class SimulatedProvisioner:
    """模拟真实环境启停。

    - 正常情况下 start/stop 成功；
    - 通过 ``fail_next_start`` 让下一次 start 失败（模拟启动中途失败）；
    - 通过 ``interrupt_in_start`` 抛出异常（模拟执行被中断）；
    - ``delay`` 可用于制造并发竞态窗口。
    """

    def __init__(
        self,
        fail_next_start: bool = False,
        interrupt_in_start: bool = False,
        delay: float = 0.0,
    ) -> None:
        self.fail_next_start = fail_next_start
        self.interrupt_in_start = interrupt_in_start
        self.delay = delay
        self.calls: list[tuple[str, str]] = []

    def start(self, env: Environment) -> None:
        self.calls.append(("start", env.env_id))
        if self.delay:
            import time

            time.sleep(self.delay)
        if self.interrupt_in_start:
            raise RuntimeError("provision interrupted")
        if self.fail_next_start:
            self.fail_next_start = False
            raise RuntimeError("image pull failed")

    def stop(self, env: Environment) -> None:
        self.calls.append(("stop", env.env_id))


@dataclass
class RejectedRequest:
    """被拒绝申请的留痕，用于解释“为何被拒绝”。"""

    request_id: str
    owner: str
    reason: str
    rejected_at: float


@dataclass
class ApplyResult:
    env: Environment
    queued: bool
    reason: str
    position: int | None = None

    def to_dict(self) -> dict:
        return {
            "env_id": self.env.env_id,
            "status": self.env.status.value,
            "queued": self.queued,
            "reason": self.reason,
            "position": self.position,
        }


# 占用槽位的状态
_SLOT_HOLDING = {
    EnvStatus.ALLOCATED,
    EnvStatus.STARTING,
    EnvStatus.RUNNING,
    EnvStatus.STOPPED,
}

# 终态
_TERMINAL = {
    EnvStatus.DESTROYED,
    EnvStatus.EXPIRED,
    EnvStatus.CANCELLED,
    EnvStatus.FAILED,
}


@dataclass
class CapacitySnapshot:
    capacity: int
    max_queue: int
    occupied_slots: int
    free_slots: int
    queue_length: int
    active_envs: int = 0

    def to_dict(self) -> dict:
        return {
            "capacity": self.capacity,
            "max_queue": self.max_queue,
            "occupied_slots": self.occupied_slots,
            "free_slots": self.free_slots,
            "queue_length": self.queue_length,
            "active_envs": self.active_envs,
        }


class EnvManager:
    """线程安全的本地环境管理器。"""

    def __init__(
        self,
        capacity: int = 3,
        max_queue: int | None = None,
        lease_ttl_seconds: float = 60.0,
        clock: Clock | None = None,
        provisioner: SimulatedProvisioner | None = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self.max_queue = capacity if max_queue is None else max_queue
        self.lease_ttl = float(lease_ttl_seconds)
        self.clock = clock or SystemClock()
        self.provisioner = provisioner or SimulatedProvisioner()

        self._lock = threading.RLock()
        self._envs: dict[str, Environment] = {}
        self._by_request: dict[str, str] = {}  # request_id -> env_id
        self._by_credential: dict[str, str] = {}  # credential -> env_id
        self._slots: set[int] = set()  # 已占用槽位号
        self._queue: deque[str] = deque()  # env_id，FIFO
        self._rejected: dict[str, RejectedRequest] = {}

    # ============================================================
    # 内部工具
    # ============================================================
    def _occupied(self) -> int:
        return len(self._slots)

    def _free_slot(self) -> int | None:
        for i in range(self.capacity):
            if i not in self._slots:
                return i
        return None

    def _promote_locked(self) -> None:
        """在持锁状态下，按 FIFO 尽量提升队首，填满所有当前空槽。

        跳过已取消/失效的排队项。调用时机：槽位被释放之后
        （销毁、到期回收、启动失败回滚、排队取消仅释放队列位置时也安全）。
        """
        while self._queue and self._occupied() < self.capacity:
            env_id = self._queue[0]
            env = self._envs.get(env_id)
            if env is None or env.cancelled or env.status != EnvStatus.PENDING:
                self._queue.popleft()
                continue
            slot = self._free_slot()
            if slot is None:
                return
            self._slots.add(slot)
            env.slot = slot
            env.status = EnvStatus.ALLOCATED
            self._queue.popleft()

    def _new_credential_locked(self) -> str:
        while True:
            cred = "cred_" + secrets.token_hex(12)
            if cred not in self._by_credential:
                return cred

    def _authorize_locked(self, env: Environment, credential: str | None) -> None:
        """统一鉴权：先验凭证，再验归属，再验环境是否仍可访问。"""
        if not credential:
            raise InvalidCredential("missing credential", env_id=env.env_id)
        cred_env_id = self._by_credential.get(credential)
        if cred_env_id is None:
            raise InvalidCredential("credential not recognized", env_id=env.env_id)
        if cred_env_id != env.env_id:
            # 凭证属于另一个环境：典型的越权访问
            raise Unauthorized(
                "credential does not belong to this environment",
                env_id=env.env_id,
            )
        if env.status in (EnvStatus.DESTROYED, EnvStatus.CANCELLED):
            raise CredentialExpired(
                f"credential invalidated by {env.status.value}",
                env_id=env.env_id,
            )
        if env.status == EnvStatus.EXPIRED:
            raise LeaseExpired("lease expired and reaped", env_id=env.env_id)

    # ============================================================
    # 申请 / 取消
    # ============================================================
    def apply(self, request_id: str, owner: str) -> ApplyResult:
        if not request_id or not owner:
            raise ValueError("request_id and owner are required")
        with self._lock:
            now = self.clock.now()

            # 此前被明确拒绝：重试同一申请返回同样的拒绝（幂等）
            rejected = self._rejected.get(request_id)
            if rejected is not None:
                if rejected.owner != owner:
                    raise IdempotencyConflict(
                        "request_id reused by a different owner"
                    )
                raise CapacityExhausted(rejected.reason)

            # 幂等：相同 request_id 直接返回既有结果，绝不重复占用
            existing_id = self._by_request.get(request_id)
            if existing_id is not None:
                existing = self._envs[existing_id]
                if existing.owner != owner:
                    raise IdempotencyConflict(
                        "request_id reused by a different owner",
                        env_id=existing_id,
                    )
                if existing.status == EnvStatus.PENDING and not existing.cancelled:
                    pos = (
                        list(self._queue).index(existing_id) + 1
                        if existing_id in self._queue
                        else None
                    )
                    return ApplyResult(
                        existing, True, "idempotent: already queued", pos
                    )
                return ApplyResult(
                    existing,
                    existing.status == EnvStatus.PENDING,
                    f"idempotent: {existing.status.value.lower()}",
                )

            env = Environment(
                env_id="env_" + uuid.uuid4().hex[:12],
                request_id=request_id,
                owner=owner,
                credential=self._new_credential_locked(),
                status=EnvStatus.PENDING,
                created_at=now,
                enqueued_at=now,
            )

            slot = self._free_slot()
            if slot is not None:
                self._slots.add(slot)
                env.slot = slot
                env.status = EnvStatus.ALLOCATED
                self._envs[env.env_id] = env
                self._by_request[request_id] = env.env_id
                self._by_credential[env.credential] = env.env_id
                return ApplyResult(
                    env, False, f"allocated slot {slot} immediately"
                )

            if len(self._queue) < self.max_queue:
                env.enqueued_at = now
                self._envs[env.env_id] = env
                self._by_request[request_id] = env.env_id
                self._by_credential[env.credential] = env.env_id
                self._queue.append(env.env_id)
                return ApplyResult(
                    env,
                    True,
                    f"queued: all {self.capacity} slots occupied",
                    position=len(self._queue),
                )

            # 槽满且队列满：明确拒绝并留痕（不保留 env/凭证映射）
            reason = (
                f"rejected: {self.capacity} slots and "
                f"{self.max_queue} queue positions all occupied"
            )
            self._rejected[request_id] = RejectedRequest(
                request_id, owner, reason, now
            )
            raise CapacityExhausted(reason, env_id=env.env_id)

    def cancel_pending(self, request_id: str, owner: str) -> Environment:
        with self._lock:
            env_id = self._by_request.get(request_id)
            if env_id is None:
                raise EnvironmentNotFound(f"unknown request: {request_id}")
            env = self._envs[env_id]
            if env.owner != owner:
                raise Unauthorized(
                    "cannot cancel another owner's request", env_id=env_id
                )
            if env.cancelled or env.status == EnvStatus.CANCELLED:
                raise AlreadyCancelled("request already cancelled", env_id=env_id)
            if env.status != EnvStatus.PENDING:
                raise InvalidStateTransition(
                    f"cannot cancel in status {env.status.value}",
                    env_id=env_id,
                )
            env.cancelled = True
            env.status = EnvStatus.CANCELLED
            # 若仍在队列中，立即摘除并尝试补位
            if env_id in self._queue:
                self._queue.remove(env_id)
                self._promote_locked()
            return env

    def _expire_if_due_locked(self, env: Environment) -> None:
        """惰性回收：RUNNING 且租约到期 -> EXPIRED 并释放槽位。"""
        if (
            env.status == EnvStatus.RUNNING
            and env.lease is not None
            and env.lease.is_expired(self.clock.now())
        ):
            env.status = EnvStatus.EXPIRED
            env.lease = None
            if env.slot is not None:
                self._slots.discard(env.slot)
                env.slot = None
            # 回收产生空槽，尝试补位
            self._promote_locked()

    # ============================================================
    # 生命周期
    # ============================================================
    def start(self, env_id: str, credential: str) -> Environment:
        with self._lock:
            env = self._envs.get(env_id)
            if env is None:
                raise EnvironmentNotFound(f"unknown env: {env_id}")
            self._authorize_locked(env, credential)
            self._expire_if_due_locked(env)

            if env.status == EnvStatus.RUNNING:
                # 幂等：重复启动直接返回当前环境
                return env
            if env.status == EnvStatus.STARTING:
                raise InvalidStateTransition(
                    "start already in progress", env_id=env_id
                )
            if env.status == EnvStatus.PENDING:
                raise InvalidStateTransition(
                    "environment is still queued", env_id=env_id
                )
            if env.status == EnvStatus.STOPPED:
                # 重启：槽位仍保留，重新进入启动
                env.status = EnvStatus.STARTING
                acquired_slot = None
            elif env.status == EnvStatus.ALLOCATED:
                env.status = EnvStatus.STARTING
                acquired_slot = None
            elif env.status == EnvStatus.FAILED:
                # 失败后重试：必须重新获得槽位，拿不到就明确拒绝
                slot = self._free_slot()
                if slot is None:
                    raise CapacityExhausted(
                        "retry blocked: no free slot", env_id=env_id
                    )
                self._slots.add(slot)
                env.slot = slot
                env.fail_reason = None
                env.status = EnvStatus.STARTING
                acquired_slot = slot
            else:  # EXPIRED -> LeaseExpired（authorize 已拦，兜底）
                raise InvalidStateTransition(
                    f"cannot start in status {env.status.value}", env_id=env_id
                )

        # 外部副作用在锁外执行；失败必须回滚，绝不残留占用
        try:
            self.provisioner.start(env)
        except Exception as exc:  # noqa: BLE001 - 模拟一切启动失败/中断
            with self._lock:
                env.status = EnvStatus.FAILED
                env.fail_reason = f"{type(exc).__name__}: {exc}"
                env.lease = None
                if env.slot is not None:
                    self._slots.discard(env.slot)
                    env.slot = None
                self._promote_locked()
            raise StartupFailed(
                f"environment failed to start: {exc}", env_id=env_id
            ) from exc

        with self._lock:
            # 启动期间理论上不允许 destroy（STARTING 被拒），故此处仍在启动中
            env.status = EnvStatus.RUNNING
            now = self.clock.now()
            env.lease = Lease(
                expires_at=now + self.lease_ttl, ttl_seconds=self.lease_ttl
            )
            env.fail_reason = None
            return env

    def stop(self, env_id: str, credential: str) -> Environment:
        with self._lock:
            env = self._envs.get(env_id)
            if env is None:
                raise EnvironmentNotFound(f"unknown env: {env_id}")
            self._authorize_locked(env, credential)
            self._expire_if_due_locked(env)

            if env.status == EnvStatus.EXPIRED:
                raise LeaseExpired("lease expired and reaped", env_id=env_id)
            if env.status == EnvStatus.STOPPED:
                return env  # 幂等
            if env.status == EnvStatus.RUNNING:
                env.status = EnvStatus.STOPPED
                env.lease = None  # 租约暂停计时
            elif env.status == EnvStatus.STARTING:
                raise InvalidStateTransition(
                    "cannot stop while starting", env_id=env_id
                )
            else:
                raise InvalidStateTransition(
                    f"cannot stop in status {env.status.value}", env_id=env_id
                )

        # 停止副作用不释放槽位；模拟实现不会失败
        self.provisioner.stop(env)
        return env

    def destroy(self, env_id: str, credential: str) -> Environment:
        with self._lock:
            env = self._envs.get(env_id)
            if env is None:
                raise EnvironmentNotFound(f"unknown env: {env_id}")
            self._authorize_locked(env, credential)

            if env.status == EnvStatus.STARTING:
                raise InvalidStateTransition(
                    "cannot destroy while start is in progress", env_id=env_id
                )
            if env.status == EnvStatus.PENDING:
                raise InvalidStateTransition(
                    "queued request must be cancelled, not destroyed",
                    env_id=env_id,
                )
            if env.status in (EnvStatus.DESTROYED, EnvStatus.EXPIRED):
                raise InvalidStateTransition(
                    f"environment already {env.status.value.lower()}",
                    env_id=env_id,
                )

            was_running = env.status == EnvStatus.RUNNING
            env.status = EnvStatus.DESTROYED
            env.lease = None
            if env.slot is not None:
                self._slots.discard(env.slot)
                env.slot = None
            # 凭证随销毁立即失效
            self._by_credential.pop(env.credential, None)
            self._promote_locked()

        if was_running:
            self.provisioner.stop(env)
        return env

    def reap_expired(self) -> list[Environment]:
        """扫描并回收所有到期 RUNNING 环境，释放槽位并补位。"""
        reaped: list[Environment] = []
        with self._lock:
            now = self.clock.now()
            for env in list(self._envs.values()):
                if (
                    env.status == EnvStatus.RUNNING
                    and env.lease is not None
                    and env.lease.is_expired(now)
                ):
                    env.status = EnvStatus.EXPIRED
                    env.lease = None
                    if env.slot is not None:
                        self._slots.discard(env.slot)
                        env.slot = None
                    reaped.append(env)
            # 可能释放多个槽位，一次性按 FIFO 填满空槽
            if reaped:
                self._promote_locked()
        if reaped:
            for env in reaped:
                self.provisioner.stop(env)
        return reaped

    # ============================================================
    # 查询
    # ============================================================
    def get(self, env_id: str, credential: str | None = None) -> Environment:
        with self._lock:
            env = self._envs.get(env_id)
            if env is None:
                raise EnvironmentNotFound(f"unknown env: {env_id}")
            if credential is not None:
                self._expire_if_due_locked(env)
                self._authorize_locked(env, credential)
            return env

    def snapshot(self) -> CapacitySnapshot:
        with self._lock:
            occupied = self._occupied()
            active = sum(
                1
                for e in self._envs.values()
                if e.status in _SLOT_HOLDING
            )
            return CapacitySnapshot(
                capacity=self.capacity,
                max_queue=self.max_queue,
                occupied_slots=occupied,
                free_slots=self.capacity - occupied,
                queue_length=len(self._queue),
                active_envs=active,
            )

    def explain(self, request_id: str) -> dict:
        """说明某个申请当前为何处于该状态（已分配/排队/拒绝/终态）。"""
        with self._lock:
            rejected = self._rejected.get(request_id)
            env_id = self._by_request.get(request_id)
            if rejected is not None and env_id is None:
                return {
                    "request_id": request_id,
                    "outcome": "rejected",
                    "reason": rejected.reason,
                }
            if env_id is None:
                raise EnvironmentNotFound(f"unknown request: {request_id}")
            env = self._envs[env_id]
            if env.status == EnvStatus.PENDING:
                pos = (
                    list(self._queue).index(env_id) + 1
                    if env_id in self._queue
                    else None
                )
                return {
                    "request_id": request_id,
                    "env_id": env_id,
                    "outcome": "queued",
                    "position": pos,
                    "reason": f"all {self.capacity} slots occupied; FIFO position {pos}",
                }
            if env.status in (EnvStatus.ALLOCATED, EnvStatus.STARTING,
                              EnvStatus.RUNNING, EnvStatus.STOPPED):
                return {
                    "request_id": request_id,
                    "env_id": env_id,
                    "outcome": env.status.value.lower(),
                    "slot": env.slot,
                    "reason": f"holding slot {env.slot}",
                }
            if env.status == EnvStatus.FAILED:
                return {
                    "request_id": request_id,
                    "env_id": env_id,
                    "outcome": "failed",
                    "reason": env.fail_reason,
                }
            return {
                "request_id": request_id,
                "env_id": env_id,
                "outcome": env.status.value.lower(),
                "reason": f"terminal status {env.status.value}",
            }
