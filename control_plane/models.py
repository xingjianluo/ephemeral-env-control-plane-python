"""核心数据模型：只包含类型定义，不含业务逻辑。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum


class EnvStatus(str, Enum):
    """环境生命周期状态。

    PENDING    已受理但容量不足，进入 FIFO 等待队列（不占槽位）
    ALLOCATED  已从队列提升并占用槽位，尚未启动
    STARTING   启动过程中（占用槽位，失败将回滚）
    RUNNING    运行中（占用槽位，持有有效租约）
    STOPPED    已停止但保留（占用槽位，租约暂停）
    FAILED     启动失败（不占槽位，可重试启动或销毁）
    DESTROYED  已销毁（终态，不占槽位）
    EXPIRED    租约到期被回收（终态，不占槽位）
    CANCELLED  排队等待期间被取消（终态，不占槽位）
    """

    PENDING = "PENDING"
    ALLOCATED = "ALLOCATED"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    DESTROYED = "DESTROYED"
    EXPIRED = "EXPIRED"
    CANCELLED = "CANCELLED"


@dataclass
class Lease:
    """环境租约：仅在 RUNNING 期间计时。"""

    expires_at: float
    ttl_seconds: float

    def is_expired(self, now: float) -> bool:
        return now >= self.expires_at


@dataclass
class Environment:
    """一个预览环境的完整本地状态。"""

    env_id: str
    request_id: str
    owner: str
    credential: str
    status: EnvStatus
    slot: int | None = None
    created_at: float = 0.0
    lease: Lease | None = None
    fail_reason: str | None = None
    # 排队次序（队首在队列中的位置由管理器维护）
    enqueued_at: float = 0.0
    # 等待队列中的取消标记
    cancelled: bool = False
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)
