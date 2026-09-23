"""业务错误类型。

每类失败原因对应一个独立异常，并带稳定的 ``code``，
调用方既能用类型区分，也能用 code 区分，不会被一律归为同一种失败。
"""

from __future__ import annotations


class ControlPlaneError(Exception):
    """所有控制面业务错误的基类。"""

    code: str = "control_plane_error"
    http_status: int = 400

    def __init__(self, message: str, *, env_id: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.env_id = env_id

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message, "env_id": self.env_id}


class EnvironmentNotFound(ControlPlaneError):
    code = "environment_not_found"
    http_status = 404


class InvalidStateTransition(ControlPlaneError):
    """当前状态不允许执行该操作。"""

    code = "invalid_state_transition"
    http_status = 409


class CapacityExhausted(ControlPlaneError):
    """运行槽与等待队列均满，申请被明确拒绝。"""

    code = "capacity_exhausted"
    http_status = 503


class LeaseExpired(ControlPlaneError):
    """租约已到期，环境已被回收。"""

    code = "lease_expired"
    http_status = 410


class InvalidCredential(ControlPlaneError):
    """缺少凭证或凭证无法识别。"""

    code = "invalid_credential"
    http_status = 401


class Unauthorized(ControlPlaneError):
    """凭证有效，但操作的环境不属于该主体。"""

    code = "unauthorized"
    http_status = 403


class CredentialExpired(ControlPlaneError):
    """凭证随环境终态（销毁/回收）失效。"""

    code = "credential_expired"
    http_status = 401


class AlreadyCancelled(ControlPlaneError):
    code = "already_cancelled"
    http_status = 409


class StartupFailed(ControlPlaneError):
    code = "startup_failed"
    http_status = 502


class IdempotencyConflict(ControlPlaneError):
    """相同 request_id 但申请内容（owner）不一致。"""

    code = "idempotency_conflict"
    http_status = 409
