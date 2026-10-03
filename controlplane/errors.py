"""Domain errors.

Each failure reason has a distinct stable ``code`` so callers (and tests)
can tell apart illegal transitions, permission problems, expiry, etc.
instead of receiving one generic failure.
"""

from __future__ import annotations


class ControlPlaneError(Exception):
    """Base class for all control-plane errors."""

    code: str = "internal_error"
    http_status: int = 400

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code


class EnvNotFoundError(ControlPlaneError):
    code = "env_not_found"
    http_status = 404


class InvalidTokenError(ControlPlaneError):
    """Token does not exist / cannot be parsed."""

    code = "invalid_token"
    http_status = 401


class TokenExpiredError(ControlPlaneError):
    """Token is structurally valid but its lease has expired."""

    code = "token_expired"
    http_status = 401


class EnvAccessDeniedError(ControlPlaneError):
    """Valid credential, but it does not own the target environment."""

    code = "env_access_denied"
    http_status = 403


class IllegalTransitionError(ControlPlaneError):
    code = "illegal_transition"
    http_status = 409

    def __init__(self, from_state: str, action: str, reason: str) -> None:
        super().__init__(
            f"illegal transition: {action} from {from_state} ({reason})"
        )
        self.from_state = from_state
        self.action = action
        self.reason = reason


class EnvDestroyedError(ControlPlaneError):
    code = "env_destroyed"
    http_status = 410


class EnvExpiredError(ControlPlaneError):
    code = "env_expired"
    http_status = 410


class EnvCanceledError(ControlPlaneError):
    code = "env_canceled"
    http_status = 410


class CapacityExhaustedError(ControlPlaneError):
    """All slots taken and the wait queue is full: request is rejected."""

    code = "capacity_exhausted"
    http_status = 503

    def __init__(self, message: str | None = None, env_id: str | None = None):
        super().__init__(message or self.code)
        self.env_id = env_id


class IdempotencyConflictError(ControlPlaneError):
    """Same idempotency key reused with a different request body."""

    code = "idempotency_conflict"
    http_status = 409


class ProvisioningError(ControlPlaneError):
    """Environment start (provisioning) failed; slot is released."""

    code = "provisioning_failed"
    http_status = 502

    def __init__(self, message: str | None = None, env: object | None = None):
        super().__init__(message or self.code)
        self.env = env


class StartTimeoutError(ControlPlaneError):
    """Provisioning exceeded the startup time limit; slot was reclaimed."""

    code = "start_timeout"
    http_status = 504

    def __init__(self, message: str | None = None, env: object | None = None):
        super().__init__(message or self.code)
        self.env = env


class StartAbortedError(ControlPlaneError):
    """Provisioning was actively aborted by the owner; slot was reclaimed."""

    code = "start_aborted"
    http_status = 409

    def __init__(self, message: str | None = None, env: object | None = None):
        super().__init__(message or self.code)
        self.env = env
