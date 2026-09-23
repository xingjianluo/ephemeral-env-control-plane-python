"""Credentials are scoped to one environment only. Cross-env access,
forged/expired credentials and reusing a destroyed env's token are rejected.
"""

from __future__ import annotations

import pytest

from controlplane.errors import (
    EnvAccessDeniedError,
    EnvDestroyedError,
    EnvNotFoundError,
    InvalidTokenError,
    TokenExpiredError,
)
from controlplane.models import FakeClock
from controlplane.service import ControlPlane

from ._logging import log, section


def test_credential_is_scoped_to_its_environment():
    section("[credentials] a token only works on its own environment")
    clock = FakeClock()
    plane = ControlPlane(capacity=2, default_lease_seconds=100, clock=clock)
    a = plane.apply("alice").environment
    b = plane.apply("bob").environment

    # use b's token against a's env
    with pytest.raises(EnvAccessDeniedError):
        plane.start(a.env_id, b.token)
    with pytest.raises(EnvAccessDeniedError):
        plane.stop(a.env_id, b.token)
    with pytest.raises(EnvAccessDeniedError):
        plane.destroy(a.env_id, b.token)
    log("cross-env start/stop/destroy -> env_access_denied")

    # valid token, unknown target
    with pytest.raises(EnvAccessDeniedError):
        plane.status("env_does_not_exist", a.token)
    log("valid token against unknown env -> env_access_denied")

    # forged token, existing target
    with pytest.raises(InvalidTokenError):
        plane.status(a.env_id, "tok_forged")
    log("forged token -> invalid_token")

    # both unknown -> not found (no env-id fishing via bad token)
    with pytest.raises(EnvNotFoundError):
        plane.status("env_unknown", "tok_unknown")
    log("unknown env + unknown token -> env_not_found")

    # each token is distinct
    assert a.token != b.token
    log("each environment received an independent random credential")


def test_credential_expires_with_lease_and_revoked_on_destroy():
    section("[credentials] overdue and destroyed credentials are rejected")
    clock = FakeClock()
    plane = ControlPlane(capacity=2, default_lease_seconds=10, clock=clock)
    a = plane.apply("alice").environment
    plane.start(a.env_id, a.token)

    clock.advance(10)
    with pytest.raises(TokenExpiredError) as ei:
        plane.status(a.env_id, a.token)
    assert ei.value.code == "token_expired"
    log("first use at/after lease end -> token_expired")
    # slot released even though expiry was detected via credential use
    assert plane.capacity_view().used_slots == 0
    log("expiry detected on credential use still released the slot")

    # destroy revokes credential with its own code
    b = plane.apply("bob").environment
    plane.destroy(b.env_id, b.token)
    with pytest.raises(EnvDestroyedError):
        plane.status(b.env_id, b.token)
    log("destroyed credential -> env_destroyed (distinct from token_expired)")
