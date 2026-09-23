"""HTTP API for the local control plane.

Endpoints (all env-scoped operations require ``X-Auth-Token``):

* ``POST /environments``               apply (admit / queue / reject)
* ``POST /environments/{id}/start``    start / provision
* ``POST /environments/{id}/stop``     stop (release slot, restartable)
* ``POST /environments/{id}/destroy``  destroy (terminal)
* ``POST /environments/{id}/cancel``   cancel while queued (terminal)
* ``GET  /environments/{id}``          owner-scoped status
* ``GET  /environments/{id}/explain``  why queued / rejected / admitted
* ``GET  /capacity``                   capacity + queue snapshot
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

from .errors import ControlPlaneError
from .service import ControlPlane, env_to_dict

logger = logging.getLogger("controlplane")


# ---- request / response schemas --------------------------------------

from pydantic import BaseModel, Field  # noqa: E402


class ApplyRequest(BaseModel):
    owner: str = Field(min_length=1)
    lease_seconds: int | None = Field(default=None, ge=1)
    idempotency_key: str | None = None
    fingerprint: str | None = None


def _env_payload(env) -> dict:
    return env_to_dict(env)


def create_app(
    plane: ControlPlane | None = None,
    reaper_interval: float = 0.1,
) -> FastAPI:
    plane = plane or ControlPlane()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        reaper = plane.start_reaper(interval=reaper_interval)
        logger.info("lease reaper started (interval=%ss)", reaper_interval)
        try:
            yield
        finally:
            plane.stop_reaper()

    app = FastAPI(
        title="Local Ephemeral Environment Control Plane",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.exception_handler(ControlPlaneError)
    async def handle_domain_error(request: Request, exc: ControlPlaneError):
        body = {
            "error": {
                "code": exc.code,
                "message": exc.message,
            }
        }
        extra: dict = {}
        for attr in ("from_state", "action", "reason", "env_id"):
            value = getattr(exc, attr, None)
            if value is not None:
                extra[attr] = value
        if extra:
            body["error"]["details"] = extra
        env = getattr(exc, "env", None)
        if env is not None:
            body["error"]["environment"] = _env_payload(env)
        return JSONResponse(status_code=exc.http_status, content=body)

    @app.post("/environments", status_code=200)
    def apply_environment(req: ApplyRequest):
        outcome = plane.apply(
            owner=req.owner,
            lease_seconds=req.lease_seconds,
            idempotency_key=req.idempotency_key,
            fingerprint=req.fingerprint,
        )
        return {
            "environment": _env_payload(outcome.environment),
            "deduplicated": outcome.deduplicated,
            "disposition": outcome.disposition,
            "reason": outcome.reason,
            "capacity": outcome.capacity.as_dict(),
        }

    def _token(x_auth_token: str | None) -> str:
        if not x_auth_token:
            # Raised as a domain error so it renders uniformly.
            from .errors import InvalidTokenError

            raise InvalidTokenError("missing X-Auth-Token")
        return x_auth_token

    @app.post("/environments/{env_id}/start")
    def start_environment(env_id: str, x_auth_token: str | None = Header(default=None)):
        token = _token(x_auth_token)
        outcome = plane.start(env_id, token)
        return {
            "environment": _env_payload(outcome.environment),
            "disposition": outcome.disposition,
            "reason": outcome.reason,
            "capacity": outcome.capacity.as_dict(),
        }

    @app.post("/environments/{env_id}/stop")
    def stop_environment(env_id: str, x_auth_token: str | None = Header(default=None)):
        env = plane.stop(env_id, _token(x_auth_token))
        return {"environment": _env_payload(env)}

    @app.post("/environments/{env_id}/destroy")
    def destroy_environment(env_id: str, x_auth_token: str | None = Header(default=None)):
        env = plane.destroy(env_id, _token(x_auth_token))
        return {"environment": _env_payload(env)}

    @app.post("/environments/{env_id}/cancel")
    def cancel_environment(env_id: str, x_auth_token: str | None = Header(default=None)):
        env = plane.cancel(env_id, _token(x_auth_token))
        return {"environment": _env_payload(env)}

    @app.get("/environments/{env_id}")
    def get_environment(env_id: str, x_auth_token: str | None = Header(default=None)):
        env = plane.status(env_id, _token(x_auth_token))
        return {"environment": _env_payload(env)}

    @app.get("/environments/{env_id}/explain")
    def explain_environment(env_id: str):
        return plane.explain(env_id)

    @app.get("/capacity")
    def get_capacity():
        return plane.capacity_view().as_dict()

    @app.get("/")
    def root():
        return {
            "service": "ephemeral-env-control-plane",
            "capacity": plane.capacity_view().as_dict(),
        }

    return app
