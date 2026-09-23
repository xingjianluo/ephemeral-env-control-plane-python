"""FastAPI HTTP 层：把本地控制面暴露为 REST 接口。

错误统一映射为带稳定 ``code`` 的 JSON 响应，不同失败原因可区分。
"""

from __future__ import annotations

import os
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import ControlPlaneError, InvalidCredential
from .manager import EnvManager, SystemClock


class ApplyBody(BaseModel):
    request_id: str = Field(min_length=1)
    owner: str = Field(min_length=1)


def _env_dict(env) -> dict:
    return {
        "env_id": env.env_id,
        "request_id": env.request_id,
        "owner": env.owner,
        "status": env.status.value,
        "slot": env.slot,
        "fail_reason": env.fail_reason,
        "lease": (
            {
                "expires_at": env.lease.expires_at,
                "ttl_seconds": env.lease.ttl_seconds,
            }
            if env.lease
            else None
        ),
    }


def create_app(
    capacity: int | None = None,
    max_queue: int | None = None,
    lease_ttl_seconds: float | None = None,
    reaper_interval: float | None = None,
    manager: EnvManager | None = None,
) -> FastAPI:
    """构造 FastAPI 应用。

    reaper_interval>0 时启动后台租约回收线程；默认 0 表示仅惰性回收，
    便于本地与测试环境保持确定性。
    """
    if manager is None:
        capacity = int(os.environ.get("ECP_CAPACITY", capacity or 3))
        max_queue = int(os.environ.get("ECP_MAX_QUEUE", max_queue or capacity))
        lease_ttl_seconds = float(
            os.environ.get("ECP_LEASE_TTL", lease_ttl_seconds or 60.0)
        )
        manager = EnvManager(
            capacity=capacity,
            max_queue=max_queue,
            lease_ttl_seconds=lease_ttl_seconds,
            clock=SystemClock(),
        )

    if reaper_interval is None:
        reaper_interval = float(os.environ.get("ECP_REAPER_INTERVAL", 0.0))

    stop_reaper = threading.Event()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        thread: threading.Thread | None = None
        if reaper_interval and reaper_interval > 0:
            def _loop() -> None:
                while not stop_reaper.wait(reaper_interval):
                    try:
                        manager.reap_expired()
                    except Exception:  # noqa: BLE001
                        pass

            thread = threading.Thread(
                target=_loop, name="lease-reaper", daemon=True
            )
            thread.start()
        app.state.manager = manager
        try:
            yield
        finally:
            stop_reaper.set()
            if thread is not None:
                thread.join(timeout=1.0)

    app = FastAPI(
        title="Ephemeral Environment Control Plane (local)",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.manager = manager

    def _cred(x_credential: str | None) -> str:
        if not x_credential:
            raise InvalidCredential("missing X-Credential header")
        return x_credential

    @app.exception_handler(ControlPlaneError)
    async def _handle_cp_error(request, exc: ControlPlaneError):  # noqa: ANN001
        return JSONResponse(
            status_code=exc.http_status,
            content={"error": exc.to_dict()},
        )

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/environments")
    def apply(body: ApplyBody):
        mgr = app.state.manager
        result = mgr.apply(body.request_id, body.owner)
        payload = _env_dict(result.env)
        payload["queued"] = result.queued
        payload["reason"] = result.reason
        payload["position"] = result.position
        # 首次受理返回凭证；重复申请也返回，便于客户端恢复
        payload["credential"] = result.env.credential
        return payload

    @app.post("/environments/{env_id}/cancel")
    def cancel(env_id: str, x_owner: str = Header(default="", alias="X-Owner")):
        # 排队取消通过 request_id 语义更清晰，这里按 env_id 支持
        mgr = app.state.manager
        env = mgr.get(env_id)
        return _env_dict(mgr.cancel_pending(env.request_id, x_owner))

    def _cred(x_credential: str | None) -> str:
        if not x_credential:
            raise InvalidCredential("missing X-Credential header")
        return x_credential

    @app.post("/environments/{env_id}/start")
    def start(env_id: str, x_credential: str | None = Header(default=None)):
        env = app.state.manager.start(env_id, _cred(x_credential))
        return _env_dict(env)

    @app.post("/environments/{env_id}/stop")
    def stop(env_id: str, x_credential: str | None = Header(default=None)):
        env = app.state.manager.stop(env_id, _cred(x_credential))
        return _env_dict(env)

    @app.delete("/environments/{env_id}")
    def destroy(env_id: str, x_credential: str | None = Header(default=None)):
        env = app.state.manager.destroy(env_id, _cred(x_credential))
        return _env_dict(env)

    @app.get("/environments/{env_id}")
    def describe(env_id: str, x_credential: str | None = Header(default=None)):
        return _env_dict(app.state.manager.get(env_id, x_credential))

    @app.get("/capacity")
    def capacity_view():
        return app.state.manager.snapshot().to_dict()

    @app.get("/requests/{request_id}")
    def explain(request_id: str):
        return app.state.manager.explain(request_id)

    @app.post("/internal/reap")
    def reap():
        reaped = app.state.manager.reap_expired()
        return {"reaped": [e.env_id for e in reaped]}

    return app
