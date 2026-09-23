"""本地服务入口：uv run uvicorn main:app。"""

from __future__ import annotations

from control_plane.api import create_app

app = create_app()
