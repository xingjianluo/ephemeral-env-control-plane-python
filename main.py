"""Local HTTP entrypoint: ``uv run uvicorn main:app``."""

from controlplane.api import create_app
from controlplane.service import ControlPlane

plane = ControlPlane()
app = create_app(plane)
