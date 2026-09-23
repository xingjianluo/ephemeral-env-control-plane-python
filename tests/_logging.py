"""Small helpers so every scenario prints its criteria and final state."""

from __future__ import annotations

import threading

_lock = threading.Lock()


def section(title: str) -> None:
    line = "=" * 72
    with _lock:
        print(f"\n{line}\n{title}\n{line}")


def log(message: str) -> None:
    with _lock:
        print(f"  - {message}")
