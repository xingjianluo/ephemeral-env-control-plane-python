"""pytest 共享夹具与判定打印工具。"""

from __future__ import annotations

import pytest

from control_plane.manager import EnvManager, ManualClock, SimulatedProvisioner


@pytest.fixture()
def clock():
    return ManualClock(start=1000.0)


@pytest.fixture()
def provisioner():
    return SimulatedProvisioner()


@pytest.fixture()
def manager(clock, provisioner):
    return EnvManager(
        capacity=3,
        max_queue=3,
        lease_ttl_seconds=10.0,
        clock=clock,
        provisioner=provisioner,
    )


def banner(title: str) -> None:
    print(f"\n===== {title} =====", flush=True)


def verdict(name: str, ok: bool, detail: str = "") -> None:
    """把判定依据与结论打印到测试日志。"""
    print(f"[{'PASS' if ok else 'FAIL'}] {name} :: {detail}", flush=True)
