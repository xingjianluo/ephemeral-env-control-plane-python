"""测试判定打印工具：把依据与结论输出到测试日志。"""

from __future__ import annotations


def banner(title: str) -> None:
    print(f"\n===== {title} =====", flush=True)


def verdict(name: str, ok: bool, detail: str = "") -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name} :: {detail}", flush=True)
