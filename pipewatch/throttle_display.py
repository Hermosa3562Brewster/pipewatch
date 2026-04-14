"""Display helpers for pipeline throttle status."""
from __future__ import annotations

from typing import Dict

from pipewatch.pipeline_throttle import PipelineThrottleManager, ThrottleStatus


def _fmt_last_run(status: ThrottleStatus) -> str:
    if status.last_run_at is None:
        return "never"
    return status.last_run_at.strftime("%Y-%m-%d %H:%M:%S")


def _allowed_badge(status: ThrottleStatus) -> str:
    return "\033[32mALLOWED\033[0m" if not status.is_throttled else "\033[33mTHROTTLED\033[0m"


def render_throttle_table(manager: PipelineThrottleManager) -> str:
    statuses = manager.all_statuses()
    if not statuses:
        return "No throttle configurations registered."

    header = f"{'Pipeline':<22} {'Status':<18} {'Last Run':<22} {'Min Interval (s)':<18} {'Wait (s)'}"
    sep = "-" * 90
    rows = [header, sep]
    for name, st in statuses.items():
        rows.append(
            f"{name:<22} {_allowed_badge(st):<27} {_fmt_last_run(st):<22} "
            f"{st.min_interval_seconds:<18.1f} {st.seconds_until_allowed:.1f}"
        )
    return "\n".join(rows)


def render_throttle_summary(manager: PipelineThrottleManager) -> str:
    statuses = manager.all_statuses()
    total = len(statuses)
    throttled = sum(1 for s in statuses.values() if s.is_throttled)
    allowed = total - throttled
    lines = [
        "=== Throttle Summary ===",
        f"Total configured : {total}",
        f"Allowed          : {allowed}",
        f"Throttled        : {throttled}",
    ]
    return "\n".join(lines)


def print_throttle(manager: PipelineThrottleManager) -> None:
    print(render_throttle_summary(manager))
    print()
    print(render_throttle_table(manager))
