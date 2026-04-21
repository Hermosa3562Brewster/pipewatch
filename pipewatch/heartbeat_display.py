"""Display helpers for pipeline heartbeat status."""
from __future__ import annotations

from typing import List

from pipewatch.pipeline_heartbeat import HeartbeatStatus, HeartbeatTracker


def _colored_state(missed: bool) -> str:
    if missed:
        return "\033[31mMISSED\033[0m"
    return "\033[32mOK\033[0m"


def _fmt_seconds(seconds: float | None) -> str:
    if seconds is None:
        return "never"
    if seconds < 60:
        return f"{seconds:.0f}s"
    return f"{seconds / 60:.1f}m"


def render_heartbeat_table(statuses: List[HeartbeatStatus]) -> str:
    if not statuses:
        return "No heartbeat monitors configured."
    header = f"{'Pipeline':<25} {'State':<12} {'Last Beat':<12} {'Interval':<12}"
    sep = "-" * len(header)
    rows = [header, sep]
    for s in statuses:
        state_str = _colored_state(s.missed)
        last_str = _fmt_seconds(s.seconds_since)
        interval_str = f"{s.expected_interval.total_seconds():.0f}s"
        rows.append(f"{s.pipeline:<25} {state_str:<20} {last_str:<12} {interval_str:<12}")
    return "\n".join(rows)


def render_heartbeat_summary(statuses: List[HeartbeatStatus]) -> str:
    if not statuses:
        return "No heartbeat monitors configured."
    total = len(statuses)
    missed = sum(1 for s in statuses if s.missed)
    healthy = total - missed
    lines = [
        "=== Heartbeat Summary ===",
        f"  Monitored : {total}",
        f"  Healthy   : {healthy}",
        f"  Missed    : {missed}",
    ]
    if missed:
        lines.append("  Missed pipelines:")
        for s in statuses:
            if s.missed:
                lines.append(f"    - {s.summary()}")
    return "\n".join(lines)


def print_heartbeat(tracker: HeartbeatTracker, now=None) -> None:
    statuses = tracker.check_all(now=now)
    print(render_heartbeat_table(statuses))
    print()
    print(render_heartbeat_summary(statuses))
