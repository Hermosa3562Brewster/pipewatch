"""Display helpers for back-pressure stats."""
from __future__ import annotations

from typing import List

from pipewatch.pipeline_backpressure import BackpressureStats, BackpressureTracker


_PRESSURE_BADGE = {True: "\033[91m[PRESSURE]\033[0m", False: "\033[92m[OK]\033[0m"}


def _fmt_lag(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{seconds / 60:.1f}m"


def render_backpressure_table(tracker: BackpressureTracker) -> str:
    pipelines = tracker.all_pipelines()
    if not pipelines:
        return "No back-pressure data recorded."

    header = f"{'Pipeline':<24} {'Avg Depth':>10} {'Max Lag':>10} {'Status':>14}"
    sep = "-" * len(header)
    rows: List[str] = [header, sep]

    for name in sorted(pipelines):
        s = tracker.stats(name)
        badge = _PRESSURE_BADGE[s.is_under_pressure]
        rows.append(
            f"{name:<24} {s.avg_queue_depth:>10.1f} {_fmt_lag(s.max_lag_seconds):>10} {badge:>14}"
        )

    return "\n".join(rows)


def render_backpressure_summary(tracker: BackpressureTracker) -> str:
    pipelines = tracker.all_pipelines()
    total = len(pipelines)
    under_pressure = sum(
        1 for p in pipelines if tracker.stats(p).is_under_pressure
    )
    lines = [
        "=== Back-pressure Summary ===",
        f"Tracked pipelines : {total}",
        f"Under pressure    : {under_pressure}",
        f"Healthy           : {total - under_pressure}",
    ]
    return "\n".join(lines)


def print_backpressure(tracker: BackpressureTracker) -> None:
    print(render_backpressure_summary(tracker))
    print()
    print(render_backpressure_table(tracker))
