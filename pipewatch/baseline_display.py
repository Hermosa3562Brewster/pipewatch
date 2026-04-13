"""Rendering helpers for pipeline baseline comparisons."""
from __future__ import annotations

from typing import Dict, List

from pipewatch.pipeline_baseline import BaselineDelta, BaselineSnapshot, BaselineTracker


def _delta_pct_str(value: float) -> str:
    if value > 0:
        return f"+{value:.1f}%"
    if value < 0:
        return f"{value:.1f}%"
    return "0.0%"


def _regression_badge(delta: BaselineDelta) -> str:
    if delta.is_regression:
        return "[REGRESS]"
    if delta.is_improvement:
        return "[IMPROVE]"
    return "[OK]     "


def render_baseline_table(tracker: BaselineTracker) -> str:
    baselines = tracker.all_baselines()
    if not baselines:
        return "No baselines recorded."

    header = f"{'Pipeline':<20} {'Avg Duration':>14} {'Error Rate':>12} {'Throughput':>12} {'Recorded At'}"
    sep = "-" * len(header)
    rows = [header, sep]
    for name, snap in sorted(baselines.items()):
        rows.append(
            f"{name:<20} {snap.avg_duration:>13.2f}s {snap.error_rate:>11.1%} "
            f"{snap.avg_throughput:>11.1f}  {snap.recorded_at[:19]}"
        )
    return "\n".join(rows)


def render_delta_table(deltas: List[BaselineDelta]) -> str:
    if not deltas:
        return "No deltas to display."

    header = f"{'Pipeline':<20} {'Status':<12} {'Duration Δ':>12} {'Error Rate Δ':>14} {'Throughput Δ':>13}"
    sep = "-" * len(header)
    rows = [header, sep]
    for d in sorted(deltas, key=lambda x: x.pipeline):
        rows.append(
            f"{d.pipeline:<20} {_regression_badge(d):<12} "
            f"{_delta_pct_str(d.duration_delta_pct):>12} "
            f"{d.error_rate_delta:>+13.1%} "
            f"{_delta_pct_str(d.throughput_delta_pct):>13}"
        )
    return "\n".join(rows)


def render_baseline_summary(tracker: BaselineTracker, deltas: List[BaselineDelta]) -> str:
    total = len(tracker.all_baselines())
    regressions = sum(1 for d in deltas if d.is_regression)
    improvements = sum(1 for d in deltas if d.is_improvement)
    lines = [
        "=== Baseline Summary ===",
        f"Tracked pipelines : {total}",
        f"Regressions       : {regressions}",
        f"Improvements      : {improvements}",
    ]
    return "\n".join(lines)


def print_baseline(tracker: BaselineTracker, deltas: List[BaselineDelta]) -> None:
    print(render_baseline_summary(tracker, deltas))
    print()
    print(render_delta_table(deltas))
    print()
    print(render_baseline_table(tracker))
