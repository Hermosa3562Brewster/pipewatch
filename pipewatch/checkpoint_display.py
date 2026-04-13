"""Rendering helpers for pipeline checkpoint data."""

from __future__ import annotations

from typing import List, Optional

from pipewatch.pipeline_checkpoints import Checkpoint, CheckpointTracker, CheckpointRegistry


def _fmt_duration(duration_ms: Optional[float]) -> str:
    if duration_ms is None:
        return "—"
    if duration_ms < 1000:
        return f"{duration_ms:.1f} ms"
    return f"{duration_ms / 1000:.2f} s"


def _fmt_metadata(metadata: dict) -> str:
    if not metadata:
        return ""
    return "  " + "  ".join(f"{k}={v}" for k, v in metadata.items())


def render_checkpoint_list(tracker: CheckpointTracker, n: int = 10) -> str:
    """Render the last N checkpoints for a pipeline as a table string."""
    checkpoints = tracker.last_n(n)
    pipeline = tracker.pipeline_name
    header = f"Checkpoints — {pipeline}"
    divider = "─" * 52
    lines = [header, divider, f"  {'#':<4} {'Name':<22} {'Duration':<12} {'Metadata'}", divider]
    if not checkpoints:
        lines.append("  (no checkpoints recorded)")
    else:
        for idx, cp in enumerate(checkpoints, start=1):
            dur = _fmt_duration(cp.duration_ms)
            meta = _fmt_metadata(cp.metadata)
            lines.append(f"  {idx:<4} {cp.name:<22} {dur:<12}{meta}")
    lines.append(divider)
    return "\n".join(lines)


def render_checkpoint_summary(registry: CheckpointRegistry) -> str:
    """Render a one-line summary per pipeline showing latest checkpoint."""
    names = registry.all_pipeline_names()
    if not names:
        return "No checkpoint data available."
    divider = "─" * 52
    lines = ["Checkpoint Summary", divider, f"  {'Pipeline':<24} {'Latest Checkpoint':<20} {'Duration'}", divider]
    for name in sorted(names):
        tracker = registry.tracker(name)
        latest = tracker.latest()
        if latest is None:
            lines.append(f"  {name:<24} {'—':<20} —")
        else:
            dur = _fmt_duration(latest.duration_ms)
            lines.append(f"  {name:<24} {latest.name:<20} {dur}")
    lines.append(divider)
    return "\n".join(lines)


def print_checkpoints(tracker: CheckpointTracker, n: int = 10) -> None:
    print(render_checkpoint_list(tracker, n))


def print_checkpoint_summary(registry: CheckpointRegistry) -> None:
    print(render_checkpoint_summary(registry))
