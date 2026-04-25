"""Display helpers for pipeline fingerprint tracking."""
from __future__ import annotations

from typing import Dict, List

from pipewatch.pipeline_fingerprint import FingerprintRecord, FingerprintTracker


def _changed_badge(changed: bool) -> str:
    return "\033[31m CHANGED \033[0m" if changed else "\033[32m STABLE  \033[0m"


def render_fingerprint_table(tracker: FingerprintTracker, reference: Dict[str, dict]) -> str:
    """Render a table comparing latest fingerprint against a reference payload."""
    pipelines = tracker.all_pipelines()
    if not pipelines:
        return "No fingerprint data recorded."

    lines = [
        f"{'Pipeline':<30} {'Fingerprint':<12} {'Changed':<10} {'Recorded At'}",
        "-" * 75,
    ]
    for name in sorted(pipelines):
        latest = tracker.latest(name)
        if latest is None:
            continue
        ref = reference.get(name, {})
        changed = tracker.has_changed(name, ref) if ref else False
        short_fp = latest.fingerprint[:10] + "..."
        lines.append(
            f"{name:<30} {short_fp:<12} {_changed_badge(changed):<10} {latest.recorded_at}"
        )
    return "\n".join(lines)


def render_fingerprint_history(tracker: FingerprintTracker, pipeline: str, n: int = 5) -> str:
    records = tracker.history(pipeline, n)
    if not records:
        return f"No fingerprint history for '{pipeline}'."

    lines = [f"Fingerprint history for: {pipeline}", "-" * 55]
    for rec in reversed(records):
        lines.append(f"  {rec.fingerprint[:12]}...  {rec.recorded_at}")
    return "\n".join(lines)


def render_fingerprint_summary(tracker: FingerprintTracker) -> str:
    pipelines = tracker.all_pipelines()
    total = len(pipelines)
    lines = [
        "=== Fingerprint Summary ===",
        f"Tracked pipelines : {total}",
    ]
    if total:
        lines.append("Pipelines         : " + ", ".join(sorted(pipelines)))
    return "\n".join(lines)


def print_fingerprint(tracker: FingerprintTracker, reference: Dict[str, dict]) -> None:
    print(render_fingerprint_summary(tracker))
    print()
    print(render_fingerprint_table(tracker, reference))
