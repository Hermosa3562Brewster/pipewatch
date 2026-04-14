"""Display helpers for the dead-letter queue."""
from __future__ import annotations

from typing import Optional

from pipewatch.pipeline_deadletter import DeadLetterQueue


_HEADER = "{:<22} {:>8} {:>10} {}"
_ROW = "{:<22} {:>8} {:>10} {}"


def render_deadletter_table(queue: DeadLetterQueue, pipeline: Optional[str] = None) -> str:
    pipelines = [pipeline] if pipeline else queue.all_pipelines()
    if not pipelines:
        return "No dead-letter entries."

    lines = [_HEADER.format("Pipeline", "Total", "Retryable", "Latest Reason")]
    lines.append("-" * 70)
    for name in sorted(pipelines):
        entries = queue.get(name)
        if not entries:
            continue
        retryable = sum(1 for e in entries if e.retryable)
        latest = entries[-1].reason[:30] if entries else ""
        lines.append(_ROW.format(name[:22], len(entries), retryable, latest))
    return "\n".join(lines)


def render_deadletter_detail(queue: DeadLetterQueue, pipeline: str) -> str:
    entries = queue.get(pipeline)
    if not entries:
        return f"No dead-letter entries for '{pipeline}'."
    lines = [f"Dead-letter entries for '{pipeline}' ({len(entries)} total):", ""]
    for i, e in enumerate(entries, 1):
        lines.append(f"  [{i}] {e.summary()}")
        lines.append(f"       payload: {e.payload[:60]}")
    return "\n".join(lines)


def render_deadletter_summary(queue: DeadLetterQueue) -> str:
    total = queue.total_count()
    counts = queue.counts()
    retryable = sum(
        sum(1 for e in queue.get(p) if e.retryable) for p in counts
    )
    lines = [
        "=== Dead-Letter Summary ===",
        f"  Pipelines with entries : {len(counts)}",
        f"  Total entries          : {total}",
        f"  Retryable              : {retryable}",
        f"  Non-retryable          : {total - retryable}",
    ]
    return "\n".join(lines)


def print_deadletter(queue: DeadLetterQueue, pipeline: Optional[str] = None) -> None:
    print(render_deadletter_summary(queue))
    print()
    print(render_deadletter_table(queue, pipeline=pipeline))
