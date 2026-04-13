"""Display helpers for pipeline retry tracking."""
from __future__ import annotations

from typing import Dict

from pipewatch.pipeline_retry import RetryTracker


def _fmt_rate(rate: float) -> str:
    return f"{rate * 100:.1f}%"


def render_retry_table(tracker: RetryTracker) -> str:
    names = tracker.all_pipeline_names()
    if not names:
        return "No retry data available."

    header = f"{'Pipeline':<30} {'Retries':>8} {'Max Attempt':>12} {'Success Rate':>13}"
    sep = "-" * len(header)
    rows = [header, sep]

    for name in sorted(names):
        total = tracker.total_retries(name)
        max_att = tracker.max_attempt(name)
        rate = _fmt_rate(tracker.success_rate(name))
        rows.append(f"{name:<30} {total:>8} {max_att:>12} {rate:>13}")

    return "\n".join(rows)


def render_retry_detail(tracker: RetryTracker, pipeline_name: str, last_n: int = 10) -> str:
    records = tracker.get(pipeline_name, last_n=last_n)
    if not records:
        return f"No retry records for '{pipeline_name}'."

    lines = [f"Retry history for '{pipeline_name}' (last {last_n}):", ""]
    for rec in records:
        status = "OK" if rec.succeeded else "FAIL"
        ts = rec.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        reason = f" | {rec.reason}" if rec.reason else ""
        lines.append(f"  [{ts}] attempt={rec.attempt} status={status}{reason}")

    return "\n".join(lines)


def render_retry_summary(tracker: RetryTracker) -> str:
    names = tracker.all_pipeline_names()
    total = sum(tracker.total_retries(n) for n in names)
    lines = [
        "=== Retry Summary ===",
        f"Pipelines with retries : {len(names)}",
        f"Total retry records    : {total}",
    ]
    if names:
        worst = max(names, key=lambda n: tracker.total_retries(n))
        lines.append(f"Most retried pipeline  : {worst} ({tracker.total_retries(worst)} retries)")
    return "\n".join(lines)


def print_retry(tracker: RetryTracker) -> None:
    print(render_retry_summary(tracker))
    print()
    print(render_retry_table(tracker))
