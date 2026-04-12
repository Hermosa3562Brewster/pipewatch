"""Summarise pipeline run history for CLI display."""

from typing import List, Optional
from pipewatch.history import PipelineHistory, load_history, DEFAULT_HISTORY_FILE


def _trend_arrow(values: List[float]) -> str:
    """Return a simple trend indicator based on last two values."""
    if len(values) < 2:
        return "  "
    delta = values[-1] - values[-2]
    if delta > 0:
        return "↑ "
    if delta < 0:
        return "↓ "
    return "→ "


def format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "N/A"
    minutes, secs = divmod(int(seconds), 60)
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def render_history_summary(history: PipelineHistory, n: int = 5) -> str:
    """Return a formatted multi-line summary string for a pipeline's history."""
    runs = history.last_n_runs(n)
    if not runs:
        return f"[{history.pipeline_name}] No history recorded."

    lines = [f"  Pipeline : {history.pipeline_name}"]
    lines.append(f"  Total runs stored : {len(history.runs)}")
    lines.append(f"  Avg error rate    : {history.average_error_rate():.2f}%")
    lines.append(f"  Avg duration      : {format_duration(history.average_duration())}")
    lines.append(f"  Last {len(runs)} runs:")

    error_rates = [r.error_rate for r in runs]
    for i, run in enumerate(runs):
        arrow = _trend_arrow(error_rates[: i + 1])
        dur = format_duration(run.duration_seconds)
        lines.append(
            f"    {run.started_at[:10]}  status={run.status:<10} "
            f"err={run.error_rate:.1f}% {arrow} dur={dur}"
        )
    return "\n".join(lines)


def render_all_summaries(path: str = DEFAULT_HISTORY_FILE, n: int = 5) -> str:
    """Load history file and render summaries for every pipeline."""
    histories = load_history(path)
    if not histories:
        return "No pipeline history found."
    sections = []
    for name in sorted(histories):
        sections.append(render_history_summary(histories[name], n=n))
    separator = "\n" + "-" * 50 + "\n"
    return separator.join(sections)
