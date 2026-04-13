"""Render pipeline diffs to the terminal."""

from __future__ import annotations

from typing import List

from pipewatch.pipeline_diff import PipelineDiff


_RESET = "\033[0m"
_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_BOLD = "\033[1m"


def _delta_str(value: float, precision: int = 0) -> str:
    """Format a numeric delta with a leading sign and optional colour."""
    if value > 0:
        formatted = f"+{value:.{precision}f}"
        return f"{_RED}{formatted}{_RESET}"
    if value < 0:
        formatted = f"{value:.{precision}f}"
        return f"{_GREEN}{formatted}{_RESET}"
    return f"{'±0':>4}"


def _status_change_str(diff: PipelineDiff) -> str:
    if not diff.status_changed:
        return ""
    prev = diff.prev_status or "new"
    curr = diff.curr_status or "?"
    colour = _RED if diff.is_degraded else _GREEN
    return f" {colour}({prev} → {curr}){_RESET}"


def render_diff_table(diffs: List[PipelineDiff]) -> str:
    """Return a formatted table string summarising pipeline diffs."""
    if not diffs:
        return "No pipeline changes to display.\n"

    header = (
        f"{_BOLD}{'Pipeline':<24} {'Δ Success':>10} {'Δ Failure':>10}"
        f" {'Δ Err%':>8}  Status{_RESET}"
    )
    sep = "-" * 70
    rows = [header, sep]

    for d in sorted(diffs, key=lambda x: x.name):
        err_delta = _delta_str(d.error_rate_delta * 100, precision=1)
        rows.append(
            f"{d.name:<24}"
            f" {_delta_str(d.success_delta):>10}"
            f" {_delta_str(d.failure_delta):>10}"
            f" {err_delta:>8}"
            f"{_status_change_str(d)}"
        )

    rows.append(sep)
    degraded = sum(1 for d in diffs if d.is_degraded)
    improved = sum(1 for d in diffs if d.is_improved)
    rows.append(
        f"  {_RED}{degraded} degraded{_RESET}  "
        f"{_GREEN}{improved} improved{_RESET}  "
        f"{len(diffs)} total"
    )
    return "\n".join(rows) + "\n"


def print_diff(diffs: List[PipelineDiff]) -> None:  # pragma: no cover
    print(render_diff_table(diffs))
