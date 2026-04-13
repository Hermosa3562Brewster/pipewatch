"""Render pipeline health reports to the terminal."""

from typing import Sequence

from pipewatch.pipeline_health import HealthReport

_GRADE_COLOR = {
    "A": "\033[92m",   # green
    "B": "\033[96m",   # cyan
    "C": "\033[93m",   # yellow
    "D": "\033[33m",   # orange-ish
    "F": "\033[91m",   # red
}
_RESET = "\033[0m"


def _colored_grade(grade: str) -> str:
    color = _GRADE_COLOR.get(grade, "")
    return f"{color}{grade}{_RESET}"


def render_health_table(reports: Sequence[HealthReport]) -> str:
    """Return a formatted table of health reports."""
    if not reports:
        return "No pipeline health data available."

    header = f"{'Pipeline':<24} {'Grade':>5}  {'Score':>6}  {'Error Rate':>10}  {'Runs':>6}  Status"
    sep = "-" * len(header)
    rows = [header, sep]

    for r in sorted(reports, key=lambda x: x.score):
        grade_str = _colored_grade(r.grade)
        note_suffix = f"  ({r.note})" if r.note else ""
        rows.append(
            f"{r.name:<24} {grade_str:>5}  {r.score:>6.2%}  "
            f"{r.error_rate:>10.2%}  {r.total_runs:>6}  {r.status}{note_suffix}"
        )

    return "\n".join(rows)


def render_health_summary(reports: Sequence[HealthReport]) -> str:
    """Return a one-line summary across all pipelines."""
    if not reports:
        return "Health summary: no data."

    healthy = sum(1 for r in reports if r.status == "healthy")
    degraded = sum(1 for r in reports if r.status == "degraded")
    critical = sum(1 for r in reports if r.status == "critical")
    unknown = sum(1 for r in reports if r.status == "unknown")
    avg_score = sum(r.score for r in reports) / len(reports)

    return (
        f"Health summary — "
        f"healthy: {healthy}  degraded: {degraded}  "
        f"critical: {critical}  unknown: {unknown}  "
        f"avg score: {avg_score:.2%}"
    )


def print_health(reports: Sequence[HealthReport]) -> None:
    print(render_health_summary(reports))
    print()
    print(render_health_table(reports))
