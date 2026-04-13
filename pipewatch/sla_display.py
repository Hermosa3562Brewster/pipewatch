"""Rendering helpers for SLA reports."""
from __future__ import annotations

from typing import Dict, List

from pipewatch.pipeline_sla import SLAReport

_GRADE_COLORS = {
    "OK": "\033[92m",      # green
    "WARN": "\033[93m",    # yellow
    "BREACH": "\033[91m",  # red
}
_RESET = "\033[0m"


def _colored_grade(grade: str) -> str:
    color = _GRADE_COLORS.get(grade, "")
    return f"{color}{grade}{_RESET}"


def render_sla_table(reports: List[SLAReport]) -> str:
    """Render a table of SLA reports."""
    if not reports:
        return "No SLA data available."

    header = f"{'Pipeline':<24} {'Grade':<8} {'Error Rate':>12} {'Avg Dur(s)':>12} {'Runs':>6}"
    sep = "-" * len(header)
    rows = [header, sep]
    for r in sorted(reports, key=lambda x: x.pipeline_name):
        grade_str = _colored_grade(r.grade)
        rows.append(
            f"{r.pipeline_name:<24} {grade_str:<8} {r.error_rate:>11.2%} "
            f"{r.avg_duration:>12.1f} {r.total_runs:>6}"
        )
    return "\n".join(rows)


def render_sla_violations(reports: List[SLAReport]) -> str:
    """Render violation details for non-compliant pipelines."""
    breached = [r for r in reports if not r.compliant]
    if not breached:
        return "All pipelines are within SLA."

    lines = ["SLA Violations:", "-" * 40]
    for r in breached:
        lines.append(f"  [{r.grade}] {r.pipeline_name}")
        for v in r.violations:
            lines.append(f"      - {v}")
    return "\n".join(lines)


def render_sla_summary(reports: List[SLAReport]) -> str:
    """Render a one-line summary of SLA compliance across all pipelines."""
    total = len(reports)
    compliant = sum(1 for r in reports if r.compliant)
    breached = total - compliant
    pct = (compliant / total * 100) if total else 0.0
    return (
        f"SLA Summary: {compliant}/{total} pipelines compliant "
        f"({pct:.1f}%)  |  {breached} breach(es)"
    )


def print_sla(reports: List[SLAReport]) -> None:
    """Print full SLA dashboard to stdout."""
    print(render_sla_summary(reports))
    print()
    print(render_sla_table(reports))
    print()
    print(render_sla_violations(reports))
