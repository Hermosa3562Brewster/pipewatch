"""Rich-text display helpers for pipeline scorecards."""
from __future__ import annotations

from typing import Dict

from pipewatch.pipeline_scorecard import ScorecardReport

_GRADE_COLOURS = {
    "A": "\033[92m",   # green
    "B": "\033[94m",   # blue
    "C": "\033[93m",   # yellow
    "D": "\033[33m",   # dark yellow
    "F": "\033[91m",   # red
}
_RESET = "\033[0m"


def _colored_grade(grade: str) -> str:
    colour = _GRADE_COLOURS.get(grade, "")
    return f"{colour}{grade}{_RESET}" if colour else grade


def render_scorecard_table(reports: Dict[str, ScorecardReport]) -> str:
    if not reports:
        return "No scorecard data available."
    header = f"{'Pipeline':<24} {'Score':>7} {'Grade':>6}"
    sep = "-" * len(header)
    rows = [header, sep]
    for name, report in sorted(reports.items()):
        score_str = f"{report.weighted_score:6.1f}"
        grade_str = _colored_grade(report.grade)
        rows.append(f"{name:<24} {score_str} {grade_str:>6}")
    return "\n".join(rows)


def render_scorecard_detail(report: ScorecardReport) -> str:
    lines = [
        f"Scorecard: {report.pipeline}",
        f"  Weighted Score : {report.weighted_score:.1f}",
        f"  Grade          : {_colored_grade(report.grade)}",
        "  Dimensions:",
    ]
    for dim in report.dimensions:
        note = f"  [{dim.note}]" if dim.note else ""
        lines.append(
            f"    {dim.name:<20} score={dim.score:5.1f}  weight={dim.weight:.1f}{note}"
        )
    return "\n".join(lines)


def render_scorecard_summary(reports: Dict[str, ScorecardReport]) -> str:
    if not reports:
        return "No scorecard data."
    scores = [r.weighted_score for r in reports.values()]
    avg = sum(scores) / len(scores)
    best = max(reports.values(), key=lambda r: r.weighted_score)
    worst = min(reports.values(), key=lambda r: r.weighted_score)
    lines = [
        "=== Scorecard Summary ===",
        f"  Pipelines : {len(reports)}",
        f"  Avg Score : {avg:.1f}",
        f"  Best      : {best.pipeline} ({best.weighted_score:.1f} / {best.grade})",
        f"  Worst     : {worst.pipeline} ({worst.weighted_score:.1f} / {worst.grade})",
    ]
    return "\n".join(lines)


def print_scorecard(reports: Dict[str, ScorecardReport]) -> None:
    print(render_scorecard_summary(reports))
    print()
    print(render_scorecard_table(reports))
