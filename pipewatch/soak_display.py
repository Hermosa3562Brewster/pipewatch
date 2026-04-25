"""Display helpers for soak test reports."""

from __future__ import annotations

from typing import Dict

from pipewatch.pipeline_soak import SoakReport, SoakTracker


_PASS = "\033[32mPASS\033[0m"
_FAIL = "\033[31mFAIL\033[0m"


def _pass_badge(passed: bool) -> str:
    return _PASS if passed else _FAIL


def render_soak_table(reports: Dict[str, SoakReport]) -> str:
    if not reports:
        return "No soak configurations registered."
    header = f"{'Pipeline':<22} {'Status':<10} {'Runs':>6} {'Failures':>9} {'Error%':>8} {'Window(s)':>10}"
    sep = "-" * len(header)
    rows = [header, sep]
    for name, r in sorted(reports.items()):
        pct = f"{r.error_rate * 100:.1f}%"
        rows.append(
            f"{name:<22} {_pass_badge(r.passed):<19} {r.runs_in_window:>6} "
            f"{r.failures_in_window:>9} {pct:>8} {r.required_seconds:>10.0f}"
        )
    return "\n".join(rows)


def render_soak_summary(reports: Dict[str, SoakReport]) -> str:
    if not reports:
        return "No soak data available."
    total = len(reports)
    passed = sum(1 for r in reports.values() if r.passed)
    failed = total - passed
    lines = [
        "=== Soak Test Summary ===",
        f"Total : {total}",
        f"Passed: {passed}",
        f"Failed: {failed}",
    ]
    if failed:
        lines.append("Failing pipelines:")
        for name, r in sorted(reports.items()):
            if not r.passed:
                lines.append(f"  - {r.summary()}")
    return "\n".join(lines)


def print_soak(tracker: SoakTracker) -> None:
    reports = tracker.check_all()
    print(render_soak_table(reports))
    print()
    print(render_soak_summary(reports))
