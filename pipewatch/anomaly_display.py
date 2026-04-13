"""Display helpers for pipeline anomaly detection results."""

from __future__ import annotations

from typing import List

from pipewatch.pipeline_anomaly import AnomalyResult

_SEVERITY_COLORS = {
    "high": "\033[91m",    # bright red
    "medium": "\033[93m",  # yellow
    "low": "\033[96m",     # cyan
}
_RESET = "\033[0m"


def _colored_severity(severity: str) -> str:
    color = _SEVERITY_COLORS.get(severity, "")
    return f"{color}{severity.upper():6}{_RESET}"


def render_anomaly_table(results: List[AnomalyResult]) -> str:
    """Render a table of anomaly results (all results, flagging anomalies)."""
    if not results:
        return "No anomaly data available."

    header = f"{'Pipeline':<20} {'Metric':<14} {'Value':>8} {'Mean':>8} {'StdDev':>8} {'Z':>6} {'Severity':<8} Anomaly"
    sep = "-" * len(header)
    rows = [header, sep]
    for r in results:
        flag = "\u26a0\ufe0f " if r.is_anomaly else "   "
        rows.append(
            f"{r.pipeline:<20} {r.metric:<14} {r.value:>8.3f} {r.mean:>8.3f} "
            f"{r.std_dev:>8.3f} {r.z_score:>6.2f} {_colored_severity(r.severity):<8} {flag}"
        )
    return "\n".join(rows)


def render_anomaly_summary(results: List[AnomalyResult]) -> str:
    """Render a short summary of detected anomalies."""
    anomalies = [r for r in results if r.is_anomaly]
    total = len(results)
    n = len(anomalies)
    lines = [f"Anomaly Summary: {n}/{total} pipeline metrics flagged"]
    if anomalies:
        lines.append("")
        for r in anomalies:
            lines.append(f"  {r.summary()}")
    return "\n".join(lines)


def print_anomalies(results: List[AnomalyResult]) -> None:
    """Print anomaly table and summary to stdout."""
    print(render_anomaly_table(results))
    print()
    print(render_anomaly_summary(results))
