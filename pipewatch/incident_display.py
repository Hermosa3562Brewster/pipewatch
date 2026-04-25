"""Render incident tables and summaries for the CLI."""
from __future__ import annotations

from typing import List

from pipewatch.pipeline_incident import Incident, IncidentLog

_SEV_COLORS = {
    "critical": "\033[91m",
    "high": "\033[93m",
    "medium": "\033[94m",
    "low": "\033[92m",
}
_RESET = "\033[0m"


def _colored_severity(severity: str) -> str:
    color = _SEV_COLORS.get(severity.lower(), "")
    return f"{color}{severity.upper()}{_RESET}" if color else severity.upper()


def _status_badge(status: str) -> str:
    if status == "open":
        return "\033[91m● OPEN\033[0m"
    return "\033[92m✔ RESOLVED\033[0m"


def render_incident_table(incidents: List[Incident]) -> str:
    if not incidents:
        return "No incidents found."
    header = f"{'ID':<14} {'Pipeline':<20} {'Severity':<10} {'Status':<18} {'Description'}"
    sep = "-" * 80
    rows = [header, sep]
    for inc in incidents:
        sev = _colored_severity(inc.severity)
        badge = _status_badge(inc.status)
        rows.append(f"{inc.incident_id:<14} {inc.pipeline:<20} {sev:<10} {badge:<18} {inc.description}")
    return "\n".join(rows)


def render_incident_summary(log: IncidentLog) -> str:
    all_open = log.all_open()
    pipelines = log.all_pipelines()
    total = sum(len(log.get(p)) for p in pipelines)
    lines = [
        "=== Incident Summary ===",
        f"Total incidents : {total}",
        f"Open incidents  : {len(all_open)}",
        f"Pipelines       : {len(pipelines)}",
    ]
    if all_open:
        lines.append("\nOpen incidents:")
        for inc in all_open:
            lines.append(f"  {inc.incident_id}  [{inc.severity.upper()}] {inc.pipeline} — {inc.description}")
    return "\n".join(lines)


def print_incidents(log: IncidentLog, pipeline: str | None = None, status: str | None = None) -> None:
    if pipeline:
        incidents = log.get(pipeline, status=status)
    elif status == "open":
        incidents = log.all_open()
    else:
        incidents: list = []
        for p in log.all_pipelines():
            incidents.extend(log.get(p, status=status))
    print(render_incident_table(incidents))
    print()
    print(render_incident_summary(log))
