"""Display helpers for pipeline audit logs."""
from __future__ import annotations

from typing import List, Optional

from pipewatch.pipeline_audit import AuditEntry, AuditLog

_ACTION_COLORS = {
    "created": "\033[32m",
    "updated": "\033[34m",
    "deleted": "\033[31m",
    "enabled": "\033[32m",
    "disabled": "\033[33m",
    "reset": "\033[35m",
}
_RESET = "\033[0m"


def _colored_action(action: str) -> str:
    color = _ACTION_COLORS.get(action, "")
    return f"{color}{action.upper():<8}{_RESET}"


def render_audit_table(entries: List[AuditEntry], title: str = "Audit Log") -> str:
    if not entries:
        return f"--- {title} ---\nNo audit entries found.\n"
    lines = [f"--- {title} ---"]
    header = f"  {'Timestamp':<22} {'Action':<10} {'Pipeline':<20} {'Actor':<15} Detail"
    lines.append(header)
    lines.append("-" * 85)
    for e in entries:
        ts = e.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        action_str = _colored_action(e.action)
        lines.append(f"  {ts:<22} {action_str} {e.pipeline:<20} {e.actor:<15} {e.detail}")
    return "\n".join(lines) + "\n"


def render_audit_for_pipeline(log: AuditLog, pipeline: str, last_n: Optional[int] = None) -> str:
    entries = log.get(pipeline, last_n=last_n)
    return render_audit_table(entries, title=f"Audit Log: {pipeline}")


def render_audit_summary(log: AuditLog) -> str:
    pipelines = log.pipelines()
    if not pipelines:
        return "--- Audit Summary ---\nNo pipelines audited.\n"
    lines = ["--- Audit Summary ---"]
    lines.append(f"  {'Pipeline':<25} {'Total Entries':<15} {'Last Action':<12} Last Actor")
    lines.append("-" * 70)
    for name in sorted(pipelines):
        entries = log.get(name)
        total = len(entries)
        last = entries[-1] if entries else None
        last_action = last.action if last else "-"
        last_actor = last.actor if last else "-"
        lines.append(f"  {name:<25} {total:<15} {last_action:<12} {last_actor}")
    return "\n".join(lines) + "\n"


def print_audit(log: AuditLog, pipeline: Optional[str] = None, last_n: Optional[int] = None) -> None:
    if pipeline:
        print(render_audit_for_pipeline(log, pipeline, last_n=last_n))
    else:
        all_entries = log.all_entries()
        if last_n is not None:
            all_entries = all_entries[-last_n:]
        print(render_audit_table(all_entries))
    print(render_audit_summary(log))
