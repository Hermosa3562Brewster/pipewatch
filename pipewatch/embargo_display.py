"""Display helpers for pipeline embargo windows."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pipewatch.pipeline_embargo import EmbargoManager


def _active_badge(active: bool) -> str:
    return "\033[91m EMBARGOED \033[0m" if active else "\033[92m  OK  \033[0m"


def render_embargo_table(
    manager: EmbargoManager, at: Optional[datetime] = None
) -> str:
    pipelines = manager.all_pipelines()
    if not pipelines:
        return "No embargo windows configured."

    header = f"{'Pipeline':<22} {'Start':<8} {'End':<8} {'Status':<12} Reason"
    sep = "-" * 72
    rows = [header, sep]
    for name in sorted(pipelines):
        for w in manager.windows_for(name):
            badge = _active_badge(w.is_active(at)) if w.enabled else "  disabled  "
            rows.append(
                f"{name:<22} {w.start_time.isoformat():<8} "
                f"{w.end_time.isoformat():<8} {badge:<12} {w.reason}"
            )
    return "\n".join(rows)


def render_embargo_summary(manager: EmbargoManager, at: Optional[datetime] = None) -> str:
    pipelines = manager.all_pipelines()
    total = sum(len(manager.windows_for(p)) for p in pipelines)
    embargoed = [p for p in pipelines if manager.is_embargoed(p, at)]
    lines = [
        "=== Embargo Summary ===",
        f"Pipelines with windows : {len(pipelines)}",
        f"Total windows          : {total}",
        f"Currently embargoed    : {len(embargoed)}",
    ]
    if embargoed:
        lines.append("Embargoed: " + ", ".join(sorted(embargoed)))
    return "\n".join(lines)


def print_embargo(manager: EmbargoManager, at: Optional[datetime] = None) -> None:
    print(render_embargo_table(manager, at))
    print()
    print(render_embargo_summary(manager, at))
