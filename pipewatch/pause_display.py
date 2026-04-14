"""Display helpers for pipeline pause state."""
from __future__ import annotations

from typing import List

from pipewatch.pipeline_pause import PauseManager, PauseRecord


def _fmt_dt(dt) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "ongoing"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def render_pause_table(manager: PauseManager, pipelines: List[str]) -> str:
    lines = [f"{'Pipeline':<24} {'Status':<10} {'Paused At':<22} {'Duration':<14} Reason"]
    lines.append("-" * 90)
    for name in sorted(pipelines):
        history = manager.history(name)
        if not history:
            lines.append(f"{name:<24} {'active':<10} {'—':<22} {'—':<14} —")
            continue
        rec = history[-1]
        status = "PAUSED" if rec.is_active() else "active"
        paused_at = _fmt_dt(rec.paused_at)
        duration = _fmt_duration(rec.duration_seconds())
        reason = rec.reason or "—"
        lines.append(f"{name:<24} {status:<10} {paused_at:<22} {duration:<14} {reason}")
    return "\n".join(lines)


def render_pause_summary(manager: PauseManager) -> str:
    paused = manager.all_paused()
    count = len(paused)
    lines = ["=== Pause Summary ==="]
    lines.append(f"Currently paused: {count}")
    if paused:
        for name in sorted(paused):
            rec = manager._active_record(name)
            reason = f" — {rec.reason}" if rec and rec.reason else ""
            lines.append(f"  • {name}{reason}")
    return "\n".join(lines)


def print_pause_status(manager: PauseManager, pipelines: List[str]) -> None:
    print(render_pause_summary(manager))
    print()
    print(render_pause_table(manager, pipelines))
