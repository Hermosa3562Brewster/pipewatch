"""Display helpers for pipeline runbook entries."""

from __future__ import annotations

from typing import List

from pipewatch.pipeline_runbook import Runbook, RunbookEntry


def _format_steps(steps: List[str], indent: int = 4) -> str:
    pad = " " * indent
    return "\n".join(f"{pad}{i + 1}. {s}" for i, s in enumerate(steps))


def render_entry(entry: RunbookEntry) -> str:
    """Render a single RunbookEntry as a formatted multi-line string."""
    lines = [
        f"  Title     : {entry.title}",
        f"  Condition : {entry.condition}",
        f"  Author    : {entry.author}",
        f"  Created   : {entry.created_at}",
        "  Steps:",
        _format_steps(entry.steps),
    ]
    return "\n".join(lines)


def render_runbook_for_pipeline(runbook: Runbook, pipeline: str) -> str:
    """Render all runbook entries for a specific pipeline."""
    entries = runbook.get(pipeline)
    lines = [f"Runbook — {pipeline}"]
    lines.append("-" * 40)
    if not entries:
        lines.append("  (no entries)")
    else:
        for entry in entries:
            lines.append(render_entry(entry))
            lines.append("")
    return "\n".join(lines)


def render_runbook_table(runbook: Runbook) -> str:
    """Render all runbook entries as a compact table."""
    pipelines = runbook.all_pipelines()
    lines = [f"{'Pipeline':<20} {'Title':<25} {'Condition':<30} Author"]
    lines.append("-" * 85)
    if not pipelines:
        lines.append("  (no runbook entries)")
    else:
        for name in pipelines:
            for e in runbook.get(name):
                lines.append(
                    f"{e.pipeline:<20} {e.title:<25} {e.condition:<30} {e.author}"
                )
    return "\n".join(lines)


def render_runbook_summary(runbook: Runbook) -> str:
    """Render a one-line summary of the runbook's entry and pipeline counts."""
    total = runbook.total_entries()
    n_pipes = len(runbook.all_pipelines())
    return (
        f"Runbook Summary: {total} entr{'y' if total == 1 else 'ies'} "
        f"across {n_pipes} pipeline{'s' if n_pipes != 1 else ''}"
    )


def print_runbook(runbook: Runbook) -> None:
    """Print the full runbook table followed by a summary line."""
    print(render_runbook_table(runbook))
    print()
    print(render_runbook_summary(runbook))
