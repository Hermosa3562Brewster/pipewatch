"""Display helpers for pipeline changelog entries."""

from __future__ import annotations

from typing import List, Optional

from pipewatch.pipeline_changelog import ChangeEntry, Changelog

_TYPE_COLORS = {
    "status_change": "\033[94m",     # blue
    "threshold_crossed": "\033[93m", # yellow
    "config_update": "\033[96m",     # cyan
    "note_added": "\033[92m",        # green
    "alert_fired": "\033[91m",       # red
}
_RESET = "\033[0m"


def _colored_type(change_type: str) -> str:
    color = _TYPE_COLORS.get(change_type, "")
    return f"{color}{change_type}{_RESET}" if color else change_type


def _format_entry(entry: ChangeEntry) -> str:
    ts = entry.timestamp.strftime("%Y-%m-%d %H:%M:%S")
    ctype = _colored_type(entry.change_type)
    return f"  {ts}  {ctype:<30}  {entry.description}"


def render_changelog_for_pipeline(changelog: Changelog, pipeline: str,
                                  last_n: Optional[int] = None) -> str:
    entries = changelog.get(pipeline, last_n=last_n)
    lines = [f"Changelog — {pipeline}", "-" * 60]
    if not entries:
        lines.append("  (no changes recorded)")
    else:
        lines.extend(_format_entry(e) for e in reversed(entries))
    return "\n".join(lines)


def render_changelog_table(changelog: Changelog, last_n: Optional[int] = 20) -> str:
    entries = changelog.all_entries(last_n=last_n)
    header = f"{'Timestamp':<22} {'Pipeline':<20} {'Type':<22} Description"
    sep = "-" * 80
    lines = ["Recent Changes", sep, header, sep]
    if not entries:
        lines.append("  (no changes recorded)")
    else:
        for e in reversed(entries):
            ts = e.timestamp.strftime("%Y-%m-%d %H:%M:%S")
            ctype = _colored_type(e.change_type)
            lines.append(f"{ts:<22} {e.pipeline:<20} {ctype:<30} {e.description}")
    return "\n".join(lines)


def render_changelog_summary(changelog: Changelog) -> str:
    all_e = changelog.all_entries()
    type_counts: dict = {}
    for e in all_e:
        type_counts[e.change_type] = type_counts.get(e.change_type, 0) + 1
    lines = ["Changelog Summary", "-" * 40,
             f"  Total entries : {len(all_e)}",
             f"  Pipelines     : {len(changelog.pipelines())}"]
    for ctype, count in sorted(type_counts.items()):
        lines.append(f"  {ctype:<22}: {count}")
    return "\n".join(lines)


def print_changelog(changelog: Changelog, pipeline: Optional[str] = None,
                    last_n: Optional[int] = 20) -> None:
    if pipeline:
        print(render_changelog_for_pipeline(changelog, pipeline, last_n=last_n))
    else:
        print(render_changelog_table(changelog, last_n=last_n))
    print()
    print(render_changelog_summary(changelog))
