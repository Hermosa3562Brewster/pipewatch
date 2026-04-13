"""Render pipeline notes to the terminal."""

from __future__ import annotations

from typing import List, Optional

from pipewatch.pipeline_notes import NoteBook, PipelineNote


def _format_note(note: PipelineNote, index: int) -> str:
    ts = note.created_at.strftime("%Y-%m-%d %H:%M:%S")
    return f"  [{index:>2}] {ts}  ({note.author})  {note.message}"


def render_notes_for_pipeline(pipeline: str, notes: List[PipelineNote]) -> str:
    if not notes:
        return f"No notes for pipeline '{pipeline}'."
    lines = [f"Notes for '{pipeline}' ({len(notes)} total):"]
    for i, note in enumerate(notes, start=1):
        lines.append(_format_note(note, i))
    return "\n".join(lines)


def render_notes_summary(notebook: NoteBook) -> str:
    pipelines = notebook.all_pipelines()
    if not pipelines:
        return "No pipeline notes recorded."
    lines = ["Pipeline Notes Summary", "=" * 40]
    for name in sorted(pipelines):
        count = notebook.total_count(name)
        latest = notebook.get(name, last_n=1)
        snippet = latest[0].message[:40] + ("..." if len(latest[0].message) > 40 else "") if latest else ""
        lines.append(f"  {name:<25} {count:>3} note(s)   latest: {snippet}")
    return "\n".join(lines)


def print_notes(pipeline: str, notebook: NoteBook, last_n: Optional[int] = None) -> None:
    notes = notebook.get(pipeline, last_n=last_n)
    print(render_notes_for_pipeline(pipeline, notes))


def print_notes_summary(notebook: NoteBook) -> None:
    print(render_notes_summary(notebook))
