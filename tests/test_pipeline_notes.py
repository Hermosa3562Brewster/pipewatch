"""Tests for pipeline notes and notes display."""

from __future__ import annotations

import pytest
from pipewatch.pipeline_notes import NoteBook, PipelineNote
from pipewatch.notes_display import render_notes_for_pipeline, render_notes_summary


@pytest.fixture
def notebook() -> NoteBook:
    return NoteBook(max_per_pipeline=5)


def test_add_note_returns_pipeline_note(notebook):
    note = notebook.add("etl_main", "Looks healthy", author="alice")
    assert isinstance(note, PipelineNote)
    assert note.pipeline == "etl_main"
    assert note.message == "Looks healthy"
    assert note.author == "alice"


def test_get_returns_added_notes(notebook):
    notebook.add("etl_main", "Note A")
    notebook.add("etl_main", "Note B")
    notes = notebook.get("etl_main")
    assert len(notes) == 2
    assert notes[0].message == "Note A"
    assert notes[1].message == "Note B"


def test_get_last_n(notebook):
    for i in range(4):
        notebook.add("pipe", f"msg {i}")
    notes = notebook.get("pipe", last_n=2)
    assert len(notes) == 2
    assert notes[-1].message == "msg 3"


def test_max_per_pipeline_cap(notebook):
    for i in range(7):
        notebook.add("pipe", f"msg {i}")
    notes = notebook.get("pipe")
    assert len(notes) == 5
    assert notes[0].message == "msg 2"  # oldest evicted


def test_get_unknown_pipeline_returns_empty(notebook):
    assert notebook.get("nonexistent") == []


def test_all_pipelines_lists_names(notebook):
    notebook.add("alpha", "note")
    notebook.add("beta", "note")
    pipelines = notebook.all_pipelines()
    assert "alpha" in pipelines
    assert "beta" in pipelines


def test_clear_removes_notes(notebook):
    notebook.add("pipe", "hello")
    notebook.clear("pipe")
    assert notebook.get("pipe") == []
    assert "pipe" not in notebook.all_pipelines()


def test_total_count(notebook):
    notebook.add("pipe", "a")
    notebook.add("pipe", "b")
    assert notebook.total_count("pipe") == 2


def test_note_to_dict_roundtrip():
    note = NoteBook().add("pipe", "test message", author="bob")
    d = note.to_dict()
    restored = PipelineNote.from_dict(d)
    assert restored.pipeline == note.pipeline
    assert restored.message == note.message
    assert restored.author == note.author
    assert restored.created_at == note.created_at


def test_render_notes_for_pipeline_empty():
    result = render_notes_for_pipeline("pipe", [])
    assert "No notes" in result
    assert "pipe" in result


def test_render_notes_for_pipeline_shows_messages(notebook):
    notebook.add("pipe", "First note", author="ops")
    notebook.add("pipe", "Second note")
    notes = notebook.get("pipe")
    result = render_notes_for_pipeline("pipe", notes)
    assert "First note" in result
    assert "Second note" in result
    assert "ops" in result


def test_render_notes_summary_no_notes():
    nb = NoteBook()
    result = render_notes_summary(nb)
    assert "No pipeline notes" in result


def test_render_notes_summary_shows_pipeline_names(notebook):
    notebook.add("alpha", "msg")
    notebook.add("beta", "another")
    result = render_notes_summary(notebook)
    assert "alpha" in result
    assert "beta" in result
