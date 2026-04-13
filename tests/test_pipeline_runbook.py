"""Tests for pipeline_runbook and runbook_display."""

import pytest

from pipewatch.pipeline_runbook import RunbookEntry, Runbook
from pipewatch.runbook_display import (
    render_entry,
    render_runbook_for_pipeline,
    render_runbook_table,
    render_runbook_summary,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def runbook() -> Runbook:
    return Runbook()


def _make_entry(pipeline: str = "etl_orders", title: str = "High Error Rate") -> RunbookEntry:
    return RunbookEntry(
        pipeline=pipeline,
        title=title,
        condition="error_rate > 0.1",
        steps=["Check source DB connectivity", "Restart the ingestion service"],
        author="alice",
    )


# ---------------------------------------------------------------------------
# RunbookEntry tests
# ---------------------------------------------------------------------------

def test_to_dict_keys():
    entry = _make_entry()
    d = entry.to_dict()
    assert set(d.keys()) == {"pipeline", "title", "condition", "steps", "author", "created_at"}


def test_to_dict_values():
    entry = _make_entry()
    d = entry.to_dict()
    assert d["pipeline"] == "etl_orders"
    assert d["title"] == "High Error Rate"
    assert d["steps"] == ["Check source DB connectivity", "Restart the ingestion service"]


def test_from_dict_roundtrip():
    entry = _make_entry()
    restored = RunbookEntry.from_dict(entry.to_dict())
    assert restored.pipeline == entry.pipeline
    assert restored.title == entry.title
    assert restored.steps == entry.steps
    assert restored.author == entry.author


def test_summary_contains_pipeline_and_title():
    entry = _make_entry()
    s = entry.summary()
    assert "etl_orders" in s
    assert "High Error Rate" in s


# ---------------------------------------------------------------------------
# Runbook tests
# ---------------------------------------------------------------------------

def test_add_and_get(runbook):
    entry = _make_entry()
    runbook.add(entry)
    results = runbook.get("etl_orders")
    assert len(results) == 1
    assert results[0].title == "High Error Rate"


def test_get_unknown_pipeline_returns_empty(runbook):
    assert runbook.get("nonexistent") == []


def test_remove_entry(runbook):
    runbook.add(_make_entry())
    removed = runbook.remove("etl_orders", "High Error Rate")
    assert removed is True
    assert runbook.get("etl_orders") == []


def test_remove_nonexistent_returns_false(runbook):
    assert runbook.remove("etl_orders", "Ghost Title") is False


def test_all_pipelines_sorted(runbook):
    runbook.add(_make_entry("zzz_pipe"))
    runbook.add(_make_entry("aaa_pipe"))
    assert runbook.all_pipelines() == ["aaa_pipe", "zzz_pipe"]


def test_find_by_condition(runbook):
    runbook.add(_make_entry())
    runbook.add(RunbookEntry("other", "Slow", "latency > 30s", ["scale up"]))
    results = runbook.find_by_condition("error_rate")
    assert len(results) == 1
    assert results[0].title == "High Error Rate"


def test_total_entries(runbook):
    runbook.add(_make_entry("p1"))
    runbook.add(_make_entry("p2"))
    runbook.add(_make_entry("p1", title="Another"))
    assert runbook.total_entries() == 3


# ---------------------------------------------------------------------------
# Display tests
# ---------------------------------------------------------------------------

def test_render_entry_contains_title():
    entry = _make_entry()
    text = render_entry(entry)
    assert "High Error Rate" in text


def test_render_entry_lists_steps():
    entry = _make_entry()
    text = render_entry(entry)
    assert "Check source DB connectivity" in text


def test_render_for_pipeline_no_entries(runbook):
    text = render_runbook_for_pipeline(runbook, "missing")
    assert "(no entries)" in text


def test_render_for_pipeline_shows_entry(runbook):
    runbook.add(_make_entry())
    text = render_runbook_for_pipeline(runbook, "etl_orders")
    assert "High Error Rate" in text


def test_render_table_empty(runbook):
    text = render_runbook_table(runbook)
    assert "(no runbook entries)" in text


def test_render_table_contains_pipeline_name(runbook):
    runbook.add(_make_entry())
    text = render_runbook_table(runbook)
    assert "etl_orders" in text


def test_render_summary_counts(runbook):
    runbook.add(_make_entry("p1"))
    runbook.add(_make_entry("p2"))
    text = render_runbook_summary(runbook)
    assert "2 entries" in text
    assert "2 pipelines" in text
