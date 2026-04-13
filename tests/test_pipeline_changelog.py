"""Tests for pipeline_changelog and changelog_display."""

import pytest
from pipewatch.pipeline_changelog import ChangeEntry, Changelog, VALID_CHANGE_TYPES
from pipewatch.changelog_display import (
    render_changelog_for_pipeline,
    render_changelog_table,
    render_changelog_summary,
)


# ---------------------------------------------------------------------------
# ChangeEntry
# ---------------------------------------------------------------------------

def test_to_dict_keys():
    entry = ChangeEntry(pipeline="p1", change_type="status_change", description="went idle")
    keys = entry.to_dict().keys()
    assert {"pipeline", "change_type", "description", "timestamp", "metadata"} == set(keys)


def test_to_dict_values():
    entry = ChangeEntry(pipeline="p1", change_type="alert_fired", description="err high",
                        metadata={"rule": "err_rate"})
    d = entry.to_dict()
    assert d["pipeline"] == "p1"
    assert d["change_type"] == "alert_fired"
    assert d["metadata"] == {"rule": "err_rate"}


def test_from_dict_roundtrip():
    entry = ChangeEntry(pipeline="p2", change_type="config_update", description="interval changed")
    restored = ChangeEntry.from_dict(entry.to_dict())
    assert restored.pipeline == entry.pipeline
    assert restored.change_type == entry.change_type
    assert restored.description == entry.description
    assert restored.timestamp == entry.timestamp


def test_summary_contains_pipeline_and_type():
    entry = ChangeEntry(pipeline="etl", change_type="note_added", description="routine check")
    s = entry.summary()
    assert "etl" in s
    assert "note_added" in s


# ---------------------------------------------------------------------------
# Changelog
# ---------------------------------------------------------------------------

@pytest.fixture
def changelog():
    return Changelog(max_per_pipeline=5)


def test_record_returns_entry(changelog):
    e = changelog.record("p1", "status_change", "started")
    assert isinstance(e, ChangeEntry)
    assert e.pipeline == "p1"


def test_get_returns_recorded_entries(changelog):
    changelog.record("p1", "status_change", "started")
    changelog.record("p1", "alert_fired", "error spike")
    entries = changelog.get("p1")
    assert len(entries) == 2


def test_get_last_n(changelog):
    for i in range(4):
        changelog.record("p1", "status_change", f"change {i}")
    assert len(changelog.get("p1", last_n=2)) == 2


def test_max_per_pipeline_cap(changelog):
    for i in range(8):
        changelog.record("p1", "config_update", f"update {i}")
    assert len(changelog.get("p1")) == 5


def test_invalid_change_type_raises(changelog):
    with pytest.raises(ValueError, match="Unknown change_type"):
        changelog.record("p1", "explode", "bad type")


def test_all_entries_sorted_by_time(changelog):
    changelog.record("p1", "status_change", "a")
    changelog.record("p2", "alert_fired", "b")
    changelog.record("p1", "note_added", "c")
    all_e = changelog.all_entries()
    timestamps = [e.timestamp for e in all_e]
    assert timestamps == sorted(timestamps)


def test_pipelines_returns_known_names(changelog):
    changelog.record("alpha", "status_change", "x")
    changelog.record("beta", "config_update", "y")
    assert set(changelog.pipelines()) == {"alpha", "beta"}


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def test_render_for_pipeline_contains_name(changelog):
    changelog.record("pipe_a", "status_change", "running")
    out = render_changelog_for_pipeline(changelog, "pipe_a")
    assert "pipe_a" in out


def test_render_for_pipeline_empty(changelog):
    out = render_changelog_for_pipeline(changelog, "ghost")
    assert "no changes" in out.lower()


def test_render_table_contains_entries(changelog):
    changelog.record("p1", "alert_fired", "high error rate")
    out = render_changelog_table(changelog)
    assert "p1" in out
    assert "high error rate" in out


def test_render_summary_shows_total(changelog):
    changelog.record("p1", "status_change", "a")
    changelog.record("p2", "note_added", "b")
    out = render_changelog_summary(changelog)
    assert "2" in out
    assert "Pipelines" in out
