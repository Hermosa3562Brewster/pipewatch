"""Tests for pipeline_events and event_display."""

import pytest
from datetime import datetime
from pipewatch.pipeline_events import PipelineEvent, EventLog, EVENT_TYPES
from pipewatch.event_display import render_event_list, render_event_summary, _colored_type


# --- PipelineEvent ---

def test_event_to_dict_keys():
    e = PipelineEvent(pipeline="etl", event_type="started", message="boot")
    d = e.to_dict()
    assert set(d.keys()) == {"pipeline", "event_type", "timestamp", "message"}


def test_event_to_dict_values():
    e = PipelineEvent(pipeline="etl", event_type="completed", message="done")
    d = e.to_dict()
    assert d["pipeline"] == "etl"
    assert d["event_type"] == "completed"
    assert d["message"] == "done"


def test_event_from_dict_roundtrip():
    e = PipelineEvent(pipeline="pipe", event_type="failed", message="oops")
    restored = PipelineEvent.from_dict(e.to_dict())
    assert restored.pipeline == e.pipeline
    assert restored.event_type == e.event_type
    assert restored.message == e.message
    assert isinstance(restored.timestamp, datetime)


# --- EventLog ---

def test_record_valid_event():
    log = EventLog()
    e = log.record("pipe-a", "started")
    assert e.pipeline == "pipe-a"
    assert e.event_type == "started"
    assert len(log) == 1


def test_record_invalid_event_type_raises():
    log = EventLog()
    with pytest.raises(ValueError, match="Unknown event type"):
        log.record("pipe", "exploded")


def test_events_for_filters_by_pipeline():
    log = EventLog()
    log.record("alpha", "started")
    log.record("beta", "failed")
    log.record("alpha", "completed")
    result = log.events_for("alpha")
    assert len(result) == 2
    assert all(e.pipeline == "alpha" for e in result)


def test_recent_returns_last_n():
    log = EventLog()
    for i in range(10):
        log.record("p", "started")
    assert len(log.recent(5)) == 5


def test_by_type_filters_correctly():
    log = EventLog()
    log.record("p", "started")
    log.record("p", "failed")
    log.record("p", "failed")
    assert len(log.by_type("failed")) == 2
    assert len(log.by_type("started")) == 1


def test_summary_counts_all_types():
    log = EventLog()
    log.record("p", "started")
    log.record("p", "completed")
    log.record("p", "failed")
    s = log.summary()
    assert s["started"] == 1
    assert s["completed"] == 1
    assert s["failed"] == 1
    assert s["stalled"] == 0


def test_max_events_enforced():
    log = EventLog(max_events=5)
    for _ in range(10):
        log.record("p", "started")
    assert len(log) == 5


def test_clear_empties_log():
    log = EventLog()
    log.record("p", "started")
    log.clear()
    assert len(log) == 0


# --- event_display ---

def test_render_event_list_empty():
    result = render_event_list([])
    assert "no events" in result


def test_render_event_list_contains_pipeline_name():
    log = EventLog()
    log.record("my-pipeline", "completed", "all good")
    result = render_event_list(log.recent())
    assert "my-pipeline" in result


def test_render_event_summary_shows_total():
    log = EventLog()
    log.record("p", "started")
    log.record("p", "failed")
    result = render_event_summary(log)
    assert "Total events" in result
    assert "2" in result


def test_colored_type_returns_string():
    for t in EVENT_TYPES:
        out = _colored_type(t)
        assert isinstance(out, str)
        assert t.upper() in out
