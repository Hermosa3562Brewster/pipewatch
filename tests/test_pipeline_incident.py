"""Tests for pipewatch.pipeline_incident."""
from __future__ import annotations

import pytest
from datetime import timezone

from pipewatch.pipeline_incident import Incident, IncidentLog


@pytest.fixture
def log():
    return IncidentLog()


def test_open_returns_incident(log):
    inc = log.open("etl_orders", "high", "Latency spike")
    assert isinstance(inc, Incident)


def test_open_sets_status_open(log):
    inc = log.open("etl_orders", "high", "Latency spike")
    assert inc.status == "open"


def test_open_sets_pipeline_and_severity(log):
    inc = log.open("etl_orders", "critical", "Data loss")
    assert inc.pipeline == "etl_orders"
    assert inc.severity == "critical"


def test_open_invalid_severity_raises(log):
    with pytest.raises(ValueError, match="Invalid severity"):
        log.open("etl_orders", "extreme", "Bad")


def test_resolve_returns_incident(log):
    inc = log.open("etl_orders", "low", "Minor issue")
    resolved = log.resolve(inc.incident_id)
    assert resolved is not None
    assert resolved.status == "resolved"


def test_resolve_sets_resolved_at(log):
    inc = log.open("etl_orders", "medium", "Timeout")
    resolved = log.resolve(inc.incident_id)
    assert resolved.resolved_at is not None
    assert resolved.resolved_at.tzinfo == timezone.utc


def test_resolve_unknown_id_returns_none(log):
    result = log.resolve("nonexistent")
    assert result is None


def test_resolve_stores_notes(log):
    inc = log.open("etl_orders", "low", "Slow")
    resolved = log.resolve(inc.incident_id, notes="Fixed upstream")
    assert resolved.notes == "Fixed upstream"


def test_get_returns_incidents_for_pipeline(log):
    log.open("pipe_a", "low", "issue 1")
    log.open("pipe_a", "high", "issue 2")
    log.open("pipe_b", "medium", "other")
    assert len(log.get("pipe_a")) == 2


def test_get_filters_by_status(log):
    inc = log.open("pipe_a", "low", "issue")
    log.resolve(inc.incident_id)
    log.open("pipe_a", "high", "another")
    open_only = log.get("pipe_a", status="open")
    assert len(open_only) == 1
    assert open_only[0].status == "open"


def test_all_open_returns_only_open(log):
    inc = log.open("pipe_a", "low", "x")
    log.open("pipe_b", "high", "y")
    log.resolve(inc.incident_id)
    open_list = log.all_open()
    assert all(i.status == "open" for i in open_list)
    assert len(open_list) == 1


def test_to_dict_keys(log):
    inc = log.open("pipe_a", "critical", "crash")
    d = inc.to_dict()
    for key in ("incident_id", "pipeline", "severity", "description", "status", "opened_at", "resolved_at", "notes"):
        assert key in d


def test_from_dict_roundtrip(log):
    inc = log.open("pipe_a", "medium", "slow")
    log.resolve(inc.incident_id)
    d = inc.to_dict()
    restored = Incident.from_dict(d)
    assert restored.incident_id == inc.incident_id
    assert restored.pipeline == inc.pipeline
    assert restored.status == "resolved"


def test_summary_contains_pipeline_and_severity(log):
    inc = log.open("pipe_a", "critical", "total failure")
    s = inc.summary()
    assert "pipe_a" in s
    assert "CRITICAL" in s


def test_duration_seconds_none_while_open(log):
    inc = log.open("pipe_a", "low", "slow")
    assert inc.duration_seconds() is None


def test_duration_seconds_after_resolve(log):
    inc = log.open("pipe_a", "low", "slow")
    log.resolve(inc.incident_id)
    assert inc.duration_seconds() is not None
    assert inc.duration_seconds() >= 0


def test_max_per_pipeline_cap():
    log = IncidentLog(max_per_pipeline=3)
    for i in range(5):
        log.open("pipe_a", "low", f"issue {i}")
    assert len(log.get("pipe_a")) == 3
