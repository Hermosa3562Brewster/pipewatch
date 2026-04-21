"""Tests for pipewatch.heartbeat_display."""
from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_heartbeat import HeartbeatTracker
from pipewatch.heartbeat_display import (
    render_heartbeat_table,
    render_heartbeat_summary,
)

_NOW = datetime(2024, 6, 1, 12, 0, 0)


@pytest.fixture
def tracker():
    t = HeartbeatTracker()
    t.configure("pipe_a", interval_seconds=60)
    t.configure("pipe_b", interval_seconds=120)
    t.beat("pipe_a", at=_NOW)
    t.beat("pipe_b", at=_NOW - timedelta(seconds=200))
    return t


def test_render_table_empty():
    result = render_heartbeat_table([])
    assert "No heartbeat" in result


def test_render_table_contains_pipeline_names(tracker):
    statuses = tracker.check_all(now=_NOW + timedelta(seconds=10))
    result = render_heartbeat_table(statuses)
    assert "pipe_a" in result
    assert "pipe_b" in result


def test_render_table_contains_header(tracker):
    statuses = tracker.check_all(now=_NOW + timedelta(seconds=10))
    result = render_heartbeat_table(statuses)
    assert "Pipeline" in result
    assert "State" in result


def test_render_table_shows_missed(tracker):
    statuses = tracker.check_all(now=_NOW + timedelta(seconds=10))
    result = render_heartbeat_table(statuses)
    assert "MISSED" in result


def test_render_table_shows_ok(tracker):
    statuses = tracker.check_all(now=_NOW + timedelta(seconds=10))
    result = render_heartbeat_table(statuses)
    assert "OK" in result


def test_render_summary_empty():
    result = render_heartbeat_summary([])
    assert "No heartbeat" in result


def test_render_summary_contains_header(tracker):
    statuses = tracker.check_all(now=_NOW + timedelta(seconds=10))
    result = render_heartbeat_summary(statuses)
    assert "Heartbeat Summary" in result


def test_render_summary_shows_counts(tracker):
    statuses = tracker.check_all(now=_NOW + timedelta(seconds=10))
    result = render_heartbeat_summary(statuses)
    assert "Monitored" in result
    assert "Missed" in result
    assert "Healthy" in result


def test_render_summary_lists_missed_pipelines(tracker):
    statuses = tracker.check_all(now=_NOW + timedelta(seconds=10))
    result = render_heartbeat_summary(statuses)
    assert "pipe_b" in result
