"""Tests for pipewatch.pipeline_heartbeat."""
from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_heartbeat import (
    HeartbeatRecord,
    HeartbeatTracker,
)


@pytest.fixture
def tracker():
    return HeartbeatTracker(max_per_pipeline=10)


_NOW = datetime(2024, 6, 1, 12, 0, 0)


def test_beat_returns_record(tracker):
    rec = tracker.beat("pipe_a", at=_NOW)
    assert isinstance(rec, HeartbeatRecord)
    assert rec.pipeline == "pipe_a"
    assert rec.timestamp == _NOW


def test_beat_default_source(tracker):
    rec = tracker.beat("pipe_a", at=_NOW)
    assert rec.source == "default"


def test_beat_custom_source(tracker):
    rec = tracker.beat("pipe_a", source="scheduler", at=_NOW)
    assert rec.source == "scheduler"


def test_last_beat_none_for_unconfigured(tracker):
    assert tracker.last_beat("unknown") is None


def test_last_beat_returns_latest(tracker):
    t1 = _NOW
    t2 = _NOW + timedelta(seconds=30)
    tracker.beat("pipe_a", at=t1)
    tracker.beat("pipe_a", at=t2)
    assert tracker.last_beat("pipe_a").timestamp == t2


def test_check_unconfigured_returns_none(tracker):
    tracker.beat("pipe_a", at=_NOW)
    assert tracker.check("pipe_a", now=_NOW) is None


def test_check_never_seen_is_missed(tracker):
    tracker.configure("pipe_a", interval_seconds=60)
    status = tracker.check("pipe_a", now=_NOW)
    assert status.missed is True
    assert status.seconds_since is None


def test_check_within_interval_is_ok(tracker):
    tracker.configure("pipe_a", interval_seconds=60)
    tracker.beat("pipe_a", at=_NOW)
    now = _NOW + timedelta(seconds=30)
    status = tracker.check("pipe_a", now=now)
    assert status.missed is False
    assert status.seconds_since == pytest.approx(30.0)


def test_check_beyond_interval_is_missed(tracker):
    tracker.configure("pipe_a", interval_seconds=60)
    tracker.beat("pipe_a", at=_NOW)
    now = _NOW + timedelta(seconds=90)
    status = tracker.check("pipe_a", now=now)
    assert status.missed is True


def test_check_all_returns_all_configured(tracker):
    tracker.configure("pipe_a", interval_seconds=60)
    tracker.configure("pipe_b", interval_seconds=120)
    tracker.beat("pipe_a", at=_NOW)
    tracker.beat("pipe_b", at=_NOW)
    statuses = tracker.check_all(now=_NOW + timedelta(seconds=10))
    names = {s.pipeline for s in statuses}
    assert names == {"pipe_a", "pipe_b"}


def test_max_per_pipeline_cap(tracker):
    t = HeartbeatTracker(max_per_pipeline=3)
    for i in range(5):
        t.beat("pipe_a", at=_NOW + timedelta(seconds=i))
    assert len(t._beats["pipe_a"]) == 3


def test_record_to_dict_roundtrip():
    rec = HeartbeatRecord(pipeline="p", timestamp=_NOW, source="cli")
    d = rec.to_dict()
    assert d["pipeline"] == "p"
    assert d["source"] == "cli"
    restored = HeartbeatRecord.from_dict(d)
    assert restored.pipeline == rec.pipeline
    assert restored.timestamp == rec.timestamp
    assert restored.source == rec.source


def test_summary_ok(tracker):
    tracker.configure("pipe_a", interval_seconds=60)
    tracker.beat("pipe_a", at=_NOW)
    status = tracker.check("pipe_a", now=_NOW + timedelta(seconds=5))
    assert "OK" in status.summary()
    assert "pipe_a" in status.summary()


def test_summary_missed_never_seen(tracker):
    tracker.configure("pipe_a", interval_seconds=60)
    status = tracker.check("pipe_a", now=_NOW)
    assert "MISSED" in status.summary()
    assert "never" in status.summary()


def test_pipelines_returns_configured(tracker):
    tracker.configure("pipe_a", interval_seconds=30)
    tracker.configure("pipe_b", interval_seconds=60)
    assert set(tracker.pipelines()) == {"pipe_a", "pipe_b"}
