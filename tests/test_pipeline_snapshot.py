"""Tests for pipewatch.pipeline_snapshot."""

from datetime import datetime

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.pipeline_snapshot import (
    PipelineSnapshot,
    diff_snapshots,
    snapshot_from_metrics,
    snapshot_map,
    snapshots_with_changes,
)


def _make_metrics(name: str, successes: int = 5, failures: int = 1) -> PipelineMetrics:
    m = PipelineMetrics(name)
    for _ in range(successes):
        m.start()
        m.record_success(duration=1.0)
    for _ in range(failures):
        m.start()
        m.record_failure("error")
    return m


def test_snapshot_from_metrics_captures_name():
    m = _make_metrics("pipe_a")
    snap = snapshot_from_metrics(m)
    assert snap.name == "pipe_a"


def test_snapshot_from_metrics_captures_counts():
    m = _make_metrics("pipe_b", successes=4, failures=2)
    snap = snapshot_from_metrics(m)
    assert snap.successes == 4
    assert snap.failures == 2
    assert snap.total_runs == 6


def test_snapshot_from_metrics_captures_error_rate():
    m = _make_metrics("pipe_c", successes=3, failures=1)
    snap = snapshot_from_metrics(m)
    assert abs(snap.error_rate - 0.25) < 1e-6


def test_snapshot_from_metrics_has_captured_at():
    m = _make_metrics("pipe_d")
    snap = snapshot_from_metrics(m)
    assert isinstance(snap.captured_at, datetime)


def test_snapshot_to_dict_keys():
    m = _make_metrics("pipe_e")
    snap = snapshot_from_metrics(m)
    d = snap.to_dict()
    expected_keys = {"name", "status", "total_runs", "successes", "failures",
                     "error_rate", "avg_duration", "captured_at"}
    assert expected_keys == set(d.keys())


def test_snapshot_map_covers_all_pipelines():
    metrics_map = {
        "a": _make_metrics("a"),
        "b": _make_metrics("b"),
    }
    snaps = snapshot_map(metrics_map)
    assert set(snaps.keys()) == {"a", "b"}


def test_diff_snapshots_detects_failure_change():
    m = _make_metrics("pipe_f", successes=5, failures=0)
    before = snapshot_from_metrics(m)
    m.start()
    m.record_failure("boom")
    after = snapshot_from_metrics(m)
    changes = diff_snapshots(before, after)
    assert "failures" in changes
    assert changes["failures"]["before"] == 0
    assert changes["failures"]["after"] == 1


def test_diff_snapshots_no_changes_when_equal():
    m = _make_metrics("pipe_g")
    snap = snapshot_from_metrics(m)
    changes = diff_snapshots(snap, snap)
    assert changes == {}


def test_snapshots_with_changes_detects_new_pipeline():
    before_map = {}
    after_map = snapshot_map({"new_pipe": _make_metrics("new_pipe")})
    result = snapshots_with_changes(before_map, after_map)
    assert len(result) == 1
    assert result[0]["name"] == "new_pipe"
    assert result[0]["new"] is True


def test_snapshots_with_changes_detects_updated_pipeline():
    m = _make_metrics("pipe_h", successes=2, failures=0)
    before_map = snapshot_map({"pipe_h": m})
    m.start()
    m.record_failure("err")
    after_map = snapshot_map({"pipe_h": m})
    result = snapshots_with_changes(before_map, after_map)
    assert len(result) == 1
    assert result[0]["new"] is False
    assert "failures" in result[0]["changes"]


def test_snapshots_with_changes_empty_when_no_change():
    m = _make_metrics("pipe_i")
    snap_map = snapshot_map({"pipe_i": m})
    result = snapshots_with_changes(snap_map, snap_map)
    assert result == []
