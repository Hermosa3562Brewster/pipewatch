"""Tests for pipewatch.pipeline_latency."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone

from pipewatch.history import RunRecord
from pipewatch.pipeline_latency import (
    LatencyStats,
    compute_latency,
    compute_latency_map,
)


def _make_record(name: str, duration: float | None, status: str = "success") -> RunRecord:
    now = datetime.now(timezone.utc).isoformat()
    return RunRecord(
        pipeline_name=name,
        status=status,
        started_at=now,
        ended_at=now,
        duration_seconds=duration,
        error_message=None,
    )


# --- LatencyStats unit tests ---

def test_latency_stats_empty():
    s = LatencyStats(name="p")
    assert s.count == 0
    assert s.mean is None
    assert s.minimum is None
    assert s.maximum is None
    assert s.p95 is None


def test_latency_stats_single_sample():
    s = LatencyStats(name="p", samples=[5.0])
    assert s.count == 1
    assert s.mean == pytest.approx(5.0)
    assert s.minimum == pytest.approx(5.0)
    assert s.maximum == pytest.approx(5.0)
    assert s.p95 == pytest.approx(5.0)


def test_latency_stats_mean():
    s = LatencyStats(name="p", samples=[1.0, 3.0, 5.0])
    assert s.mean == pytest.approx(3.0)


def test_latency_stats_min_max():
    s = LatencyStats(name="p", samples=[2.0, 8.0, 5.0])
    assert s.minimum == pytest.approx(2.0)
    assert s.maximum == pytest.approx(8.0)


def test_latency_stats_p95_large_sample():
    samples = list(range(1, 101))  # 1..100
    s = LatencyStats(name="p", samples=[float(x) for x in samples])
    # 95th percentile index = int(100 * 0.95) - 1 = 94 => sorted[94] = 95
    assert s.p95 == pytest.approx(95.0)


# --- compute_latency ---

def test_compute_latency_raises_on_empty():
    with pytest.raises(ValueError):
        compute_latency([])


def test_compute_latency_filters_none_durations():
    records = [
        _make_record("pipe", None),
        _make_record("pipe", 4.0),
    ]
    stats = compute_latency(records)
    assert stats.count == 1
    assert stats.mean == pytest.approx(4.0)


def test_compute_latency_filters_negative_durations():
    records = [
        _make_record("pipe", -1.0),
        _make_record("pipe", 2.0),
    ]
    stats = compute_latency(records)
    assert stats.count == 1


def test_compute_latency_name_preserved():
    records = [_make_record("my-pipeline", 1.5)]
    stats = compute_latency(records)
    assert stats.name == "my-pipeline"


# --- compute_latency_map ---

def test_compute_latency_map_keys():
    history_map = {
        "a": [_make_record("a", 1.0)],
        "b": [_make_record("b", 2.0)],
    }
    result = compute_latency_map(history_map)
    assert set(result.keys()) == {"a", "b"}


def test_compute_latency_map_empty_pipeline():
    history_map = {"empty": []}
    result = compute_latency_map(history_map)
    assert result["empty"].count == 0
    assert result["empty"].mean is None


def test_compute_latency_map_values():
    history_map = {
        "pipe": [_make_record("pipe", 3.0), _make_record("pipe", 7.0)]
    }
    result = compute_latency_map(history_map)
    assert result["pipe"].mean == pytest.approx(5.0)
