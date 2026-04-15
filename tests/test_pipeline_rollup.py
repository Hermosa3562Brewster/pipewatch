"""Tests for pipeline_rollup and rollup_display."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_rollup import (
    RollupBucket,
    compute_rollup,
    compute_rollup_map,
)
from pipewatch.rollup_display import render_rollup_table, render_rollup_summary
from pipewatch.history import RunRecord


def _make_record(status: str, started_at: datetime, duration: float = 10.0) -> RunRecord:
    return RunRecord(
        pipeline="test",
        status=status,
        started_at=started_at,
        completed_at=started_at + timedelta(seconds=duration),
        duration_seconds=duration,
        error_message=None,
    )


DAY1 = datetime(2024, 6, 1, 10, 0, 0)
DAY2 = datetime(2024, 6, 2, 14, 0, 0)


@pytest.fixture
def records():
    return [
        _make_record("success", DAY1, 10.0),
        _make_record("success", DAY1, 20.0),
        _make_record("failure", DAY1, 5.0),
        _make_record("success", DAY2, 15.0),
        _make_record("failure", DAY2, 8.0),
    ]


def test_compute_rollup_daily_bucket_count(records):
    rollup = compute_rollup("test", records, granularity="daily")
    assert len(rollup.buckets) == 2


def test_compute_rollup_daily_labels(records):
    rollup = compute_rollup("test", records, granularity="daily")
    labels = [b.label for b in rollup.buckets]
    assert "2024-06-01" in labels
    assert "2024-06-02" in labels


def test_compute_rollup_counts(records):
    rollup = compute_rollup("test", records, granularity="daily")
    b = rollup.buckets[0]  # 2024-06-01
    assert b.total_runs == 3
    assert b.successes == 2
    assert b.failures == 1


def test_compute_rollup_error_rate(records):
    rollup = compute_rollup("test", records, granularity="daily")
    b = rollup.buckets[0]
    assert abs(b.error_rate - 1 / 3) < 1e-9


def test_compute_rollup_avg_duration(records):
    rollup = compute_rollup("test", records, granularity="daily")
    b = rollup.buckets[0]  # durations: 10, 20, 5 -> avg 11.666...
    assert abs(b.avg_duration - 35.0 / 3) < 1e-9


def test_compute_rollup_hourly_creates_more_buckets():
    recs = [
        _make_record("success", datetime(2024, 6, 1, 10, 0)),
        _make_record("success", datetime(2024, 6, 1, 11, 0)),
    ]
    rollup = compute_rollup("test", recs, granularity="hourly")
    assert len(rollup.buckets) == 2
    assert rollup.buckets[0].label == "2024-06-01 10:00"


def test_compute_rollup_invalid_granularity():
    with pytest.raises(ValueError, match="granularity"):
        compute_rollup("test", [], granularity="weekly")


def test_rollup_total_runs(records):
    rollup = compute_rollup("test", records, granularity="daily")
    assert rollup.total_runs() == 5


def test_rollup_total_failures(records):
    rollup = compute_rollup("test", records, granularity="daily")
    assert rollup.total_failures() == 2


def test_bucket_to_dict_keys():
    b = RollupBucket(
        label="2024-06-01",
        window_start=datetime(2024, 6, 1),
        window_end=datetime(2024, 6, 2),
        total_runs=3,
        successes=2,
        failures=1,
        total_duration=30.0,
    )
    d = b.to_dict()
    for key in ("label", "window_start", "window_end", "total_runs",
                "successes", "failures", "error_rate", "avg_duration"):
        assert key in d


def test_compute_rollup_map(records):
    records_map = {"alpha": records, "beta": records[:2]}
    rollup_map = compute_rollup_map(records_map, granularity="daily")
    assert set(rollup_map.keys()) == {"alpha", "beta"}
    assert rollup_map["beta"].total_runs() == 2


def test_render_rollup_table_contains_pipeline_name(records):
    rollup = compute_rollup("myflow", records, granularity="daily")
    out = render_rollup_table(rollup)
    assert "myflow" in out


def test_render_rollup_table_contains_window_labels(records):
    rollup = compute_rollup("myflow", records, granularity="daily")
    out = render_rollup_table(rollup)
    assert "2024-06-01" in out
    assert "2024-06-02" in out


def test_render_rollup_table_empty():
    from pipewatch.pipeline_rollup import PipelineRollup
    rollup = PipelineRollup(pipeline="empty", granularity="daily", buckets=[])
    out = render_rollup_table(rollup)
    assert "No rollup data" in out


def test_render_rollup_summary_contains_pipeline_names(records):
    rollup_map = compute_rollup_map({"alpha": records, "beta": records[:2]})
    out = render_rollup_summary(rollup_map)
    assert "alpha" in out
    assert "beta" in out


def test_render_rollup_summary_empty():
    out = render_rollup_summary({})
    assert "No rollup data" in out
