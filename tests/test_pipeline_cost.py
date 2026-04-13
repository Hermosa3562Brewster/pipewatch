"""Tests for pipewatch.pipeline_cost."""

import pytest
from pipewatch.pipeline_cost import CostRecord, CostTracker


@pytest.fixture()
def tracker() -> CostTracker:
    return CostTracker(default_rate=0.01)


# --- CostRecord ---

def test_cost_record_to_dict_keys():
    rec = CostRecord("etl", "run-1", 10.0, 0.01, 0.1)
    keys = rec.to_dict().keys()
    assert set(keys) == {"pipeline_name", "run_id", "duration_seconds", "cost_per_second", "total_cost"}


def test_cost_record_from_dict_roundtrip():
    original = CostRecord("etl", "run-1", 10.0, 0.01, 0.1)
    restored = CostRecord.from_dict(original.to_dict())
    assert restored.pipeline_name == original.pipeline_name
    assert restored.run_id == original.run_id
    assert restored.total_cost == original.total_cost


# --- CostTracker.record ---

def test_record_uses_default_rate(tracker):
    rec = tracker.record("pipe-a", "r1", 100.0)
    assert rec.cost_per_second == 0.01
    assert abs(rec.total_cost - 1.0) < 1e-9


def test_record_uses_custom_rate(tracker):
    tracker.set_rate("pipe-b", 0.05)
    rec = tracker.record("pipe-b", "r1", 20.0)
    assert rec.cost_per_second == 0.05
    assert abs(rec.total_cost - 1.0) < 1e-9


def test_record_stores_entry(tracker):
    tracker.record("pipe-a", "r1", 10.0)
    tracker.record("pipe-a", "r2", 20.0)
    records = tracker.get_records("pipe-a")
    assert len(records) == 2


def test_get_records_unknown_pipeline_returns_empty(tracker):
    assert tracker.get_records("nonexistent") == []


# --- CostTracker.summary ---

def test_summary_none_for_unknown_pipeline(tracker):
    assert tracker.summary("ghost") is None


def test_summary_total_cost(tracker):
    tracker.record("pipe-a", "r1", 10.0)
    tracker.record("pipe-a", "r2", 30.0)
    s = tracker.summary("pipe-a")
    assert s is not None
    assert abs(s.total_cost - 0.4) < 1e-6


def test_summary_avg_cost_per_run(tracker):
    tracker.record("pipe-a", "r1", 10.0)
    tracker.record("pipe-a", "r2", 30.0)
    s = tracker.summary("pipe-a")
    assert abs(s.avg_cost_per_run - 0.2) < 1e-6


def test_summary_max_cost_run(tracker):
    tracker.record("pipe-a", "r1", 5.0)
    tracker.record("pipe-a", "r2", 50.0)
    s = tracker.summary("pipe-a")
    assert abs(s.max_cost_run - 0.5) < 1e-6


def test_summary_total_runs(tracker):
    for i in range(4):
        tracker.record("pipe-a", f"r{i}", float(i + 1))
    s = tracker.summary("pipe-a")
    assert s.total_runs == 4


# --- CostTracker.all_summaries ---

def test_all_summaries_returns_one_per_pipeline(tracker):
    tracker.record("alpha", "r1", 10.0)
    tracker.record("beta", "r1", 20.0)
    summaries = tracker.all_summaries()
    names = {s.pipeline_name for s in summaries}
    assert names == {"alpha", "beta"}


def test_all_summaries_empty_tracker_returns_empty(tracker):
    assert tracker.all_summaries() == []
