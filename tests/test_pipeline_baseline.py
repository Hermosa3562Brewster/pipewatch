"""Tests for pipeline_baseline and baseline_display modules."""
from __future__ import annotations

import pytest

from pipewatch.pipeline_baseline import BaselineDelta, BaselineSnapshot, BaselineTracker
from pipewatch.baseline_display import (
    render_baseline_table,
    render_delta_table,
    render_baseline_summary,
)


def _snap(name: str, dur: float = 10.0, err: float = 0.05, thr: float = 100.0) -> BaselineSnapshot:
    return BaselineSnapshot(pipeline=name, avg_duration=dur, error_rate=err, avg_throughput=thr)


# ── BaselineSnapshot ──────────────────────────────────────────────────────────

def test_to_dict_keys():
    snap = _snap("pipe-a")
    keys = snap.to_dict().keys()
    assert {"pipeline", "avg_duration", "error_rate", "avg_throughput", "recorded_at"} == set(keys)


def test_from_dict_roundtrip():
    snap = _snap("pipe-b", dur=5.5, err=0.1, thr=200.0)
    restored = BaselineSnapshot.from_dict(snap.to_dict())
    assert restored.pipeline == snap.pipeline
    assert restored.avg_duration == pytest.approx(snap.avg_duration)
    assert restored.error_rate == pytest.approx(snap.error_rate)
    assert restored.avg_throughput == pytest.approx(snap.avg_throughput)


# ── BaselineTracker ───────────────────────────────────────────────────────────

def test_set_and_get_baseline():
    tracker = BaselineTracker()
    snap = _snap("pipe-a")
    tracker.set_baseline(snap)
    assert tracker.get_baseline("pipe-a") is snap


def test_get_missing_returns_none():
    tracker = BaselineTracker()
    assert tracker.get_baseline("ghost") is None


def test_remove_existing_returns_true():
    tracker = BaselineTracker()
    tracker.set_baseline(_snap("pipe-a"))
    assert tracker.remove("pipe-a") is True
    assert tracker.get_baseline("pipe-a") is None


def test_remove_missing_returns_false():
    tracker = BaselineTracker()
    assert tracker.remove("ghost") is False


def test_compute_delta_no_baseline_returns_none():
    tracker = BaselineTracker()
    delta = tracker.compute_delta("pipe-a", _snap("pipe-a"))
    assert delta is None


def test_compute_delta_values():
    tracker = BaselineTracker()
    tracker.set_baseline(_snap("pipe-a", dur=10.0, err=0.05, thr=100.0))
    current = _snap("pipe-a", dur=12.0, err=0.08, thr=120.0)
    delta = tracker.compute_delta("pipe-a", current)
    assert delta is not None
    assert delta.duration_delta_pct == pytest.approx(20.0)
    assert delta.error_rate_delta == pytest.approx(0.03)
    assert delta.throughput_delta_pct == pytest.approx(20.0)


def test_regression_flag_set_on_high_duration_delta():
    delta = BaselineDelta(pipeline="p", duration_delta_pct=25.0, error_rate_delta=0.0, throughput_delta_pct=0.0)
    assert delta.is_regression is True


def test_improvement_flag():
    delta = BaselineDelta(pipeline="p", duration_delta_pct=-15.0, error_rate_delta=-0.01, throughput_delta_pct=5.0)
    assert delta.is_improvement is True


# ── baseline_display ──────────────────────────────────────────────────────────

def test_render_baseline_table_empty():
    tracker = BaselineTracker()
    assert "No baselines" in render_baseline_table(tracker)


def test_render_baseline_table_contains_pipeline_name():
    tracker = BaselineTracker()
    tracker.set_baseline(_snap("pipe-x"))
    assert "pipe-x" in render_baseline_table(tracker)


def test_render_delta_table_empty():
    assert "No deltas" in render_delta_table([])


def test_render_delta_table_contains_regression_badge():
    deltas = [BaselineDelta("pipe-a", duration_delta_pct=30.0, error_rate_delta=0.1, throughput_delta_pct=-5.0)]
    output = render_delta_table(deltas)
    assert "REGRESS" in output
    assert "pipe-a" in output


def test_render_summary_counts():
    tracker = BaselineTracker()
    tracker.set_baseline(_snap("pipe-a"))
    tracker.set_baseline(_snap("pipe-b"))
    deltas = [
        BaselineDelta("pipe-a", 25.0, 0.1, 0.0),
        BaselineDelta("pipe-b", -15.0, -0.01, 5.0),
    ]
    summary = render_baseline_summary(tracker, deltas)
    assert "2" in summary   # tracked pipelines
    assert "Regressions" in summary
    assert "Improvements" in summary
