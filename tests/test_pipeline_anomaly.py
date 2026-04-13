"""Tests for pipeline anomaly detection."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.pipeline_anomaly import (
    AnomalyDetector,
    AnomalyResult,
    detect_anomaly,
    detect_error_rate_anomaly,
)
from pipewatch.history import RunRecord


def _make_record(successes: int, failures: int) -> RunRecord:
    now = datetime.now(timezone.utc).isoformat()
    return RunRecord(
        pipeline="pipe",
        started_at=now,
        completed_at=now,
        status="ok",
        successes=successes,
        failures=failures,
        error_rate=failures / max(successes + failures, 1),
        duration_seconds=1.0,
    )


def test_detect_anomaly_returns_none_for_small_history():
    result = detect_anomaly("p", "metric", 5.0, [1.0, 2.0], threshold=2.0)
    assert result is None


def test_detect_anomaly_not_anomalous_within_threshold():
    history = [1.0, 1.1, 0.9, 1.0, 1.05]
    result = detect_anomaly("p", "metric", 1.02, history, threshold=2.0)
    assert result is not None
    assert result.is_anomaly is False


def test_detect_anomaly_flags_clear_outlier():
    history = [1.0, 1.0, 1.0, 1.0, 1.0]
    result = detect_anomaly("p", "metric", 100.0, history, threshold=2.0)
    assert result is not None
    assert result.is_anomaly is True


def test_detect_anomaly_z_score_zero_for_flat_history():
    history = [5.0, 5.0, 5.0, 5.0]
    result = detect_anomaly("p", "metric", 5.0, history, threshold=2.0)
    assert result is not None
    assert result.z_score == 0.0
    assert result.is_anomaly is False


def test_anomaly_result_severity_high():
    r = AnomalyResult("p", "m", 10.0, 1.0, 1.0, 9.0, True, "high")
    assert r.severity == "high"
    assert "HIGH" in r.summary()


def test_anomaly_result_summary_contains_pipeline_and_metric():
    r = AnomalyResult("my_pipe", "error_rate", 0.9, 0.1, 0.05, 16.0, True, "high")
    s = r.summary()
    assert "my_pipe" in s
    assert "error_rate" in s


def test_detect_error_rate_anomaly_returns_none_for_few_records():
    records = [_make_record(10, 0)] * 3
    result = detect_error_rate_anomaly("pipe", records)
    assert result is None


def test_detect_error_rate_anomaly_normal_returns_result():
    records = [_make_record(10, 0)] * 5  # all good, last also good
    result = detect_error_rate_anomaly("pipe", records)
    assert result is not None
    assert result.is_anomaly is False


def test_detect_error_rate_anomaly_spike_detected():
    good = [_make_record(100, 0)] * 8
    bad = _make_record(0, 100)
    result = detect_error_rate_anomaly("pipe", good + [bad])
    assert result is not None
    assert result.is_anomaly is True
    assert result.metric == "error_rate"


def test_anomaly_detector_analyse_stores_results():
    detector = AnomalyDetector(threshold=2.0)
    good = [_make_record(10, 0)] * 8
    bad = _make_record(0, 10)
    detector.analyse("pipe", good + [bad])
    assert len(detector.anomalies()) == 1


def test_anomaly_detector_clear_resets_results():
    detector = AnomalyDetector(threshold=2.0)
    good = [_make_record(10, 0)] * 8
    bad = _make_record(0, 10)
    detector.analyse("pipe", good + [bad])
    detector.clear()
    assert detector.anomalies() == []


def test_anomaly_detector_no_anomaly_not_stored_in_anomalies():
    detector = AnomalyDetector(threshold=2.0)
    records = [_make_record(10, 0)] * 9
    detector.analyse("pipe", records)
    assert detector.anomalies() == []
