"""Tests for pipewatch.pipeline_flap."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List

import pytest

from pipewatch.history import RunRecord
from pipewatch.pipeline_flap import (
    FlapReport,
    _transitions,
    detect_flap,
    detect_flap_map,
)


def _make_record(pipeline: str, status: str, offset_seconds: int = 0) -> RunRecord:
    base = datetime(2024, 1, 1, 12, 0, 0)
    started = base + timedelta(seconds=offset_seconds)
    return RunRecord(
        pipeline=pipeline,
        status=status,
        started_at=started,
        ended_at=started + timedelta(seconds=1),
        duration_seconds=1.0,
        records_processed=0,
        error=None,
    )


# --- _transitions ---

def test_transitions_empty():
    assert _transitions([]) == 0


def test_transitions_single():
    assert _transitions(["success"]) == 0


def test_transitions_no_change():
    assert _transitions(["success", "success", "success"]) == 0


def test_transitions_alternating():
    states = ["success", "failure", "success", "failure"]
    assert _transitions(states) == 3


def test_transitions_one_flip():
    assert _transitions(["success", "success", "failure"]) == 1


# --- detect_flap ---

def test_detect_flap_returns_none_for_single_record():
    records = [_make_record("pipe", "success", 0)]
    assert detect_flap(records, "pipe") is None


def test_detect_flap_returns_none_for_empty_records():
    assert detect_flap([], "pipe") is None


def test_detect_flap_not_flapping_when_stable():
    records = [_make_record("pipe", "success", i) for i in range(10)]
    report = detect_flap(records, "pipe", window=10, threshold=4)
    assert report is not None
    assert report.flapping is False
    assert report.transitions == 0


def test_detect_flap_flags_flapping_pipeline():
    statuses = ["success", "failure"] * 5  # 10 records, 9 transitions
    records = [_make_record("pipe", s, i) for i, s in enumerate(statuses)]
    report = detect_flap(records, "pipe", window=10, threshold=4)
    assert report is not None
    assert report.flapping is True
    assert report.transitions >= 4


def test_detect_flap_respects_window():
    # First 5 records alternate, last 5 are all success
    statuses = ["success", "failure"] * 3 + ["success"] * 4
    records = [_make_record("pipe", s, i) for i, s in enumerate(statuses)]
    # window=4 picks only the most recent 4 (all success)
    report = detect_flap(records, "pipe", window=4, threshold=2)
    assert report is not None
    assert report.flapping is False


def test_detect_flap_report_fields():
    records = [
        _make_record("pipe", "success", 0),
        _make_record("pipe", "failure", 1),
        _make_record("pipe", "success", 2),
    ]
    report = detect_flap(records, "pipe", threshold=2)
    assert report.pipeline == "pipe"
    assert report.threshold == 2
    assert isinstance(report.last_states, list)
    assert len(report.last_states) == 3


def test_detect_flap_summary_contains_pipeline():
    records = [_make_record("alpha", "success", i) for i in range(5)]
    report = detect_flap(records, "alpha")
    assert "alpha" in report.summary()


def test_detect_flap_summary_stable_label():
    records = [_make_record("alpha", "success", i) for i in range(5)]
    report = detect_flap(records, "alpha")
    assert "stable" in report.summary()


def test_detect_flap_summary_flapping_label():
    statuses = ["success", "failure"] * 5
    records = [_make_record("beta", s, i) for i, s in enumerate(statuses)]
    report = detect_flap(records, "beta", threshold=3)
    assert "FLAPPING" in report.summary()


# --- detect_flap_map ---

def test_detect_flap_map_includes_all_named_pipelines():
    records = [
        _make_record("a", "success", 0),
        _make_record("a", "failure", 1),
        _make_record("b", "success", 0),
        _make_record("b", "success", 1),
    ]
    result = detect_flap_map(records, ["a", "b"])
    assert "a" in result
    assert "b" in result


def test_detect_flap_map_excludes_pipelines_with_no_data():
    records = [_make_record("a", "success", 0)]
    result = detect_flap_map(records, ["a", "unknown"])
    # "unknown" has 0 records -> None -> excluded
    assert "unknown" not in result
