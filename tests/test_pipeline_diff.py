"""Tests for pipewatch.pipeline_diff."""

from __future__ import annotations

import pytest

from pipewatch.metrics import PipelineMetrics
from pipewatch.pipeline_diff import (
    PipelineDiff,
    compute_diff,
    diff_snapshot_maps,
)
from pipewatch.pipeline_snapshot import snapshot_map


def _make_metrics(name: str, successes: int, failures: int) -> PipelineMetrics:
    m = PipelineMetrics(name=name)
    m.successes = successes
    m.failures = failures
    m.status = "running" if failures == 0 else "failed"
    return m


@pytest.fixture()
def before_map():
    return {
        "alpha": _make_metrics("alpha", 10, 2),
        "beta": _make_metrics("beta", 5, 0),
    }


@pytest.fixture()
def after_map():
    return {
        "alpha": _make_metrics("alpha", 12, 4),
        "beta": _make_metrics("beta", 8, 0),
        "gamma": _make_metrics("gamma", 3, 1),
    }


def test_compute_diff_returns_all_after_pipelines(before_map, after_map):
    diffs = compute_diff(before_map, after_map)
    names = {d.name for d in diffs}
    assert names == {"alpha", "beta", "gamma"}


def test_success_delta_computed_correctly(before_map, after_map):
    diffs = {d.name: d for d in compute_diff(before_map, after_map)}
    assert diffs["alpha"].success_delta == 2
    assert diffs["beta"].success_delta == 3


def test_failure_delta_computed_correctly(before_map, after_map):
    diffs = {d.name: d for d in compute_diff(before_map, after_map)}
    assert diffs["alpha"].failure_delta == 2
    assert diffs["beta"].failure_delta == 0


def test_new_pipeline_has_none_prev_status(before_map, after_map):
    diffs = {d.name: d for d in compute_diff(before_map, after_map)}
    assert diffs["gamma"].prev_status is None
    assert diffs["gamma"].status_changed is True


def test_unchanged_status_not_flagged(before_map, after_map):
    diffs = {d.name: d for d in compute_diff(before_map, after_map)}
    assert diffs["beta"].status_changed is False


def test_is_degraded_when_error_rate_increases(before_map, after_map):
    diffs = {d.name: d for d in compute_diff(before_map, after_map)}
    # alpha: before error_rate=2/12≈0.167, after=4/16=0.25 → delta>0
    assert diffs["alpha"].is_degraded is True


def test_is_improved_when_error_rate_decreases():
    before = {"pipe": _make_metrics("pipe", 5, 5)}
    after = {"pipe": _make_metrics("pipe", 15, 5)}
    diffs = {d.name: d for d in compute_diff(before, after)}
    assert diffs["pipe"].is_improved is True


def test_no_change_pipeline_not_degraded_or_improved():
    m = _make_metrics("stable", 10, 0)
    before = {"stable": m}
    after = {"stable": _make_metrics("stable", 10, 0)}
    diffs = {d.name: d for d in compute_diff(before, after)}
    assert diffs["stable"].is_degraded is False
    assert diffs["stable"].is_improved is False


def test_empty_maps_produce_no_diffs():
    assert compute_diff({}, {}) == []
