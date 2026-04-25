"""Tests for fingerprint_display module."""
from pipewatch.pipeline_fingerprint import FingerprintTracker
from pipewatch.fingerprint_display import (
    render_fingerprint_table,
    render_fingerprint_history,
    render_fingerprint_summary,
)


@pytest.fixture
def tracker():
    t = FingerprintTracker()
    t.record("alpha", {"stage": "load", "rows": 100})
    t.record("beta", {"stage": "extract", "rows": 50})
    return t


import pytest


@pytest.fixture
def tracker():
    t = FingerprintTracker()
    t.record("alpha", {"stage": "load", "rows": 100})
    t.record("beta", {"stage": "extract", "rows": 50})
    return t


def test_render_table_empty_tracker():
    t = FingerprintTracker()
    result = render_fingerprint_table(t, {})
    assert "No fingerprint" in result


def test_render_table_contains_pipeline_names(tracker):
    result = render_fingerprint_table(tracker, {})
    assert "alpha" in result
    assert "beta" in result


def test_render_table_contains_header(tracker):
    result = render_fingerprint_table(tracker, {})
    assert "Pipeline" in result
    assert "Fingerprint" in result


def test_render_table_shows_stable_when_data_unchanged(tracker):
    ref = {"alpha": {"stage": "load", "rows": 100}}
    result = render_fingerprint_table(tracker, ref)
    assert "STABLE" in result


def test_render_table_shows_changed_when_data_differs(tracker):
    ref = {"alpha": {"stage": "load", "rows": 999}}
    result = render_fingerprint_table(tracker, ref)
    assert "CHANGED" in result


def test_render_history_empty_pipeline():
    t = FingerprintTracker()
    result = render_fingerprint_history(t, "unknown")
    assert "No fingerprint history" in result


def test_render_history_contains_pipeline_name(tracker):
    result = render_fingerprint_history(tracker, "alpha")
    assert "alpha" in result


def test_render_history_contains_fingerprint_prefix(tracker):
    from pipewatch.pipeline_fingerprint import compute_fingerprint
    fp = compute_fingerprint({"stage": "load", "rows": 100})
    result = render_fingerprint_history(tracker, "alpha")
    assert fp[:12] in result


def test_render_summary_shows_total(tracker):
    result = render_fingerprint_summary(tracker)
    assert "2" in result


def test_render_summary_contains_header(tracker):
    result = render_fingerprint_summary(tracker)
    assert "Fingerprint Summary" in result
