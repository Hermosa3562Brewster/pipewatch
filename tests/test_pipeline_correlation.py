"""Tests for pipewatch.pipeline_correlation."""
import math
import pytest

from pipewatch.pipeline_correlation import (
    CorrelationResult,
    CorrelationTracker,
    _pearson,
)


# ---------------------------------------------------------------------------
# _pearson helpers
# ---------------------------------------------------------------------------

def test_pearson_perfect_positive():
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    r = _pearson(xs, xs)
    assert r == pytest.approx(1.0)


def test_pearson_perfect_negative():
    xs = [1.0, 2.0, 3.0]
    ys = [3.0, 2.0, 1.0]
    r = _pearson(xs, ys)
    assert r == pytest.approx(-1.0)


def test_pearson_returns_none_for_single_sample():
    assert _pearson([0.5], [0.5]) is None


def test_pearson_returns_none_for_flat_series():
    # zero std-dev -> undefined
    assert _pearson([1.0, 1.0, 1.0], [1.0, 2.0, 3.0]) is None


def test_pearson_uncorrelated_near_zero():
    xs = [0.0, 1.0, 0.0, 1.0]
    ys = [0.5, 0.5, 0.5, 0.5]
    assert _pearson(xs, ys) is None  # flat ys -> undefined


# ---------------------------------------------------------------------------
# CorrelationResult
# ---------------------------------------------------------------------------

def _make_result(r: float) -> CorrelationResult:
    return CorrelationResult(pipeline_a="a", pipeline_b="b", coefficient=r, sample_count=10)


def test_strength_strong():
    assert _make_result(0.9).strength() == "strong"


def test_strength_moderate():
    assert _make_result(0.6).strength() == "moderate"


def test_strength_weak():
    assert _make_result(0.3).strength() == "weak"


def test_strength_negligible():
    assert _make_result(0.1).strength() == "negligible"


def test_direction_positive():
    assert _make_result(0.5).direction() == "positive"


def test_direction_negative():
    assert _make_result(-0.5).direction() == "negative"


def test_summary_contains_pipeline_names():
    s = _make_result(0.75).summary()
    assert "a" in s and "b" in s


def test_summary_contains_coefficient():
    s = _make_result(0.75).summary()
    assert "0.750" in s


# ---------------------------------------------------------------------------
# CorrelationTracker
# ---------------------------------------------------------------------------

@pytest.fixture()
def tracker() -> CorrelationTracker:
    return CorrelationTracker(max_samples=50)


def test_record_and_series(tracker):
    tracker.record("pipe_a", 0.1)
    tracker.record("pipe_a", 0.2)
    assert tracker.series("pipe_a") == [0.1, 0.2]


def test_max_samples_respected(tracker):
    t = CorrelationTracker(max_samples=3)
    for v in [0.1, 0.2, 0.3, 0.4]:
        t.record("p", v)
    assert len(t.series("p")) == 3
    assert t.series("p") == [0.2, 0.3, 0.4]


def test_correlate_returns_none_for_insufficient_data(tracker):
    tracker.record("a", 0.1)
    tracker.record("b", 0.2)
    assert tracker.correlate("a", "b") is None


def test_correlate_known_positive(tracker):
    for i in range(10):
        tracker.record("a", float(i))
        tracker.record("b", float(i) * 2)
    result = tracker.correlate("a", "b")
    assert result is not None
    assert result.coefficient == pytest.approx(1.0, abs=1e-6)


def test_correlate_known_negative(tracker):
    for i in range(10):
        tracker.record("a", float(i))
        tracker.record("b", float(9 - i))
    result = tracker.correlate("a", "b")
    assert result is not None
    assert result.coefficient == pytest.approx(-1.0, abs=1e-6)


def test_all_pairs_count(tracker):
    for i in range(5):
        tracker.record("x", float(i))
        tracker.record("y", float(i))
        tracker.record("z", float(9 - i))
    pairs = tracker.all_pairs()
    assert len(pairs) == 3  # (x,y), (x,z), (y,z)


def test_top_correlated_respects_n(tracker):
    for i in range(10):
        tracker.record("a", float(i))
        tracker.record("b", float(i))
        tracker.record("c", float(9 - i))
    top = tracker.top_correlated(n=1)
    assert len(top) == 1
    assert abs(top[0].coefficient) == pytest.approx(1.0, abs=1e-6)


def test_pipelines_returns_all_tracked(tracker):
    tracker.record("alpha", 0.0)
    tracker.record("beta", 0.0)
    assert set(tracker.pipelines()) == {"alpha", "beta"}
