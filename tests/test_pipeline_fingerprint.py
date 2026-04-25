"""Tests for pipeline_fingerprint module."""
import pytest
from pipewatch.pipeline_fingerprint import (
    FingerprintRecord,
    FingerprintTracker,
    compute_fingerprint,
)


# ---------------------------------------------------------------------------
# compute_fingerprint
# ---------------------------------------------------------------------------

def test_compute_fingerprint_returns_64_char_hex():
    fp = compute_fingerprint({"key": "value"})
    assert len(fp) == 64
    assert all(c in "0123456789abcdef" for c in fp)


def test_compute_fingerprint_stable_for_same_input():
    data = {"a": 1, "b": [1, 2, 3]}
    assert compute_fingerprint(data) == compute_fingerprint(data)


def test_compute_fingerprint_differs_for_different_input():
    assert compute_fingerprint({"a": 1}) != compute_fingerprint({"a": 2})


def test_compute_fingerprint_key_order_independent():
    assert compute_fingerprint({"a": 1, "b": 2}) == compute_fingerprint({"b": 2, "a": 1})


# ---------------------------------------------------------------------------
# FingerprintRecord
# ---------------------------------------------------------------------------

def test_to_dict_keys():
    rec = FingerprintRecord(pipeline="p", fingerprint="abc", recorded_at="2024-01-01", metadata={})
    keys = set(rec.to_dict().keys())
    assert keys == {"pipeline", "fingerprint", "recorded_at", "metadata"}


def test_from_dict_roundtrip():
    rec = FingerprintRecord(pipeline="pipe1", fingerprint="abc123", recorded_at="2024-01-01T00:00:00", metadata={"env": "prod"})
    restored = FingerprintRecord.from_dict(rec.to_dict())
    assert restored.pipeline == rec.pipeline
    assert restored.fingerprint == rec.fingerprint
    assert restored.metadata == rec.metadata


def test_summary_contains_pipeline_and_fingerprint():
    rec = FingerprintRecord(pipeline="my_pipe", fingerprint="abcdef1234567890", recorded_at="2024-01-01", metadata={})
    s = rec.summary()
    assert "my_pipe" in s
    assert "abcdef12" in s


# ---------------------------------------------------------------------------
# FingerprintTracker
# ---------------------------------------------------------------------------

@pytest.fixture
def tracker():
    return FingerprintTracker(max_per_pipeline=5)


def test_record_returns_fingerprint_record(tracker):
    rec = tracker.record("pipe1", {"stage": "load"})
    assert isinstance(rec, FingerprintRecord)
    assert rec.pipeline == "pipe1"


def test_latest_returns_most_recent(tracker):
    tracker.record("pipe1", {"v": 1})
    tracker.record("pipe1", {"v": 2})
    latest = tracker.latest("pipe1")
    assert latest is not None
    assert latest.fingerprint == compute_fingerprint({"v": 2})


def test_latest_returns_none_for_unknown(tracker):
    assert tracker.latest("unknown") is None


def test_has_changed_true_for_new_pipeline(tracker):
    assert tracker.has_changed("new_pipe", {"x": 1}) is True


def test_has_changed_false_for_same_data(tracker):
    data = {"stage": "extract", "rows": 100}
    tracker.record("pipe1", data)
    assert tracker.has_changed("pipe1", data) is False


def test_has_changed_true_after_data_changes(tracker):
    tracker.record("pipe1", {"rows": 100})
    assert tracker.has_changed("pipe1", {"rows": 200}) is True


def test_history_returns_last_n(tracker):
    for i in range(5):
        tracker.record("pipe1", {"i": i})
    hist = tracker.history("pipe1", n=3)
    assert len(hist) == 3


def test_max_per_pipeline_cap(tracker):
    for i in range(10):
        tracker.record("pipe1", {"i": i})
    assert len(tracker.history("pipe1", n=100)) == 5


def test_all_pipelines_returns_registered(tracker):
    tracker.record("alpha", {})
    tracker.record("beta", {})
    assert set(tracker.all_pipelines()) == {"alpha", "beta"}


def test_metadata_stored_on_record(tracker):
    rec = tracker.record("pipe1", {}, metadata={"env": "staging"})
    assert rec.metadata["env"] == "staging"
