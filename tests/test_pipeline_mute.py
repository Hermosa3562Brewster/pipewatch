"""Tests for pipewatch.pipeline_mute."""
from datetime import datetime, timedelta

import pytest

from pipewatch.pipeline_mute import MuteManager, MuteRecord


@pytest.fixture
def manager() -> MuteManager:
    return MuteManager()


def _now() -> datetime:
    return datetime.utcnow()


# --- MuteRecord ---

def test_to_dict_keys():
    rec = MuteRecord(
        pipeline="etl",
        muted_at=datetime(2024, 1, 1, 12, 0, 0),
        expires_at=datetime(2024, 1, 1, 13, 0, 0),
        reason="maintenance",
    )
    d = rec.to_dict()
    assert set(d.keys()) == {"pipeline", "muted_at", "expires_at", "reason"}


def test_from_dict_roundtrip():
    rec = MuteRecord(
        pipeline="etl",
        muted_at=datetime(2024, 1, 1, 12, 0, 0),
        expires_at=datetime(2024, 1, 1, 13, 0, 0),
        reason="deploy",
    )
    restored = MuteRecord.from_dict(rec.to_dict())
    assert restored.pipeline == rec.pipeline
    assert restored.reason == rec.reason
    assert restored.expires_at == rec.expires_at


def test_is_active_inside_window():
    now = _now()
    rec = MuteRecord(pipeline="p", muted_at=now - timedelta(minutes=5),
                     expires_at=now + timedelta(minutes=5))
    assert rec.is_active(now) is True


def test_is_active_outside_window():
    now = _now()
    rec = MuteRecord(pipeline="p", muted_at=now - timedelta(hours=2),
                     expires_at=now - timedelta(hours=1))
    assert rec.is_active(now) is False


def test_duration_seconds():
    now = _now()
    rec = MuteRecord(pipeline="p", muted_at=now,
                     expires_at=now + timedelta(seconds=300))
    assert rec.duration_seconds() == pytest.approx(300.0)


def test_summary_contains_pipeline_and_state():
    now = _now()
    rec = MuteRecord(pipeline="etl", muted_at=now,
                     expires_at=now + timedelta(hours=1), reason="test")
    s = rec.summary()
    assert "etl" in s
    assert "active" in s


# --- MuteManager ---

def test_mute_creates_record(manager):
    rec = manager.mute("etl", duration_seconds=60)
    assert isinstance(rec, MuteRecord)
    assert rec.pipeline == "etl"


def test_is_muted_returns_true_immediately(manager):
    manager.mute("etl", duration_seconds=120)
    assert manager.is_muted("etl") is True


def test_is_muted_returns_false_for_unknown(manager):
    assert manager.is_muted("unknown") is False


def test_is_muted_false_after_expiry(manager):
    past = _now() - timedelta(hours=2)
    rec = MuteRecord(pipeline="etl", muted_at=past,
                     expires_at=past + timedelta(seconds=10))
    manager._records["etl"] = [rec]
    assert manager.is_muted("etl") is False


def test_unmute_deactivates_active_mute(manager):
    manager.mute("etl", duration_seconds=3600)
    assert manager.is_muted("etl") is True
    result = manager.unmute("etl")
    assert result is True
    assert manager.is_muted("etl") is False


def test_unmute_returns_false_for_already_expired(manager):
    result = manager.unmute("no-such-pipeline")
    assert result is False


def test_active_mute_returns_record(manager):
    manager.mute("etl", duration_seconds=600, reason="deploy window")
    rec = manager.active_mute("etl")
    assert rec is not None
    assert rec.reason == "deploy window"


def test_active_mute_returns_none_when_expired(manager):
    past = _now() - timedelta(hours=1)
    rec = MuteRecord(pipeline="etl", muted_at=past,
                     expires_at=past + timedelta(seconds=1))
    manager._records["etl"] = [rec]
    assert manager.active_mute("etl") is None


def test_history_returns_all_records(manager):
    manager.mute("etl", duration_seconds=60)
    manager.mute("etl", duration_seconds=120)
    assert len(manager.history("etl")) == 2


def test_all_pipelines_lists_registered(manager):
    manager.mute("alpha", 60)
    manager.mute("beta", 60)
    names = manager.all_pipelines()
    assert "alpha" in names
    assert "beta" in names
