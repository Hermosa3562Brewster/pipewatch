"""Tests for pipewatch.pipeline_pause."""
from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from pipewatch.pipeline_pause import PauseManager, PauseRecord


@pytest.fixture
def manager() -> PauseManager:
    return PauseManager()


def test_pause_creates_active_record(manager):
    rec = manager.pause("etl_a")
    assert rec.pipeline == "etl_a"
    assert rec.is_active()
    assert rec.resumed_at is None


def test_pause_stores_reason(manager):
    rec = manager.pause("etl_a", reason="maintenance")
    assert rec.reason == "maintenance"


def test_is_paused_returns_true_after_pause(manager):
    manager.pause("etl_a")
    assert manager.is_paused("etl_a")


def test_is_paused_returns_false_for_unknown(manager):
    assert not manager.is_paused("unknown")


def test_double_pause_raises(manager):
    manager.pause("etl_a")
    with pytest.raises(ValueError, match="already paused"):
        manager.pause("etl_a")


def test_resume_clears_active_record(manager):
    manager.pause("etl_a")
    rec = manager.resume("etl_a")
    assert rec.resumed_at is not None
    assert not manager.is_paused("etl_a")


def test_resume_unknown_raises(manager):
    with pytest.raises(ValueError, match="not paused"):
        manager.resume("ghost")


def test_duration_seconds_after_resume(manager):
    manager.pause("etl_a")
    time.sleep(0.05)
    rec = manager.resume("etl_a")
    assert rec.duration_seconds() is not None
    assert rec.duration_seconds() >= 0.0


def test_duration_seconds_none_while_active(manager):
    rec = manager.pause("etl_a")
    assert rec.duration_seconds() is None


def test_history_returns_all_records(manager):
    manager.pause("etl_a")
    manager.resume("etl_a")
    manager.pause("etl_a")
    assert len(manager.history("etl_a")) == 2


def test_all_paused_returns_currently_paused(manager):
    manager.pause("etl_a")
    manager.pause("etl_b")
    manager.resume("etl_a")
    paused = manager.all_paused()
    assert "etl_b" in paused
    assert "etl_a" not in paused


def test_to_dict_keys(manager):
    rec = manager.pause("etl_a", reason="test")
    d = rec.to_dict()
    assert set(d.keys()) == {"pipeline", "paused_at", "resumed_at", "reason"}


def test_from_dict_roundtrip(manager):
    rec = manager.pause("etl_a", reason="roundtrip")
    manager.resume("etl_a")
    rec2 = PauseRecord.from_dict(rec.to_dict())
    assert rec2.pipeline == rec.pipeline
    assert rec2.reason == rec.reason
    assert rec2.resumed_at is not None


def test_from_dict_no_resumed_at():
    data = {
        "pipeline": "etl_x",
        "paused_at": datetime.now(timezone.utc).isoformat(),
        "resumed_at": None,
        "reason": "",
    }
    rec = PauseRecord.from_dict(data)
    assert rec.is_active()
