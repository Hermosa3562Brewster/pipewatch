"""Tests for pipeline_audit module."""
from __future__ import annotations

import pytest
from datetime import timezone

from pipewatch.pipeline_audit import AuditEntry, AuditLog, _VALID_ACTIONS


@pytest.fixture
def log() -> AuditLog:
    return AuditLog()


def test_record_returns_audit_entry(log: AuditLog) -> None:
    entry = log.record("pipe_a", "created", "alice")
    assert isinstance(entry, AuditEntry)


def test_record_stores_entry(log: AuditLog) -> None:
    log.record("pipe_a", "updated", "bob", detail="changed interval")
    entries = log.get("pipe_a")
    assert len(entries) == 1
    assert entries[0].detail == "changed interval"


def test_record_invalid_action_raises(log: AuditLog) -> None:
    with pytest.raises(ValueError, match="Unknown audit action"):
        log.record("pipe_a", "exploded", "alice")


def test_all_valid_actions_accepted(log: AuditLog) -> None:
    for action in _VALID_ACTIONS:
        log.record("pipe_x", action, "tester")
    assert len(log.get("pipe_x")) == len(_VALID_ACTIONS)


def test_get_last_n_returns_tail(log: AuditLog) -> None:
    for i in range(5):
        log.record("pipe_a", "updated", f"user_{i}")
    result = log.get("pipe_a", last_n=3)
    assert len(result) == 3
    assert result[-1].actor == "user_4"


def test_get_unknown_pipeline_returns_empty(log: AuditLog) -> None:
    assert log.get("nonexistent") == []


def test_all_entries_sorted_by_timestamp(log: AuditLog) -> None:
    log.record("pipe_b", "created", "alice")
    log.record("pipe_a", "updated", "bob")
    entries = log.all_entries()
    timestamps = [e.timestamp for e in entries]
    assert timestamps == sorted(timestamps)


def test_pipelines_lists_all_seen(log: AuditLog) -> None:
    log.record("pipe_a", "created", "alice")
    log.record("pipe_b", "created", "bob")
    assert set(log.pipelines()) == {"pipe_a", "pipe_b"}


def test_clear_removes_entries(log: AuditLog) -> None:
    log.record("pipe_a", "created", "alice")
    log.clear("pipe_a")
    assert log.get("pipe_a") == []
    assert "pipe_a" not in log.pipelines()


def test_max_per_pipeline_cap() -> None:
    log = AuditLog(max_per_pipeline=3)
    for i in range(5):
        log.record("pipe_a", "updated", f"u{i}")
    assert len(log.get("pipe_a")) == 3


def test_to_dict_keys(log: AuditLog) -> None:
    entry = log.record("pipe_a", "deleted", "carol", detail="removed", metadata={"reason": "cleanup"})
    d = entry.to_dict()
    assert set(d.keys()) == {"pipeline", "action", "actor", "detail", "timestamp", "metadata"}


def test_from_dict_roundtrip(log: AuditLog) -> None:
    entry = log.record("pipe_a", "reset", "dave", detail="manual reset")
    restored = AuditEntry.from_dict(entry.to_dict())
    assert restored.pipeline == entry.pipeline
    assert restored.action == entry.action
    assert restored.actor == entry.actor
    assert restored.detail == entry.detail


def test_summary_contains_pipeline_and_action(log: AuditLog) -> None:
    entry = log.record("pipe_z", "enabled", "ops")
    s = entry.summary()
    assert "pipe_z" in s
    assert "enabled" in s
    assert "ops" in s


def test_timestamp_is_utc(log: AuditLog) -> None:
    entry = log.record("pipe_a", "created", "system")
    assert entry.timestamp.tzinfo == timezone.utc
