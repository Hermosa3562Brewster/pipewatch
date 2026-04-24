"""Tests for audit_display module."""
from __future__ import annotations

import pytest

from pipewatch.pipeline_audit import AuditLog
from pipewatch.audit_display import (
    render_audit_table,
    render_audit_for_pipeline,
    render_audit_summary,
)


@pytest.fixture
def log() -> AuditLog:
    al = AuditLog()
    al.record("pipe_a", "created", "alice", detail="initial setup")
    al.record("pipe_a", "updated", "bob", detail="changed interval")
    al.record("pipe_b", "disabled", "carol", detail="maintenance")
    return al


def test_render_table_empty() -> None:
    result = render_audit_table([])
    assert "No audit entries" in result


def test_render_table_contains_pipeline_name(log: AuditLog) -> None:
    entries = log.all_entries()
    result = render_audit_table(entries)
    assert "pipe_a" in result
    assert "pipe_b" in result


def test_render_table_contains_header(log: AuditLog) -> None:
    entries = log.all_entries()
    result = render_audit_table(entries)
    assert "Timestamp" in result
    assert "Action" in result
    assert "Actor" in result


def test_render_table_contains_detail(log: AuditLog) -> None:
    entries = log.all_entries()
    result = render_audit_table(entries)
    assert "initial setup" in result
    assert "maintenance" in result


def test_render_for_pipeline_only_shows_that_pipeline(log: AuditLog) -> None:
    result = render_audit_for_pipeline(log, "pipe_a")
    assert "pipe_a" in result
    # pipe_b entries should not appear
    assert "maintenance" not in result


def test_render_for_pipeline_respects_last_n(log: AuditLog) -> None:
    # pipe_a has 2 entries; last_n=1 should only show the latest
    result = render_audit_for_pipeline(log, "pipe_a", last_n=1)
    assert "changed interval" in result
    assert "initial setup" not in result


def test_render_summary_contains_pipeline_names(log: AuditLog) -> None:
    result = render_audit_summary(log)
    assert "pipe_a" in result
    assert "pipe_b" in result


def test_render_summary_empty_log() -> None:
    result = render_audit_summary(AuditLog())
    assert "No pipelines audited" in result


def test_render_summary_shows_entry_count(log: AuditLog) -> None:
    result = render_audit_summary(log)
    # pipe_a has 2 entries
    assert "2" in result


def test_render_summary_shows_last_action(log: AuditLog) -> None:
    result = render_audit_summary(log)
    assert "updated" in result
    assert "disabled" in result
