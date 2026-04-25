"""Tests for pipewatch.incident_display."""
from __future__ import annotations

import pytest

from pipewatch.pipeline_incident import IncidentLog
from pipewatch.incident_display import (
    render_incident_table,
    render_incident_summary,
)


@pytest.fixture
def log_with_incidents():
    log = IncidentLog()
    inc1 = log.open("etl_orders", "high", "Latency spike")
    log.open("etl_users", "critical", "Data loss")
    log.resolve(inc1.incident_id)
    return log


def test_render_table_empty():
    incidents = []
    result = render_incident_table(incidents)
    assert "No incidents" in result


def test_render_table_contains_pipeline_name(log_with_incidents):
    incidents = log_with_incidents.get("etl_orders")
    result = render_incident_table(incidents)
    assert "etl_orders" in result


def test_render_table_contains_header(log_with_incidents):
    incidents = log_with_incidents.all_open()
    result = render_incident_table(incidents)
    assert "Pipeline" in result
    assert "Severity" in result


def test_render_table_shows_resolved_status(log_with_incidents):
    incidents = log_with_incidents.get("etl_orders", status="resolved")
    result = render_incident_table(incidents)
    assert "RESOLVED" in result


def test_render_table_shows_open_status(log_with_incidents):
    incidents = log_with_incidents.all_open()
    result = render_incident_table(incidents)
    assert "OPEN" in result


def test_render_summary_contains_header(log_with_incidents):
    result = render_incident_summary(log_with_incidents)
    assert "Incident Summary" in result


def test_render_summary_shows_total(log_with_incidents):
    result = render_incident_summary(log_with_incidents)
    assert "2" in result  # two incidents total


def test_render_summary_shows_open_count(log_with_incidents):
    result = render_incident_summary(log_with_incidents)
    assert "Open incidents" in result
    assert "1" in result


def test_render_summary_empty_log():
    log = IncidentLog()
    result = render_incident_summary(log)
    assert "0" in result
