"""Tests for pipewatch alert rules, alert manager, and CLI display utilities."""

import pytest
from pipewatch.metrics import PipelineMetrics
from pipewatch.alerts import AlertRule, AlertManager
from pipewatch.display import render_metrics, render_alerts, render_dashboard


# ---------------------------------------------------------------------------
# AlertRule tests
# ---------------------------------------------------------------------------

def test_alert_rule_gt_triggered():
    rule = AlertRule(name="High Error Rate", metric="error_rate", threshold=0.1, operator="gt")
    assert rule.check(0.2) is True


def test_alert_rule_gt_not_triggered():
    rule = AlertRule(name="High Error Rate", metric="error_rate", threshold=0.1, operator="gt")
    assert rule.check(0.05) is False


def test_alert_rule_lt_triggered():
    rule = AlertRule(name="Low Throughput", metric="throughput", threshold=10.0, operator="lt")
    assert rule.check(5.0) is True


def test_alert_rule_invalid_operator():
    rule = AlertRule(name="Bad", metric="error_rate", threshold=0.5, operator="eq")
    with pytest.raises(ValueError, match="Unknown operator"):
        rule.check(0.5)


def test_alert_rule_description_contains_name():
    rule = AlertRule(name="Spike", metric="error_rate", threshold=0.5, operator="gt", message="Error rate too high")
    desc = rule.description(0.75)
    assert "Spike" in desc
    assert "Error rate too high" in desc


# ---------------------------------------------------------------------------
# AlertManager tests
# ---------------------------------------------------------------------------

def _make_metrics(success=8, failure=2, throughput=5.0):
    m = PipelineMetrics(pipeline_name="test-pipe")
    m.success_count = success
    m.failure_count = failure
    m.total_count = success + failure
    m._throughput_override = throughput
    # Patch throughput property via monkeypatch is not needed; use direct attribute
    return m


def test_alert_manager_triggers_on_high_error_rate():
    m = PipelineMetrics(pipeline_name="pipe")
    for _ in range(8):
        m.record_success()
    for _ in range(2):
        m.record_failure("err")

    manager = AlertManager()
    manager.add_rule(AlertRule("High Error", "error_rate", 0.15, "gt"))
    alerts = manager.evaluate(m)
    assert len(alerts) == 1
    assert "High Error" in alerts[0]


def test_alert_manager_no_alerts_when_healthy():
    m = PipelineMetrics(pipeline_name="pipe")
    for _ in range(10):
        m.record_success()

    manager = AlertManager()
    manager.add_rule(AlertRule("High Error", "error_rate", 0.15, "gt"))
    alerts = manager.evaluate(m)
    assert alerts == []


# ---------------------------------------------------------------------------
# Display tests
# ---------------------------------------------------------------------------

def test_render_metrics_contains_pipeline_name():
    m = PipelineMetrics(pipeline_name="my-pipeline")
    output = render_metrics(m)
    assert "my-pipeline" in output


def test_render_metrics_shows_status():
    m = PipelineMetrics(pipeline_name="pipe")
    m.record_success()
    output = render_metrics(m)
    assert "IDLE" in output or "RUNNING" in output or "COMPLETED" in output


def test_render_alerts_empty():
    output = render_alerts([])
    assert "No active alerts" in output


def test_render_alerts_shows_messages():
    output = render_alerts(["[ALERT] High Error: error too high (current=0.2500)"])
    assert "ALERT" in output


def test_render_dashboard_combines_both():
    m = PipelineMetrics(pipeline_name="dash-pipe")
    output = render_dashboard(m, ["[ALERT] test alert"])
    assert "dash-pipe" in output
    assert "test alert" in output
