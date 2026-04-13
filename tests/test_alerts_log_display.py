"""Tests for pipewatch.alerts_log_display."""

from datetime import datetime

from pipewatch.pipeline_alerts_log import AlertsLog, FiredAlert
from pipewatch.alerts_log_display import (
    render_alerts_log_table,
    render_alerts_log_summary,
)


def _make_alert(pipeline: str = "pipe_a", rule: str = "high_errors") -> FiredAlert:
    return FiredAlert(
        pipeline=pipeline,
        rule_name=rule,
        metric="error_rate",
        operator=">",
        threshold=0.1,
        actual_value=0.25,
        fired_at=datetime(2024, 6, 1, 9, 0, 0),
    )


def test_render_table_empty_log():
    log = AlertsLog()
    result = render_alerts_log_table(log)
    assert "No alerts" in result


def test_render_table_contains_pipeline_name():
    log = AlertsLog()
    log.record(_make_alert(pipeline="my_pipeline"))
    result = render_alerts_log_table(log)
    assert "my_pipeline" in result


def test_render_table_contains_rule_name():
    log = AlertsLog()
    log.record(_make_alert(rule="low_throughput"))
    result = render_alerts_log_table(log)
    assert "low_throughput" in result


def test_render_table_contains_threshold_and_actual():
    log = AlertsLog()
    log.record(_make_alert())
    result = render_alerts_log_table(log)
    assert "0.1000" in result
    assert "0.2500" in result


def test_render_table_respects_last_n():
    log = AlertsLog()
    for i in range(10):
        log.record(_make_alert(rule=f"rule_{i}"))
    result = render_alerts_log_table(log, last_n=3)
    assert "rule_9" in result
    assert "rule_0" not in result


def test_render_summary_contains_header():
    log = AlertsLog()
    result = render_alerts_log_summary(log)
    assert "Alerts Log Summary" in result


def test_render_summary_shows_total():
    log = AlertsLog()
    log.record(_make_alert())
    log.record(_make_alert())
    result = render_alerts_log_summary(log)
    assert "2" in result


def test_render_summary_lists_pipelines():
    log = AlertsLog()
    log.record(_make_alert(pipeline="alpha"))
    log.record(_make_alert(pipeline="beta"))
    result = render_alerts_log_summary(log)
    assert "alpha" in result
    assert "beta" in result
