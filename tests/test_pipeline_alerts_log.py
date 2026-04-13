"""Tests for pipewatch.pipeline_alerts_log."""

from datetime import datetime

import pytest

from pipewatch.pipeline_alerts_log import AlertsLog, FiredAlert


def _make_alert(pipeline: str = "pipe_a", rule: str = "high_errors") -> FiredAlert:
    return FiredAlert(
        pipeline=pipeline,
        rule_name=rule,
        metric="error_rate",
        operator=">",
        threshold=0.1,
        actual_value=0.25,
        fired_at=datetime(2024, 1, 15, 12, 0, 0),
    )


def test_fired_alert_to_dict_keys():
    alert = _make_alert()
    d = alert.to_dict()
    assert set(d.keys()) == {
        "pipeline", "rule_name", "metric", "operator",
        "threshold", "actual_value", "fired_at",
    }


def test_fired_alert_to_dict_values():
    alert = _make_alert()
    d = alert.to_dict()
    assert d["pipeline"] == "pipe_a"
    assert d["threshold"] == 0.1
    assert d["actual_value"] == 0.25


def test_fired_alert_from_dict_roundtrip():
    alert = _make_alert()
    restored = FiredAlert.from_dict(alert.to_dict())
    assert restored.pipeline == alert.pipeline
    assert restored.rule_name == alert.rule_name
    assert restored.threshold == alert.threshold
    assert restored.actual_value == alert.actual_value
    assert restored.fired_at == alert.fired_at


def test_fired_alert_summary_contains_pipeline():
    alert = _make_alert(pipeline="my_pipe")
    assert "my_pipe" in alert.summary()


def test_fired_alert_summary_contains_rule():
    alert = _make_alert(rule="low_throughput")
    assert "low_throughput" in alert.summary()


def test_alerts_log_starts_empty():
    log = AlertsLog()
    assert log.count() == 0
    assert log.get_all() == []


def test_alerts_log_record_and_count():
    log = AlertsLog()
    log.record(_make_alert())
    log.record(_make_alert())
    assert log.count() == 2


def test_alerts_log_get_for_pipeline():
    log = AlertsLog()
    log.record(_make_alert(pipeline="alpha"))
    log.record(_make_alert(pipeline="beta"))
    log.record(_make_alert(pipeline="alpha"))
    results = log.get_for_pipeline("alpha")
    assert len(results) == 2
    assert all(r.pipeline == "alpha" for r in results)


def test_alerts_log_get_last_n():
    log = AlertsLog()
    for i in range(10):
        log.record(_make_alert(rule=f"rule_{i}"))
    last3 = log.get_last_n(3)
    assert len(last3) == 3
    assert last3[-1].rule_name == "rule_9"


def test_alerts_log_max_entries_cap():
    log = AlertsLog(max_entries=5)
    for i in range(8):
        log.record(_make_alert(rule=f"rule_{i}"))
    assert log.count() == 5
    assert log.get_all()[0].rule_name == "rule_3"


def test_alerts_log_clear():
    log = AlertsLog()
    log.record(_make_alert())
    log.clear()
    assert log.count() == 0


def test_alerts_log_pipelines_with_alerts():
    log = AlertsLog()
    log.record(_make_alert(pipeline="p1"))
    log.record(_make_alert(pipeline="p2"))
    log.record(_make_alert(pipeline="p1"))
    pipelines = log.pipelines_with_alerts()
    assert "p1" in pipelines
    assert "p2" in pipelines
    assert len(pipelines) == 2
