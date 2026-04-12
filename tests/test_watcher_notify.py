"""Tests for PipelineWatcher and notification helpers."""

import pytest

from pipewatch.alerts import AlertManager, AlertRule
from pipewatch.metrics import PipelineMetrics
from pipewatch.notify import AlertEvent, InMemoryNotifier, StdoutNotifier
from pipewatch.watcher import PipelineWatcher


# ---------------------------------------------------------------------------
# InMemoryNotifier
# ---------------------------------------------------------------------------

def test_in_memory_notifier_collects_events():
    n = InMemoryNotifier()
    n("pipe1", "error rate high")
    n("pipe2", "throughput low")
    assert len(n.events) == 2
    assert n.events[0].pipeline == "pipe1"


def test_in_memory_notifier_respects_max():
    n = InMemoryNotifier(max_events=3)
    for i in range(5):
        n(f"p{i}", "msg")
    assert len(n.events) == 3


def test_in_memory_notifier_latest():
    n = InMemoryNotifier()
    for i in range(10):
        n("p", f"msg{i}")
    latest = n.latest(3)
    assert len(latest) == 3
    assert latest[-1].description == "msg9"


def test_in_memory_notifier_clear():
    n = InMemoryNotifier()
    n("p", "x")
    n.clear()
    assert n.events == []


def test_alert_event_format():
    e = AlertEvent(pipeline="etl", description="too slow", timestamp="2024-01-01T00:00:00")
    fmt = e.format()
    assert "etl" in fmt
    assert "too slow" in fmt
    assert "2024-01-01" in fmt


def test_stdout_notifier_prints(capsys):
    n = StdoutNotifier()
    n("mypipe", "latency spike")
    captured = capsys.readouterr()
    assert "mypipe" in captured.out
    assert "latency spike" in captured.out


# ---------------------------------------------------------------------------
# PipelineWatcher
# ---------------------------------------------------------------------------

def _make_watcher(error_threshold=0.5):
    m = PipelineMetrics(name="demo")
    m.start()
    for _ in range(4):
        m.record_failure()
    for _ in range(1):
        m.record_success()

    am = AlertManager()
    am.add_rule(AlertRule("error_rate", "error_rate", "gt", error_threshold))

    notifier = InMemoryNotifier()
    watcher = PipelineWatcher(
        metrics={"demo": m},
        alert_manager=am,
        check_interval=999,
    )
    watcher.register_alert_callback(notifier)
    return watcher, notifier


def test_watcher_evaluate_alerts_fires_callback():
    watcher, notifier = _make_watcher(error_threshold=0.5)
    watcher._evaluate_alerts()
    assert len(notifier.events) == 1
    assert notifier.events[0].pipeline == "demo"


def test_watcher_evaluate_alerts_no_fire_below_threshold():
    watcher, notifier = _make_watcher(error_threshold=0.99)
    watcher._evaluate_alerts()
    assert len(notifier.events) == 0


def test_watcher_multiple_callbacks():
    watcher, n1 = _make_watcher(error_threshold=0.1)
    n2 = InMemoryNotifier()
    watcher.register_alert_callback(n2)
    watcher._evaluate_alerts()
    assert len(n1.events) == 1
    assert len(n2.events) == 1
