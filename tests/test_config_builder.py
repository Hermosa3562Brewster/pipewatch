"""Tests for pipewatch.config_builder module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.alerts import AlertManager, AlertRule
from pipewatch.config_builder import (
    alert_rules_from_manager,
    build_config,
    pipelines_from_metrics,
    save_config,
)
from pipewatch.metrics import PipelineMetrics


def _make_metrics(name: str) -> PipelineMetrics:
    m = PipelineMetrics(name=name)
    return m


def test_pipelines_from_metrics_names() -> None:
    metrics = [_make_metrics("pipe_a"), _make_metrics("pipe_b")]
    cfgs = pipelines_from_metrics(metrics, interval_seconds=45)
    assert [c.name for c in cfgs] == ["pipe_a", "pipe_b"]


def test_pipelines_from_metrics_interval() -> None:
    metrics = [_make_metrics("p")]
    cfgs = pipelines_from_metrics(metrics, interval_seconds=90)
    assert cfgs[0].interval_seconds == 90


def test_pipelines_from_metrics_enabled_by_default() -> None:
    cfgs = pipelines_from_metrics([_make_metrics("p")])
    assert cfgs[0].enabled is True


def test_alert_rules_from_manager() -> None:
    manager = AlertManager()
    manager.add_rule(AlertRule(metric="error_rate", operator="gt", threshold=0.05))
    manager.add_rule(AlertRule(metric="throughput", operator="lt", threshold=10.0))
    rules = alert_rules_from_manager("etl_main", manager)
    assert len(rules) == 2
    assert all(r.pipeline == "etl_main" for r in rules)
    assert rules[0].metric == "error_rate"
    assert rules[1].operator == "lt"


def test_alert_rules_label_populated() -> None:
    manager = AlertManager()
    manager.add_rule(AlertRule(metric="error_rate", operator="gt", threshold=0.1))
    rules = alert_rules_from_manager("p", manager)
    assert rules[0].label != ""


def test_build_config_no_managers() -> None:
    metrics = [_make_metrics("alpha"), _make_metrics("beta")]
    cfg = build_config(metrics)
    assert len(cfg.pipelines) == 2
    assert cfg.alert_rules == []


def test_build_config_with_managers() -> None:
    metrics = [_make_metrics("alpha")]
    manager = AlertManager()
    manager.add_rule(AlertRule(metric="error_rate", operator="gt", threshold=0.2))
    cfg = build_config(metrics, managers={"alpha": manager})
    assert len(cfg.alert_rules) == 1
    assert cfg.alert_rules[0].pipeline == "alpha"


def test_build_config_dirs() -> None:
    cfg = build_config([], history_dir="/h", export_dir="/e")
    assert cfg.history_dir == "/h"
    assert cfg.export_dir == "/e"


def test_save_config_writes_json(tmp_path: Path) -> None:
    metrics = [_make_metrics("pipe1")]
    cfg = build_config(metrics)
    out = tmp_path / "out" / "cfg.json"
    save_config(cfg, out)
    assert out.exists()
    data = json.loads(out.read_text())
    assert data["pipelines"][0]["name"] == "pipe1"


def test_save_config_creates_parent_dirs(tmp_path: Path) -> None:
    cfg = build_config([])
    out = tmp_path / "deep" / "nested" / "config.json"
    save_config(cfg, out)
    assert out.exists()
