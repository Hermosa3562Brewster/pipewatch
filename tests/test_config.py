"""Tests for pipewatch.config module."""

from __future__ import annotations

import json
import pytest
from pathlib import Path

from pipewatch.config import (
    load_config,
    config_to_dict,
    PipewatchConfig,
    PipelineConfig,
    AlertRuleConfig,
)


@pytest.fixture()
def config_file(tmp_path: Path) -> Path:
    data = {
        "pipelines": [
            {"name": "etl_main", "interval_seconds": 30, "enabled": True, "tags": ["prod"]},
            {"name": "etl_backup", "interval_seconds": 120, "enabled": False, "tags": []},
        ],
        "alert_rules": [
            {"pipeline": "etl_main", "metric": "error_rate", "operator": "gt", "threshold": 0.1, "label": "High errors"},
        ],
        "history_dir": "/tmp/history",
        "export_dir": "/tmp/exports",
    }
    p = tmp_path / "pipewatch.json"
    p.write_text(json.dumps(data))
    return p


def test_load_config_pipelines(config_file: Path) -> None:
    cfg = load_config(config_file)
    assert len(cfg.pipelines) == 2
    assert cfg.pipelines[0].name == "etl_main"
    assert cfg.pipelines[0].interval_seconds == 30
    assert cfg.pipelines[0].enabled is True
    assert cfg.pipelines[0].tags == ["prod"]


def test_load_config_disabled_pipeline(config_file: Path) -> None:
    cfg = load_config(config_file)
    assert cfg.pipelines[1].enabled is False


def test_load_config_alert_rules(config_file: Path) -> None:
    cfg = load_config(config_file)
    assert len(cfg.alert_rules) == 1
    rule = cfg.alert_rules[0]
    assert rule.pipeline == "etl_main"
    assert rule.metric == "error_rate"
    assert rule.operator == "gt"
    assert rule.threshold == pytest.approx(0.1)
    assert rule.label == "High errors"


def test_load_config_dirs(config_file: Path) -> None:
    cfg = load_config(config_file)
    assert cfg.history_dir == "/tmp/history"
    assert cfg.export_dir == "/tmp/exports"


def test_load_config_defaults(tmp_path: Path) -> None:
    p = tmp_path / "minimal.json"
    p.write_text(json.dumps({"pipelines": [{"name": "pipe1"}]}))
    cfg = load_config(p)
    assert cfg.pipelines[0].interval_seconds == 60
    assert cfg.pipelines[0].enabled is True
    assert cfg.pipelines[0].tags == []
    assert cfg.history_dir == ".pipewatch_history"
    assert cfg.export_dir == ".pipewatch_exports"


def test_load_config_file_not_found() -> None:
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/pipewatch.json")


def test_config_to_dict_roundtrip(config_file: Path) -> None:
    cfg = load_config(config_file)
    d = config_to_dict(cfg)
    assert d["pipelines"][0]["name"] == "etl_main"
    assert d["alert_rules"][0]["threshold"] == pytest.approx(0.1)
    assert d["history_dir"] == "/tmp/history"


def test_config_to_dict_empty() -> None:
    cfg = PipewatchConfig()
    d = config_to_dict(cfg)
    assert d["pipelines"] == []
    assert d["alert_rules"] == []
