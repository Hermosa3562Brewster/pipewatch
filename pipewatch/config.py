"""Configuration loader for pipewatch pipelines and alert rules."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class PipelineConfig:
    name: str
    interval_seconds: int = 60
    enabled: bool = True
    tags: list[str] = field(default_factory=list)


@dataclass
class AlertRuleConfig:
    pipeline: str
    metric: str
    operator: str
    threshold: float
    label: str = ""


@dataclass
class PipewatchConfig:
    pipelines: list[PipelineConfig] = field(default_factory=list)
    alert_rules: list[AlertRuleConfig] = field(default_factory=list)
    history_dir: str = ".pipewatch_history"
    export_dir: str = ".pipewatch_exports"


def _parse_pipeline(raw: dict[str, Any]) -> PipelineConfig:
    return PipelineConfig(
        name=raw["name"],
        interval_seconds=int(raw.get("interval_seconds", 60)),
        enabled=bool(raw.get("enabled", True)),
        tags=list(raw.get("tags", [])),
    )


def _parse_alert_rule(raw: dict[str, Any]) -> AlertRuleConfig:
    return AlertRuleConfig(
        pipeline=raw["pipeline"],
        metric=raw["metric"],
        operator=raw["operator"],
        threshold=float(raw["threshold"]),
        label=raw.get("label", ""),
    )


def load_config(path: str | Path) -> PipewatchConfig:
    """Load a pipewatch JSON config file and return a PipewatchConfig."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with p.open() as fh:
        raw = json.load(fh)
    pipelines = [_parse_pipeline(r) for r in raw.get("pipelines", [])]
    alert_rules = [_parse_alert_rule(r) for r in raw.get("alert_rules", [])]
    return PipewatchConfig(
        pipelines=pipelines,
        alert_rules=alert_rules,
        history_dir=raw.get("history_dir", ".pipewatch_history"),
        export_dir=raw.get("export_dir", ".pipewatch_exports"),
    )


def config_to_dict(cfg: PipewatchConfig) -> dict[str, Any]:
    """Serialize a PipewatchConfig back to a plain dictionary."""
    return {
        "pipelines": [
            {
                "name": p.name,
                "interval_seconds": p.interval_seconds,
                "enabled": p.enabled,
                "tags": p.tags,
            }
            for p in cfg.pipelines
        ],
        "alert_rules": [
            {
                "pipeline": a.pipeline,
                "metric": a.metric,
                "operator": a.operator,
                "threshold": a.threshold,
                "label": a.label,
            }
            for a in cfg.alert_rules
        ],
        "history_dir": cfg.history_dir,
        "export_dir": cfg.export_dir,
    }
