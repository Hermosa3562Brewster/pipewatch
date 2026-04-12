"""Helpers to build PipewatchConfig objects from existing runtime state."""

from __future__ import annotations

from pipewatch.alerts import AlertManager, AlertRule
from pipewatch.config import (
    AlertRuleConfig,
    PipelineConfig,
    PipewatchConfig,
    config_to_dict,
)
from pipewatch.metrics import PipelineMetrics
import json
from pathlib import Path


def pipelines_from_metrics(
    metrics: list[PipelineMetrics],
    interval_seconds: int = 60,
) -> list[PipelineConfig]:
    """Create PipelineConfig entries from a list of PipelineMetrics objects."""
    return [
        PipelineConfig(
            name=m.name,
            interval_seconds=interval_seconds,
            enabled=True,
        )
        for m in metrics
    ]


def alert_rules_from_manager(
    pipeline_name: str,
    manager: AlertManager,
) -> list[AlertRuleConfig]:
    """Extract AlertRuleConfig entries from an AlertManager for a given pipeline."""
    rules: list[AlertRuleConfig] = []
    for rule in manager.rules:
        rules.append(
            AlertRuleConfig(
                pipeline=pipeline_name,
                metric=rule.metric,
                operator=rule.operator,
                threshold=rule.threshold,
                label=rule.description(),
            )
        )
    return rules


def build_config(
    metrics: list[PipelineMetrics],
    managers: dict[str, AlertManager] | None = None,
    history_dir: str = ".pipewatch_history",
    export_dir: str = ".pipewatch_exports",
    interval_seconds: int = 60,
) -> PipewatchConfig:
    """Build a full PipewatchConfig from runtime objects."""
    pipelines = pipelines_from_metrics(metrics, interval_seconds=interval_seconds)
    alert_rules: list[AlertRuleConfig] = []
    if managers:
        for pipeline_name, manager in managers.items():
            alert_rules.extend(alert_rules_from_manager(pipeline_name, manager))
    return PipewatchConfig(
        pipelines=pipelines,
        alert_rules=alert_rules,
        history_dir=history_dir,
        export_dir=export_dir,
    )


def save_config(cfg: PipewatchConfig, path: str | Path) -> None:
    """Serialize a PipewatchConfig to a JSON file."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as fh:
        json.dump(config_to_dict(cfg), fh, indent=2)
