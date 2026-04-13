"""Snapshot utilities for capturing and comparing pipeline states."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from pipewatch.metrics import PipelineMetrics


@dataclass
class PipelineSnapshot:
    """Immutable point-in-time capture of a pipeline's metrics."""

    name: str
    status: str
    total_runs: int
    successes: int
    failures: int
    error_rate: float
    avg_duration: float
    captured_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status,
            "total_runs": self.total_runs,
            "successes": self.successes,
            "failures": self.failures,
            "error_rate": round(self.error_rate, 4),
            "avg_duration": round(self.avg_duration, 4),
            "captured_at": self.captured_at.isoformat(),
        }


def snapshot_from_metrics(metrics: PipelineMetrics) -> PipelineSnapshot:
    """Create a PipelineSnapshot from a live PipelineMetrics instance."""
    return PipelineSnapshot(
        name=metrics.name,
        status=metrics.status,
        total_runs=metrics.total_runs,
        successes=metrics.successes,
        failures=metrics.failures,
        error_rate=metrics.error_rate(),
        avg_duration=metrics.avg_duration(),
    )


def snapshot_map(metrics_map: Dict[str, PipelineMetrics]) -> Dict[str, PipelineSnapshot]:
    """Snapshot all pipelines in a metrics map."""
    return {name: snapshot_from_metrics(m) for name, m in metrics_map.items()}


def diff_snapshots(
    before: PipelineSnapshot, after: PipelineSnapshot
) -> Dict[str, dict]:
    """Return a dict of fields that changed between two snapshots."""
    fields = ["status", "total_runs", "successes", "failures", "error_rate", "avg_duration"]
    changes = {}
    for f in fields:
        v_before = getattr(before, f)
        v_after = getattr(after, f)
        if v_before != v_after:
            changes[f] = {"before": v_before, "after": v_after}
    return changes


def snapshots_with_changes(
    before_map: Dict[str, PipelineSnapshot],
    after_map: Dict[str, PipelineSnapshot],
) -> List[Dict]:
    """Return list of pipelines that changed between two snapshot maps."""
    results = []
    for name, after in after_map.items():
        before = before_map.get(name)
        if before is None:
            results.append({"name": name, "new": True, "changes": {}})
        else:
            changes = diff_snapshots(before, after)
            if changes:
                results.append({"name": name, "new": False, "changes": changes})
    return results
