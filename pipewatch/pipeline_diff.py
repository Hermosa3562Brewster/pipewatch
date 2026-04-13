"""Compute and render diffs between two pipeline snapshot maps."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from pipewatch.pipeline_snapshot import PipelineSnapshot, snapshot_map
from pipewatch.metrics import PipelineMetrics


@dataclass
class PipelineDiff:
    """Describes changes for a single pipeline between two snapshots."""

    name: str
    success_delta: int
    failure_delta: int
    error_rate_delta: float
    status_changed: bool
    prev_status: Optional[str]
    curr_status: Optional[str]

    @property
    def is_degraded(self) -> bool:
        """True if error rate increased or pipeline newly failed."""
        return self.error_rate_delta > 0 or (
            self.status_changed and self.curr_status == "failed"
        )

    @property
    def is_improved(self) -> bool:
        """True if error rate decreased or pipeline recovered."""
        return self.error_rate_delta < 0 or (
            self.status_changed
            and self.prev_status == "failed"
            and self.curr_status != "failed"
        )


def diff_snapshot_maps(
    before: Dict[str, PipelineSnapshot],
    after: Dict[str, PipelineSnapshot],
) -> List[PipelineDiff]:
    """Return a list of PipelineDiff for all pipelines present in *after*."""
    diffs: List[PipelineDiff] = []
    for name, curr in after.items():
        prev = before.get(name)
        if prev is None:
            prev_successes = 0
            prev_failures = 0
            prev_error_rate = 0.0
            prev_status = None
        else:
            prev_successes = prev.successes
            prev_failures = prev.failures
            prev_error_rate = prev.error_rate
            prev_status = prev.status

        diffs.append(
            PipelineDiff(
                name=name,
                success_delta=curr.successes - prev_successes,
                failure_delta=curr.failures - prev_failures,
                error_rate_delta=round(curr.error_rate - prev_error_rate, 4),
                status_changed=curr.status != prev_status,
                prev_status=prev_status,
                curr_status=curr.status,
            )
        )
    return diffs


def compute_diff(
    before: Dict[str, PipelineMetrics],
    after: Dict[str, PipelineMetrics],
) -> List[PipelineDiff]:
    """Convenience wrapper that accepts metrics maps directly."""
    return diff_snapshot_maps(snapshot_map(before), snapshot_map(after))
