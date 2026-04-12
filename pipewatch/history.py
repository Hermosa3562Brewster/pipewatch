"""Pipeline run history tracking and persistence."""

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import List, Optional

DEFAULT_HISTORY_FILE = ".pipewatch_history.json"


@dataclass
class RunRecord:
    pipeline_name: str
    started_at: str
    completed_at: Optional[str]
    status: str
    total_records: int
    success_count: int
    failure_count: int
    error_rate: float
    duration_seconds: Optional[float]

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> "RunRecord":
        return RunRecord(**data)


@dataclass
class PipelineHistory:
    pipeline_name: str
    runs: List[RunRecord] = field(default_factory=list)

    def add_run(self, record: RunRecord) -> None:
        self.runs.append(record)

    def last_n_runs(self, n: int = 10) -> List[RunRecord]:
        return self.runs[-n:]

    def average_error_rate(self) -> float:
        if not self.runs:
            return 0.0
        return sum(r.error_rate for r in self.runs) / len(self.runs)

    def average_duration(self) -> Optional[float]:
        durations = [r.duration_seconds for r in self.runs if r.duration_seconds is not None]
        if not durations:
            return None
        return sum(durations) / len(durations)


def load_history(path: str = DEFAULT_HISTORY_FILE) -> dict:
    """Load history from JSON file. Returns dict keyed by pipeline name."""
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        raw = json.load(f)
    result = {}
    for name, data in raw.items():
        runs = [RunRecord.from_dict(r) for r in data.get("runs", [])]
        result[name] = PipelineHistory(pipeline_name=name, runs=runs)
    return result


def save_history(histories: dict, path: str = DEFAULT_HISTORY_FILE) -> None:
    """Persist history dict to JSON file."""
    serializable = {
        name: {"runs": [r.to_dict() for r in h.runs]}
        for name, h in histories.items()
    }
    with open(path, "w") as f:
        json.dump(serializable, f, indent=2)


def record_run(metrics, path: str = DEFAULT_HISTORY_FILE) -> None:
    """Append a completed PipelineMetrics snapshot to persistent history."""
    from pipewatch.metrics import PipelineMetrics  # avoid circular

    duration = None
    if metrics.started_at and metrics.completed_at:
        fmt = "%Y-%m-%dT%H:%M:%S"
        try:
            start = datetime.strptime(metrics.started_at, fmt)
            end = datetime.strptime(metrics.completed_at, fmt)
            duration = (end - start).total_seconds()
        except ValueError:
            pass

    record = RunRecord(
        pipeline_name=metrics.pipeline_name,
        started_at=metrics.started_at or "",
        completed_at=metrics.completed_at,
        status=metrics.status,
        total_records=metrics.total_records,
        success_count=metrics.success_count,
        failure_count=metrics.failure_count,
        error_rate=metrics.error_rate(),
        duration_seconds=duration,
    )

    histories = load_history(path)
    if metrics.pipeline_name not in histories:
        histories[metrics.pipeline_name] = PipelineHistory(pipeline_name=metrics.pipeline_name)
    histories[metrics.pipeline_name].add_run(record)
    save_history(histories, path)
