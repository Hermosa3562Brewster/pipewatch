"""Track and report retry attempts for pipeline runs."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class RetryRecord:
    pipeline_name: str
    attempt: int
    timestamp: datetime
    reason: str = ""
    succeeded: bool = False

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "attempt": self.attempt,
            "timestamp": self.timestamp.isoformat(),
            "reason": self.reason,
            "succeeded": self.succeeded,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RetryRecord":
        return cls(
            pipeline_name=data["pipeline_name"],
            attempt=data["attempt"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            reason=data.get("reason", ""),
            succeeded=data.get("succeeded", False),
        )


@dataclass
class RetryTracker:
    _records: Dict[str, List[RetryRecord]] = field(default_factory=dict)
    max_per_pipeline: int = 50

    def record(self, pipeline_name: str, attempt: int, reason: str = "", succeeded: bool = False) -> RetryRecord:
        rec = RetryRecord(
            pipeline_name=pipeline_name,
            attempt=attempt,
            timestamp=datetime.utcnow(),
            reason=reason,
            succeeded=succeeded,
        )
        bucket = self._records.setdefault(pipeline_name, [])
        bucket.append(rec)
        if len(bucket) > self.max_per_pipeline:
            self._records[pipeline_name] = bucket[-self.max_per_pipeline :]
        return rec

    def get(self, pipeline_name: str, last_n: Optional[int] = None) -> List[RetryRecord]:
        records = self._records.get(pipeline_name, [])
        if last_n is not None:
            return records[-last_n:]
        return list(records)

    def total_retries(self, pipeline_name: str) -> int:
        return len(self._records.get(pipeline_name, []))

    def max_attempt(self, pipeline_name: str) -> int:
        records = self._records.get(pipeline_name, [])
        if not records:
            return 0
        return max(r.attempt for r in records)

    def success_rate(self, pipeline_name: str) -> float:
        records = self._records.get(pipeline_name, [])
        if not records:
            return 0.0
        return sum(1 for r in records if r.succeeded) / len(records)

    def clear(self, pipeline_name: str) -> None:
        self._records.pop(pipeline_name, None)

    def all_pipeline_names(self) -> List[str]:
        return list(self._records.keys())
