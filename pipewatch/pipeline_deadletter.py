"""Dead-letter queue tracking for failed pipeline records."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional


@dataclass
class DeadLetterEntry:
    pipeline: str
    reason: str
    payload: str
    failed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    retryable: bool = True

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "reason": self.reason,
            "payload": self.payload,
            "failed_at": self.failed_at.isoformat(),
            "retryable": self.retryable,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DeadLetterEntry":
        return cls(
            pipeline=data["pipeline"],
            reason=data["reason"],
            payload=data["payload"],
            failed_at=datetime.fromisoformat(data["failed_at"]),
            retryable=data.get("retryable", True),
        )

    def summary(self) -> str:
        tagryable]" if self.retryable else "[dead]"
        ts = self.failed_at.strftime("%Y-%m-%d %H:%M:%S")
        return f"{tag} {self.pipeline} @ {ts}: {self.reason}"


class DeadLetterQueue:
    """Per-pipeline dead-letter queue with optional capacity cap."""

    def __init__(self, max_per_pipeline: int = 100) -> None:
        self._max = max_per_pipeline
        self._store: Dict[str, List[DeadLetterEntry]] = {}

    def push(self, entry: DeadLetterEntry) -> None:
        bucket = self._store.setdefault(entry.pipeline, [])
        bucket.append(entry)
        if len(bucket) > self._max:
            bucket.pop(0)

    def get(self, pipeline: str) -> List[DeadLetterEntry]:
        return list(self._store.get(pipeline, []))

    def get_retryable(self, pipeline: str) -> List[DeadLetterEntry]:
        return [e for e in self.get(pipeline) if e.retryable]

    def purge(self, pipeline: str) -> int:
        removed = len(self._store.pop(pipeline, []))
        return removed

    def all_pipelines(self) -> List[str]:
        return list(self._store.keys())

    def total_count(self) -> int:
        return sum(len(v) for v in self._store.values())

    def counts(self) -> Dict[str, int]:
        return {k: len(v) for k, v in self._store.items()}
