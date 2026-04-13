"""Persistent log of fired alerts for post-mortem analysis."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class FiredAlert:
    pipeline: str
    rule_name: str
    metric: str
    operator: str
    threshold: float
    actual_value: float
    fired_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "rule_name": self.rule_name,
            "metric": self.metric,
            "operator": self.operator,
            "threshold": self.threshold,
            "actual_value": self.actual_value,
            "fired_at": self.fired_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FiredAlert":
        return cls(
            pipeline=d["pipeline"],
            rule_name=d["rule_name"],
            metric=d["metric"],
            operator=d["operator"],
            threshold=float(d["threshold"]),
            actual_value=float(d["actual_value"]),
            fired_at=datetime.fromisoformat(d["fired_at"]),
        )

    def summary(self) -> str:
        return (
            f"[{self.fired_at.strftime('%Y-%m-%d %H:%M:%S')}] "
            f"{self.pipeline} | {self.rule_name}: "
            f"{self.metric} {self.operator} {self.threshold} "
            f"(actual={self.actual_value:.4f})"
        )


class AlertsLog:
    """In-memory log of FiredAlert entries with optional cap."""

    def __init__(self, max_entries: int = 500) -> None:
        self._entries: List[FiredAlert] = []
        self.max_entries = max_entries

    def record(self, alert: FiredAlert) -> None:
        self._entries.append(alert)
        if len(self._entries) > self.max_entries:
            self._entries = self._entries[-self.max_entries :]

    def get_all(self) -> List[FiredAlert]:
        return list(self._entries)

    def get_for_pipeline(self, pipeline: str) -> List[FiredAlert]:
        return [e for e in self._entries if e.pipeline == pipeline]

    def get_last_n(self, n: int) -> List[FiredAlert]:
        return self._entries[-n:]

    def clear(self) -> None:
        self._entries.clear()

    def count(self) -> int:
        return len(self._entries)

    def pipelines_with_alerts(self) -> List[str]:
        seen: List[str] = []
        for e in self._entries:
            if e.pipeline not in seen:
                seen.append(e.pipeline)
        return seen
