"""Track a changelog of significant state changes per pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional


@dataclass
class ChangeEntry:
    pipeline: str
    change_type: str          # e.g. 'status_change', 'threshold_crossed', 'config_update'
    description: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "change_type": self.change_type,
            "description": self.description,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ChangeEntry":
        return cls(
            pipeline=data["pipeline"],
            change_type=data["change_type"],
            description=data["description"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            metadata=data.get("metadata", {}),
        )

    def summary(self) -> str:
        ts = self.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        return f"[{ts}] {self.pipeline} | {self.change_type}: {self.description}"


VALID_CHANGE_TYPES = {"status_change", "threshold_crossed", "config_update", "note_added", "alert_fired"}


class Changelog:
    def __init__(self, max_per_pipeline: int = 50) -> None:
        self._entries: Dict[str, List[ChangeEntry]] = {}
        self._max = max_per_pipeline

    def record(self, pipeline: str, change_type: str, description: str,
               metadata: Optional[Dict[str, str]] = None) -> ChangeEntry:
        if change_type not in VALID_CHANGE_TYPES:
            raise ValueError(f"Unknown change_type '{change_type}'. Valid: {VALID_CHANGE_TYPES}")
        entry = ChangeEntry(
            pipeline=pipeline,
            change_type=change_type,
            description=description,
            metadata=metadata or {},
        )
        bucket = self._entries.setdefault(pipeline, [])
        bucket.append(entry)
        if len(bucket) > self._max:
            self._entries[pipeline] = bucket[-self._max:]
        return entry

    def get(self, pipeline: str, last_n: Optional[int] = None) -> List[ChangeEntry]:
        entries = self._entries.get(pipeline, [])
        return entries[-last_n:] if last_n else list(entries)

    def all_entries(self, last_n: Optional[int] = None) -> List[ChangeEntry]:
        combined = sorted(
            (e for bucket in self._entries.values() for e in bucket),
            key=lambda e: e.timestamp,
        )
        return combined[-last_n:] if last_n else combined

    def pipelines(self) -> List[str]:
        return list(self._entries.keys())
