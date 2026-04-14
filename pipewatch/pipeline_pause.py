"""Pause and resume tracking for pipelines."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional


@dataclass
class PauseRecord:
    pipeline: str
    paused_at: datetime
    resumed_at: Optional[datetime] = None
    reason: str = ""

    def is_active(self) -> bool:
        return self.resumed_at is None

    def duration_seconds(self) -> Optional[float]:
        if self.resumed_at is None:
            return None
        return (self.resumed_at - self.paused_at).total_seconds()

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "paused_at": self.paused_at.isoformat(),
            "resumed_at": self.resumed_at.isoformat() if self.resumed_at else None,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PauseRecord":
        resumed_raw = data.get("resumed_at")
        return cls(
            pipeline=data["pipeline"],
            paused_at=datetime.fromisoformat(data["paused_at"]),
            resumed_at=datetime.fromisoformat(resumed_raw) if resumed_raw else None,
            reason=data.get("reason", ""),
        )


class PauseManager:
    def __init__(self) -> None:
        self._records: Dict[str, List[PauseRecord]] = {}

    def pause(self, pipeline: str, reason: str = "") -> PauseRecord:
        if self.is_paused(pipeline):
            raise ValueError(f"Pipeline '{pipeline}' is already paused.")
        record = PauseRecord(
            pipeline=pipeline,
            paused_at=datetime.now(timezone.utc),
            reason=reason,
        )
        self._records.setdefault(pipeline, []).append(record)
        return record

    def resume(self, pipeline: str) -> PauseRecord:
        record = self._active_record(pipeline)
        if record is None:
            raise ValueError(f"Pipeline '{pipeline}' is not paused.")
        record.resumed_at = datetime.now(timezone.utc)
        return record

    def is_paused(self, pipeline: str) -> bool:
        return self._active_record(pipeline) is not None

    def _active_record(self, pipeline: str) -> Optional[PauseRecord]:
        for rec in reversed(self._records.get(pipeline, [])):
            if rec.is_active():
                return rec
        return None

    def history(self, pipeline: str) -> List[PauseRecord]:
        return list(self._records.get(pipeline, []))

    def all_paused(self) -> List[str]:
        return [p for p in self._records if self.is_paused(p)]
