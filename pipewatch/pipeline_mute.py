"""Mute management for pipelines — suppress alerts for a pipeline for a given duration."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class MuteRecord:
    pipeline: str
    muted_at: datetime
    expires_at: datetime
    reason: str = ""

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        return self.muted_at <= now < self.expires_at

    def duration_seconds(self) -> float:
        return (self.expires_at - self.muted_at).total_seconds()

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "muted_at": self.muted_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "MuteRecord":
        return cls(
            pipeline=data["pipeline"],
            muted_at=datetime.fromisoformat(data["muted_at"]),
            expires_at=datetime.fromisoformat(data["expires_at"]),
            reason=data.get("reason", ""),
        )

    def summary(self) -> str:
        state = "active" if self.is_active() else "expired"
        return f"{self.pipeline} muted until {self.expires_at.isoformat()} [{state}] — {self.reason}"


class MuteManager:
    def __init__(self) -> None:
        self._records: Dict[str, List[MuteRecord]] = {}

    def mute(self, pipeline: str, duration_seconds: float, reason: str = "",
             now: Optional[datetime] = None) -> MuteRecord:
        now = now or datetime.utcnow()
        record = MuteRecord(
            pipeline=pipeline,
            muted_at=now,
            expires_at=now + timedelta(seconds=duration_seconds),
            reason=reason,
        )
        self._records.setdefault(pipeline, []).append(record)
        return record

    def unmute(self, pipeline: str) -> bool:
        """Expire all active mutes for a pipeline immediately."""
        records = self._records.get(pipeline, [])
        now = datetime.utcnow()
        changed = False
        for r in records:
            if r.is_active(now):
                r.expires_at = now
                changed = True
        return changed

    def is_muted(self, pipeline: str, now: Optional[datetime] = None) -> bool:
        now = now or datetime.utcnow()
        return any(r.is_active(now) for r in self._records.get(pipeline, []))

    def active_mute(self, pipeline: str, now: Optional[datetime] = None) -> Optional[MuteRecord]:
        now = now or datetime.utcnow()
        for r in reversed(self._records.get(pipeline, [])):
            if r.is_active(now):
                return r
        return None

    def history(self, pipeline: str) -> List[MuteRecord]:
        return list(self._records.get(pipeline, []))

    def all_pipelines(self) -> List[str]:
        return list(self._records.keys())
