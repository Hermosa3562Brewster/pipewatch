"""Pipeline embargo: suppress runs during defined time windows."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Dict, List, Optional


@dataclass
class EmbargoWindow:
    """A time window during which a pipeline should not run."""

    pipeline: str
    start_time: time  # wall-clock start (UTC)
    end_time: time    # wall-clock end   (UTC)
    reason: str = ""
    enabled: bool = True

    def is_active(self, at: Optional[datetime] = None) -> bool:
        """Return True if *at* falls inside this embargo window."""
        if not self.enabled:
            return False
        now = (at or datetime.utcnow()).time().replace(tzinfo=None)
        start = self.start_time.replace(tzinfo=None)
        end = self.end_time.replace(tzinfo=None)
        if start <= end:
            return start <= now < end
        # overnight window e.g. 22:00 – 06:00
        return now >= start or now < end

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "reason": self.reason,
            "enabled": self.enabled,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "EmbargoWindow":
        return cls(
            pipeline=data["pipeline"],
            start_time=time.fromisoformat(data["start_time"]),
            end_time=time.fromisoformat(data["end_time"]),
            reason=data.get("reason", ""),
            enabled=data.get("enabled", True),
        )

    def summary(self) -> str:
        tag = "active" if self.enabled else "disabled"
        return (
            f"{self.pipeline}: {self.start_time.isoformat()}–"
            f"{self.end_time.isoformat()} [{tag}]"
            + (f" — {self.reason}" if self.reason else "")
        )


class EmbargoManager:
    """Manage embargo windows across pipelines."""

    def __init__(self) -> None:
        self._windows: Dict[str, List[EmbargoWindow]] = {}

    def add(self, window: EmbargoWindow) -> None:
        self._windows.setdefault(window.pipeline, []).append(window)

    def remove(self, pipeline: str, index: int) -> None:
        windows = self._windows.get(pipeline, [])
        if 0 <= index < len(windows):
            windows.pop(index)

    def windows_for(self, pipeline: str) -> List[EmbargoWindow]:
        return list(self._windows.get(pipeline, []))

    def is_embargoed(self, pipeline: str, at: Optional[datetime] = None) -> bool:
        return any(w.is_active(at) for w in self._windows.get(pipeline, []))

    def all_pipelines(self) -> List[str]:
        return list(self._windows.keys())
