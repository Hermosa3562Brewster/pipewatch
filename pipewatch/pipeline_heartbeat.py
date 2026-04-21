"""Pipeline heartbeat tracking — records periodic pings and detects missed beats."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class HeartbeatRecord:
    pipeline: str
    timestamp: datetime
    source: str = "default"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "HeartbeatRecord":
        return cls(
            pipeline=d["pipeline"],
            timestamp=datetime.fromisoformat(d["timestamp"]),
            source=d.get("source", "default"),
        )


@dataclass
class HeartbeatStatus:
    pipeline: str
    last_beat: Optional[datetime]
    expected_interval: timedelta
    missed: bool
    seconds_since: Optional[float]

    def summary(self) -> str:
        state = "MISSED" if self.missed else "OK"
        if self.seconds_since is None:
            return f"{self.pipeline}: {state} (never seen)"
        return f"{self.pipeline}: {state} ({self.seconds_since:.0f}s ago)"


class HeartbeatTracker:
    """Tracks heartbeats for multiple pipelines and detects missed beats."""

    def __init__(self, max_per_pipeline: int = 100) -> None:
        self._beats: Dict[str, List[HeartbeatRecord]] = {}
        self._intervals: Dict[str, timedelta] = {}
        self._max = max_per_pipeline

    def configure(self, pipeline: str, interval_seconds: float) -> None:
        self._intervals[pipeline] = timedelta(seconds=interval_seconds)
        if pipeline not in self._beats:
            self._beats[pipeline] = []

    def beat(self, pipeline: str, source: str = "default", at: Optional[datetime] = None) -> HeartbeatRecord:
        ts = at or datetime.utcnow()
        record = HeartbeatRecord(pipeline=pipeline, timestamp=ts, source=source)
        self._beats.setdefault(pipeline, []).append(record)
        if len(self._beats[pipeline]) > self._max:
            self._beats[pipeline] = self._beats[pipeline][-self._max:]
        return record

    def last_beat(self, pipeline: str) -> Optional[HeartbeatRecord]:
        beats = self._beats.get(pipeline, [])
        return beats[-1] if beats else None

    def check(self, pipeline: str, now: Optional[datetime] = None) -> Optional[HeartbeatStatus]:
        if pipeline not in self._intervals:
            return None
        now = now or datetime.utcnow()
        interval = self._intervals[pipeline]
        last = self.last_beat(pipeline)
        if last is None:
            return HeartbeatStatus(pipeline=pipeline, last_beat=None,
                                   expected_interval=interval, missed=True, seconds_since=None)
        diff = (now - last.timestamp).total_seconds()
        missed = diff > interval.total_seconds()
        return HeartbeatStatus(pipeline=pipeline, last_beat=last.timestamp,
                               expected_interval=interval, missed=missed, seconds_since=diff)

    def check_all(self, now: Optional[datetime] = None) -> List[HeartbeatStatus]:
        return [self.check(p, now=now) for p in self._intervals if self.check(p, now=now) is not None]

    def pipelines(self) -> List[str]:
        return list(self._intervals.keys())
