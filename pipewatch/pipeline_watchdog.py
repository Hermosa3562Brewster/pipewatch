"""Watchdog: detect pipelines that have not run within an expected interval."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class WatchdogConfig:
    max_silence_seconds: float  # alert if no run started within this window

    @property
    def max_silence(self) -> timedelta:
        return timedelta(seconds=self.max_silence_seconds)


@dataclass
class WatchdogReport:
    pipeline: str
    last_started_at: Optional[datetime]
    silence_seconds: float
    threshold_seconds: float
    is_stale: bool

    def summary(self) -> str:
        state = "STALE" if self.is_stale else "OK"
        last = self.last_started_at.isoformat() if self.last_started_at else "never"
        return (
            f"[{state}] {self.pipeline}: last run {last}, "
            f"silent {self.silence_seconds:.0f}s / threshold {self.threshold_seconds:.0f}s"
        )


class PipelineWatchdog:
    def __init__(self) -> None:
        self._configs: Dict[str, WatchdogConfig] = {}
        self._last_seen: Dict[str, datetime] = {}

    def configure(self, pipeline: str, max_silence_seconds: float) -> None:
        self._configs[pipeline] = WatchdogConfig(max_silence_seconds)

    def heartbeat(self, pipeline: str, at: Optional[datetime] = None) -> None:
        """Record that a pipeline run was observed."""
        self._last_seen[pipeline] = at or datetime.utcnow()

    def check(self, pipeline: str, now: Optional[datetime] = None) -> Optional[WatchdogReport]:
        cfg = self._configs.get(pipeline)
        if cfg is None:
            return None
        now = now or datetime.utcnow()
        last = self._last_seen.get(pipeline)
        if last is None:
            silence = float("inf")
        else:
            silence = (now - last).total_seconds()
        is_stale = silence > cfg.max_silence_seconds
        return WatchdogReport(
            pipeline=pipeline,
            last_started_at=last,
            silence_seconds=silence if silence != float("inf") else -1,
            threshold_seconds=cfg.max_silence_seconds,
            is_stale=is_stale,
        )

    def check_all(self, now: Optional[datetime] = None) -> List[WatchdogReport]:
        now = now or datetime.utcnow()
        return [r for p in self._configs if (r := self.check(p, now)) is not None]

    def stale_pipelines(self, now: Optional[datetime] = None) -> List[WatchdogReport]:
        return [r for r in self.check_all(now) if r.is_stale]

    def configured_pipelines(self) -> List[str]:
        return list(self._configs.keys())
