"""Track concurrent execution slots and contention for pipelines."""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional


@dataclass
class ConcurrencyConfig:
    max_slots: int = 1


@dataclass
class ConcurrencyStatus:
    pipeline: str
    max_slots: int
    active_slots: int
    total_acquired: int
    total_rejected: int

    @property
    def utilisation_pct(self) -> float:
        if self.max_slots == 0:
            return 0.0
        return round(self.active_slots / self.max_slots * 100, 1)

    @property
    def is_saturated(self) -> bool:
        return self.active_slots >= self.max_slots

    def summary(self) -> str:
        return (
            f"{self.pipeline}: {self.active_slots}/{self.max_slots} slots "
            f"({self.utilisation_pct}% utilised)"
        )


class ConcurrencyTracker:
    def __init__(self) -> None:
        self._configs: Dict[str, ConcurrencyConfig] = {}
        self._active: Dict[str, int] = {}
        self._acquired: Dict[str, int] = {}
        self._rejected: Dict[str, int] = {}

    def configure(self, pipeline: str, max_slots: int = 1) -> None:
        self._configs[pipeline] = ConcurrencyConfig(max_slots=max_slots)
        self._active.setdefault(pipeline, 0)
        self._acquired.setdefault(pipeline, 0)
        self._rejected.setdefault(pipeline, 0)

    def acquire(self, pipeline: str) -> bool:
        cfg = self._configs.get(pipeline)
        if cfg is None:
            return True
        if self._active.get(pipeline, 0) >= cfg.max_slots:
            self._rejected[pipeline] = self._rejected.get(pipeline, 0) + 1
            return False
        self._active[pipeline] = self._active.get(pipeline, 0) + 1
        self._acquired[pipeline] = self._acquired.get(pipeline, 0) + 1
        return True

    def release(self, pipeline: str) -> None:
        current = self._active.get(pipeline, 0)
        if current > 0:
            self._active[pipeline] = current - 1

    def status(self, pipeline: str) -> Optional[ConcurrencyStatus]:
        cfg = self._configs.get(pipeline)
        if cfg is None:
            return None
        return ConcurrencyStatus(
            pipeline=pipeline,
            max_slots=cfg.max_slots,
            active_slots=self._active.get(pipeline, 0),
            total_acquired=self._acquired.get(pipeline, 0),
            total_rejected=self._rejected.get(pipeline, 0),
        )

    def all_statuses(self) -> Dict[str, ConcurrencyStatus]:
        return {p: self.status(p) for p in self._configs}
