"""Flap detection: identifies pipelines that oscillate rapidly between success and failure."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import RunRecord


@dataclass
class FlapReport:
    pipeline: str
    transitions: int        # number of status flips in the window
    flapping: bool
    last_states: List[str]  # most-recent-first ordered list of 'success'/'failure'
    threshold: int

    def summary(self) -> str:
        state = "FLAPPING" if self.flapping else "stable"
        return (
            f"{self.pipeline}: {state} "
            f"({self.transitions} transitions, threshold={self.threshold})"
        )


def _transitions(states: List[str]) -> int:
    """Count the number of adjacent state changes in *states* (oldest-first)."""
    if len(states) < 2:
        return 0
    return sum(1 for a, b in zip(states, states[1:]) if a != b)


def detect_flap(
    records: List[RunRecord],
    pipeline: str,
    window: int = 10,
    threshold: int = 4,
) -> Optional[FlapReport]:
    """Return a FlapReport for *pipeline* using the last *window* run records.

    Returns ``None`` when there are fewer than 2 records (not enough data).
    """
    recent = [
        r for r in sorted(records, key=lambda r: r.started_at, reverse=True)
        if r.pipeline == pipeline
    ][:window]

    if len(recent) < 2:
        return None

    # Reverse so we have oldest-first for transition counting
    states = [r.status for r in reversed(recent)]
    flips = _transitions(states)
    return FlapReport(
        pipeline=pipeline,
        transitions=flips,
        flapping=flips >= threshold,
        last_states=list(reversed(states)),  # most-recent-first for display
        threshold=threshold,
    )


def detect_flap_map(
    records: List[RunRecord],
    pipeline_names: List[str],
    window: int = 10,
    threshold: int = 4,
) -> dict:
    """Return a mapping of pipeline name -> FlapReport for all named pipelines."""
    result = {}
    for name in pipeline_names:
        report = detect_flap(records, name, window=window, threshold=threshold)
        if report is not None:
            result[name] = report
    return result
