"""Pipeline fingerprinting: detect structural changes in pipeline runs."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class FingerprintRecord:
    pipeline: str
    fingerprint: str
    recorded_at: str
    metadata: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "fingerprint": self.fingerprint,
            "recorded_at": self.recorded_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "FingerprintRecord":
        return cls(
            pipeline=d["pipeline"],
            fingerprint=d["fingerprint"],
            recorded_at=d["recorded_at"],
            metadata=d.get("metadata", {}),
        )

    def summary(self) -> str:
        return f"{self.pipeline} [{self.fingerprint[:8]}] @ {self.recorded_at}"


def compute_fingerprint(data: dict) -> str:
    """Compute a stable SHA-256 fingerprint from a dict."""
    serialised = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialised.encode()).hexdigest()


class FingerprintTracker:
    def __init__(self, max_per_pipeline: int = 50) -> None:
        self._max = max_per_pipeline
        self._records: Dict[str, List[FingerprintRecord]] = {}

    def record(self, pipeline: str, data: dict, metadata: Optional[Dict[str, str]] = None) -> FingerprintRecord:
        fp = compute_fingerprint(data)
        rec = FingerprintRecord(
            pipeline=pipeline,
            fingerprint=fp,
            recorded_at=datetime.utcnow().isoformat(),
            metadata=metadata or {},
        )
        bucket = self._records.setdefault(pipeline, [])
        bucket.append(rec)
        if len(bucket) > self._max:
            bucket.pop(0)
        return rec

    def latest(self, pipeline: str) -> Optional[FingerprintRecord]:
        bucket = self._records.get(pipeline, [])
        return bucket[-1] if bucket else None

    def has_changed(self, pipeline: str, data: dict) -> bool:
        """Return True if fingerprint differs from the most recent record."""
        last = self.latest(pipeline)
        if last is None:
            return True
        return last.fingerprint != compute_fingerprint(data)

    def history(self, pipeline: str, n: int = 10) -> List[FingerprintRecord]:
        return self._records.get(pipeline, [])[-n:]

    def all_pipelines(self) -> List[str]:
        return list(self._records.keys())
