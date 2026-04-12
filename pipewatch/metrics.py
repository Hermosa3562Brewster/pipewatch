"""Core metrics model for tracking ETL pipeline health and throughput."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class PipelineMetrics:
    """Snapshot of metrics for a single ETL pipeline."""

    name: str
    records_processed: int = 0
    records_failed: int = 0
    bytes_transferred: int = 0
    started_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    status: str = "idle"  # idle | running | error | completed

    def record_success(self, count: int, bytes_count: int = 0) -> None:
        """Register successfully processed records."""
        self.records_processed += count
        self.bytes_transferred += bytes_count
        self.last_updated = datetime.utcnow()
        self.status = "running"

    def record_failure(self, count: int) -> None:
        """Register failed records."""
        self.records_failed += count
        self.last_updated = datetime.utcnow()
        self.status = "error"

    def start(self) -> None:
        """Mark the pipeline as started."""
        self.started_at = datetime.utcnow()
        self.last_updated = self.started_at
        self.status = "running"

    def complete(self) -> None:
        """Mark the pipeline as completed."""
        self.last_updated = datetime.utcnow()
        self.status = "completed"

    @property
    def error_rate(self) -> float:
        """Return the fraction of failed records (0.0 – 1.0)."""
        total = self.records_processed + self.records_failed
        return self.records_failed / total if total > 0 else 0.0

    @property
    def throughput(self) -> float:
        """Records per second since the pipeline started."""
        if self.started_at is None or self.records_processed == 0:
            return 0.0
        elapsed = (datetime.utcnow() - self.started_at).total_seconds()
        return self.records_processed / elapsed if elapsed > 0 else 0.0

    def to_dict(self) -> dict:
        """Serialise metrics to a plain dictionary."""
        return {
            "name": self.name,
            "status": self.status,
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "bytes_transferred": self.bytes_transferred,
            "error_rate": round(self.error_rate, 4),
            "throughput_rps": round(self.throughput, 2),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
        }
