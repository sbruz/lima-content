from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class ProgressSnapshot:
    total: int
    completed: int
    failed: int
    skipped: int
    in_progress: int
    elapsed_sec: float
    eta_sec: Optional[float]


class Progress:
    """Thread-safe progress tracker with ETA calculation."""

    def __init__(self, step_name: str) -> None:
        self.step_name = step_name
        self._total = 0
        self._completed = 0
        self._failed = 0
        self._skipped = 0
        self._in_progress = 0
        self._start_ts: Optional[float] = None
        self._lock = threading.Lock()

    def start(self, total: int) -> None:
        with self._lock:
            self._total = total
            self._completed = 0
            self._failed = 0
            self._skipped = 0
            self._in_progress = 0
            self._start_ts = time.monotonic()

    def begin_job(self) -> None:
        with self._lock:
            self._in_progress += 1

    def mark_completed(self) -> None:
        with self._lock:
            self._completed += 1
            self._in_progress = max(0, self._in_progress - 1)

    def mark_failed(self) -> None:
        with self._lock:
            self._failed += 1
            self._in_progress = max(0, self._in_progress - 1)

    def mark_skipped(self) -> None:
        with self._lock:
            self._skipped += 1
            self._in_progress = max(0, self._in_progress - 1)

    def finish(self) -> None:
        with self._lock:
            self._in_progress = 0

    def snapshot(self) -> ProgressSnapshot:
        with self._lock:
            elapsed = 0.0
            if self._start_ts is not None:
                elapsed = time.monotonic() - self._start_ts
            processed = self._completed + self._failed + self._skipped
            rate = processed / elapsed if elapsed and processed else 0.0
            remaining = max(0, self._total - processed)
            eta = (remaining / rate) if rate else None

            return ProgressSnapshot(
                total=self._total,
                completed=self._completed,
                failed=self._failed,
                skipped=self._skipped,
                in_progress=self._in_progress,
                elapsed_sec=elapsed,
                eta_sec=eta,
            )

    def as_dict(self) -> Dict[str, Optional[float]]:
        snap = self.snapshot()
        return {
            "total": snap.total,
            "completed": snap.completed,
            "failed": snap.failed,
            "skipped": snap.skipped,
            "in_progress": snap.in_progress,
            "elapsed_sec": snap.elapsed_sec,
            "eta_sec": snap.eta_sec,
        }


__all__ = ["Progress", "ProgressSnapshot"]
