from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional

from src.config import RateLimit


@dataclass
class _Bucket:
    capacity: int
    refill_rate: float  # tokens per second
    tokens: float
    updated_at: float
    lock: threading.Lock


class RateLimiter:
    """Token-bucket based rate limiter supporting multiple named buckets."""

    def __init__(self, plan: Dict[str, RateLimit]):
        self._buckets: Dict[str, _Bucket] = {}
        now = time.monotonic()
        for name, cfg in plan.items():
            refill_rate = cfg.calls_per_minute / 60.0
            self._buckets[name] = _Bucket(
                capacity=cfg.burst,
                refill_rate=refill_rate,
                tokens=float(cfg.burst),
                updated_at=now,
                lock=threading.Lock(),
            )

    def __enter__(self) -> "RateLimiter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def acquire(self, bucket_name: str, weight: float = 1.0) -> None:
        bucket = self._buckets.get(bucket_name)
        if bucket is None:
            return

        while True:
            with bucket.lock:
                self._refill(bucket)
                if bucket.tokens >= weight:
                    bucket.tokens -= weight
                    return
                wait_time = (weight - bucket.tokens) / bucket.refill_rate

            time.sleep(max(wait_time, 0.05))

    def _refill(self, bucket: _Bucket) -> None:
        now = time.monotonic()
        elapsed = now - bucket.updated_at
        if elapsed <= 0:
            return
        bucket.tokens = min(
            bucket.capacity, bucket.tokens + elapsed * bucket.refill_rate
        )
        bucket.updated_at = now


__all__ = ["RateLimiter"]
