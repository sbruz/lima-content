from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Sequence, Tuple


@dataclass(frozen=True)
class Job:
    """Single unit of work (category, subcategory, coach, etc.)."""

    step: str
    payload: Dict[str, Any]
    entity_key: str

    @property
    def job_id(self) -> str:
        return f"{self.step}:{self.entity_key}"


def make_job(step: str, payload: Dict[str, Any], *, key_fields: Sequence[str]) -> Job:
    parts = [str(payload[field]) for field in key_fields if field in payload]
    entity_key = ":".join(parts) if parts else uuid.uuid4().hex
    return Job(step=step, payload=payload, entity_key=entity_key)


def deduplicate_jobs(jobs: Iterable[Job]) -> List[Job]:
    seen = {}
    for job in jobs:
        seen[job.entity_key] = job
    return list(seen.values())


def filter_by_range(
    items: Sequence[Any],
    range_pair: Tuple[int, int],
    key_getter: Callable[[Any], Any] = lambda x: x,
) -> List[Any]:
    """Filter items by 1-based inclusive range (end = -1 means until the end)."""
    start, end = range_pair
    start = max(1, start)
    filtered = []
    for index, item in enumerate(items, start=1):
        if index < start:
            continue
        if end != -1 and index > end:
            break
        filtered.append(item)
    return filtered


__all__ = ["Job", "make_job", "deduplicate_jobs", "filter_by_range"]
