from __future__ import annotations

from typing import Any, Callable, Iterable, List, Protocol

from .jobs import Job


HookFn = Callable[[Job], None]


class HookRegistry:
    """Registry of callbacks executed before and after each job."""

    def __init__(
        self,
        before_job: Iterable[HookFn] | None = None,
        after_job: Iterable[HookFn] | None = None,
        on_error: Iterable[Callable[[Job, Exception], None]] | None = None,
    ) -> None:
        self._before_job: List[HookFn] = list(before_job or [])
        self._after_job: List[HookFn] = list(after_job or [])
        self._on_error: List[Callable[[Job, Exception], None]] = list(on_error or [])

    def register_before(self, fn: HookFn) -> None:
        self._before_job.append(fn)

    def register_after(self, fn: HookFn) -> None:
        self._after_job.append(fn)

    def register_error(self, fn: Callable[[Job, Exception], None]) -> None:
        self._on_error.append(fn)

    def run_before(self, job: Job) -> None:
        for fn in self._before_job:
            fn(job)

    def run_after(self, job: Job) -> None:
        for fn in self._after_job:
            fn(job)

    def run_error(self, job: Job, error: Exception) -> None:
        for fn in self._on_error:
            fn(job, error)


__all__ = ["HookRegistry", "HookFn"]
