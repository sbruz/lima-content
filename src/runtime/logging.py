from __future__ import annotations

import contextvars
import pathlib
import sys
import threading
import uuid
from contextlib import contextmanager
from functools import wraps
from typing import Any, Callable, Optional

from loguru import logger as _base_logger

from src.config import LoggingSettings

_correlation_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "correlation_id", default=None
)
_job_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "job_id", default=None
)
_step_name: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "step_name", default=None
)
_attempt_no: contextvars.ContextVar[int] = contextvars.ContextVar(
    "attempt_no", default=1
)


def setup_logging(settings: LoggingSettings) -> None:
    """Configure loguru sinks for console + structured file output."""
    _base_logger.remove()

    console_format = (
        "<green>{time:HH:mm:ss}</green> "
        "<level>{level.icon} {level.name:<6}</level> "
        "{extra[context]:<32} "
        "<level>{message}</level>"
    )

    def _default_extra(record: dict[str, Any]) -> None:
        step = _step_name.get() or "-"
        job_id = _job_id.get() or "-"
        correlation = _correlation_id.get() or "-"
        thread_name = threading.current_thread().name

        record["extra"].setdefault("step", step)
        record["extra"].setdefault("job_id", job_id)
        record["extra"].setdefault("correlation_id", correlation)
        record["extra"].setdefault("thread", thread_name)
        record["extra"].setdefault("attempt", _attempt_no.get())

        job_display = "-"
        if job_id != "-":
            job_display = job_id.split(":")[-1]

        corr_display = "-"
        if correlation != "-":
            corr_display = correlation[:6]

        thread_display = ""
        if thread_name != "MainThread":
            thread_display = thread_name.replace("ThreadPoolExecutor-", "T")

        attempt_value = _attempt_no.get()

        context_parts = []
        if step != "-":
            context_parts.append(step)
        if job_display and job_display != "-":
            context_parts.append(f"#{job_display}")
        if corr_display != "-":
            context_parts.append(f"@{corr_display}")
        if attempt_value:
            context_parts.append(f"A{attempt_value}")
        if thread_display:
            context_parts.append(f"[{thread_display}]")

        record["extra"]["context"] = " ".join(context_parts) if context_parts else "-"

    _base_logger.configure(patcher=_default_extra)
    _base_logger.add(
        sys.stderr,
        level=settings.level.upper(),
        format=console_format,
        filter=_console_filter,
    )

    log_path = pathlib.Path(settings.file).expanduser()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    _purge_old_logs(log_path)
    _base_logger.add(
        log_path,
        level=settings.level.upper(),
        rotation="10 MB",
        compression="zip",
        enqueue=True,
        serialize=True,
    )


logger = _base_logger


@contextmanager
def log_context(step: str, job_identifier: Optional[str]) -> Any:
    """Context manager to bind step + job id for the current log scope."""
    step_token = _step_name.set(step)
    job_token = _job_id.set(job_identifier)
    try:
        yield logger.bind(step=step, job_id=job_identifier)
    finally:
        _step_name.reset(step_token)
        _job_id.reset(job_token)


def new_correlation_id() -> str:
    return uuid.uuid4().hex


@contextmanager
def correlation_scope(correlation_id: Optional[str] = None) -> Any:
    token = _correlation_id.set(correlation_id or new_correlation_id())
    try:
        yield _correlation_id.get()
    finally:
        _correlation_id.reset(token)


def with_correlation_id(
    resolver: Optional[Callable[..., Optional[str]]] = None,
) -> Callable:
    """Decorator to auto-manage correlation id around the wrapped callable."""

    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            corr = resolver(*args, **kwargs) if resolver else None
            with correlation_scope(corr):
                return fn(*args, **kwargs)

        return wrapper

    return decorator


@contextmanager
def attempt_scope(attempt: int) -> Any:
    token = _attempt_no.set(max(1, attempt))
    try:
        yield
    finally:
        _attempt_no.reset(token)


def _purge_old_logs(log_path: pathlib.Path) -> None:
    """Delete previous log files (including rotations) before a new run."""
    try:
        for candidate in log_path.parent.glob(f"{log_path.name}*"):
            if candidate.is_file():
                try:
                    candidate.unlink()
                except OSError:
                    pass
    except FileNotFoundError:
        # Directory does not exist yet
        pass


def _console_filter(record: dict[str, Any]) -> bool:
    """Filter console logs to show only business events."""
    module = record.get("module", "")
    func = record.get("function", "")

    # Business-critical modules/functions
    business_points = {
        ("step1_localize_categories", "process"),
        ("runner", "run"),
        ("runner", "_process_job"),
        ("runner", "_execute_job"),
        ("main", "main"),
    }
    if (module, func) in business_points:
        return True

    # Allow explicit business markers
    message = record.get("message", "")
    if message.startswith("[BUSINESS]") or message.startswith("[PROG]"):
        return True

    return False


__all__ = [
    "logger",
    "setup_logging",
    "log_context",
    "with_correlation_id",
    "new_correlation_id",
    "correlation_scope",
    "attempt_scope",
]
