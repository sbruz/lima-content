from __future__ import annotations

import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Optional, Protocol

from src.config import Config, RetrySettings

from .errors import FatalStepError, JobRetryExceededError
from .hooks import HookRegistry
from .jobs import Job
from .logging import attempt_scope, correlation_scope, log_context, logger
from .progress import Progress
from .rate_limit import RateLimiter
from .retrying import retry


class StepProtocol(Protocol):
    NAME: str

    def process(self, job: Job) -> None:  # pragma: no cover - protocol
        ...


class StepRunner:
    """Execute pipeline steps concurrently using thread pools."""

    def __init__(
        self,
        step_impl: StepProtocol,
        config: Config,
        rate_limiter: Optional[RateLimiter] = None,
        hooks: Optional[HookRegistry] = None,
    ) -> None:
        self.step_impl = step_impl
        self.step_name = getattr(step_impl, "NAME", step_impl.__class__.__name__)
        self.threads = config.threads
        self.progress = Progress(step_name=self.step_name)
        self.hooks = hooks or HookRegistry()
        self.rate_limiter = rate_limiter or RateLimiter(config.rate_limits)
        setattr(self.step_impl, "rate_limiter", self.rate_limiter)
        self.retry_settings: RetrySettings = config.retry
        self._shutdown_requested = threading.Event()
        self._attempt_counters: Dict[str, int] = {}
        self._safe_process = retry(
            attempts=self.retry_settings.attempts,
            delays_sec=self.retry_settings.delays_sec,
        )(self._execute_job)

    def run(self, jobs: Iterable[Job]) -> Progress:
        job_list = list(jobs)
        self.progress.start(total=len(job_list))

        with ThreadPoolExecutor(max_workers=self.threads) as pool:
            future_map = {
                pool.submit(self._process_job, job): job for job in job_list
            }
            for future in as_completed(future_map):
                job = future_map[future]
                try:
                    future.result()
                except Exception as exc:  # pragma: no cover - defensive logging
                    logger.exception(
                        "Task failed irrecoverably",
                        job_id=job.job_id,
                        error=str(exc),
                        step=self.step_name,
                    )

        self.progress.finish()
        return self.progress

    def _process_job(self, job: Job) -> None:
        if self._shutdown_requested.is_set():
            logger.warning("Skipping job due to shutdown request", job_id=job.job_id)
            self.progress.mark_skipped()
            return

        with log_context(self.step_name, job.job_id):
            self.progress.begin_job()
            try:
                with correlation_scope():
                    self._safe_process(job)
            except FatalStepError as exc:
                self._attempt_counters.pop(job.job_id, None)
                self.progress.mark_failed()
                self.hooks.run_error(job, exc)
                logger.error("Job failed fatally", error=str(exc))
                raise
            except JobRetryExceededError as exc:
                self._attempt_counters.pop(job.job_id, None)
                self.progress.mark_failed()
                self.hooks.run_error(job, exc)
                logger.error("Job failed after retries", error=str(exc))
                return
            except Exception as exc:  # pragma: no cover - defensive
                self._attempt_counters.pop(job.job_id, None)
                self.progress.mark_failed()
                self.hooks.run_error(job, exc)
                logger.exception("Job failed unexpectedly", error=str(exc))
                raise
            else:
                self.progress.mark_completed()
                snap = self.progress.snapshot()
                logger.info(
                    "[BUSINESS] Progress | done={}/{} elapsed={:.2f}s eta={}",
                    snap.completed,
                    snap.total,
                    snap.elapsed_sec or 0.0,
                    f"{snap.eta_sec:.2f}s" if snap.eta_sec else "-",
                )

    def _execute_job(self, job: Job) -> None:
        job_id = job.job_id
        attempt = self._attempt_counters.get(job_id, 0) + 1
        self._attempt_counters[job_id] = attempt
        if isinstance(job.payload, dict):
            job.payload["_attempt"] = attempt
        self.hooks.run_before(job)
        started_at = time.perf_counter()
        with attempt_scope(attempt):
            with self.rate_limiter:
                self.step_impl.process(job)
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        self.hooks.run_after(job)
        logger.info(
            "[BUSINESS] Job completed | attempt={} duration={:.2f}s",
            attempt,
            elapsed_ms / 1000.0,
        )
        self._attempt_counters.pop(job_id, None)

    def request_shutdown(self) -> None:
        logger.warning("Shutdown requested; pending jobs will be skipped")
        self._shutdown_requested.set()


def register_signal_handlers(runner: StepRunner) -> None:
    def _handle(sig, frame):  # pragma: no cover - signal handler
        runner.request_shutdown()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)


__all__ = ["StepRunner", "register_signal_handlers", "StepProtocol"]
