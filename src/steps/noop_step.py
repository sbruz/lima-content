from __future__ import annotations

from typing import Any, Dict, List

from src.config import Config
from src.runtime import Job, logger, make_job
from src.steps.base import BaseStep


class NoOpStep(BaseStep):
    """Placeholder step implementation until real logic is added."""

    NAME = "noop"

    def __init__(self, config: Config, name: str) -> None:
        super().__init__(config)
        self.NAME = name  # override for config integration

    def load_jobs(self) -> List[Job]:
        if not self.should_run():
            return []
        return [make_job(self.NAME, {"placeholder": True}, key_fields=["placeholder"])]

    def process(self, job: Job) -> None:
        logger.info("No-op step executed", step=self.NAME, job_id=job.job_id)


__all__ = ["NoOpStep"]
