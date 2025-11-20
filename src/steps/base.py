from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, List

from src.config import Config
from src.runtime import Job


class BaseStep(ABC):
    NAME: str

    def __init__(self, config: Config) -> None:
        self.config = config

    @abstractmethod
    def load_jobs(self) -> List[Job]:
        """Prepare jobs for the runner."""

    @abstractmethod
    def process(self, job: Job) -> None:
        """Process a single job."""

    def should_run(self) -> bool:
        return self.config.steps.get(self.NAME, True)


__all__ = ["BaseStep"]
