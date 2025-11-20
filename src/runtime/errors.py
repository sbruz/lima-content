class PipelineError(Exception):
    """Base exception for pipeline runtime failures."""


class FatalStepError(PipelineError):
    """Raised when retrying is not desired (validation, idempotency, etc.)."""


class RetryableStepError(PipelineError):
    """Raised when a step failure should be retried."""


class JobRetryExceededError(PipelineError):
    """Raised when all retry attempts have been exhausted."""


__all__ = [
    "PipelineError",
    "FatalStepError",
    "RetryableStepError",
    "JobRetryExceededError",
]
