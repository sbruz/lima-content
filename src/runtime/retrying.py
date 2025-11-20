from __future__ import annotations

import time
from functools import wraps
from typing import Any, Callable, Iterable, Tuple

from .errors import FatalStepError, JobRetryExceededError, RetryableStepError
from .logging import logger


def retry(
    attempts: int = 3,
    delays_sec: Iterable[int] = (5, 10),
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Retry decorator with exponential-style backoff defined via delays sequence."""

    delays = tuple(delays_sec)

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_error: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except FatalStepError:
                    raise
                except RetryableStepError as err:
                    last_error = err
                except Exception as err:  # pragma: no cover - fall back to retry
                    last_error = err

                if attempt >= attempts:
                    logger.error(
                        "Retry attempts exhausted",
                        attempt=attempt,
                    )
                    raise JobRetryExceededError(str(last_error)) from last_error

                delay = delays[min(attempt - 1, len(delays) - 1)]
                logger.warning(
                    "Retrying after failure | attempt={} delay={}s error={}",
                    attempt,
                    delay,
                    str(last_error),
                )
                time.sleep(delay)

        return wrapper

    return decorator


__all__ = ["retry"]
