"""
SellerIQ SP-API Utilities
=========================
Shared helpers for SP-API ingestion scripts.

Primary export: `with_sp_api_retry(...)` — a decorator that wraps any SP-API call
and transparently retries on transient failures (throttling, 5xx errors, transient
network errors) using exponential backoff.

Why this exists
---------------
SP-API's `createReport` endpoint has a strict rate limit (~1 req/min, burst 15).
Without retries, a single 429 from Amazon kills the whole weekly pipeline.
With retries, the same call sleeps and tries again, and the pipeline survives.

Usage
-----
    from sp_api_utils import with_sp_api_retry

    @with_sp_api_retry(max_attempts=5)
    def create_report_call(reports_api, ...):
        return reports_api.create_report(...)

Backoff schedule (default): 60s, 120s, 240s, 480s, 600s (capped).
"""

from __future__ import annotations

import functools
import logging
import time
from typing import Callable, TypeVar

from sp_api.base.exceptions import (
    SellingApiRequestThrottledException,
    SellingApiServerException,
    SellingApiTemporarilyUnavailableException,
)

log = logging.getLogger(__name__)

# Exception types that are worth retrying. Anything else (auth errors, bad
# request, etc.) should fail fast — retrying won't help.
RETRYABLE_EXCEPTIONS = (
    SellingApiRequestThrottledException,
    SellingApiServerException,
    SellingApiTemporarilyUnavailableException,
)

# Default backoff schedule in seconds. After the last entry, we give up.
DEFAULT_BACKOFF_SECONDS = (60, 120, 240, 480, 600)

T = TypeVar("T")


def with_sp_api_retry(
    max_attempts: int = 5,
    backoff_seconds: tuple[int, ...] = DEFAULT_BACKOFF_SECONDS,
    operation_name: str | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator that retries an SP-API call on transient failures.

    Parameters
    ----------
    max_attempts:
        Total number of attempts (including the first one). Default 5.
    backoff_seconds:
        Sleep duration between attempts. If max_attempts > len(backoff_seconds),
        the final value is reused for any extra attempts.
    operation_name:
        Optional name used in log messages. If not provided, the wrapped
        function's name is used.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        op_name = operation_name or func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exc: Exception | None = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except RETRYABLE_EXCEPTIONS as exc:
                    last_exc = exc

                    if attempt >= max_attempts:
                        log.error(
                            "[%s] FAILED after %d attempts: %s",
                            op_name, attempt, exc,
                        )
                        raise

                    # Pick a sleep duration. Reuse final value if we run past
                    # the end of the backoff schedule.
                    sleep_idx = min(attempt - 1, len(backoff_seconds) - 1)
                    sleep_for = backoff_seconds[sleep_idx]

                    exc_kind = type(exc).__name__
                    log.warning(
                        "[%s] %s on attempt %d/%d — sleeping %ds before retry: %s",
                        op_name, exc_kind, attempt, max_attempts, sleep_for, exc,
                    )
                    time.sleep(sleep_for)

            # Should never reach here, but keep mypy happy.
            assert last_exc is not None
            raise last_exc

        return wrapper

    return decorator


def sleep_with_log(seconds: int, reason: str) -> None:
    """
    Sleep `seconds` and log why. Used for inter-marketplace pacing,
    where we deliberately wait between calls to avoid burst exhaustion.
    """
    if seconds <= 0:
        return
    log.info("Sleeping %ds — %s", seconds, reason)
    time.sleep(seconds)
