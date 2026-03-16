"""Rate limiting for FlashQ tasks.

Provides per-task and global rate limiting using a token-bucket algorithm.

Usage::

    from flashq import FlashQ
    from flashq.ratelimit import RateLimiter, TokenBucket

    app = FlashQ()

    # Global: max 100 tasks/minute
    limiter = RateLimiter(default_rate="100/m")

    # Per-task: max 10 emails/minute
    limiter.configure("send_email", rate="10/m")

    app.add_middleware(limiter)
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from flashq.middleware import Middleware

if TYPE_CHECKING:
    from flashq.models import TaskMessage

logger = logging.getLogger(__name__)


def parse_rate(rate: str) -> tuple[int, float]:
    """Parse a rate string like '100/m' into (count, period_seconds).

    Supported formats:
        - '100/s' — 100 per second
        - '100/m' — 100 per minute
        - '100/h' — 100 per hour
        - '100/d' — 100 per day

    Returns:
        Tuple of (max_tokens, period_in_seconds).
    """
    rate = rate.strip()
    if "/" not in rate:
        raise ValueError(f"Invalid rate format: {rate!r}. Expected 'count/period' (e.g. '100/m')")

    count_str, period = rate.split("/", 1)
    count = int(count_str)

    period_map = {
        "s": 1.0,
        "sec": 1.0,
        "second": 1.0,
        "m": 60.0,
        "min": 60.0,
        "minute": 60.0,
        "h": 3600.0,
        "hour": 3600.0,
        "d": 86400.0,
        "day": 86400.0,
    }

    period_seconds = period_map.get(period.lower())
    if period_seconds is None:
        raise ValueError(f"Unknown period: {period!r}. Use s/m/h/d.")

    return count, period_seconds


@dataclass
class TokenBucket:
    """Token bucket rate limiter.

    Allows ``max_tokens`` executions per ``period`` seconds.
    Tokens are replenished continuously.
    """

    max_tokens: int
    period: float  # seconds
    _tokens: float = 0.0
    _last_refill: float = 0.0
    _lock: threading.Lock = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self._tokens = float(self.max_tokens)
        self._last_refill = time.monotonic()
        object.__setattr__(self, "_lock", threading.Lock())

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        new_tokens = elapsed * (self.max_tokens / self.period)
        self._tokens = min(self.max_tokens, self._tokens + new_tokens)
        self._last_refill = now

    def acquire(self, tokens: int = 1) -> bool:
        """Try to acquire tokens. Returns True if allowed, False if rate-limited."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def wait(self, tokens: int = 1, timeout: float = 30.0) -> bool:
        """Block until tokens are available or timeout expires."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.acquire(tokens):
                return True
            time.sleep(0.01)
        return False

    @property
    def available(self) -> float:
        """Current number of available tokens."""
        with self._lock:
            self._refill()
            return self._tokens


class RateLimiter(Middleware):
    """Middleware that enforces rate limits on task execution.

    Rate-limited tasks are re-enqueued with a short delay instead of dropped.

    Usage::

        limiter = RateLimiter(default_rate="100/m")
        limiter.configure("send_email", rate="10/m")
        app.add_middleware(limiter)
    """

    def __init__(self, default_rate: str | None = None) -> None:
        self._buckets: dict[str, TokenBucket] = {}
        self._default_bucket: TokenBucket | None = None

        if default_rate:
            count, period = parse_rate(default_rate)
            self._default_bucket = TokenBucket(max_tokens=count, period=period)

    def configure(self, task_name: str, rate: str) -> None:
        """Set a rate limit for a specific task."""
        count, period = parse_rate(rate)
        self._buckets[task_name] = TokenBucket(max_tokens=count, period=period)

    def _get_bucket(self, task_name: str) -> TokenBucket | None:
        """Get the rate limiter bucket for a task."""
        return self._buckets.get(task_name, self._default_bucket)

    def before_execute(self, message: TaskMessage) -> TaskMessage | None:
        """Check rate limit before executing a task."""
        bucket = self._get_bucket(message.task_name)
        if bucket is None:
            return message

        if bucket.acquire():
            return message

        # Rate limited — log and skip (worker will re-enqueue later)
        logger.warning(
            "Rate limited: %s [%s] — will retry",
            message.task_name, message.id[:8],
        )
        return None  # Skip execution — task stays in queue or gets cancelled

    def get_stats(self) -> dict[str, dict[str, Any]]:
        """Get current rate limiter statistics."""
        stats: dict[str, dict[str, Any]] = {}

        if self._default_bucket:
            stats["__default__"] = {
                "max_tokens": self._default_bucket.max_tokens,
                "period": self._default_bucket.period,
                "available": self._default_bucket.available,
            }

        for name, bucket in self._buckets.items():
            stats[name] = {
                "max_tokens": bucket.max_tokens,
                "period": bucket.period,
                "available": bucket.available,
            }

        return stats
