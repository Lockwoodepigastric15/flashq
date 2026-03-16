"""Tests for rate limiting."""

from __future__ import annotations

import time

import pytest

from flashq.models import TaskMessage
from flashq.ratelimit import RateLimiter, TokenBucket, parse_rate


class TestParseRate:
    def test_per_second(self):
        count, period = parse_rate("100/s")
        assert count == 100
        assert period == 1.0

    def test_per_minute(self):
        count, period = parse_rate("60/m")
        assert count == 60
        assert period == 60.0

    def test_per_hour(self):
        count, period = parse_rate("1000/h")
        assert count == 1000
        assert period == 3600.0

    def test_per_day(self):
        count, period = parse_rate("10000/d")
        assert count == 10000
        assert period == 86400.0

    def test_long_form(self):
        assert parse_rate("10/minute") == (10, 60.0)
        assert parse_rate("10/second") == (10, 1.0)
        assert parse_rate("10/hour") == (10, 3600.0)
        assert parse_rate("10/day") == (10, 86400.0)

    def test_whitespace(self):
        count, period = parse_rate("  50/m  ")
        assert count == 50
        assert period == 60.0

    def test_invalid_no_slash(self):
        with pytest.raises(ValueError, match="Invalid rate"):
            parse_rate("100m")

    def test_invalid_period(self):
        with pytest.raises(ValueError, match="Unknown period"):
            parse_rate("100/x")

    def test_invalid_count(self):
        with pytest.raises(ValueError):
            parse_rate("abc/s")


class TestTokenBucket:
    def test_initial_tokens(self):
        bucket = TokenBucket(max_tokens=10, period=1.0)
        assert bucket.available == 10.0

    def test_acquire(self):
        bucket = TokenBucket(max_tokens=5, period=1.0)
        assert bucket.acquire() is True
        assert bucket.available < 5.0

    def test_exhaust_tokens(self):
        bucket = TokenBucket(max_tokens=3, period=60.0)
        assert bucket.acquire() is True
        assert bucket.acquire() is True
        assert bucket.acquire() is True
        assert bucket.acquire() is False  # exhausted

    def test_refill(self):
        bucket = TokenBucket(max_tokens=10, period=1.0)
        # Exhaust all tokens
        for _ in range(10):
            bucket.acquire()
        assert bucket.acquire() is False

        # Wait for refill
        time.sleep(0.2)
        assert bucket.available > 0

    def test_wait_succeeds(self):
        bucket = TokenBucket(max_tokens=10, period=0.5)
        for _ in range(10):
            bucket.acquire()

        # Should refill within timeout
        assert bucket.wait(timeout=1.0) is True

    def test_wait_timeout(self):
        bucket = TokenBucket(max_tokens=1, period=100.0)
        bucket.acquire()

        # Should timeout
        assert bucket.wait(timeout=0.1) is False

    def test_acquire_multiple(self):
        bucket = TokenBucket(max_tokens=10, period=60.0)
        assert bucket.acquire(5) is True
        assert bucket.acquire(5) is True
        assert bucket.acquire(1) is False

    def test_thread_safety(self):
        import threading

        bucket = TokenBucket(max_tokens=100, period=60.0)
        acquired = []
        lock = threading.Lock()

        def consume():
            for _ in range(20):
                if bucket.acquire():
                    with lock:
                        acquired.append(1)

        threads = [threading.Thread(target=consume) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(acquired) == 100  # exactly max_tokens acquired


class TestRateLimiter:
    def test_no_limit(self):
        limiter = RateLimiter()
        msg = TaskMessage(task_name="any_task")
        result = limiter.before_execute(msg)
        assert result is msg

    def test_default_limit_allows(self):
        limiter = RateLimiter(default_rate="100/s")
        msg = TaskMessage(task_name="any_task")
        result = limiter.before_execute(msg)
        assert result is msg

    def test_default_limit_blocks(self):
        limiter = RateLimiter(default_rate="2/m")
        msg = TaskMessage(task_name="task")

        assert limiter.before_execute(msg) is not None
        assert limiter.before_execute(msg) is not None
        assert limiter.before_execute(msg) is None  # blocked

    def test_per_task_limit(self):
        limiter = RateLimiter()
        limiter.configure("email", rate="1/m")

        msg_email = TaskMessage(task_name="email")
        msg_other = TaskMessage(task_name="other")

        assert limiter.before_execute(msg_email) is not None
        assert limiter.before_execute(msg_email) is None  # blocked

        # Other tasks not affected
        assert limiter.before_execute(msg_other) is not None

    def test_per_task_overrides_default(self):
        limiter = RateLimiter(default_rate="1000/s")
        limiter.configure("slow_task", rate="1/m")

        msg = TaskMessage(task_name="slow_task")
        assert limiter.before_execute(msg) is not None
        assert limiter.before_execute(msg) is None  # per-task limit applies

    def test_get_stats(self):
        limiter = RateLimiter(default_rate="100/m")
        limiter.configure("email", rate="10/m")

        stats = limiter.get_stats()
        assert "__default__" in stats
        assert stats["__default__"]["max_tokens"] == 100
        assert "email" in stats
        assert stats["email"]["max_tokens"] == 10

    def test_get_stats_empty(self):
        limiter = RateLimiter()
        assert limiter.get_stats() == {}
