"""Comprehensive tests for middleware system."""

from __future__ import annotations

import logging
import threading
import time

import pytest

from flashq import FlashQ, Middleware, TaskState
from flashq.backends.sqlite import SQLiteBackend
from flashq.middleware import LoggingMiddleware, MiddlewareStack, TimeoutMiddleware
from flashq.models import TaskMessage
from flashq.worker import Worker


@pytest.fixture
def backend(tmp_path):
    b = SQLiteBackend(path=str(tmp_path / "mw_test.db"))
    b.setup()
    yield b
    b.teardown()


@pytest.fixture
def app(backend):
    return FlashQ(backend=backend, name="mw-test")


# ---------------------------------------------------------------------------
# Test Middleware base class defaults
# ---------------------------------------------------------------------------


class TestMiddlewareBase:
    def test_before_execute_returns_message(self):
        mw = Middleware()
        msg = TaskMessage(task_name="test")
        assert mw.before_execute(msg) is msg

    def test_after_execute_is_noop(self):
        mw = Middleware()
        msg = TaskMessage(task_name="test")
        mw.after_execute(msg, "result")  # should not raise

    def test_on_error_returns_false(self):
        mw = Middleware()
        msg = TaskMessage(task_name="test")
        result = mw.on_error(msg, ValueError("test"))
        assert result is False

    def test_on_retry_is_noop(self):
        mw = Middleware()
        msg = TaskMessage(task_name="test")
        mw.on_retry(msg, ValueError("test"), 60.0)  # should not raise

    def test_on_dead_is_noop(self):
        mw = Middleware()
        msg = TaskMessage(task_name="test")
        mw.on_dead(msg, ValueError("test"))  # should not raise

    def test_on_worker_start_is_noop(self):
        Middleware().on_worker_start()

    def test_on_worker_stop_is_noop(self):
        Middleware().on_worker_stop()


# ---------------------------------------------------------------------------
# Recording middleware for assertions
# ---------------------------------------------------------------------------


class RecordingMiddleware(Middleware):
    def __init__(self):
        self.events: list[str] = []

    def before_execute(self, message):
        self.events.append(f"before:{message.task_name}")
        return message

    def after_execute(self, message, result):
        self.events.append(f"after:{message.task_name}:{result}")

    def on_error(self, message, exc):
        self.events.append(f"error:{message.task_name}:{exc}")
        return False

    def on_retry(self, message, exc, countdown):
        self.events.append(f"retry:{message.task_name}:{countdown}")

    def on_dead(self, message, exc):
        self.events.append(f"dead:{message.task_name}")

    def on_worker_start(self):
        self.events.append("worker_start")

    def on_worker_stop(self):
        self.events.append("worker_stop")


class SkipMiddleware(Middleware):
    def before_execute(self, message):
        if "skip" in message.task_name:
            return None
        return message


class SuppressMiddleware(Middleware):
    def on_error(self, message, exc):
        return True


class FaultyMiddleware(Middleware):
    """Middleware that raises in every hook."""

    def before_execute(self, message):
        raise RuntimeError("faulty before")

    def after_execute(self, message, result):
        raise RuntimeError("faulty after")

    def on_error(self, message, exc):
        raise RuntimeError("faulty on_error")

    def on_retry(self, message, exc, countdown):
        raise RuntimeError("faulty on_retry")

    def on_dead(self, message, exc):
        raise RuntimeError("faulty on_dead")

    def on_worker_start(self):
        raise RuntimeError("faulty start")

    def on_worker_stop(self):
        raise RuntimeError("faulty stop")


# ---------------------------------------------------------------------------
# MiddlewareStack unit tests
# ---------------------------------------------------------------------------


class TestMiddlewareStack:
    def test_add_and_list(self):
        stack = MiddlewareStack()
        mw = RecordingMiddleware()
        stack.add(mw)
        assert mw in stack.middlewares

    def test_remove_by_type(self):
        stack = MiddlewareStack()
        stack.add(RecordingMiddleware())
        stack.add(SkipMiddleware())
        stack.remove(RecordingMiddleware)
        assert len(stack.middlewares) == 1
        assert isinstance(stack.middlewares[0], SkipMiddleware)

    def test_before_execute_chain(self):
        stack = MiddlewareStack()
        recorder = RecordingMiddleware()
        stack.add(recorder)
        msg = TaskMessage(task_name="test.task")
        result = stack.before_execute(msg)
        assert result is not None
        assert "before:test.task" in recorder.events

    def test_before_execute_skip(self):
        stack = MiddlewareStack()
        stack.add(SkipMiddleware())
        msg = TaskMessage(task_name="skip_this")
        result = stack.before_execute(msg)
        assert result is None

    def test_before_execute_faulty_returns_none(self):
        stack = MiddlewareStack()
        stack.add(FaultyMiddleware())
        msg = TaskMessage(task_name="test")
        result = stack.before_execute(msg)
        assert result is None  # exception causes skip

    def test_after_execute_faulty_doesnt_propagate(self, caplog):
        stack = MiddlewareStack()
        stack.add(FaultyMiddleware())
        msg = TaskMessage(task_name="test")
        # Should not raise
        stack.after_execute(msg, "result")

    def test_on_error_faulty_doesnt_propagate(self):
        stack = MiddlewareStack()
        stack.add(FaultyMiddleware())
        msg = TaskMessage(task_name="test")
        result = stack.on_error(msg, ValueError("test"))
        assert result is False

    def test_on_retry_faulty_doesnt_propagate(self):
        stack = MiddlewareStack()
        stack.add(FaultyMiddleware())
        msg = TaskMessage(task_name="test")
        # Should not raise
        stack.on_retry(msg, ValueError("test"), 60.0)

    def test_on_dead_faulty_doesnt_propagate(self):
        stack = MiddlewareStack()
        stack.add(FaultyMiddleware())
        msg = TaskMessage(task_name="test")
        stack.on_dead(msg, ValueError("test"))

    def test_on_worker_start_faulty(self):
        stack = MiddlewareStack()
        stack.add(FaultyMiddleware())
        stack.on_worker_start()  # should not raise

    def test_on_worker_stop_faulty(self):
        stack = MiddlewareStack()
        stack.add(FaultyMiddleware())
        stack.on_worker_stop()  # should not raise

    def test_middleware_ordering(self):
        stack = MiddlewareStack()
        events = []

        class First(Middleware):
            def before_execute(self, message):
                events.append("first")
                return message

        class Second(Middleware):
            def before_execute(self, message):
                events.append("second")
                return message

        stack.add(First())
        stack.add(Second())
        stack.before_execute(TaskMessage(task_name="test"))
        assert events == ["first", "second"]

    def test_on_error_suppress(self):
        stack = MiddlewareStack()
        stack.add(SuppressMiddleware())
        msg = TaskMessage(task_name="test")
        result = stack.on_error(msg, ValueError("test"))
        assert result is True


# ---------------------------------------------------------------------------
# Built-in middleware tests
# ---------------------------------------------------------------------------


class TestLoggingMiddleware:
    def test_logging_before_execute(self, caplog):
        mw = LoggingMiddleware()
        msg = TaskMessage(task_name="my.task", id="abc123")
        with caplog.at_level(logging.INFO, logger="flashq.middleware"):
            result = mw.before_execute(msg)
        assert result is msg

    def test_logging_after_execute(self, caplog):
        mw = LoggingMiddleware()
        msg = TaskMessage(task_name="my.task", id="abc123")
        with caplog.at_level(logging.INFO, logger="flashq.middleware"):
            mw.after_execute(msg, "done")

    def test_logging_on_error(self, caplog):
        mw = LoggingMiddleware()
        msg = TaskMessage(task_name="my.task", id="abc123")
        with caplog.at_level(logging.ERROR, logger="flashq.middleware"):
            result = mw.on_error(msg, ValueError("boom"))
        assert result is False


class TestTimeoutMiddleware:
    def test_tracks_start_time(self):
        mw = TimeoutMiddleware()
        msg = TaskMessage(task_name="test", id="abc")
        result = mw.before_execute(msg)
        assert result is msg
        assert "abc" in mw._start_times

    def test_cleans_up_after_execute(self):
        mw = TimeoutMiddleware()
        msg = TaskMessage(task_name="test", id="abc")
        mw.before_execute(msg)
        mw.after_execute(msg, "ok")
        assert "abc" not in mw._start_times

    def test_cleans_up_on_error(self):
        mw = TimeoutMiddleware()
        msg = TaskMessage(task_name="test", id="abc")
        mw.before_execute(msg)
        mw.on_error(msg, ValueError("boom"))
        assert "abc" not in mw._start_times


# ---------------------------------------------------------------------------
# Full integration with FlashQ app + Worker
# ---------------------------------------------------------------------------


class TestMiddlewareIntegration:
    def test_add_middleware_to_app(self, app):
        mw = RecordingMiddleware()
        app.add_middleware(mw)
        assert mw in app.middleware.middlewares

    def test_middleware_lifecycle_on_success(self, app, backend):
        recorder = RecordingMiddleware()
        app.add_middleware(recorder)

        @app.task(name="success_task")
        def success_task() -> str:
            return "ok"

        success_task.delay()

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1.5)
        worker.stop()
        t.join(timeout=5)

        assert "worker_start" in recorder.events
        assert any("before:" in e for e in recorder.events)
        assert any("after:" in e and "ok" in e for e in recorder.events)
        assert "worker_stop" in recorder.events

    def test_middleware_skip_task(self, app, backend):
        app.add_middleware(SkipMiddleware())

        @app.task(name="skip_me")
        def skipped_task() -> None:
            pass

        handle = skipped_task.delay()

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1)
        worker.stop()
        t.join(timeout=5)

        # Task should be cancelled by middleware
        state = handle.get_state()
        assert state == TaskState.CANCELLED

    def test_middleware_suppress_error(self, app, backend):
        app.add_middleware(SuppressMiddleware())

        @app.task(name="suppressed_fail", max_retries=0)
        def failing() -> None:
            raise ValueError("suppressed")

        handle = failing.delay()

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1)
        worker.stop()
        t.join(timeout=5)

        result = backend.get_result(handle.id)
        assert result is not None
        assert result.state == TaskState.SUCCESS

    def test_middleware_on_retry(self, app, backend):
        recorder = RecordingMiddleware()
        app.add_middleware(recorder)

        @app.task(name="retry_me", max_retries=1, retry_delay=0.1)
        def will_fail() -> None:
            raise ValueError("will retry")

        will_fail.delay()

        worker = Worker(app, poll_interval=0.1, schedule_interval=0.3)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(3)
        worker.stop()
        t.join(timeout=5)

        assert any("retry:" in e for e in recorder.events)

    def test_middleware_on_dead(self, app, backend):
        recorder = RecordingMiddleware()
        app.add_middleware(recorder)

        @app.task(name="die_task", max_retries=0)
        def will_die() -> None:
            raise ValueError("fatal")

        will_die.delay()

        worker = Worker(app, poll_interval=0.1)
        t = threading.Thread(target=worker.start, daemon=True)
        t.start()
        time.sleep(1)
        worker.stop()
        t.join(timeout=5)

        assert any("dead:" in e for e in recorder.events)
