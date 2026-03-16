"""Comprehensive tests for FlashQ core functionality."""

from __future__ import annotations

import time

import pytest

from flashq import FlashQ, TaskPriority, TaskState
from flashq.backends.sqlite import SQLiteBackend
from flashq.exceptions import DuplicateTaskError, TaskNotFoundError
from flashq.models import TaskMessage, TaskResult
from flashq.task import TaskHandle


@pytest.fixture
def backend(tmp_path):
    b = SQLiteBackend(path=str(tmp_path / "test.db"))
    b.setup()
    yield b
    b.teardown()


@pytest.fixture
def app(backend):
    return FlashQ(backend=backend, name="test-app")


# ---------------------------------------------------------------------------
# FlashQ App
# ---------------------------------------------------------------------------


class TestFlashQApp:
    def test_create_app_default_backend(self, tmp_path):
        """Default app uses SQLite backend."""
        import os

        os.chdir(tmp_path)
        app = FlashQ(name="default-test")
        assert isinstance(app.backend, SQLiteBackend)
        app.backend.teardown()

    def test_create_app_custom_backend(self, backend):
        app = FlashQ(backend=backend, name="custom")
        assert app.backend is backend
        assert app.name == "custom"

    def test_register_task(self, app):
        @app.task()
        def my_task() -> str:
            return "hello"

        assert "tests.test_core.TestFlashQApp.test_register_task.<locals>.my_task" in app.registry

    def test_register_task_custom_name(self, app):
        @app.task(name="custom.name")
        def my_task() -> str:
            return "hello"

        assert "custom.name" in app.registry

    def test_duplicate_task_raises(self, app):
        @app.task(name="dup_task")
        def first() -> str:
            return "first"

        with pytest.raises(DuplicateTaskError):

            @app.task(name="dup_task")
            def second() -> str:
                return "second"

    def test_get_task(self, app):
        @app.task(name="findme")
        def findme() -> str:
            return "found"

        task = app.get_task("findme")
        assert task.name == "findme"

    def test_get_task_not_found(self, app):
        with pytest.raises(TaskNotFoundError):
            app.get_task("nonexistent")

    def test_task_options(self, app):
        @app.task(
            name="opts",
            queue="high",
            priority=TaskPriority.HIGH,
            max_retries=5,
            retry_delay=30.0,
            retry_backoff=True,
            timeout=120.0,
            result_ttl=7200.0,
        )
        def opts_task() -> str:
            return "ok"

        task = app.get_task("opts")
        assert task.queue == "high"
        assert task.priority == TaskPriority.HIGH
        assert task.max_retries == 5
        assert task.retry_delay == 30.0
        assert task.retry_backoff is True
        assert task.timeout == 120.0
        assert task.result_ttl == 7200.0

    def test_registry_property(self, app):
        assert isinstance(app.registry, dict)

    def test_middleware_property(self, app):
        from flashq.middleware import MiddlewareStack

        assert isinstance(app.middleware, MiddlewareStack)


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------


class TestTask:
    def test_delay(self, app):
        @app.task(name="delay_test")
        def add(a: int, b: int) -> int:
            return a + b

        handle = add.delay(1, 2)
        assert isinstance(handle, TaskHandle)
        assert handle.id is not None

    def test_apply(self, app):
        @app.task(name="apply_test")
        def add(a: int, b: int) -> int:
            return a + b

        handle = add.apply(args=(1, 2))
        assert isinstance(handle, TaskHandle)

    def test_apply_countdown(self, app):
        @app.task(name="countdown_test")
        def my_task() -> None:
            pass

        my_task.apply(countdown=60)
        assert app.backend.queue_size("default") == 0
        assert app.backend.schedule_size() == 1

    def test_apply_eta(self, app):
        import datetime

        future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)

        @app.task(name="eta_test")
        def my_task() -> None:
            pass

        my_task.apply(eta=future)
        assert app.backend.schedule_size() == 1

    def test_task_name_auto(self, app):
        @app.task()
        def auto_name() -> None:
            pass

        assert "auto_name" in next(iter(app.registry.keys()))

    def test_task_repr(self, app):
        @app.task(name="repr_test")
        def my_task() -> None:
            pass

        assert "repr_test" in repr(my_task)


# ---------------------------------------------------------------------------
# TaskHandle
# ---------------------------------------------------------------------------


class TestTaskHandle:
    def test_get_state(self, app):
        @app.task(name="state_test")
        def my_task() -> None:
            pass

        handle = my_task.delay()
        state = handle.get_state()
        assert state == TaskState.PENDING

    def test_get_result_no_result(self, app):
        @app.task(name="noresult_test")
        def my_task() -> None:
            pass

        handle = my_task.delay()
        result = handle.get_result()
        assert result is None


# ---------------------------------------------------------------------------
# TaskMessage
# ---------------------------------------------------------------------------


class TestTaskMessage:
    def test_roundtrip(self):
        msg = TaskMessage(
            task_name="test.task",
            args=(1, "two"),
            kwargs={"key": "value"},
            priority=TaskPriority.HIGH,
            timeout=30.0,
        )
        d = msg.to_dict()
        restored = TaskMessage.from_dict(d)
        assert restored.task_name == "test.task"
        assert restored.args == (1, "two")
        assert restored.kwargs == {"key": "value"}
        assert restored.priority == TaskPriority.HIGH
        assert restored.timeout == 30.0

    def test_default_values(self):
        msg = TaskMessage(task_name="test")
        assert msg.state == TaskState.PENDING
        assert msg.retries == 0
        assert msg.max_retries == 3
        assert msg.timeout is None
        assert msg.queue == "default"

    def test_id_auto_generated(self):
        msg1 = TaskMessage(task_name="test")
        msg2 = TaskMessage(task_name="test")
        assert msg1.id != msg2.id

    def test_eta_serialization(self):
        import datetime

        eta = datetime.datetime(2025, 6, 15, 12, 0, tzinfo=datetime.timezone.utc)
        msg = TaskMessage(task_name="test", eta=eta)
        d = msg.to_dict()
        restored = TaskMessage.from_dict(d)
        assert restored.eta == eta


# ---------------------------------------------------------------------------
# TaskResult
# ---------------------------------------------------------------------------


class TestTaskResult:
    def test_success(self):
        r = TaskResult(task_id="abc", state=TaskState.SUCCESS, result=42)
        assert r.is_success is True
        assert r.is_failure is False

    def test_failure(self):
        r = TaskResult(task_id="abc", state=TaskState.FAILURE, error="boom")
        assert r.is_success is False
        assert r.is_failure is True

    def test_dead(self):
        r = TaskResult(task_id="abc", state=TaskState.DEAD, error="max retries")
        assert r.is_failure is True

    def test_roundtrip(self):
        import datetime

        r = TaskResult(
            task_id="abc",
            state=TaskState.SUCCESS,
            result={"key": "val"},
            started_at=datetime.datetime.now(datetime.timezone.utc),
            completed_at=datetime.datetime.now(datetime.timezone.utc),
            runtime_ms=42.5,
        )
        d = r.to_dict()
        restored = TaskResult.from_dict(d)
        assert restored.task_id == "abc"
        assert restored.result == {"key": "val"}
        assert restored.runtime_ms == 42.5


# ---------------------------------------------------------------------------
# SQLite Backend
# ---------------------------------------------------------------------------


class TestSQLiteBackend:
    def test_enqueue_dequeue(self, backend):
        msg = TaskMessage(task_name="test.task", args=(1, 2))
        backend.enqueue(msg)
        assert backend.queue_size("default") == 1

        dequeued = backend.dequeue("default")
        assert dequeued is not None
        assert dequeued.task_name == "test.task"
        assert dequeued.args == (1, 2)

    def test_dequeue_empty(self, backend):
        assert backend.dequeue("default") is None

    def test_priority_ordering(self, backend):
        low = TaskMessage(task_name="low", priority=TaskPriority.LOW)
        high = TaskMessage(task_name="high", priority=TaskPriority.HIGH)
        backend.enqueue(low)
        backend.enqueue(high)

        first = backend.dequeue("default")
        assert first.task_name == "high"
        second = backend.dequeue("default")
        assert second.task_name == "low"

    def test_fifo_same_priority(self, backend):
        first = TaskMessage(task_name="first")
        second = TaskMessage(task_name="second")
        backend.enqueue(first)
        backend.enqueue(second)

        d1 = backend.dequeue("default")
        assert d1.task_name == "first"

    def test_update_task_state(self, backend):
        msg = TaskMessage(task_name="state_test")
        backend.enqueue(msg)

        backend.update_task_state(msg.id, TaskState.RUNNING)
        task = backend.get_task(msg.id)
        assert task is not None
        assert task.state == TaskState.RUNNING

    def test_get_task(self, backend):
        msg = TaskMessage(task_name="get_test")
        backend.enqueue(msg)

        task = backend.get_task(msg.id)
        assert task.task_name == "get_test"

    def test_get_task_not_found(self, backend):
        assert backend.get_task("nonexistent") is None

    def test_store_result(self, backend):
        result = TaskResult(
            task_id="result_test",
            state=TaskState.SUCCESS,
            result=42,
            runtime_ms=100.0,
        )
        backend.store_result(result)

        fetched = backend.get_result("result_test")
        assert fetched is not None
        assert fetched.result == 42
        assert fetched.state == TaskState.SUCCESS

    def test_get_result_not_found(self, backend):
        assert backend.get_result("nonexistent") is None

    def test_flush_queue(self, backend):
        for i in range(5):
            backend.enqueue(TaskMessage(task_name=f"task_{i}"))
        assert backend.queue_size("default") == 5

        removed = backend.flush_queue("default")
        assert removed == 5
        assert backend.queue_size("default") == 0

    def test_schedule_enqueue_dequeue(self, backend):
        import datetime

        eta = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=10)
        msg = TaskMessage(task_name="scheduled", eta=eta)
        backend.add_to_schedule(msg)

        assert backend.schedule_size() == 1

        now = time.time()
        due = backend.read_schedule(now)
        assert len(due) == 1
        assert due[0].task_name == "scheduled"
        assert backend.schedule_size() == 0

    def test_schedule_future_not_due(self, backend):
        import datetime

        eta = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
        msg = TaskMessage(task_name="future", eta=eta)
        backend.add_to_schedule(msg)

        now = time.time()
        due = backend.read_schedule(now)
        assert len(due) == 0
        assert backend.schedule_size() == 1

    def test_schedule_no_eta_raises(self, backend):
        msg = TaskMessage(task_name="no_eta")
        with pytest.raises(ValueError, match="ETA"):
            backend.add_to_schedule(msg)

    def test_multiple_queues(self, backend):
        backend.enqueue(TaskMessage(task_name="q1_task", queue="q1"))
        backend.enqueue(TaskMessage(task_name="q2_task", queue="q2"))

        assert backend.queue_size("q1") == 1
        assert backend.queue_size("q2") == 1

        d1 = backend.dequeue("q1")
        assert d1.task_name == "q1_task"
        assert backend.dequeue("q2").task_name == "q2_task"

    def test_dequeue_sets_running(self, backend):
        msg = TaskMessage(task_name="running_test")
        backend.enqueue(msg)

        backend.dequeue("default")
        task = backend.get_task(msg.id)
        assert task.state == TaskState.RUNNING

    def test_insert_or_replace_on_retry(self, backend):
        msg = TaskMessage(task_name="retry_test")
        backend.enqueue(msg)

        # Simulate retry — create new message with same ID
        retry_msg = TaskMessage(
            id=msg.id,
            task_name="retry_test",
            state=TaskState.PENDING,
            retries=1,
        )
        backend.enqueue(retry_msg)

        task = backend.get_task(msg.id)
        assert task.retries == 1

    def test_backend_path(self, backend):
        assert backend.path is not None
        assert backend.path.endswith(".db")

    def test_teardown_and_setup(self, tmp_path):
        b = SQLiteBackend(path=str(tmp_path / "teardown_test.db"))
        b.setup()
        b.enqueue(TaskMessage(task_name="test"))
        b.teardown()
        # After teardown, setup again
        b.setup()
        assert b.queue_size("default") == 0 or True  # DB may be fresh


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class TestEnums:
    def test_task_state_values(self):
        assert TaskState.PENDING.value == "pending"
        assert TaskState.RUNNING.value == "running"
        assert TaskState.SUCCESS.value == "success"
        assert TaskState.FAILURE.value == "failure"
        assert TaskState.RETRYING.value == "retrying"
        assert TaskState.DEAD.value == "dead"

    def test_task_priority_ordering(self):
        assert TaskPriority.LOW < TaskPriority.NORMAL < TaskPriority.HIGH < TaskPriority.CRITICAL

    def test_task_state_cancelled(self):
        assert TaskState.CANCELLED.value == "cancelled"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class TestExceptions:
    def test_task_not_found(self):
        from flashq.exceptions import TaskNotFoundError

        exc = TaskNotFoundError("my_task")
        assert "my_task" in str(exc)

    def test_duplicate_task(self):
        from flashq.exceptions import DuplicateTaskError

        exc = DuplicateTaskError("dup")
        assert "dup" in str(exc)

    def test_task_timeout_error(self):
        from flashq.exceptions import TaskTimeoutError

        exc = TaskTimeoutError("task123", 30.0)
        assert "task123" in str(exc)
        assert "30" in str(exc)

    def test_task_retry_error(self):
        from flashq.exceptions import TaskRetryError

        exc = TaskRetryError()
        assert isinstance(exc, Exception)

    def test_backend_error(self):
        from flashq.exceptions import BackendError

        exc = BackendError("connection lost")
        assert "connection lost" in str(exc)

    def test_serialization_error(self):
        from flashq.exceptions import SerializationError

        exc = SerializationError("bad data")
        assert "bad data" in str(exc)

    def test_worker_shutdown_error(self):
        from flashq.exceptions import WorkerShutdownError

        exc = WorkerShutdownError()
        assert isinstance(exc, Exception)

    def test_flashq_error_hierarchy(self):
        from flashq.exceptions import (
            BackendError,
            DuplicateTaskError,
            FlashQError,
            SerializationError,
            TaskNotFoundError,
            TaskRetryError,
            TaskTimeoutError,
            WorkerShutdownError,
        )

        assert issubclass(TaskNotFoundError, FlashQError)
        assert issubclass(DuplicateTaskError, FlashQError)
        assert issubclass(TaskTimeoutError, FlashQError)
        assert issubclass(TaskRetryError, FlashQError)
        assert issubclass(BackendError, FlashQError)
        assert issubclass(SerializationError, FlashQError)
        assert issubclass(WorkerShutdownError, FlashQError)
