"""Tests for backend interface and mock tests for PostgreSQL/Redis."""

from __future__ import annotations

import pytest

from flashq.backends import BaseBackend
from flashq.enums import TaskState
from flashq.models import TaskMessage, TaskResult


class TestBaseBackendInterface:
    """Verify BaseBackend ABC contract."""

    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            BaseBackend()

    def test_required_methods(self):
        methods = [
            "setup", "teardown", "enqueue", "dequeue", "get_task",
            "update_task_state", "store_result", "get_result",
            "queue_size", "flush_queue", "add_to_schedule",
            "read_schedule", "schedule_size", "get_tasks_by_state",
            "delete_result",
        ]
        for method in methods:
            assert hasattr(BaseBackend, method), f"Missing method: {method}"


class TestPostgresBackendStructure:
    """Test PostgreSQL backend can be imported and has correct structure."""

    def test_import(self):
        from flashq.backends.postgres import PostgresBackend
        assert PostgresBackend is not None

    def test_subclass_of_base(self):
        from flashq.backends.postgres import PostgresBackend
        assert issubclass(PostgresBackend, BaseBackend)

    def test_has_all_methods(self):
        from flashq.backends.postgres import PostgresBackend
        methods = [
            "setup", "teardown", "enqueue", "dequeue", "get_task",
            "update_task_state", "store_result", "get_result",
            "queue_size", "flush_queue", "add_to_schedule",
            "read_schedule", "schedule_size",
        ]
        for method in methods:
            assert hasattr(PostgresBackend, method), f"Missing: {method}"
            assert callable(getattr(PostgresBackend, method))


class TestRedisBackendStructure:
    """Test Redis backend can be imported and has correct structure."""

    def test_import(self):
        from flashq.backends.redis import RedisBackend
        assert RedisBackend is not None

    def test_subclass_of_base(self):
        from flashq.backends.redis import RedisBackend
        assert issubclass(RedisBackend, BaseBackend)

    def test_has_all_methods(self):
        from flashq.backends.redis import RedisBackend
        methods = [
            "setup", "teardown", "enqueue", "dequeue", "get_task",
            "update_task_state", "store_result", "get_result",
            "queue_size", "flush_queue", "add_to_schedule",
            "read_schedule", "schedule_size",
        ]
        for method in methods:
            assert hasattr(RedisBackend, method), f"Missing: {method}"
            assert callable(getattr(RedisBackend, method))


class TestSQLiteBackendEdgeCases:
    """Additional edge-case tests for SQLite backend."""

    @pytest.fixture
    def backend(self, tmp_path):
        from flashq.backends.sqlite import SQLiteBackend
        b = SQLiteBackend(path=str(tmp_path / "edge_test.db"))
        b.setup()
        yield b
        b.teardown()

    def test_concurrent_dequeue(self, backend):
        """Two dequeue calls shouldn't return the same task."""
        msg = TaskMessage(task_name="race_task")
        backend.enqueue(msg)

        d1 = backend.dequeue("default")
        d2 = backend.dequeue("default")

        assert d1 is not None
        assert d2 is None  # Already claimed

    def test_eta_filtering(self, backend):
        """Tasks with future ETA shouldn't be dequeued."""
        import datetime
        future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
        msg = TaskMessage(task_name="future_task", eta=future)
        backend.enqueue(msg)

        d = backend.dequeue("default")
        assert d is None

    def test_past_eta_dequeued(self, backend):
        """Tasks with past ETA should be dequeued."""
        import datetime
        past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=10)
        msg = TaskMessage(task_name="past_task", eta=past)
        backend.enqueue(msg)

        d = backend.dequeue("default")
        assert d is not None
        assert d.task_name == "past_task"

    def test_store_result_overwrite(self, backend):
        """Storing result twice overwrites the first."""
        r1 = TaskResult(task_id="overwrite_test", state=TaskState.RUNNING)
        backend.store_result(r1)

        r2 = TaskResult(task_id="overwrite_test", state=TaskState.SUCCESS, result="done")
        backend.store_result(r2)

        fetched = backend.get_result("overwrite_test")
        assert fetched.state == TaskState.SUCCESS
        assert fetched.result == "done"

    def test_info_returns_path(self, backend):
        assert backend.path is not None
        assert backend.path.endswith(".db")

    def test_queue_isolation(self, backend):
        """Tasks in different queues don't interfere."""
        backend.enqueue(TaskMessage(task_name="q1", queue="alpha"))
        backend.enqueue(TaskMessage(task_name="q2", queue="beta"))

        assert backend.queue_size("alpha") == 1
        assert backend.queue_size("beta") == 1

        # Flush alpha, beta stays
        backend.flush_queue("alpha")
        assert backend.queue_size("alpha") == 0
        assert backend.queue_size("beta") == 1
