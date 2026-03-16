"""Tests for canvas primitives: Signature, Chain, Group, Chord."""

from __future__ import annotations

import threading
import time

import pytest

from flashq import FlashQ, Signature, chain, chord, group
from flashq.backends.sqlite import SQLiteBackend
from flashq.canvas import Chain, ChainHandle, Chord, ChordHandle, Group, GroupHandle
from flashq.worker import Worker


@pytest.fixture
def backend(tmp_path):
    b = SQLiteBackend(path=str(tmp_path / "canvas_test.db"))
    b.setup()
    yield b
    b.teardown()


@pytest.fixture
def app(backend):
    return FlashQ(backend=backend, name="canvas-test")


def _run_worker(app, duration=2.0, **kwargs):
    worker = Worker(app, poll_interval=0.1, schedule_interval=0.3, **kwargs)
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    time.sleep(duration)
    worker.stop()
    t.join(timeout=5)
    return worker


# ---------------------------------------------------------------------------
# Signature
# ---------------------------------------------------------------------------

class TestSignature:
    def test_create_signature(self, app):
        @app.task(name="add")
        def add(a: int, b: int) -> int:
            return a + b

        sig = add.s(2, 3)
        assert isinstance(sig, Signature)
        assert sig.task_name == "add"
        assert sig.args == (2, 3)
        assert sig.kwargs == {}
        assert sig.immutable is False

    def test_create_immutable_signature(self, app):
        @app.task(name="add")
        def add(a: int, b: int) -> int:
            return a + b

        sig = add.si(10, 20)
        assert sig.immutable is True
        assert sig.args == (10, 20)

    def test_signature_with_kwargs(self, app):
        @app.task(name="send_email")
        def send_email(to: str, subject: str) -> None:
            pass

        sig = send_email.s(to="user@test.com", subject="Hello")
        assert sig.kwargs == {"to": "user@test.com", "subject": "Hello"}

    def test_signature_set_options(self, app):
        @app.task(name="task")
        def task() -> None:
            pass

        sig = task.s().set(queue="high", priority=9)
        assert sig.options["queue"] == "high"
        assert sig.options["priority"] == 9

    def test_signature_roundtrip(self, app):
        @app.task(name="task")
        def task() -> None:
            pass

        sig = task.s(1, 2, key="val").set(queue="high")
        d = sig.to_dict()
        restored = Signature.from_dict(d)
        assert restored.task_name == sig.task_name
        assert restored.args == sig.args
        assert restored.kwargs == sig.kwargs
        assert restored.options["queue"] == "high"

    def test_signature_apply(self, app, backend):
        @app.task(name="apply_sig")
        def task(x: int) -> int:
            return x * 2

        sig = task.s(5)
        task_id = sig.apply(app)
        assert task_id is not None
        assert backend.queue_size("default") == 1

    def test_signature_apply_with_prev_result(self, app, backend):
        @app.task(name="chain_step")
        def step(prev: int, x: int) -> int:
            return prev + x

        sig = step.s(10)
        sig.apply(app, prev_result=5)

        msg = backend.dequeue("default")
        assert msg is not None
        assert msg.args == (5, 10)  # prev_result prepended

    def test_immutable_ignores_prev_result(self, app, backend):
        @app.task(name="immutable_step")
        def step(x: int) -> int:
            return x * 2

        sig = step.si(7)
        sig.apply(app, prev_result=999)

        msg = backend.dequeue("default")
        assert msg is not None
        assert msg.args == (7,)  # prev_result ignored


# ---------------------------------------------------------------------------
# Group
# ---------------------------------------------------------------------------

class TestGroup:
    def test_group_dispatch(self, app, backend):
        @app.task(name="group_task")
        def group_task(x: int) -> int:
            return x * 2

        g = group(group_task.s(1), group_task.s(2), group_task.s(3))
        assert isinstance(g, Group)

        handle = g.delay(app)
        assert isinstance(handle, GroupHandle)
        assert len(handle.task_ids) == 3
        assert backend.queue_size("default") == 3

    def test_group_execution(self, app, backend):
        results = []

        @app.task(name="collect")
        def collect(x: int) -> int:
            results.append(x)
            return x * 10

        g = group(collect.s(1), collect.s(2), collect.s(3))
        g.delay(app)

        _run_worker(app)

        assert sorted(results) == [1, 2, 3]

    def test_group_get_results(self, app, backend):
        @app.task(name="doubler")
        def doubler(x: int) -> int:
            return x * 2

        g = group(doubler.s(5), doubler.s(10), doubler.s(15))
        handle = g.delay(app)

        _run_worker(app)

        results = handle.get_results(timeout=5.0)
        assert results == [10, 20, 30]

    def test_group_completed_count(self, app, backend):
        @app.task(name="simple")
        def simple() -> str:
            return "done"

        g = group(simple.s(), simple.s())
        handle = g.delay(app)

        assert handle.completed == 0

        _run_worker(app)

        assert handle.completed == 2

    def test_group_timeout(self, app, backend):
        @app.task(name="never_run")
        def never_run() -> None:
            time.sleep(100)

        g = group(never_run.s())
        handle = g.delay(app)

        # Don't run worker — tasks never complete
        with pytest.raises(TimeoutError):
            handle.get_results(timeout=0.5)


# ---------------------------------------------------------------------------
# Chain
# ---------------------------------------------------------------------------

class TestChain:
    def test_chain_creation(self, app):
        @app.task(name="step_a")
        def step_a() -> int:
            return 1

        @app.task(name="step_b")
        def step_b(x: int) -> int:
            return x + 1

        c = chain(step_a.s(), step_b.s())
        assert isinstance(c, Chain)
        assert len(c.signatures) == 2

    def test_chain_dispatch(self, app, backend):
        @app.task(name="first")
        def first() -> int:
            return 1

        @app.task(name="second")
        def second(x: int) -> int:
            return x + 1

        c = chain(first.s(), second.s())
        handle = c.delay(app)

        assert isinstance(handle, ChainHandle)
        assert handle.total == 2
        # Only first task enqueued
        assert backend.queue_size("default") == 1


# ---------------------------------------------------------------------------
# Chord
# ---------------------------------------------------------------------------

class TestChord:
    def test_chord_creation(self, app):
        @app.task(name="c_add")
        def c_add(a: int, b: int) -> int:
            return a + b

        @app.task(name="c_sum")
        def c_sum(results: list[int]) -> int:
            return sum(results)

        ch = chord(
            group(c_add.s(1, 2), c_add.s(3, 4)),
            c_sum.s(),
        )
        assert isinstance(ch, Chord)
        assert len(ch.header.signatures) == 2

    def test_chord_dispatch(self, app, backend):
        @app.task(name="ch_task")
        def ch_task(x: int) -> int:
            return x

        @app.task(name="ch_callback")
        def ch_callback(results: list[int]) -> int:
            return sum(results)

        ch = chord(
            group(ch_task.s(1), ch_task.s(2), ch_task.s(3)),
            ch_callback.s(),
        )
        handle = ch.delay(app)

        assert isinstance(handle, ChordHandle)
        assert len(handle.task_ids) == 3
        assert backend.queue_size("default") == 3


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------

class TestConvenienceFunctions:
    def test_chain_function(self, app):
        @app.task(name="f1")
        def f1() -> None:
            pass

        c = chain(f1.s(), f1.s())
        assert isinstance(c, Chain)

    def test_group_function(self, app):
        @app.task(name="f2")
        def f2() -> None:
            pass

        g = group(f2.s(), f2.s())
        assert isinstance(g, Group)

    def test_chord_function(self, app):
        @app.task(name="f3")
        def f3() -> None:
            pass

        ch = chord(group(f3.s()), f3.s())
        assert isinstance(ch, Chord)
