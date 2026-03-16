"""Task composition primitives: chain, group, chord.

These allow combining tasks into complex workflows::

    from flashq import FlashQ
    from flashq.canvas import chain, group, chord

    app = FlashQ()

    @app.task()
    def add(a: int, b: int) -> int:
        return a + b

    @app.task()
    def multiply(a: int, b: int) -> int:
        return a * b

    @app.task()
    def aggregate(results: list[int]) -> int:
        return sum(results)

    # Chain: sequential execution, result of each passed to next
    chain(add.s(2, 3), multiply.s(10)).delay()

    # Group: parallel execution
    group(add.s(1, 2), add.s(3, 4), add.s(5, 6)).delay()

    # Chord: parallel tasks + callback when all complete
    chord(
        group(add.s(1, 2), add.s(3, 4)),
        aggregate.s(),
    ).delay()
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from flashq.enums import TaskState
from flashq.models import TaskMessage

if TYPE_CHECKING:
    from flashq.app import FlashQ


# ---------------------------------------------------------------------------
# Signature — a "frozen" task call
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Signature:
    """A serializable reference to a task call (task name + args + kwargs).

    Created via ``task.s(*args, **kwargs)`` or ``task.si(*args, **kwargs)``.
    """

    task_name: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    immutable: bool = False  # si() makes immutable — ignores previous result
    options: dict[str, Any] = field(default_factory=dict)

    def set(self, **opts: Any) -> Signature:
        """Return a new signature with updated options."""
        new_opts = {**self.options, **opts}
        return Signature(
            task_name=self.task_name,
            args=self.args,
            kwargs=self.kwargs,
            immutable=self.immutable,
            options=new_opts,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_name": self.task_name,
            "args": list(self.args),
            "kwargs": self.kwargs,
            "immutable": self.immutable,
            "options": self.options,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Signature:
        return cls(
            task_name=data["task_name"],
            args=tuple(data.get("args", ())),
            kwargs=data.get("kwargs", {}),
            immutable=data.get("immutable", False),
            options=data.get("options", {}),
        )

    def apply(self, app: FlashQ, prev_result: Any = None) -> str:
        """Enqueue this signature as a real task. Returns the task ID."""
        args = self.args if self.immutable or prev_result is None else (prev_result, *self.args)

        msg = TaskMessage(
            task_name=self.task_name,
            args=args,
            kwargs=self.kwargs,
            queue=self.options.get("queue", "default"),
            priority=self.options.get("priority", 0),
            max_retries=self.options.get("max_retries", 3),
        )
        app.backend.enqueue(msg)
        return msg.id


# ---------------------------------------------------------------------------
# Chain — sequential execution
# ---------------------------------------------------------------------------

@dataclass
class Chain:
    """Execute tasks sequentially, passing each result to the next.

    Usage::

        chain(add.s(2, 3), multiply.s(10)).delay(app)
    """

    signatures: list[Signature]

    def delay(self, app: FlashQ) -> ChainHandle:
        """Dispatch the chain to the backend."""
        chain_id = str(uuid.uuid4())

        # Enqueue the first task
        first = self.signatures[0]
        msg = TaskMessage(
            task_name=first.task_name,
            args=first.args,
            kwargs=first.kwargs,
            queue=first.options.get("queue", "default"),
            priority=first.options.get("priority", 0),
        )

        app.backend.enqueue(msg)
        return ChainHandle(chain_id=chain_id, app=app, total=len(self.signatures))


@dataclass
class ChainHandle:
    """Handle for a dispatched chain."""

    chain_id: str
    app: FlashQ
    total: int


# ---------------------------------------------------------------------------
# Group — parallel execution
# ---------------------------------------------------------------------------

@dataclass
class Group:
    """Execute tasks in parallel.

    Usage::

        group(add.s(1, 2), add.s(3, 4), add.s(5, 6)).delay(app)
    """

    signatures: list[Signature]

    def delay(self, app: FlashQ) -> GroupHandle:
        """Dispatch all tasks in parallel."""
        group_id = str(uuid.uuid4())
        task_ids: list[str] = []

        for sig in self.signatures:
            msg = TaskMessage(
                task_name=sig.task_name,
                args=sig.args,
                kwargs=sig.kwargs,
                queue=sig.options.get("queue", "default"),
                priority=sig.options.get("priority", 0),
            )
            app.backend.enqueue(msg)
            task_ids.append(msg.id)

        return GroupHandle(
            group_id=group_id,
            app=app,
            task_ids=task_ids,
        )


@dataclass
class GroupHandle:
    """Handle for a dispatched group."""

    group_id: str
    app: FlashQ
    task_ids: list[str]

    def get_results(self, timeout: float = 30.0) -> list[Any]:
        """Wait for all group tasks to complete and return results."""
        import time

        start = time.monotonic()
        results: dict[str, Any] = {}

        while time.monotonic() - start < timeout:
            for tid in self.task_ids:
                if tid in results:
                    continue
                result = self.app.backend.get_result(tid)
                if result is not None and result.state in (TaskState.SUCCESS, TaskState.FAILURE, TaskState.DEAD):
                    results[tid] = result.result if result.state == TaskState.SUCCESS else None

            if len(results) == len(self.task_ids):
                return [results[tid] for tid in self.task_ids]

            time.sleep(0.1)

        raise TimeoutError(f"Group {self.group_id} did not complete within {timeout}s")

    @property
    def completed(self) -> int:
        """Number of completed tasks."""
        count = 0
        for tid in self.task_ids:
            result = self.app.backend.get_result(tid)
            if result is not None and result.state in (TaskState.SUCCESS, TaskState.FAILURE, TaskState.DEAD):
                count += 1
        return count


# ---------------------------------------------------------------------------
# Chord — group + callback
# ---------------------------------------------------------------------------

@dataclass
class Chord:
    """Execute a group of tasks, then run a callback with all results.

    Usage::

        chord(
            group(add.s(1, 2), add.s(3, 4)),
            aggregate.s(),
        ).delay(app)
    """

    header: Group  # parallel tasks
    callback: Signature  # called with list of results

    def delay(self, app: FlashQ) -> ChordHandle:
        """Dispatch the chord."""
        chord_id = str(uuid.uuid4())

        # Dispatch the group
        task_ids: list[str] = []
        for sig in self.header.signatures:
            msg = TaskMessage(
                task_name=sig.task_name,
                args=sig.args,
                kwargs=sig.kwargs,
                queue=sig.options.get("queue", "default"),
                priority=sig.options.get("priority", 0),
            )
            app.backend.enqueue(msg)
            task_ids.append(msg.id)

        return ChordHandle(
            chord_id=chord_id,
            app=app,
            task_ids=task_ids,
            callback=self.callback,
        )


@dataclass
class ChordHandle:
    """Handle for a dispatched chord."""

    chord_id: str
    app: FlashQ
    task_ids: list[str]
    callback: Signature


# ---------------------------------------------------------------------------
# Convenience constructors
# ---------------------------------------------------------------------------

def chain(*signatures: Signature) -> Chain:
    """Create a chain from signatures."""
    return Chain(signatures=list(signatures))


def group(*signatures: Signature) -> Group:
    """Create a group from signatures."""
    return Group(signatures=list(signatures))


def chord(header: Group, callback: Signature) -> Chord:
    """Create a chord from a group and a callback signature."""
    return Chord(header=header, callback=callback)
