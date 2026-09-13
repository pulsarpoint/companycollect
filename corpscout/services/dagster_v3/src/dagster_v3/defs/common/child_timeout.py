"""Run a picklable function in a killable child process bounded by a timeout.

Generalised from ``esef_filings/segment_assets._parse_document_package_worker``
(the ESEF artifact parser's per-document guard, 2026-09-12): a child that
outlives its budget is terminated, then killed if it ignores SIGTERM, so one
pathological input can never wedge a worker. Always a ``spawn`` context so the
child is independently killable even from inside a ProcessPoolExecutor worker
(nested spawn is fine). ``fn`` and ``args`` must be picklable; keep ``fn`` in a
light module -- every call pays that module's import cost in the child.

The parent waits on the result pipe AND the child's sentinel and reads the
result as soon as it is available, and only then joins the child. A pickled
result larger than the OS pipe buffer (~64 KiB) blocks the child in ``send()``
until the parent reads, so joining first deadlocks until the budget kills the
child (final review 2026-09-13: a 70,000-byte result timed out; the domains
child returns every candidate with its full evidence). The artifact parser's
own guard (``segment_assets._parse_document_package_worker``) still has that
join-before-read shape; it is safe there only because its child sends a small
result (the parse payload goes to a file). Moving it onto this helper is a
spec section 8 follow-up.
"""

from collections.abc import Callable
from multiprocessing import get_context
from multiprocessing.connection import Connection, wait
from multiprocessing.process import BaseProcess
from time import monotonic
from typing import Any

DEFAULT_TERMINATE_GRACE_SECONDS = 5.0


class ChildTimeoutError(Exception):
    """The child exceeded its wall-clock budget and was killed."""

    def __init__(self, timeout_seconds: float) -> None:
        self.timeout_seconds = timeout_seconds
        super().__init__(f"child exceeded its {timeout_seconds}s budget and was killed")

    def __reduce__(self) -> tuple[Any, tuple[float]]:
        # The default Exception.__reduce__ replays the formatted message into
        # __init__, whose signature is (timeout_seconds,) -- reconstruct from
        # the real field so the error survives a ProcessPoolExecutor result
        # queue (see segment_assets.DocumentParseTimeoutError).
        return (type(self), (self.timeout_seconds,))


class ChildFailedError(Exception):
    """The child raised, or exited without sending a result."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(f"child failed: {detail}")

    def __reduce__(self) -> tuple[Any, tuple[str]]:
        return (type(self), (self.detail,))


def _child_main(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    connection: Connection,
) -> None:
    try:
        result = fn(*args)
    except Exception as exc:  # noqa: BLE001 - reported to the parent, not swallowed
        connection.send(("error", f"{type(exc).__name__}: {exc}"))
    else:
        connection.send(("ok", result))
    finally:
        connection.close()


def _receive(connection: Connection) -> tuple[Any, ...] | None:
    """The child's message, or None when it died without (fully) sending one."""
    try:
        return connection.recv()
    except EOFError:
        # The child died without calling send(): the pipe only looks ready
        # because the read end hit EOF.
        return None
    except OSError:
        # "got end of file during message": the child was killed mid-send
        # (an OOM-kill while writing a large result).
        return None


def _stop(child: BaseProcess, grace_seconds: float) -> None:
    """Terminate the child, then kill it if it ignores SIGTERM for the grace."""
    child.terminate()
    child.join(timeout=grace_seconds)
    if child.is_alive():
        child.kill()
        child.join()


def run_in_child_with_timeout(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    *,
    timeout_seconds: float,
    terminate_grace_seconds: float = DEFAULT_TERMINATE_GRACE_SECONDS,
) -> Any:
    """Return ``fn(*args)`` computed in a spawned child, or raise.

    ``ChildTimeoutError`` after ``timeout_seconds`` (the child is terminated,
    then killed after ``terminate_grace_seconds``); ``ChildFailedError`` when
    the child raised or died without a result (os._exit, OOM-kill, segfault).
    """
    ctx = get_context("spawn")
    parent_connection, child_connection = ctx.Pipe(duplex=False)
    child = ctx.Process(target=_child_main, args=(fn, args, child_connection))
    child.start()
    child_connection.close()
    deadline = monotonic() + timeout_seconds

    message: tuple[Any, ...] | None
    try:
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                _stop(child, terminate_grace_seconds)
                raise ChildTimeoutError(timeout_seconds)
            ready = wait([parent_connection, child.sentinel], timeout=remaining)
            if parent_connection in ready:
                # Read now, before joining: a large result only finishes
                # sending once the parent drains the pipe.
                message = _receive(parent_connection)
                break
            if child.sentinel in ready:
                # The child exited; a message may have raced the exit.
                message = (
                    _receive(parent_connection) if parent_connection.poll() else None
                )
                break
            # Nothing ready: the deadline passed; the loop head decides.
    finally:
        parent_connection.close()

    child.join(timeout=terminate_grace_seconds)
    if child.is_alive():
        _stop(child, terminate_grace_seconds)

    if message is None:
        raise ChildFailedError(
            f"child exited with code {child.exitcode} without a result"
        )
    if message[0] == "error":
        raise ChildFailedError(str(message[1]))
    return message[1]
