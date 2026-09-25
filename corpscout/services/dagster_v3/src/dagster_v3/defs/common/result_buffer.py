"""Acknowledged micro-batches for queue results.

ClickHouse creates one part per insert, and async inserts do not coalesce a single
writer that waits for each acknowledgement. Buffer results and insert them every few
seconds or every few hundred rows. Items leave the buffer only after the flush
callback returns, so a failed insert is retried with the same rows.
"""

import time
from collections.abc import Callable, Iterable
from typing import Generic, TypeVar

T = TypeVar("T")


class ResultBuffer(Generic[T]):
    def __init__(
        self,
        flush: Callable[[list[T]], object],
        *,
        max_items: int = 500,
        max_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._flush = flush
        self._max_items = max_items
        self._max_seconds = max_seconds
        self._clock = clock
        self._items: list[T] = []
        self._since: float | None = None

    def __len__(self) -> int:
        return len(self._items)

    def add(self, items: Iterable[T]) -> None:
        for item in items:
            if self._since is None:
                self._since = self._clock()
            self._items.append(item)
        if len(self._items) >= self._max_items:
            self.flush()
        else:
            self.flush_if_due()

    def flush_if_due(self) -> None:
        if self._since is not None and self._clock() - self._since >= self._max_seconds:
            self.flush()

    def flush(self) -> int:
        if not self._items:
            self._since = None
            return 0
        batch = list(self._items)
        self._flush(batch)
        self._items = []
        self._since = None
        return len(batch)
