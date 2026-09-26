"""Batch concurrent browser callbacks while each waits for durable acknowledgement."""

from queue import Empty, Queue
from threading import Event, Thread
from time import monotonic

from dagster_v3.defs.company_domains.results import insert_results


class BraveResultWriter:
    def __init__(self, clickhouse, *, max_items: int, max_seconds: float):
        self.clickhouse = clickhouse
        self.max_items = max_items
        self.max_seconds = max_seconds
        self.queue = Queue()
        self.error = None
        self.thread = Thread(
            target=self._write, name="brave-result-writer", daemon=True
        )

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, kind, error, traceback):
        self.queue.put(None)
        self.thread.join()
        if error is None and self.error is not None:
            raise RuntimeError(
                "Brave result publication failed; resume this task"
            ) from self.error

    def save(self, record: dict) -> None:
        if self.error is not None:
            raise RuntimeError(
                "Brave result publication failed; resume this task"
            ) from self.error
        acknowledged = Event()
        self.queue.put((record, acknowledged))
        while not acknowledged.wait(0.1):
            if self.error is not None:
                raise RuntimeError(
                    "Brave result publication failed; resume this task"
                ) from self.error
        if self.error is not None:
            raise RuntimeError(
                "Brave result publication failed; resume this task"
            ) from self.error

    def _write(self) -> None:
        batch = []
        deadline = None
        try:
            while True:
                timeout = None if deadline is None else max(0, deadline - monotonic())
                try:
                    item = self.queue.get(timeout=timeout)
                except Empty:
                    item = "flush"
                if item is not None and item != "flush":
                    batch.append(item)
                    if deadline is None:
                        deadline = monotonic() + self.max_seconds
                if batch and (
                    item is None or item == "flush" or len(batch) >= self.max_items
                ):
                    with self.clickhouse.get_connection() as client:
                        insert_results(client, [record for record, _ in batch])
                    for _, acknowledged in batch:
                        acknowledged.set()
                    batch = []
                    deadline = None
                if item is None:
                    return
        except BaseException as error:
            self.error = error
            for _, acknowledged in batch:
                acknowledged.set()
