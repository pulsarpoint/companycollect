"""A priority queue of distinct domains waiting to be scanned.

SQLite, JSON and REST adapters can all insert the same ScanEntry shape. This
module owns only in-memory ordering and removal, not persistence, acknowledgements,
retries or execution. There is at most one pending entry per domain. Pages/work
discovered during a domain scan belong to a separate queue.

    queue = ScanQueue[dict[str, str]]()
    queue.insert(ScanEntry(domain="example.com", priority=10,
                           payload={"url": "https://example.com/"}))
    entry = queue.get_next()  # Next domain to scan.
    for entry in queue:
        process(entry.payload)
"""

import heapq
import re
from collections.abc import Iterator
from dataclasses import dataclass
from threading import Lock


def _normalize_domain(domain: str) -> str:
    """Normalize an ASCII hostname without merging distinct subdomains or tenants."""
    if not isinstance(domain, str):
        raise TypeError("domain must be a hostname string")
    normalized = domain.strip().removesuffix(".").lower()
    if len(normalized) > 253 or any(
        re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) is None
        for label in normalized.split(".")
    ):
        raise ValueError("domain must be an ASCII hostname, without a URL or port")
    return normalized


@dataclass(frozen=True, slots=True, kw_only=True)
class ScanEntry[T]:
    """Immutable routing/priority metadata around caller-owned task data.

    Higher integer priorities run first; zero and negative priorities are valid.
    Payloads are opaque to the queue, retained by reference and never compared.
    Domain aliases such as www.example.com and example.com stay separate unless
    an input adapter explicitly maps them to the same domain. Adapters supply
    international domains in their canonical ASCII (IDNA) form.
    """

    domain: str
    priority: int
    payload: T

    def __post_init__(self) -> None:
        if not isinstance(self.priority, int) or isinstance(self.priority, bool):
            raise TypeError("priority must be an integer")
        object.__setattr__(self, "domain", _normalize_domain(self.domain))


class ScanQueue[T]:
    """Thread-safe domain scheduling, highest priority first and FIFO on ties.

    FIFO means accepted insertion order across all input sources, not a timestamp
    supplied by an adapter. Duplicate pending domains are ignored: their original
    priority, payload and FIFO position remain intact. insert() reports whether the
    entry was accepted. Once removed, a domain can be queued again for another scan.

    One heap orders all pending domains; a set prevents duplicates. Insertion and
    removal are O(log(number of domains)), duplicate detection is O(1).
    """

    def __init__(self) -> None:
        self._entries: list[tuple[int, int, ScanEntry[T]]] = []
        self._domains: set[str] = set()
        self._sequence: int = 0
        self._lock: Lock = Lock()

    def insert(self, entry: ScanEntry[T]) -> bool:
        """Queue a domain; return False if it already has a pending entry."""
        if not isinstance(entry, ScanEntry):
            raise TypeError("entry must be a ScanEntry")
        with self._lock:
            if entry.domain in self._domains:
                return False
            heapq.heappush(self._entries, (-entry.priority, self._sequence, entry))
            self._domains.add(entry.domain)
            self._sequence += 1
            return True

    def get_next(self) -> ScanEntry[T] | None:
        """Remove the next domain to scan, or return None immediately when empty."""
        with self._lock:
            if not self._entries:
                return None
            _, _, entry = heapq.heappop(self._entries)
            self._domains.remove(entry.domain)
            return entry

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def __iter__(self) -> Iterator[ScanEntry[T]]:
        """Drain pending domains, including insertions before the next pull.

        Stops when a pull sees an empty queue. An exhausted iterator stays exhausted;
        later insertions can be read with get_next() or a new iterator.
        Separate iterators share pending entries and never duplicate a removal.
        """
        while (entry := self.get_next()) is not None:
            yield entry
