"""Iterator-to-file-like adapters for streaming uploads with hash/byte accounting."""

import hashlib
import io
from collections.abc import Iterator


class StreamStats:
    """Accumulates sha256, byte count, and record count for a streamed payload."""

    def __init__(self) -> None:
        self._hasher = hashlib.sha256()
        self.bytes_read = 0
        self.records = 0

    def update(self, chunk: bytes) -> None:
        self._hasher.update(chunk)
        self.bytes_read += len(chunk)

    @property
    def sha256_hex(self) -> str:
        return self._hasher.hexdigest()


def observe_chunks(chunks: Iterator[bytes], stats: StreamStats) -> Iterator[bytes]:
    """Yield chunks unchanged while updating stats."""
    for chunk in chunks:
        stats.update(chunk)
        yield chunk


class IterableReader(io.RawIOBase):
    """Read-only file-like object over an iterator of byte chunks."""

    def __init__(self, chunks: Iterator[bytes]) -> None:
        self._chunks = iter(chunks)
        self._buffer = b""

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            raise ValueError(
                "IterableReader only supports bounded reads; an unbounded read "
                "would buffer the whole payload in memory"
            )
        while len(self._buffer) < size:
            try:
                self._buffer += next(self._chunks)
            except StopIteration:
                break
        data, self._buffer = self._buffer[:size], self._buffer[size:]
        return data
