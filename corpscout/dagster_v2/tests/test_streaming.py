import hashlib

from dagster_corpscout.lib.streaming import IterableReader, StreamStats, observe_chunks


def test_iterable_reader_rejects_unbounded_read():
    reader = IterableReader(iter([b"abc"]))
    try:
        reader.read()
        raise AssertionError("expected ValueError for unbounded read")
    except ValueError:
        pass


def test_iterable_reader_reads_in_sizes():
    reader = IterableReader(iter([b"abc", b"def", b"gh"]))
    assert reader.read(2) == b"ab"
    assert reader.read(4) == b"cdef"
    assert reader.read(100) == b"gh"
    assert reader.read(10) == b""


def test_iterable_reader_does_not_drain_iterator():
    consumed = []

    def chunks():
        for chunk in [b"aaa", b"bbb", b"ccc", b"ddd"]:
            consumed.append(chunk)
            yield chunk

    reader = IterableReader(chunks())
    assert reader.read(4) == b"aaab"
    assert consumed == [b"aaa", b"bbb"]


def test_observe_chunks_counts_and_hashes_once():
    stats = StreamStats()
    reader = IterableReader(observe_chunks(iter([b"hello ", b"world"]), stats))
    data = b""
    while True:
        piece = reader.read(4)
        if not piece:
            break
        data += piece
    assert data == b"hello world"
    assert stats.bytes_read == 11
    assert stats.sha256_hex == hashlib.sha256(b"hello world").hexdigest()
