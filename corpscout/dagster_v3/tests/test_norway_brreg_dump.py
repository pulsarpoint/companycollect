"""Unit tests for translator/norway_brreg/dump.py."""
from __future__ import annotations

from translator.norway_brreg.config import get_config
from translator.norway_brreg.dump import dump_to_clickhouse
from translator.queue import TranslationQueue, TranslationQueueItem
from translator.types import TranslationResult


# ---------------------------------------------------------------------------
# Fake ClickHouse client (same shape as in test_translator_flush.py)
# ---------------------------------------------------------------------------


class _FakeCHClient:
    def __init__(self):
        self.commands: list[str] = []
        self.inserts: list[tuple] = []

    def command(self, sql, parameters=None):
        self.commands.append(sql)

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, data, tuple(column_names or ())))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _enqueue_and_complete(tmp_path, texts: list[str]) -> None:
    """Seed the queue and mark all items completed."""
    q = TranslationQueue(tmp_path / "q.duckdb")
    q.initialize()
    items = [
        TranslationQueueItem(
            source_duckdb_path="clickhouse",
            source_table="corpscout.no_companies",
            source_pk="",
            source_field="activity_text_original",
            source_text=text,
            target_language="en",
        )
        for text in texts
    ]
    q.enqueue_items(items)
    claimed = q.claim_batch(limit=len(items), worker_id="test")
    q.complete_batch(
        claimed,
        [TranslationResult(item_id=c.item_id, translated_text=c.source_text.upper()) for c in claimed],
        provider="fake",
        model="fake-model",
        duration_seconds=0.1,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_dump_writes_completed_rows_to_clickhouse(tmp_path):
    _enqueue_and_complete(tmp_path, ["Holdingselskap", "Bygg"])
    ch = _FakeCHClient()
    config = get_config()

    written = dump_to_clickhouse(
        tmp_path / "q.duckdb", ch, config,
        provider="local-llm", model="test-model",
    )

    assert written == 2
    # Must have created a staging table, inserted rows, and fired the INSERT … SELECT.
    assert any("CREATE TABLE" in c and "ENGINE = Memory" in c for c in ch.commands)
    assert any("INSERT INTO corpscout.text_translations" in c for c in ch.commands)
    assert any("DROP TABLE" in c for c in ch.commands)
    # Data rows were inserted into the staging table.
    assert len(ch.inserts) == 1
    assert len(ch.inserts[0][1]) == 2


def test_dump_empty_queue_is_noop(tmp_path):
    q = TranslationQueue(tmp_path / "q.duckdb")
    q.initialize()
    ch = _FakeCHClient()
    config = get_config()

    written = dump_to_clickhouse(
        tmp_path / "q.duckdb", ch, config,
        provider="local-llm", model="m",
    )

    assert written == 0
    assert ch.commands == []


def test_dump_batches_large_result_sets(tmp_path):
    texts = [f"tekst {i}" for i in range(150)]
    _enqueue_and_complete(tmp_path, texts)
    ch = _FakeCHClient()
    config = get_config()

    written = dump_to_clickhouse(
        tmp_path / "q.duckdb", ch, config,
        provider="local-llm", model="m",
        batch_size=100,
    )

    assert written == 150
    # Two batches → two inserts into two different staging tables.
    assert len(ch.inserts) == 2
    assert len(ch.inserts[0][1]) == 100
    assert len(ch.inserts[1][1]) == 50


def test_dump_calls_heartbeat_once_per_batch(tmp_path):
    texts = [f"tekst {i}" for i in range(200)]
    _enqueue_and_complete(tmp_path, texts)
    ch = _FakeCHClient()
    config = get_config()
    heartbeats: list = []

    dump_to_clickhouse(
        tmp_path / "q.duckdb", ch, config,
        provider="local-llm", model="m",
        batch_size=100,
        heartbeat_fn=heartbeats.append,
    )

    assert len(heartbeats) == 2  # 200 rows / 100 per batch = 2 batches


def test_dump_skips_empty_translations(tmp_path):
    """flush_translations already drops empty translated_text; dump honours that."""
    q = TranslationQueue(tmp_path / "q.duckdb")
    q.initialize()
    item = TranslationQueueItem(
        source_duckdb_path="clickhouse",
        source_table="corpscout.no_companies",
        source_pk="",
        source_field="activity_text_original",
        source_text="Tomtekst",
        target_language="en",
    )
    q.enqueue_items([item])
    claimed = q.claim_batch(limit=1, worker_id="t")
    # Translate to empty string — flush_translations will skip it.
    q.complete_batch(
        claimed,
        [TranslationResult(item_id=claimed[0].item_id, translated_text="")],
        provider="fake",
        model="fake",
        duration_seconds=0.0,
    )

    ch = _FakeCHClient()
    written = dump_to_clickhouse(
        tmp_path / "q.duckdb", ch, get_config(),
        provider="local-llm", model="m",
    )
    # Empty translation → 0 written (flush_translations skips it).
    assert written == 0
