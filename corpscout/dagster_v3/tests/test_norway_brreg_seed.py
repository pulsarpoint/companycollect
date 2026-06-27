"""Unit tests for translator/norway_brreg/seed.py.

Uses a real temp DuckDB queue and a fake ClickHouse client that returns a small
PyArrow table.  Does NOT connect to a real ClickHouse instance.
"""
import pyarrow as pa
import pytest

from translator.norway_brreg.config import get_config
from translator.norway_brreg.seed import SeedResult, build_queue
from translator.queue import TranslationQueue


# ---------------------------------------------------------------------------
# Fake ClickHouse client
# ---------------------------------------------------------------------------


class _FakeCHClient:
    """Returns pre-canned Arrow tables per (sql, parameters) call; records calls."""

    def __init__(self, arrow_per_column: dict[str, pa.Table]):
        """arrow_per_column: maps original_col name → Arrow table to return."""
        self._data = arrow_per_column
        self.calls: list[dict] = []
        self.flush_calls: list[dict] = []

    def query_arrow(self, sql: str, *, parameters: dict | None = None) -> pa.Table:
        col = (parameters or {}).get("column", "")
        self.calls.append({"sql": sql, "column": col, "parameters": parameters})
        return self._data.get(col, pa.table({"source_text": pa.array([], type=pa.string())}))

    # flush_translations uses client.command() and client.insert()
    def command(self, sql, parameters=None):
        self.flush_calls.append({"type": "command", "sql": sql})

    def insert(self, table, data, column_names=None):
        self.flush_calls.append({"type": "insert", "table": table, "data": data})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dynamic_arrow(texts: list[str]) -> pa.Table:
    return pa.table({"source_text": pa.array(texts, type=pa.string())})


def _static_arrow(texts: list[str], keys: list[str]) -> pa.Table:
    return pa.table({
        "source_text": pa.array(texts, type=pa.string()),
        "static_key": pa.array(keys, type=pa.string()),
    })


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_build_queue_inserts_dynamic_items_into_duckdb(tmp_path):
    config = get_config()
    ch = _FakeCHClient({
        "articles_purpose_original": _dynamic_arrow(["Holding", "Bygg"]),
        "activity_text_original": _dynamic_arrow(["Energi"]),
        "legal_form_description_original": _static_arrow(["Aksjeselskap"], ["AS"]),
    })

    result = build_queue(config, ch, str(tmp_path / "q.duckdb"))

    assert isinstance(result, SeedResult)
    assert result.dynamic_enqueued == 3  # 2 + 1 dynamic terms
    # static term flushed directly to CH (not in DuckDB queue)
    assert result.static_flushed == 1

    # DuckDB queue must contain exactly the 3 dynamic items.
    q = TranslationQueue(tmp_path / "q.duckdb")
    q.initialize()
    summary = q.summary()
    assert summary.total_items == 3
    assert summary.pending_items == 3


def test_build_queue_item_ids_match_python_sha256(tmp_path):
    """Item IDs inserted by SQL must match the Python hash in TranslationQueueItem."""
    from translator.queue import TranslationQueueItem

    config = get_config()
    ch = _FakeCHClient({
        "articles_purpose_original": _dynamic_arrow(["Holdingselskap"]),
        "activity_text_original": _dynamic_arrow([]),
        "legal_form_description_original": _static_arrow([], []),
    })
    build_queue(config, ch, str(tmp_path / "q.duckdb"))

    # Compute expected item_id the same way Python does.
    expected = TranslationQueueItem(
        source_duckdb_path="clickhouse",
        source_table="corpscout.no_companies",
        source_pk="",
        source_field="articles_purpose_original",
        source_text="Holdingselskap",
        target_language="en",
    ).item_id

    import duckdb
    with duckdb.connect(str(tmp_path / "q.duckdb")) as conn:
        row = conn.execute("SELECT item_id FROM translation_items LIMIT 1").fetchone()
    assert row is not None
    assert row[0] == expected


def test_build_queue_idempotent_on_second_call(tmp_path):
    """Re-seeding must not duplicate items (ON CONFLICT DO NOTHING)."""
    config = get_config()
    ch = _FakeCHClient({
        "articles_purpose_original": _dynamic_arrow(["Holding"]),
        "activity_text_original": _dynamic_arrow([]),
        "legal_form_description_original": _static_arrow([], []),
    })
    build_queue(config, ch, str(tmp_path / "q.duckdb"))
    result2 = build_queue(config, ch, str(tmp_path / "q.duckdb"))

    # Second call enqueues 0 new items (idempotent).
    assert result2.dynamic_enqueued == 0
    q = TranslationQueue(tmp_path / "q.duckdb")
    q.initialize()
    assert q.summary().total_items == 1


def test_build_queue_static_unknown_code_not_flushed(tmp_path):
    """Unknown static-map codes must produce no flush rows."""
    config = get_config()
    ch = _FakeCHClient({
        "articles_purpose_original": _dynamic_arrow([]),
        "activity_text_original": _dynamic_arrow([]),
        "legal_form_description_original": _static_arrow(["Ukjent form"], ["UNKNOWN_CODE"]),
    })
    result = build_queue(config, ch, str(tmp_path / "q.duckdb"))
    assert result.static_flushed == 0
    # No INSERT command fired for unknown code.
    assert not any("INSERT INTO corpscout.text_translations" in c.get("sql", "") for c in ch.flush_calls)


def test_build_queue_calls_heartbeat(tmp_path):
    config = get_config()
    ch = _FakeCHClient({
        "articles_purpose_original": _dynamic_arrow(["A"]),
        "activity_text_original": _dynamic_arrow(["B"]),
        "legal_form_description_original": _static_arrow(["Aksjeselskap"], ["AS"]),
    })
    heartbeat_calls = []
    build_queue(config, ch, str(tmp_path / "q.duckdb"), heartbeat_fn=heartbeat_calls.append)
    # At least one heartbeat per field (3 fields).
    assert len(heartbeat_calls) >= 3


def test_build_queue_empty_source_returns_zero_counts(tmp_path):
    config = get_config()
    ch = _FakeCHClient({
        "articles_purpose_original": _dynamic_arrow([]),
        "activity_text_original": _dynamic_arrow([]),
        "legal_form_description_original": _static_arrow([], []),
    })
    result = build_queue(config, ch, str(tmp_path / "q.duckdb"))
    assert result.dynamic_enqueued == 0
    assert result.static_flushed == 0
