"""Bulk seed: ClickHouse → DuckDB translation queue via Arrow."""
from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import logging
import duckdb

from translator.clickhouse import build_scan_sql, query_arrow
from translator.flush import FlushTranslationRow, flush_translations
from translator.config import SourceConfig
from translator.queue import TranslationQueue

LOGGER = logging.getLogger(__name__)

@dataclass(frozen=True)
class SeedResult:
    dynamic_enqueued: int
    static_flushed: int


def build_queue(
    config: SourceConfig,
    ch_client: Any,
    queue_duckdb_path: str | Path,
    *,
    heartbeat_fn: Callable[[str], None] | None = None,
) -> SeedResult:
    """Seed the DuckDB translation queue for one source.

    For each dynamic field: ``query_arrow`` (LEFT ANTI JOIN) → register in
    DuckDB → bulk INSERT into queue tables, hashes computed in DuckDB SQL,
    ON CONFLICT DO NOTHING (idempotent).

    For each static field: resolve via static-map dict → flush directly to
    ``corpscout.text_translations`` (provider='static').

    Calls ``heartbeat_fn`` after every field for activity liveness.
    """
    queue_path = Path(queue_duckdb_path)
    LOGGER.info(
        "seed queue started source=%s table=%s queue_path=%s fields=%d",
        config.source_slug,
        config.ch_table,
        queue_path,
        len(config.fields),
    )

    TranslationQueue(queue_path).initialize()

    dynamic_enqueued = 0
    static_flushed = 0
    version = int(time.time())




    with duckdb.connect(str(queue_path)) as conn:
        for field in config.fields:
            sql = build_scan_sql(config, field)
            params = {"table": config.ch_table, "column": field.original_col}
            arrow_table = query_arrow(ch_client, sql, params)
            field_col = field.original_col
            ch_table = config.ch_table
            field_type = "static" if field.static_map is not None else "dynamic"
            scanned_rows = arrow_table.num_rows

            LOGGER.info(
                "seed field scanned source=%s table=%s field=%s type=%s rows=%s",
                config.source_slug,
                ch_table,
                field_col,
                field_type,
                scanned_rows,
            )


            if field.static_map is None:
                # Dynamic field → bulk insert into DuckDB queue.
                conn.register("_scan_result", arrow_table)
                try:
                    pre_count = int(
                        conn.execute("SELECT count(*) FROM translation_items").fetchone()[0]
                    )
                    conn.execute("""
                        INSERT INTO translation_items (
                            item_id, source_text, source_text_hash,
                            target_language, status, attempt_count,
                            created_at, updated_at
                        )
                        SELECT
                            sha256(sha256(source_text) || '|en'),
                            source_text,
                            sha256(source_text),
                            'en',
                            'pending',
                            0,
                            current_timestamp,
                            current_timestamp
                        FROM _scan_result
                        ON CONFLICT (item_id) DO NOTHING
                    """)
                    conn.execute(f"""
                        INSERT INTO translation_locations (
                            location_id, item_id, source_duckdb_path,
                            source_table, source_pk, source_field,
                            created_at, updated_at
                        )
                        SELECT
                            sha256(concat_ws('|', 'clickhouse', '{ch_table}', '',
                                '{field_col}', sha256(source_text), 'en')),
                            sha256(sha256(source_text) || '|en'),
                            'clickhouse', '{ch_table}', '', '{field_col}',
                            current_timestamp, current_timestamp
                        FROM _scan_result
                        ON CONFLICT (location_id) DO NOTHING
                    """)
                    post_count = int(
                        conn.execute("SELECT count(*) FROM translation_items").fetchone()[0]
                    )
                    dynamic_enqueued += post_count - pre_count
                finally:
                    try:
                        conn.unregister("_scan_result")
                    except Exception:
                        pass
            else:
                # Static field → resolve dict, write directly to CH.
                mapping = field.static_map_dict() or {}
                col_data = arrow_table.to_pydict()
                texts = col_data.get("source_text", [])
                keys = col_data.get("static_key", [""] * len(texts))

                static_rows: list[FlushTranslationRow] = []
                for source_text, static_key in zip(texts, keys):
                    translation = mapping.get(static_key or "", "")
                    if translation:
                        static_rows.append(
                            FlushTranslationRow(
                                source_column=field_col,
                                source_text=source_text,
                                translated_text=translation,
                            )
                        )
                if static_rows:
                    static_flushed += flush_translations(
                        ch_client,
                        config,
                        static_rows,
                        provider="static",
                        model="static",
                        version=version,
                        run_id="seed-static",
                    )

            if heartbeat_fn is not None:
                heartbeat_fn(f"seeded field={field_col}")

    return SeedResult(dynamic_enqueued=dynamic_enqueued, static_flushed=static_flushed)
