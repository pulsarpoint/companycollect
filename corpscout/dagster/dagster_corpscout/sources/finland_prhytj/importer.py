"""Batch importer for normalized PRH YTJ snapshot rows."""

import uuid
from datetime import datetime, timezone
from typing import BinaryIO

from dagster_corpscout.sources.finland_prhytj.normalizer import ImportRun, normalize_record
from dagster_corpscout.sources.finland_prhytj.parser import parse_snapshot
from dagster_corpscout.sources.finland_prhytj.tables import NORMALIZED_TABLE_COLUMNS, NORMALIZED_TABLES


def import_normalized_snapshot(
    *,
    clickhouse,
    stream: BinaryIO,
    run_id: str,
    truncate: bool = True,
    batch_size: int = 1000,
) -> dict[str, int]:
    client = clickhouse.client()
    if truncate:
        clickhouse.truncate_tables(client, NORMALIZED_TABLES)

    run = ImportRun(
        run_id=run_id,
        source_export_id=uuid.uuid4(),
        ingested_at=datetime.now(timezone.utc),
    )
    buffers: dict[str, list[dict]] = {table: [] for table in NORMALIZED_TABLES}
    counts = {table: 0 for table in NORMALIZED_TABLES}

    def flush(table: str) -> None:
        rows = buffers[table]
        if not rows:
            return
        clickhouse.insert_rows(client, table, NORMALIZED_TABLE_COLUMNS[table], rows)
        rows.clear()

    for parsed in parse_snapshot(stream):
        rows_by_table = normalize_record(run, parsed)
        for table in NORMALIZED_TABLES:
            rows = rows_by_table[table]
            if not rows:
                continue
            buffers[table].extend(rows)
            counts[table] += len(rows)
            if len(buffers[table]) >= batch_size:
                flush(table)

    for table in NORMALIZED_TABLES:
        flush(table)

    return counts
