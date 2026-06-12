"""Batch importer for normalized PRH YTJ snapshot rows."""

import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import BinaryIO

from dagster_corpscout.sources.finland.prh_ytj.normalizer import ImportRun, normalize_record
from dagster_corpscout.sources.finland.prh_ytj.parser import parse_snapshot
from dagster_corpscout.sources.finland.prh_ytj.tables import NORMALIZED_TABLE_COLUMNS, NORMALIZED_TABLES

DEFAULT_BATCH_SIZE = 10_000
DEFAULT_PROGRESS_INTERVAL_RECORDS = 50_000


def import_normalized_snapshot(
    *,
    clickhouse,
    stream: BinaryIO,
    run_id: str,
    truncate: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    progress_interval_records: int = DEFAULT_PROGRESS_INTERVAL_RECORDS,
    progress_logger: Callable[[str], None] | None = None,
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
    source_records = 0

    def flush(table: str) -> None:
        rows = buffers[table]
        if not rows:
            return
        clickhouse.insert_rows(client, table, NORMALIZED_TABLE_COLUMNS[table], rows)
        rows.clear()

    for parsed in parse_snapshot(stream):
        source_records += 1
        rows_by_table = normalize_record(run, parsed)
        for table in NORMALIZED_TABLES:
            rows = rows_by_table[table]
            if not rows:
                continue
            buffers[table].extend(rows)
            counts[table] += len(rows)
            if len(buffers[table]) >= batch_size:
                flush(table)
        if (
            progress_logger is not None
            and progress_interval_records > 0
            and source_records % progress_interval_records == 0
        ):
            progress_logger(
                "normalized snapshot import progress: "
                f"{source_records} source records, {sum(counts.values())} rows"
            )

    for table in NORMALIZED_TABLES:
        flush(table)

    if progress_logger is not None:
        progress_logger(
            "normalized snapshot import complete: "
            f"{source_records} source records, {sum(counts.values())} rows"
        )

    return counts
