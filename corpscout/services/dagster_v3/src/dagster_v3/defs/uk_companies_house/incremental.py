"""Forward-only incremental ingest of the Companies House Accounts Data Product.

Companies file accounts ~annually, so ingesting the daily archives and keeping the
latest filing per company (ReplacingMergeTree) converges on the latest annual report
for every iXBRL filer over ~12 months — via the free bulk product, no API limits.

Forward-only: a cursor (in the source DuckDB file) tracks the last processed archive
date; each run appends the archives published since then (bounded per run). Coverage
builds over time. A separate backfill can seed the prior 12 months later.
"""

import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.uk_companies_house import financials, raw_archives, tables

CURSOR_TABLE = f"{tables.DLT_DATASET_NAME}.ingest_cursor"
CURSOR_NAME = "accounts_archive"
ARCHIVE_SOURCE_SLUG = "uk_companies_house_accounts_archive"


def _ensure_cursor_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
    connection.execute(
        f"create table if not exists {CURSOR_TABLE} "
        "(name varchar primary key, last_archive_date varchar)"
    )


def read_cursor(connection: duckdb.DuckDBPyConnection) -> str | None:
    _ensure_cursor_table(connection)
    row = connection.execute(
        f"select last_archive_date from {CURSOR_TABLE} where name = ?", [CURSOR_NAME]
    ).fetchone()
    return row[0] if row else None


def write_cursor(
    connection: duckdb.DuckDBPyConnection, last_archive_date: str
) -> None:
    _ensure_cursor_table(connection)
    connection.execute(
        f"insert into {CURSOR_TABLE} values (?, ?) "
        "on conflict (name) do update set last_archive_date = excluded.last_archive_date",
        [CURSOR_NAME, last_archive_date],
    )


def select_new_archives(
    archives: list[tuple[str, str]], cursor: str | None, max_archives: int
) -> list[tuple[str, str]]:
    """Archives to process this run (forward-only)."""
    if not archives:
        return []
    if cursor is None:
        # forward-only start: seed from the single latest archive.
        return archives[-1:]
    newer = [a for a in archives if a[0] > cursor]
    return newer[:max_archives]


def build_incremental_metrics(
    *,
    connection: duckdb.DuckDBPyConnection,
    object_store: ObjectStoreResource,
    source_run_id: str,
    max_archives: int = 10,
    log: Callable[..., object] | None = None,
) -> dict[str, Any]:
    """Parse the new archives since the cursor into the native-GBP metrics table.

    Does NOT advance the cursor — the asset advances it only after a successful
    ClickHouse append, so a failure re-processes the same archives next run.
    """
    stored_archives = raw_archives.preferred_stored_archives(
        object_store,
        kind=raw_archives.ACCOUNTS_KIND,
    )
    archives = [
        (archive.published_date, archive.object_key) for archive in stored_archives
    ]
    archives_by_key = {archive.object_key: archive for archive in stored_archives}
    cursor = read_cursor(connection)
    selected = select_new_archives(archives, cursor, max_archives)

    rows: list[tuple[Any, ...]] = []
    parsed = 0
    for archive_date, object_key in selected:
        archive = archives_by_key[object_key]
        with tempfile.TemporaryDirectory(prefix="uk_accounts_inc_") as tmpdir:
            archive_path = Path(tmpdir) / archive.filename
            object_store.download_file(
                object_key,
                archive_path,
                bucket=raw_archives.UK_COMPANIES_HOUSE_RAW_BUCKET,
            )
            archive_rows, archive_parsed = financials.parse_archive_rows(archive_path)
        rows.extend(archive_rows)
        parsed += archive_parsed
        if log is not None:
            log(
                "Parsed UK accounts archive %s: filings=%s",
                archive_date,
                archive_parsed,
            )

    counts = financials.write_metrics_table(
        connection=connection,
        rows=rows,
        source_run_id=source_run_id,
        source_slug=ARCHIVE_SOURCE_SLUG,
        allow_empty=True,
    )
    counts.update(
        {
            "processed_archives": [d for d, _ in selected],
            "source_object_keys": [key for _, key in selected],
            "filings_parsed": parsed,
            "cursor_before": cursor or "",
        }
    )
    if log is not None:
        log(
            "Built UK incremental metrics: archives=%s filings=%s companies=%s",
            len(selected),
            parsed,
            counts["companies"],
        )
    return counts
