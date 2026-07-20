"""ESEF filings index crawl: full sweep of filings.xbrl.org into DuckDB.

Non-partitioned by design: the full index is ~25k rows / ~125 pages -- one
sweep, no per-window bookkeeping (see docs/data-source-guidelines.md on when
partitioning earns its keep). The expensive part -- fact JSON downloads -- is
handled incrementally in a later, year-partitioned asset via S3
skip-existing, not here.

No `from __future__ import annotations` -- this module defines a `@dg.asset`
and stringizing the `context: AssetExecutionContext` hint breaks Dagster's op
context-type validation (see CLAUDE.md).
"""

from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.client import (
    ESEF_INDEX_URL,
    EsefFilingsClient,
    EsefFilingRecord,
)

GROUP_NAME = "esef_filings"
ESEF_FILINGS_DUCKDB_POOL = "esef_filings_duckdb"
ESEF_FILINGS_DUCKDB_ROOT = Path("data")

FILINGS_INDEX_TABLE = "filings_index"
QUALIFIED_FILINGS_INDEX_TABLE = f"{tables.DLT_DATASET_NAME}.{FILINGS_INDEX_TABLE}"

TOP_COUNTRY_LIMIT = 10

# esef_filings.filings_index column types, keyed by name -- kept as a map
# (rather than inline in the CREATE TABLE) so the assert below catches any
# drift against tables.ESEF_FILINGS_EXPORT_COLUMNS (the ClickHouse export
# contract from migration 000149) immediately at import time.
_FILINGS_INDEX_COLUMN_TYPES: dict[str, str] = {
    "lei": "varchar",
    "entity_name": "varchar",
    "fxo_id": "varchar",
    "country": "varchar",
    "period_end": "varchar",
    "date_added": "varchar",
    "processed_at": "varchar",
    "json_url": "varchar",
    "package_url": "varchar",
    "report_url": "varchar",
    "viewer_url": "varchar",
    "package_sha256": "varchar",
    "error_count": "integer",
    "warning_count": "integer",
    "inconsistency_count": "integer",
    "has_json_facts": "boolean",
    "source_url": "varchar",
    "source_run_id": "varchar",
}
assert set(_FILINGS_INDEX_COLUMN_TYPES) == set(tables.ESEF_FILINGS_EXPORT_COLUMNS), (
    "esef_filings/assets.py _FILINGS_INDEX_COLUMN_TYPES is out of sync with "
    "tables.ESEF_FILINGS_EXPORT_COLUMNS"
)
_FILINGS_INDEX_COLUMNS_SQL = ", ".join(
    f"{name} {_FILINGS_INDEX_COLUMN_TYPES[name]}"
    for name in tables.ESEF_FILINGS_EXPORT_COLUMNS
)


def esef_filings_source_duckdb_path(
    *, root: str | Path = ESEF_FILINGS_DUCKDB_ROOT
) -> Path:
    return Path(root) / "esef_filings_source.duckdb"


def _row_from_record(
    record: EsefFilingRecord, *, source_url: str, source_run_id: str
) -> tuple[Any, ...]:
    return (
        record.lei,
        record.entity_name,
        record.fxo_id,
        record.country,
        record.period_end,
        record.date_added,
        record.processed_at,
        record.json_url,
        record.package_url,
        record.report_url,
        record.viewer_url,
        record.package_sha256,
        record.error_count,
        record.warning_count,
        record.inconsistency_count,
        record.json_url is not None,
        source_url,
        source_run_id,
    )


def _country_distribution_top(
    records: Sequence[EsefFilingRecord], *, limit: int
) -> dict[str, int]:
    counts = Counter(record.country for record in records)
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return dict(ranked[:limit])


def replace_esef_filings_index(
    *,
    connection: Any,
    records: Sequence[EsefFilingRecord],
    source_url: str,
    source_run_id: str,
) -> dict[str, Any]:
    """Full-replace esef_filings.filings_index with `records`.

    Refuses to touch the existing table on an empty crawl (raises
    ValueError before any DB statement runs), and wraps the
    create-or-replace + insert in one transaction so a mid-insert failure
    rolls back to the prior table rather than leaving it half-replaced.
    """
    if not records:
        raise ValueError(
            "ESEF filings crawl returned 0 filings -- refusing to replace "
            f"{QUALIFIED_FILINGS_INDEX_TABLE} (refuse-to-replace-on-empty)."
        )
    rows = [
        _row_from_record(record, source_url=source_url, source_run_id=source_run_id)
        for record in records
    ]

    connection.execute("begin transaction")
    try:
        connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"create or replace table {QUALIFIED_FILINGS_INDEX_TABLE} "
            f"({_FILINGS_INDEX_COLUMNS_SQL})"
        )
        connection.executemany(
            f"insert into {QUALIFIED_FILINGS_INDEX_TABLE} values "
            f"({', '.join(['?'] * len(tables.ESEF_FILINGS_EXPORT_COLUMNS))})",
            rows,
        )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise

    with_json_facts = sum(1 for record in records if record.json_url is not None)
    return {
        "row_count": len(records),
        "with_json_facts_count": with_json_facts,
        "without_json_facts_count": len(records) - with_json_facts,
        "country_distribution_top10": _country_distribution_top(
            records, limit=TOP_COUNTRY_LIMIT
        ),
        "distinct_country_count": len({record.country for record in records}),
    }


@dg.asset(
    name="esef_filings_index_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "esef_filings"},
    pool=ESEF_FILINGS_DUCKDB_POOL,
    description=(
        "Full crawl of the filings.xbrl.org filing index into DuckDB table "
        f"{QUALIFIED_FILINGS_INDEX_TABLE}. Full replace each run; refuses to "
        "replace the table on an empty crawl."
    ),
)
def esef_filings_index_duckdb(
    context: dg.AssetExecutionContext,
    esef_filings_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    client = EsefFilingsClient()
    records = list(client.iter_filings())
    context.log.info("ESEF filings index crawl fetched %d filings", len(records))

    # Checked here too (ahead of replace_esef_filings_index's own guard): the
    # dagster_duckdb DuckDBResource.get_connection() context manager only
    # closes its connection on normal exit, not on an exception raised
    # inside the `with` block, so an empty crawl must never open a
    # connection at all -- besides being the spirit of "refuse to replace
    # before touching the existing table".
    if not records:
        raise ValueError(
            "ESEF filings crawl returned 0 filings -- refusing to replace "
            f"{QUALIFIED_FILINGS_INDEX_TABLE} (refuse-to-replace-on-empty)."
        )

    with esef_filings_duckdb.get_connection() as connection:
        summary = replace_esef_filings_index(
            connection=connection,
            records=records,
            source_url=ESEF_INDEX_URL,
            source_run_id=context.run_id,
        )

    return dg.MaterializeResult(
        metadata={
            "row_count": summary["row_count"],
            "with_json_facts_count": summary["with_json_facts_count"],
            "without_json_facts_count": summary["without_json_facts_count"],
            "distinct_country_count": summary["distinct_country_count"],
            "country_distribution_top10": dg.MetadataValue.json(
                summary["country_distribution_top10"]
            ),
            "duckdb_path": str(esef_filings_source_duckdb_path()),
        }
    )


@dg.asset_check(
    asset="esef_filings_index_duckdb",
    name="filings_index_non_empty",
)
def filings_index_non_empty(
    esef_filings_duckdb: DuckDBResource,
) -> dg.AssetCheckResult:
    with read_only_duckdb_connection(esef_filings_duckdb) as connection:
        [(row_count,)] = connection.execute(
            f"select count(*) from {QUALIFIED_FILINGS_INDEX_TABLE}"
        ).fetchall()
    return dg.AssetCheckResult(
        passed=row_count > 0,
        metadata={"row_count": int(row_count)},
    )


defs = dg.Definitions(
    assets=[esef_filings_index_duckdb],
    asset_checks=[filings_index_non_empty],
    resources={
        "esef_filings_duckdb": duckdb_resource(esef_filings_source_duckdb_path()),
    },
)
