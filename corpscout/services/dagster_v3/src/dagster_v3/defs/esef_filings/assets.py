"""ESEF filing discovery and derived ClickHouse datasets.

The active ingest path is partitioned by the source's ``processed`` week,
not by the filing's fiscal ``period_end``. This is the source clock that says
when a filing became available: a filing processed today is handled today even
when its financial period ended years ago. Each index partition is fetched
with an API-side processed-time filter and upserted by stable ``fxo_id``.

``esef_document_artifacts_s3`` downloads and parses each report package once.
Four independent partitioned assets project those artifacts into facts,
contact-candidate, taxonomy-label, and disclosure datasets.

No `from __future__ import annotations` -- this module defines `@dg.asset`s
and stringizing the `context: AssetExecutionContext` hint breaks Dagster's op
context-type validation (see CLAUDE.md).
"""

import json
import tempfile
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from threading import Lock
from time import perf_counter
from typing import Any

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.esef_filings import facts, tables
from dagster_v3.defs.esef_filings.artifact_contract import ARTIFACT_SCHEMA_VERSION
from dagster_v3.defs.esef_filings.client import (
    ESEF_INDEX_URL,
    EsefFilingsClient,
    EsefFilingRecord,
)
from dagster_v3.defs.esef_filings.metrics import (
    replace_esef_financial_metrics_clickhouse,
)
from dagster_v3.defs.esef_filings.publish import (
    export_esef_filings_clickhouse,
    replace_esef_entity_registry_map_clickhouse,
)

GROUP_NAME = "esef"
ESEF_FILINGS_DUCKDB_POOL = "esef_filings_duckdb"
ESEF_FILINGS_DUCKDB_ROOT = Path("data")

FILINGS_INDEX_TABLE = tables.FILINGS_INDEX_TABLE
QUALIFIED_FILINGS_INDEX_TABLE = tables.QUALIFIED_FILINGS_INDEX_TABLE

TOP_COUNTRY_LIMIT = 10

# Long-running parsing loops emit progress after this many processed filings.
_PROGRESS_LOG_INTERVAL = 100

FACTS_TABLE = tables.FACTS_TABLE
QUALIFIED_FACTS_TABLE = tables.QUALIFIED_FACTS_TABLE
FACTS_INGESTION_STATE_TABLE = "facts_ingestion_state"
QUALIFIED_FACTS_INGESTION_STATE_TABLE = (
    f"{tables.DLT_DATASET_NAME}.{FACTS_INGESTION_STATE_TABLE}"
)
ARELLE_ARTIFACT_FACTS_PARSER_CONTRACT = f"arelle-artifact-v{ARTIFACT_SCHEMA_VERSION}"

ESEF_PROCESSED_WEEK_PARTITIONS = dg.TimeWindowPartitionsDefinition(
    start=datetime(2023, 1, 1),
    fmt="%Y-%m-%d",
    cron_schedule="0 0 * * 0",
    timezone="UTC",
    end_offset=1,
)

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
_FILINGS_INDEX_INSERT_BATCH_SIZE = 50_000
_FILINGS_INDEX_BATCH_RELATION = "_esef_filings_index_batch"
_FILINGS_INDEX_UPSERT_TABLE = "_esef_filings_index_upsert"
_FILINGS_INDEX_ARROW_SCHEMA = pa.schema(
    [
        pa.field("lei", pa.string(), nullable=False),
        pa.field("entity_name", pa.string(), nullable=False),
        pa.field("fxo_id", pa.string(), nullable=False),
        pa.field("country", pa.string(), nullable=False),
        pa.field("period_end", pa.string()),
        pa.field("date_added", pa.string()),
        pa.field("processed_at", pa.string()),
        pa.field("json_url", pa.string()),
        pa.field("package_url", pa.string()),
        pa.field("report_url", pa.string()),
        pa.field("viewer_url", pa.string()),
        pa.field("package_sha256", pa.string()),
        pa.field("error_count", pa.int32(), nullable=False),
        pa.field("warning_count", pa.int32(), nullable=False),
        pa.field("inconsistency_count", pa.int32(), nullable=False),
        pa.field("has_json_facts", pa.bool_(), nullable=False),
        pa.field("source_url", pa.string(), nullable=False),
        pa.field("source_run_id", pa.string(), nullable=False),
    ]
)
assert tuple(_FILINGS_INDEX_ARROW_SCHEMA.names) == tables.ESEF_FILINGS_EXPORT_COLUMNS

# esef_filings.facts column types, keyed by name. Same drift-guard idea as
# _FILINGS_INDEX_COLUMN_TYPES above, plus one local-only column:
# `period_end_year` (derived from period_end at parse time) is what the
# partition-scoped delete/insert keys on -- it has no ClickHouse counterpart
# (migration 000149's corpscout.esef_facts has no such column; the CH export
# in a later task derives it there via toYear(period_end) instead).
# `amount_original` is kept as text here (not a DuckDB DECIMAL column): the
# Decimal value is already validated in Python (facts.EsefFact.amount_original),
# and staging keeps raw-ish values as text per docs/data-source-guidelines.md
# §3 -- the ClickHouse export casts to Decimal128(2).
# `period_duration_end` (Finding 1 fix, migration 000149) carries a duration
# fact's own true end date, distinct from `period_end` (the filing's own
# period_end, stamped identically onto every fact) -- it lets metrics.py
# structurally exclude a prior-year comparative duration fact instead of
# relying on the filing-level period_end alone, which a comparative fact
# also matches.
_FACTS_COLUMN_TYPES: dict[str, str] = {
    "lei": "varchar",
    "fxo_id": "varchar",
    "period_end": "varchar",
    "period_end_year": "integer",
    "fact_id": "varchar",
    "concept_qname": "varchar",
    "concept_namespace": "varchar",
    "concept_local_name": "varchar",
    "period_start": "varchar",
    "period_instant": "varchar",
    "period_duration_end": "varchar",
    "unit": "varchar",
    "currency": "varchar",
    "value_kind": "varchar",
    "raw_value": "varchar",
    "amount_original": "varchar",
    "decimals": "integer",
    "dimensions": "varchar",
    "language": "varchar",
    "source_run_id": "varchar",
}
assert set(_FACTS_COLUMN_TYPES) == set(tables.ESEF_FACTS_EXPORT_COLUMNS) | {
    "period_end_year"
}, (
    "esef_filings/assets.py _FACTS_COLUMN_TYPES is out of sync with "
    "tables.ESEF_FACTS_EXPORT_COLUMNS (+ local-only period_end_year)"
)
_FACTS_COLUMNS = tuple(_FACTS_COLUMN_TYPES)
_FACTS_COLUMNS_SQL = ", ".join(
    f"{name} {_FACTS_COLUMN_TYPES[name]}" for name in _FACTS_COLUMNS
)


def esef_filings_source_duckdb_path(
    *, root: str | Path = ESEF_FILINGS_DUCKDB_ROOT
) -> Path:
    return Path(root) / "esef_filings_source.duckdb"


def _filings_index_arrow_table(
    records: Sequence[EsefFilingRecord],
    *,
    source_url: str,
    source_run_id: str,
) -> pa.Table:
    return pa.Table.from_arrays(
        [
            pa.array([record.lei for record in records], type=pa.string()),
            pa.array([record.entity_name for record in records], type=pa.string()),
            pa.array([record.fxo_id for record in records], type=pa.string()),
            pa.array([record.country for record in records], type=pa.string()),
            pa.array([record.period_end for record in records], type=pa.string()),
            pa.array([record.date_added for record in records], type=pa.string()),
            pa.array([record.processed_at for record in records], type=pa.string()),
            pa.array([record.json_url for record in records], type=pa.string()),
            pa.array([record.package_url for record in records], type=pa.string()),
            pa.array([record.report_url for record in records], type=pa.string()),
            pa.array([record.viewer_url for record in records], type=pa.string()),
            pa.array([record.package_sha256 for record in records], type=pa.string()),
            pa.array([record.error_count for record in records], type=pa.int32()),
            pa.array([record.warning_count for record in records], type=pa.int32()),
            pa.array(
                [record.inconsistency_count for record in records],
                type=pa.int32(),
            ),
            pa.array(
                [record.json_url is not None for record in records],
                type=pa.bool_(),
            ),
            pa.array([source_url] * len(records), type=pa.string()),
            pa.array([source_run_id] * len(records), type=pa.string()),
        ],
        schema=_FILINGS_INDEX_ARROW_SCHEMA,
    )


def _insert_filings_index_records(
    *,
    connection: Any,
    target_table: str,
    records: Sequence[EsefFilingRecord],
    source_url: str,
    source_run_id: str,
) -> None:
    column_names = ", ".join(tables.ESEF_FILINGS_EXPORT_COLUMNS)
    for offset in range(0, len(records), _FILINGS_INDEX_INSERT_BATCH_SIZE):
        record_batch = records[offset : offset + _FILINGS_INDEX_INSERT_BATCH_SIZE]
        arrow_table = _filings_index_arrow_table(
            record_batch,
            source_url=source_url,
            source_run_id=source_run_id,
        )
        connection.register(_FILINGS_INDEX_BATCH_RELATION, arrow_table)
        try:
            connection.execute(
                f"insert into {target_table} ({column_names}) "
                f"select {column_names} from {_FILINGS_INDEX_BATCH_RELATION}"
            )
        finally:
            connection.unregister(_FILINGS_INDEX_BATCH_RELATION)


def _country_distribution_top(
    records: Sequence[EsefFilingRecord], *, limit: int
) -> dict[str, int]:
    counts = Counter(record.country for record in records)
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return dict(ranked[:limit])


def _filing_summary(records: Sequence[EsefFilingRecord]) -> dict[str, Any]:
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


def upsert_esef_filings_index(
    *,
    connection: Any,
    records: Sequence[EsefFilingRecord],
    source_url: str,
    source_run_id: str,
) -> dict[str, Any]:
    """Upsert one processed-time API window into the cumulative index.

    ``fxo_id`` is the filing-version identity. Re-materializing a discovery
    month therefore updates changed source metadata without duplicating rows,
    while records discovered in every other month remain untouched. An empty
    API window is valid and only ensures that the target table exists.
    """
    records_by_fxo_id: dict[str, EsefFilingRecord] = {}
    for record in records:
        if record.fxo_id == "":
            raise ValueError("ESEF filing record has an empty fxo_id")
        records_by_fxo_id[record.fxo_id] = record
    unique_records = list(records_by_fxo_id.values())
    connection.execute("begin transaction")
    try:
        connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"create table if not exists {QUALIFIED_FILINGS_INDEX_TABLE} "
            f"({_FILINGS_INDEX_COLUMNS_SQL})"
        )
        if unique_records:
            connection.execute(
                f"create or replace temp table {_FILINGS_INDEX_UPSERT_TABLE} "
                f"as select * from {QUALIFIED_FILINGS_INDEX_TABLE} where false"
            )
            _insert_filings_index_records(
                connection=connection,
                target_table=_FILINGS_INDEX_UPSERT_TABLE,
                records=unique_records,
                source_url=source_url,
                source_run_id=source_run_id,
            )
            connection.execute(
                f"delete from {QUALIFIED_FILINGS_INDEX_TABLE} "
                f"where fxo_id in (select fxo_id from {_FILINGS_INDEX_UPSERT_TABLE})"
            )
            connection.execute(
                f"insert into {QUALIFIED_FILINGS_INDEX_TABLE} "
                f"select * from {_FILINGS_INDEX_UPSERT_TABLE}"
            )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise

    [(index_row_count,)] = connection.execute(
        f"select count(*) from {QUALIFIED_FILINGS_INDEX_TABLE}"
    ).fetchall()
    return {
        **_filing_summary(unique_records),
        "received_count": len(records),
        "duplicate_fxo_id_count": len(records) - len(unique_records),
        "index_row_count": int(index_row_count),
    }


# A crawl that captures fewer than this fraction of the API's own reported
# result count for the selected query (`meta.count`, surfaced via
# `EsefFilingsClient.last_reported_total`) is refused (Finding M1): a
# truncated/partial crawl -- pagination stopping early, a network blip
# mid-sweep -- can still return a nonzero, non-empty result, sailing right
# past the refuse-on-empty guard below while silently dropping most of the
# index. See `_check_crawl_completeness`.
CRAWL_COMPLETENESS_MIN_RATIO = 0.9


def _check_crawl_completeness(
    *, crawled_count: int, api_reported_total: int | None
) -> None:
    """Refuse (raises `ValueError`, before any DB statement runs) a crawl
    that captured less than `CRAWL_COMPLETENESS_MIN_RATIO` (90%) of the
    API's own reported result count for the selected query.

    A no-op when `api_reported_total` is `None` -- the API's `meta.count`
    wasn't available off the first index page (missing/malformed shape),
    so there is no baseline to compare `crawled_count` against; refusing
    on unrelated grounds isn't this guard's job (the plain
    refuse-on-empty check already covers a 0-filing crawl regardless).
    """
    if api_reported_total is None:
        return
    if crawled_count >= api_reported_total * CRAWL_COMPLETENESS_MIN_RATIO:
        return
    raise ValueError(
        f"ESEF filings crawl captured {crawled_count} filings, below "
        f"{int(CRAWL_COMPLETENESS_MIN_RATIO * 100)}% of the API-reported "
        f"total ({api_reported_total}) -- refusing to update "
        f"{QUALIFIED_FILINGS_INDEX_TABLE} (possible truncated/incomplete "
        "crawl)."
    )


@dg.asset(
    name="esef_filings_index_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "esef_filings"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ESEF_FILINGS_DUCKDB_POOL,
    description=(
        "Fetches one filings.xbrl.org processed-time week and upserts it by "
        f"fxo_id into cumulative DuckDB table {QUALIFIED_FILINGS_INDEX_TABLE}. "
        "Late filings are processed in the week they become available, "
        "regardless of fiscal period_end."
    ),
)
def esef_filings_index_duckdb(
    context: dg.AssetExecutionContext,
    esef_filings_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    client = EsefFilingsClient()
    partition_window = context.partition_time_window
    records = list(
        client.iter_filings(
            processed_from=partition_window.start,
            processed_until=partition_window.end,
        )
    )
    api_reported_total = client.last_reported_total
    context.log.info(
        "ESEF filings processed window %s fetched %d filings (api_reported_total=%s)",
        context.partition_key,
        len(records),
        api_reported_total,
    )
    _check_crawl_completeness(
        crawled_count=len(records), api_reported_total=api_reported_total
    )

    with esef_filings_duckdb.get_connection() as connection:
        summary = upsert_esef_filings_index(
            connection=connection,
            records=records,
            source_url=ESEF_INDEX_URL,
            source_run_id=context.run.run_id,
        )

    return dg.MaterializeResult(
        metadata={
            "window_row_count": summary["row_count"],
            "index_row_count": summary["index_row_count"],
            "with_json_facts_count": summary["with_json_facts_count"],
            "without_json_facts_count": summary["without_json_facts_count"],
            "distinct_country_count": summary["distinct_country_count"],
            "country_distribution_top10": dg.MetadataValue.json(
                summary["country_distribution_top10"]
            ),
            "api_reported_total": api_reported_total,
            "duplicate_fxo_id_count": summary["duplicate_fxo_id_count"],
            "processed_window_start": partition_window.start.isoformat(),
            "processed_window_end": partition_window.end.isoformat(),
            "duckdb_path": str(esef_filings_source_duckdb_path()),
        }
    )


@dg.asset_check(
    asset="esef_filings_index_duckdb",
    name="filings_index_non_empty",
    pool=ESEF_FILINGS_DUCKDB_POOL,
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


def _period_end_year(period_end: str | None) -> int | None:
    """Return a valid ISO filing period's year without imposing a ceiling."""
    if not period_end:
        return None
    try:
        return date.fromisoformat(period_end).year
    except ValueError:
        return None


def processed_week_bounds(partition_key: str) -> tuple[datetime, datetime]:
    window = ESEF_PROCESSED_WEEK_PARTITIONS.time_window_for_partition_key(partition_key)
    return window.start.astimezone(UTC), window.end.astimezone(UTC)


def _processed_window_sql_values(partition_key: str) -> tuple[str, str]:
    start, end = processed_week_bounds(partition_key)
    return (
        start.strftime("%Y-%m-%d %H:%M:%S"),
        end.strftime("%Y-%m-%d %H:%M:%S"),
    )


def _row_from_fact(
    fact: facts.EsefFact, *, period_end_year: int, source_run_id: str
) -> tuple[Any, ...]:
    return (
        fact.lei,
        fact.fxo_id,
        fact.period_end,
        period_end_year,
        fact.fact_id,
        fact.concept_qname,
        fact.concept_namespace,
        fact.concept_local_name,
        fact.period_start,
        fact.period_instant,
        fact.period_duration_end,
        fact.unit,
        fact.currency,
        fact.value_kind,
        fact.raw_value,
        None if fact.amount_original is None else str(fact.amount_original),
        fact.decimals,
        fact.dimensions,
        fact.language,
        source_run_id,
    )


# Bounds per-transaction uncommitted MVCC memory during the facts insert.
# Production incident: large backfill partitions (millions of parsed
# XBRL facts each) failed with `_duckdb.OutOfMemoryException: failed to pin
# block ... (73.8 GiB/73.8 GiB used)` on a ~412 MB DuckDB file with 92 GB RAM
# free on the host -- not a data-volume problem. Root cause: a single
# `executemany` over a WHOLE partition's rows inside ONE transaction holds every
# uncommitted row's MVCC version/undo data in memory for the entire insert;
# small partitions passed only because they were tiny. Committing every
# `_FACTS_INSERT_CHUNK_SIZE` rows as its own transaction bounds the
# uncommitted-row memory to one chunk's worth, regardless of the partition's total
# row count.
_FACTS_INSERT_CHUNK_SIZE = 50_000
_FACTS_SPOOL_SUBMISSION_SIZE = 5_000
_FACTS_S3_READ_WORKERS = 8
_FACTS_PARSE_WORKERS = 4
_FACTS_PARSE_BUFFER_MULTIPLIER = 2
_FACTS_REPLACEMENT_IDS_RELATION = "_esef_facts_replacement_ids"
_FACTS_INGESTION_STATE_RELATION = "_esef_facts_ingestion_state"
_FACTS_ARROW_TYPES = tuple(
    pa.int32() if column_type == "integer" else pa.string()
    for column_type in _FACTS_COLUMN_TYPES.values()
)


@dataclass(frozen=True)
class _ArtifactFactDocument:
    artifact_object_key: str
    lei: str
    fxo_id: str
    period_end: str
    period_end_year: int


@dataclass(frozen=True)
class _ArtifactFactParseTask:
    local_path: str
    target_directory: str
    file_prefix: str
    lei: str
    fxo_id: str
    period_end: str
    period_end_year: int
    source_run_id: str


@dataclass(frozen=True)
class _FactParseResult:
    fxo_id: str
    period_end_year: int
    fact_count: int
    fact_batches: tuple[tuple[str, int], ...]
    parse_seconds: float
    parse_error: str


def _facts_arrow_table(fact_rows: Sequence[tuple[Any, ...]]) -> pa.Table:
    """Build a typed Arrow batch in the exact DuckDB facts-column order."""
    return pa.Table.from_arrays(
        [
            pa.array(
                (row[column_index] for row in fact_rows),
                type=arrow_type,
            )
            for column_index, arrow_type in enumerate(_FACTS_ARROW_TYPES)
        ],
        names=_FACTS_COLUMNS,
    )


def _spool_fact_rows_to_parquet(
    fact_rows: Iterable[tuple[Any, ...]],
    *,
    target_directory: Path,
    file_prefix: str,
) -> list[tuple[Path, int]]:
    spool = _FactParquetSpool(
        target_directory=target_directory,
        file_prefix=file_prefix,
    )
    spool.add_fact_rows(fact_rows)
    return spool.close()


class _FactParquetSpool:
    def __init__(self, *, target_directory: Path, file_prefix: str) -> None:
        self._target_directory = target_directory
        self._target_directory.mkdir(parents=True, exist_ok=True)
        self._file_prefix = file_prefix
        self._pending_rows: list[tuple[Any, ...]] = []
        self._batches: list[tuple[Path, int]] = []
        self._lock = Lock()
        self._closed = False

    def add_fact_rows(self, fact_rows: Iterable[tuple[Any, ...]]) -> int:
        submission_size = min(
            _FACTS_SPOOL_SUBMISSION_SIZE,
            _FACTS_INSERT_CHUNK_SIZE,
        )
        submitted_count = 0
        submission: list[tuple[Any, ...]] = []
        for fact_row in fact_rows:
            submission.append(fact_row)
            submitted_count += 1
            if len(submission) == submission_size:
                self._append_submission(submission)
                submission.clear()
        if submission:
            self._append_submission(submission)
        return submitted_count

    def close(self) -> list[tuple[Path, int]]:
        with self._lock:
            if self._closed:
                return list(self._batches)
            if self._pending_rows:
                self._batches.append(
                    _write_fact_parquet_batch(
                        self._pending_rows,
                        target_directory=self._target_directory,
                        file_prefix=self._file_prefix,
                        batch_index=len(self._batches),
                    )
                )
                self._pending_rows.clear()
            self._closed = True
            return list(self._batches)

    def _append_submission(
        self,
        fact_rows: Sequence[tuple[Any, ...]],
    ) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError("Cannot append to a closed ESEF fact spool")
            offset = 0
            while offset < len(fact_rows):
                available = _FACTS_INSERT_CHUNK_SIZE - len(self._pending_rows)
                end = min(offset + available, len(fact_rows))
                self._pending_rows.extend(fact_rows[offset:end])
                offset = end
                if len(self._pending_rows) == _FACTS_INSERT_CHUNK_SIZE:
                    self._batches.append(
                        _write_fact_parquet_batch(
                            self._pending_rows,
                            target_directory=self._target_directory,
                            file_prefix=self._file_prefix,
                            batch_index=len(self._batches),
                        )
                    )
                    self._pending_rows.clear()


def _write_fact_parquet_batch(
    fact_rows: Sequence[tuple[Any, ...]],
    *,
    target_directory: Path,
    file_prefix: str,
    batch_index: int,
) -> tuple[Path, int]:
    if not fact_rows or len(fact_rows) > _FACTS_INSERT_CHUNK_SIZE:
        raise ValueError(
            "ESEF fact Parquet batch must contain between 1 and "
            f"{_FACTS_INSERT_CHUNK_SIZE} rows"
        )
    path = target_directory / f"{file_prefix}-{batch_index:06d}.parquet"
    pq.write_table(
        _facts_arrow_table(fact_rows),
        path,
        compression="zstd",
    )
    return path, len(fact_rows)


def _parse_artifact_facts_worker(
    task: _ArtifactFactParseTask,
) -> _FactParseResult:
    """Normalize one local Arelle artifact into process-local Parquet batches."""
    started = perf_counter()
    try:
        try:
            artifact = json.loads(Path(task.local_path).read_text(encoding="utf-8"))
            fact_batches = _spool_fact_rows_to_parquet(
                (
                    _row_from_fact(
                        fact,
                        period_end_year=task.period_end_year,
                        source_run_id=task.source_run_id,
                    )
                    for fact in facts.iter_artifact_facts(
                        artifact,
                        lei=task.lei,
                        fxo_id=task.fxo_id,
                        period_end=task.period_end,
                    )
                ),
                target_directory=Path(task.target_directory),
                file_prefix=task.file_prefix,
            )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            return _FactParseResult(
                fxo_id=task.fxo_id,
                period_end_year=task.period_end_year,
                fact_count=0,
                fact_batches=(),
                parse_seconds=perf_counter() - started,
                parse_error=str(exc),
            )

        return _FactParseResult(
            fxo_id=task.fxo_id,
            period_end_year=task.period_end_year,
            fact_count=sum(row_count for _, row_count in fact_batches),
            fact_batches=tuple(
                (str(path), row_count) for path, row_count in fact_batches
            ),
            parse_seconds=perf_counter() - started,
            parse_error="",
        )
    except Exception as exc:
        return _FactParseResult(
            fxo_id=task.fxo_id,
            period_end_year=task.period_end_year,
            fact_count=0,
            fact_batches=(),
            parse_seconds=perf_counter() - started,
            parse_error=str(exc),
        )


def _validate_fact_parquet_batches(
    fact_batches: Sequence[tuple[Path, int]],
) -> int:
    total = 0
    for path, expected_row_count in fact_batches:
        actual_row_count = pq.ParquetFile(path).metadata.num_rows
        if (
            expected_row_count < 1
            or expected_row_count > _FACTS_INSERT_CHUNK_SIZE
            or actual_row_count != expected_row_count
        ):
            raise ValueError(
                "Invalid ESEF fact Parquet batch metadata: "
                f"path={path} expected_rows={expected_row_count} "
                f"actual_rows={actual_row_count} "
                f"maximum_rows={_FACTS_INSERT_CHUNK_SIZE}"
            )
        total += actual_row_count
    return total


def _replace_facts_for_filings(
    *,
    connection: Any,
    filing_ids: Sequence[str],
    partition_key: str,
    fact_batches: Sequence[tuple[Path, int]],
    completed_filing_fact_counts: Mapping[str, int],
    source_run_id: str,
    parser_contract: str,
    log_info: Callable[..., object],
) -> None:
    """Delete-then-insert facts for the filing versions in one discovery month.

    Guards against the known dagster_duckdb.DuckDBResource.get_connection()
    bug: its `@contextmanager` body has no try/finally around `yield`, so
    `conn.close()` (the line after `yield`) never runs if an exception
    escapes the caller's `with` block -- the exception is raised straight
    through the generator's suspended `yield` and the connection leaks.
    Closing it ourselves before re-raising is the fix; by this point all
    fallible work (download/parse) is already done, so the only things that
    can fail here are the DELETE/INSERT/COMMIT themselves.

    MEMORY (see `_FACTS_INSERT_CHUNK_SIZE`'s comment for the production
    incident this fixes): the DELETE commits in its own transaction first,
    then each bounded Parquet batch is scanned by DuckDB into its OWN
    transaction+commit -- uncommitted MVCC memory never exceeds one batch,
    regardless of the month's total row count.
    `preserve_insertion_order` is turned off first (insertion order is
    irrelevant here: the ClickHouse export orders on its own); leaving it on
    would make DuckDB buffer rows to preserve original insertion order across
    the chunked inserts, defeating the point of chunking. `memory_limit` is
    deliberately left at DuckDB's default -- this fix bounds *uncommitted*
    transaction memory, it isn't a total-memory cap.

    CONTRACT CHANGE from the prior single-transaction version: a mid-way
    failure can now leave a PARTIAL month committed (the DELETE, plus however
    many chunks committed before the failing one). This is ACCEPTABLE, not a
    new hazard: the delete-first-on-next-run contract (this function's own
    first write) already cleans up any partial state before the next attempt
    re-inserts, and a partial partition's DuckDB rows are never read
    downstream in the meantime -- the ClickHouse facts export
    (`esef_facts_clickhouse`) only reads this table after
    `esef_filing_facts_duckdb` SUCCEEDS for the partition, and
    `esef_filings_backfill_job` deliberately excludes the exports (see the
    job definitions below), so a partial/failed partition's rows are never
    exported before the next run's delete-first cleans them up.
    """
    try:
        total = _validate_fact_parquet_batches(fact_batches)
        connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"create table if not exists {QUALIFIED_FACTS_TABLE} ({_FACTS_COLUMNS_SQL})"
        )
        connection.execute(
            f"create table if not exists {QUALIFIED_FACTS_INGESTION_STATE_TABLE} ("
            "fxo_id varchar, fact_count bigint, source_run_id varchar, "
            "completed_at timestamp, parser_contract varchar)"
        )
        connection.execute(
            f"alter table {QUALIFIED_FACTS_INGESTION_STATE_TABLE} "
            "add column if not exists parser_contract varchar default ''"
        )
        connection.execute("set preserve_insertion_order = false")
        connection.execute("begin transaction")
        unique_filing_ids = sorted(set(filing_ids))
        if unique_filing_ids:
            replacement_ids = pa.Table.from_arrays(
                [pa.array(unique_filing_ids, type=pa.string())],
                names=["fxo_id"],
            )
            connection.register(_FACTS_REPLACEMENT_IDS_RELATION, replacement_ids)
            try:
                connection.execute(
                    "create or replace temp table esef_facts_replace_filing_ids "
                    f"as select fxo_id from {_FACTS_REPLACEMENT_IDS_RELATION}"
                )
            finally:
                connection.unregister(_FACTS_REPLACEMENT_IDS_RELATION)
            [(existing_fact_row_count,)] = connection.execute(
                f"select count(*) from {QUALIFIED_FACTS_TABLE} facts "
                "join esef_facts_replace_filing_ids replacement "
                "on replacement.fxo_id = facts.fxo_id"
            ).fetchall()
            if existing_fact_row_count > 0:
                connection.execute(
                    f"delete from {QUALIFIED_FACTS_TABLE} where fxo_id in "
                    "(select fxo_id from esef_facts_replace_filing_ids)"
                )
                log_info(
                    "ESEF facts partition %s: removed %d existing fact rows",
                    partition_key,
                    existing_fact_row_count,
                )
            connection.execute(
                f"delete from {QUALIFIED_FACTS_INGESTION_STATE_TABLE} "
                "where fxo_id in (select fxo_id from esef_facts_replace_filing_ids)"
            )
        connection.execute("commit")

        inserted = 0
        columns = ", ".join(_FACTS_COLUMNS)
        for batch_path, batch_row_count in fact_batches:
            connection.execute("begin transaction")
            connection.execute(
                f"insert into {QUALIFIED_FACTS_TABLE} ({columns}) "
                f"select {columns} from read_parquet(?)",
                [str(batch_path)],
            )
            connection.execute("commit")
            inserted += batch_row_count
            log_info(
                "ESEF facts partition %s: inserted %d/%d fact rows",
                partition_key,
                inserted,
                total,
            )
        if total == 0:
            log_info("ESEF facts partition %s: inserted 0/0 fact rows", partition_key)

        if completed_filing_fact_counts:
            state_items = sorted(completed_filing_fact_counts.items())
            ingestion_state = pa.Table.from_arrays(
                [
                    pa.array(
                        (fxo_id for fxo_id, _ in state_items),
                        type=pa.string(),
                    ),
                    pa.array(
                        (fact_count for _, fact_count in state_items),
                        type=pa.int64(),
                    ),
                    pa.array([source_run_id] * len(state_items), type=pa.string()),
                    pa.array([parser_contract] * len(state_items), type=pa.string()),
                ],
                names=[
                    "fxo_id",
                    "fact_count",
                    "source_run_id",
                    "parser_contract",
                ],
            )
            connection.register(_FACTS_INGESTION_STATE_RELATION, ingestion_state)
            connection.execute("begin transaction")
            try:
                connection.execute(
                    f"insert into {QUALIFIED_FACTS_INGESTION_STATE_TABLE} "
                    "(fxo_id, fact_count, source_run_id, completed_at, "
                    "parser_contract) "
                    "select fxo_id, fact_count, source_run_id, current_timestamp, "
                    "parser_contract "
                    f"from {_FACTS_INGESTION_STATE_RELATION}"
                )
                connection.execute("commit")
            finally:
                connection.unregister(_FACTS_INGESTION_STATE_RELATION)
    except Exception:
        try:
            connection.execute("rollback")
        except Exception:
            pass
        connection.close()
        raise


def _log_facts_progress(
    log_info: Callable[..., object],
    *,
    partition_key: str,
    processed: int,
    total: int,
    s3_read: int,
    checkpointed: int,
    skipped: int,
    force: bool = False,
) -> None:
    """Log one progress line for the artifact-fact parsing loop.

    A no-op unless `processed` lands on `_PROGRESS_LOG_INTERVAL` or `force`
    is set -- `force=True` is used exactly once, after the loop ends, so the
    final tally is always logged even when `processed` isn't a multiple of
    the interval. `skipped` combines index rows that cannot produce parsed
    facts because they have no JSON URL, have an invalid period end, or lack
    the required raw S3 object. Parse failures are reported separately.
    """
    if not force and processed % _PROGRESS_LOG_INTERVAL != 0:
        return
    log_info(
        "ESEF facts partition %s: %d/%d filings processed "
        "(%d S3 read, %d checkpointed, %d skipped)",
        partition_key,
        processed,
        total,
        s3_read,
        checkpointed,
        skipped,
    )


def _load_fact_checkpoints(
    esef_filings_duckdb: DuckDBResource,
    *,
    parser_contract: str,
) -> dict[str, int]:
    """Load only checkpoints produced by the requested parser contract."""
    with read_only_duckdb_connection(esef_filings_duckdb) as connection:
        [(state_exists,)] = connection.execute(
            "select count(*) > 0 from information_schema.tables "
            "where table_schema = ? and table_name = ?",
            [tables.DLT_DATASET_NAME, FACTS_INGESTION_STATE_TABLE],
        ).fetchall()
        if not state_exists:
            return {}
        [(contract_column_exists,)] = connection.execute(
            "select count(*) > 0 from information_schema.columns "
            "where table_schema = ? and table_name = ? and column_name = ?",
            [
                tables.DLT_DATASET_NAME,
                FACTS_INGESTION_STATE_TABLE,
                "parser_contract",
            ],
        ).fetchall()
        if not contract_column_exists:
            return {}
        return dict(
            connection.execute(
                f"select fxo_id, fact_count from "
                f"{QUALIFIED_FACTS_INGESTION_STATE_TABLE} "
                "where parser_contract = ?",
                [parser_contract],
            ).fetchall()
        )


def run_esef_artifact_facts_partition(
    *,
    esef_filings_duckdb: DuckDBResource,
    object_store: Any,
    partition_key: str,
    source_run_id: str,
    log_info: Callable[..., object],
    log_warning: Callable[..., object],
) -> dict[str, int | str]:
    """Publish facts from the Arelle artifacts created for one processed week.

    The artifact-stage result is the sole work manifest. Each unique artifact
    object is downloaded once, while every source document receives its own
    source-linked fact rows. Parsing stays outside DuckDB and uses the existing
    bounded Parquet spool before a short, filing-scoped replacement.
    """
    from dagster_v3.defs.esef_filings.segment_assets import (
        ESEF_DOCUMENT_BUCKET,
        iter_esef_document_result_rows,
        local_esef_document_result,
    )

    started = perf_counter()
    processed_from, processed_until = _processed_window_sql_values(partition_key)
    checkpointed_fact_counts = _load_fact_checkpoints(
        esef_filings_duckdb,
        parser_contract=ARELLE_ARTIFACT_FACTS_PARSER_CONTRACT,
    )
    documents: list[_ArtifactFactDocument] = []
    filing_ids_to_replace: list[str] = []
    checkpointed_filing_count = 0
    checkpointed_fact_row_count = 0
    skipped_invalid_period_end = 0
    skipped_missing_artifact_key = 0
    seen_filing_ids: set[str] = set()

    with local_esef_document_result(
        object_store,
        partition_key=partition_key,
    ) as result_path:
        for raw_document in iter_esef_document_result_rows(
            result_path,
            property_name="document_rows",
        ):
            fxo_id = str(raw_document.get("source_document_id", ""))
            if fxo_id == "":
                raise ValueError("ESEF document result row has no source_document_id")
            if fxo_id in seen_filing_ids:
                raise ValueError(
                    f"ESEF document result repeats source_document_id={fxo_id}"
                )
            seen_filing_ids.add(fxo_id)
            if fxo_id in checkpointed_fact_counts:
                checkpointed_filing_count += 1
                checkpointed_fact_row_count += checkpointed_fact_counts[fxo_id]
                continue

            period_end = str(raw_document.get("period_end", ""))
            period_end_year = _period_end_year(period_end)
            if period_end_year is None:
                filing_ids_to_replace.append(fxo_id)
                skipped_invalid_period_end += 1
                continue
            artifact_object_key = str(
                raw_document.get("parsed_artifact_object_key", "")
            )
            if artifact_object_key == "":
                filing_ids_to_replace.append(fxo_id)
                skipped_missing_artifact_key += 1
                continue
            documents.append(
                _ArtifactFactDocument(
                    artifact_object_key=artifact_object_key,
                    lei=str(raw_document.get("lei", "")),
                    fxo_id=fxo_id,
                    period_end=period_end,
                    period_end_year=period_end_year,
                )
            )

    documents_by_artifact: dict[str, list[_ArtifactFactDocument]] = {}
    for document in documents:
        documents_by_artifact.setdefault(document.artifact_object_key, []).append(
            document
        )

    unique_artifact_s3_read_count = 0
    skipped_missing_artifact_object = 0
    parse_failed_count = 0
    inserted_fact_row_count = 0
    completed_filing_fact_counts: dict[str, int] = {}
    s3_io_seconds = 0.0
    worker_parse_seconds = 0.0

    log_info(
        "ESEF artifact facts processed-week partition %s: %d documents in scope, "
        "%d unique artifacts",
        partition_key,
        len(seen_filing_ids),
        len(documents_by_artifact),
    )

    with tempfile.TemporaryDirectory(prefix="esef_artifact_facts_") as tmpdir:
        temp_dir = Path(tmpdir)
        artifact_directory = temp_dir / "artifacts"
        artifact_directory.mkdir()
        spool_directory = temp_dir / "fact_batches"
        fact_batches: list[tuple[Path, int]] = []
        available_artifacts: dict[str, Path] = {}

        def download_artifact(
            artifact_object_key: str,
        ) -> tuple[str, Path | None, float]:
            io_started = perf_counter()
            if not object_store.exists(
                artifact_object_key,
                bucket=ESEF_DOCUMENT_BUCKET,
            ):
                return (
                    artifact_object_key,
                    None,
                    perf_counter() - io_started,
                )
            local_path = artifact_directory / (
                f"{sha256(artifact_object_key.encode()).hexdigest()}.json"
            )
            object_store.download_file(
                artifact_object_key,
                local_path,
                bucket=ESEF_DOCUMENT_BUCKET,
            )
            return artifact_object_key, local_path, perf_counter() - io_started

        artifact_keys = sorted(documents_by_artifact)
        with ThreadPoolExecutor(max_workers=_FACTS_S3_READ_WORKERS) as s3_executor:
            for artifact_object_key, local_path, io_seconds in s3_executor.map(
                download_artifact,
                artifact_keys,
                buffersize=_FACTS_S3_READ_WORKERS * 2,
            ):
                s3_io_seconds += io_seconds
                if local_path is None:
                    missing_documents = documents_by_artifact[artifact_object_key]
                    skipped_missing_artifact_object += len(missing_documents)
                    filing_ids_to_replace.extend(
                        document.fxo_id for document in missing_documents
                    )
                    log_warning(
                        "ESEF Arelle artifact is missing: object_key=%s "
                        "source_document_count=%d",
                        artifact_object_key,
                        len(missing_documents),
                    )
                    continue
                available_artifacts[artifact_object_key] = local_path
                unique_artifact_s3_read_count += 1

        pending: dict[Future[_FactParseResult], tuple[_ArtifactFactDocument, str]] = {}

        def complete_parse_futures(
            completed: set[Future[_FactParseResult]],
        ) -> None:
            nonlocal inserted_fact_row_count
            nonlocal parse_failed_count
            nonlocal worker_parse_seconds
            for future in completed:
                document, artifact_object_key = pending.pop(future)
                parsed = future.result()
                worker_parse_seconds += parsed.parse_seconds
                if parsed.parse_error != "":
                    parse_failed_count += 1
                    log_warning(
                        "ESEF Arelle artifact fact parse failed: fxo_id=%s "
                        "object_key=%s error=%s",
                        document.fxo_id,
                        artifact_object_key,
                        parsed.parse_error,
                    )
                    continue
                completed_filing_fact_counts[document.fxo_id] = parsed.fact_count
                inserted_fact_row_count += parsed.fact_count
                fact_batches.extend(
                    (Path(path), row_count) for path, row_count in parsed.fact_batches
                )

        with ProcessPoolExecutor(max_workers=_FACTS_PARSE_WORKERS) as parse_executor:
            for artifact_object_key in sorted(available_artifacts):
                local_path = available_artifacts[artifact_object_key]
                for document in documents_by_artifact[artifact_object_key]:
                    filing_ids_to_replace.append(document.fxo_id)
                    future = parse_executor.submit(
                        _parse_artifact_facts_worker,
                        _ArtifactFactParseTask(
                            local_path=str(local_path),
                            target_directory=str(spool_directory),
                            file_prefix=(
                                f"facts-{sha256(document.fxo_id.encode()).hexdigest()}"
                            ),
                            lei=document.lei,
                            fxo_id=document.fxo_id,
                            period_end=document.period_end,
                            period_end_year=document.period_end_year,
                            source_run_id=source_run_id,
                        ),
                    )
                    pending[future] = (document, artifact_object_key)
                    if len(pending) >= (
                        _FACTS_PARSE_WORKERS * _FACTS_PARSE_BUFFER_MULTIPLIER
                    ):
                        completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                        complete_parse_futures(completed)

            while pending:
                completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                complete_parse_futures(completed)

        fact_batches.sort(key=lambda batch: str(batch[0]))
        spooled_fact_row_count = sum(row_count for _, row_count in fact_batches)
        if spooled_fact_row_count != inserted_fact_row_count:
            raise RuntimeError(
                "ESEF artifact fact spool row count does not match parsed row "
                f"count: spooled={spooled_fact_row_count} "
                f"parsed={inserted_fact_row_count}"
            )

        with esef_filings_duckdb.get_connection() as connection:
            _replace_facts_for_filings(
                connection=connection,
                filing_ids=filing_ids_to_replace,
                partition_key=partition_key,
                fact_batches=fact_batches,
                completed_filing_fact_counts=completed_filing_fact_counts,
                source_run_id=source_run_id,
                parser_contract=ARELLE_ARTIFACT_FACTS_PARSER_CONTRACT,
                log_info=log_info,
            )

    skipped_count = (
        skipped_invalid_period_end
        + skipped_missing_artifact_key
        + skipped_missing_artifact_object
    )
    if seen_filing_ids:
        _log_facts_progress(
            log_info,
            partition_key=partition_key,
            processed=len(seen_filing_ids),
            total=len(seen_filing_ids),
            s3_read=unique_artifact_s3_read_count,
            checkpointed=checkpointed_filing_count,
            skipped=skipped_count,
            force=True,
        )

    return {
        "filings_in_scope": len(seen_filing_ids),
        "unique_artifact_count": len(documents_by_artifact),
        "unique_artifact_s3_read_count": unique_artifact_s3_read_count,
        "s3_read_count": unique_artifact_s3_read_count,
        "checkpointed_filing_count": checkpointed_filing_count,
        "checkpointed_fact_row_count": checkpointed_fact_row_count,
        "skipped_invalid_period_end": skipped_invalid_period_end,
        "skipped_missing_artifact_key": skipped_missing_artifact_key,
        "skipped_missing_artifact_object": skipped_missing_artifact_object,
        "partition_key": partition_key,
        "processed_window_start": processed_from,
        "processed_window_end": processed_until,
        "parse_failed_count": parse_failed_count,
        "inserted_fact_row_count": inserted_fact_row_count,
        "fact_row_count": checkpointed_fact_row_count + inserted_fact_row_count,
        "parser_contract": ARELLE_ARTIFACT_FACTS_PARSER_CONTRACT,
        "parse_worker_count": _FACTS_PARSE_WORKERS,
        "s3_io_seconds": int(s3_io_seconds),
        "parse_worker_seconds": int(worker_parse_seconds),
        "wall_seconds": int(perf_counter() - started),
    }


class EsefFilingsClickhouseExportConfig(dg.Config):
    # Shrink-guard override (Finding M1; see publish.py's module docstring
    # and sweden_financial/clickhouse.py's guard_against_clickhouse_table_shrink)
    # -- MUST stay False by default. Only set True via explicit run config
    # for a confirmed-intentional shrink of a populated esef_filings table,
    # never as a standing default.
    allow_shrink: bool = False


@dg.asset(
    name="esef_filings_clickhouse",
    deps=[
        dg.AssetDep(
            dg.AssetKey("esef_filings_index_duckdb"),
            partition_mapping=dg.AllPartitionMapping(),
        )
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "esef_filings"},
    pool=ESEF_FILINGS_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_ESEF_FILINGS_TABLE},
    description=(
        "Full-replaces corpscout.esef_filings from DuckDB "
        f"{QUALIFIED_FILINGS_INDEX_TABLE} via the shared stage+EXCHANGE "
        "ClickHouse exporter, guarded against a replace that would shrink "
        "the table by more than half (see guard_against_clickhouse_table_shrink)."
    ),
)
def esef_filings_clickhouse(
    context: dg.AssetExecutionContext,
    config: EsefFilingsClickhouseExportConfig,
    esef_filings_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(esef_filings_duckdb) as connection:
        rows = export_esef_filings_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
            allow_shrink=config.allow_shrink,
        )
    return dg.MaterializeResult(
        metadata={
            "rows_exported": rows,
            "clickhouse_table": tables.QUALIFIED_ESEF_FILINGS_TABLE,
        }
    )


@dg.asset(
    name="esef_entity_registry_map_clickhouse",
    deps=["esef_filings_clickhouse", "gleif_reference_clickhouse"],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "esef_filings", "gleif"},
    metadata={"table": tables.QUALIFIED_ESEF_ENTITY_REGISTRY_MAP_TABLE},
    description=(
        "Rebuilds corpscout.esef_entity_registry_map entirely in ClickHouse "
        "from corpscout.gleif_lei_records, scoped to LEIs present in "
        "corpscout.esef_filings. ClickHouse-native stage+INSERT-SELECT+"
        "EXCHANGE; no DuckDB input."
    ),
)
def esef_entity_registry_map_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    rows = replace_esef_entity_registry_map_clickhouse(
        clickhouse=clickhouse,
        source_run_id=context.run.run_id,
        log=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "rows_exported": rows,
            "clickhouse_table": tables.QUALIFIED_ESEF_ENTITY_REGISTRY_MAP_TABLE,
        }
    )


class EsefFinancialMetricsClickhouseExportConfig(dg.Config):
    # Shrink-guard override (see sweden_financial/clickhouse.py's
    # guard_against_clickhouse_table_shrink) -- MUST stay False by default.
    # Only set True via explicit run config for a confirmed-intentional
    # shrink of a populated esef_financial_metrics table, never as a
    # standing default.
    allow_shrink: bool = False


@dg.asset(
    name="esef_financial_metrics_clickhouse",
    deps=[
        dg.AssetDep(
            dg.AssetKey("esef_facts_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
        dg.AssetKey("esef_filings_clickhouse"),
        dg.AssetKey("exchange_rates_v2_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "clickhouse", "esef_filings"},
    metadata={"table": tables.QUALIFIED_ESEF_FINANCIAL_METRICS_TABLE},
    description=(
        "Rebuilds corpscout.esef_financial_metrics entirely in ClickHouse "
        "from corpscout.esef_facts + corpscout.esef_filings (+ "
        "corpscout.exchange_rates for USD conversion). ClickHouse-native "
        "stage+INSERT-SELECT+EXCHANGE, guarded against a staged replace that "
        "would shrink the table by more than half (see "
        "guard_against_clickhouse_table_shrink); no DuckDB input, no pool."
    ),
)
def esef_financial_metrics_clickhouse(
    context: dg.AssetExecutionContext,
    config: EsefFinancialMetricsClickhouseExportConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    result = replace_esef_financial_metrics_clickhouse(
        clickhouse=clickhouse,
        source_run_id=context.run.run_id,
        log=context.log.info,
        allow_shrink=config.allow_shrink,
    )
    return dg.MaterializeResult(
        metadata={
            "rows_exported": result["rows_exported"],
            "excluded_sentinel_period_end_count": result[
                "excluded_sentinel_period_end_count"
            ],
            "clickhouse_table": tables.QUALIFIED_ESEF_FINANCIAL_METRICS_TABLE,
        }
    )


# --- Jobs + schedule (Task 7) ------------------------------------------------
#
# esef_filings_refresh_job selects the processed-week ingest, deterministic
# document evidence, and cumulative ClickHouse publications as one job. Fiscal
# year remains document data and never controls orchestration.
# This is safe to combine into a single job
# for a reason sweden_financial's history makes concrete: sweden's weekly
# chain needed a 2026-07-20 de-partitioning redesign because it keeps a
# SEPARATE DuckDB file PER YEAR, so a partition-scoped incremental ClickHouse
# export became order-dependent against the yearly backfill (the 2026-07-18
# incident -- see sweden_financial/docs/sweden_financial-design.md). ESEF
# keeps its ENTIRE dataset in ONE DuckDB file (esef_filings_source.duckdb):
# the discovery-partitioned facts step delete-then-inserts only the filing
# versions in its processed week, and the unpartitioned
# exports downstream always full-replace ClickHouse from a read of the WHOLE
# file (every processed week materialized so far) -- there is no split-file
# order-independence hazard to design around here, so the exports don't need
# their own separate job the way sweden's do.
#
# Mixing a partitioned selection with unpartitioned assets in ONE
# `define_asset_job` is legal Dagster, verified by reading
# `JobDefinition._get_partitions_def`
# (.venv/.../dagster/_core/definitions/job_definition.py): it collects only
# the non-None `partitions_def`s across the job's selected assets and
# requires exactly one unique value; assets with `partitions_def=None` are
# simply excluded from that check and execute unconditionally on every
# launch of the job (not gated by the run's `partition_key`). Both
# incremental assets share the exact same
# `ESEF_PROCESSED_WEEK_PARTITIONS` object, so this job resolves to that
# single partitions_def; the unpartitioned assets in the selection are
# unpartitioned and simply run every time.
ESEF_FILINGS_TIMEZONE = "Europe/Belgrade"

ESEF_FILINGS_INGEST_SELECTION = dg.AssetSelection.assets(
    "esef_filings_index_duckdb",
    "esef_filings_clickhouse",
    "esef_entity_registry_map_clickhouse",
)

ESEF_DOCUMENT_EVIDENCE_SELECTION = dg.AssetSelection.assets(
    "esef_document_extraction_manifest_s3",
    "esef_document_artifacts_s3",
    "esef_filing_facts_duckdb",
    "esef_document_contact_candidates_duckdb",
    "esef_document_concept_labels_duckdb",
    "esef_disclosures_duckdb",
    "esef_facts_clickhouse",
    "esef_document_contact_candidates_clickhouse",
    "esef_document_concept_labels_clickhouse",
    "esef_disclosures_clickhouse",
)

ESEF_FILINGS_REFRESH_SELECTION = (
    ESEF_FILINGS_INGEST_SELECTION | ESEF_DOCUMENT_EVIDENCE_SELECTION
)

# UI-launched backfill: one run per processed week, including partition-scoped
# publication. Unpartitioned enrichment and aggregate metrics run separately.
ESEF_FILINGS_BACKFILL_SELECTION = dg.AssetSelection.assets(
    "esef_filings_index_duckdb",
    "esef_document_extraction_manifest_s3",
    "esef_document_artifacts_s3",
    "esef_filing_facts_duckdb",
    "esef_document_contact_candidates_duckdb",
    "esef_document_concept_labels_duckdb",
    "esef_disclosures_duckdb",
    "esef_facts_clickhouse",
    "esef_document_contact_candidates_clickhouse",
    "esef_document_concept_labels_clickhouse",
    "esef_disclosures_clickhouse",
)

esef_filings_refresh_job = dg.define_asset_job(
    "esef_filings_refresh_job",
    selection=ESEF_FILINGS_REFRESH_SELECTION,
)

esef_filings_backfill_job = dg.define_asset_job(
    "esef_filings_backfill_job",
    selection=ESEF_FILINGS_BACKFILL_SELECTION,
)


def _esef_filings_refresh_run_request(
    context: dg.ScheduleEvaluationContext,
) -> Iterator[dg.RunRequest]:
    """Resolve the last complete UTC source-processed week."""
    if context.scheduled_execution_time is None:
        now = datetime.now(tz=UTC)
    else:
        now = context.scheduled_execution_time.astimezone(UTC)
    current_week = (now - timedelta(days=(now.weekday() + 1) % 7)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    closed_week = current_week - timedelta(days=7)
    tick_key = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    partition_key = closed_week.strftime("%Y-%m-%d")
    yield dg.RunRequest(
        run_key=f"{tick_key}:{partition_key}",
        partition_key=partition_key,
    )


esef_filings_refresh_weekly = dg.ScheduleDefinition(
    name="esef_filings_refresh_weekly",
    job=esef_filings_refresh_job,
    # (minute, hour) must be unique across every schedule in defs/ (see
    # tests/test_schedule_cron_contracts.py) -- "50 5" collided with
    # finland_verotax_schedule's "50 5 12 11 *"; "10 5" is free.
    cron_schedule="10 5 * * 0",
    execution_timezone=ESEF_FILINGS_TIMEZONE,
    execution_fn=_esef_filings_refresh_run_request,
    # STOPPED until Task 8 validates a live run and flips it on in the UI.
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[
        esef_filings_index_duckdb,
        esef_filings_clickhouse,
        esef_entity_registry_map_clickhouse,
        esef_financial_metrics_clickhouse,
    ],
    asset_checks=[filings_index_non_empty],
    jobs=[
        esef_filings_refresh_job,
        esef_filings_backfill_job,
    ],
    schedules=[
        esef_filings_refresh_weekly,
    ],
    resources={
        "esef_filings_duckdb": duckdb_resource(esef_filings_source_duckdb_path()),
    },
)
