"""ESEF filings index crawl + year-partitioned fact download/parse.

`esef_filings_index_duckdb` is a non-partitioned full sweep of
filings.xbrl.org (~25k rows / ~125 pages -- one sweep, no per-window
bookkeeping; see docs/data-source-guidelines.md on when partitioning earns
its keep). The expensive part -- downloading each filing's OIM xBRL-JSON
fact export and parsing it -- is `esef_filing_facts_duckdb`: a
year-partitioned asset (keyed by `toYear(period_end)`) that downloads to S3
with skip-existing (fxo_id is versioned upstream, so the object key is
stable per filing version) and partition-scope-replaces only its own
`period_end_year` in `esef_filings.facts`.

`esef_report_xhtml_s3` is a sibling year-partitioned asset that archives each
filing's rendered report XHTML to S3 -- a pure archive layer (no parsing,
no DuckDB write of its own) that is the corpus for a future embeddings/
LLM-search pass over annual reports. It only ever opens the local index
read-only for scope resolution.

No `from __future__ import annotations` -- this module defines `@dg.asset`s
and stringizing the `context: AssetExecutionContext` hint breaks Dagster's op
context-type validation (see CLAUDE.md).
"""

import json
import tempfile
from collections import Counter
from collections.abc import Callable, Sequence
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.common.tags import HEAVY_BULK_RUN_TAGS
from dagster_v3.defs.esef_filings import facts, tables
from dagster_v3.defs.esef_filings.client import (
    ESEF_INDEX_URL,
    EsefFilingsClient,
    EsefFilingRecord,
)
from dagster_v3.defs.esef_filings.metrics import (
    replace_esef_financial_metrics_clickhouse,
)
from dagster_v3.defs.esef_filings.publish import (
    export_esef_facts_clickhouse,
    export_esef_filings_clickhouse,
    replace_esef_entity_registry_map_clickhouse,
)

GROUP_NAME = "esef_filings"
ESEF_FILINGS_DUCKDB_POOL = "esef_filings_duckdb"
ESEF_FILINGS_DUCKDB_ROOT = Path("data")

FILINGS_INDEX_TABLE = tables.FILINGS_INDEX_TABLE
QUALIFIED_FILINGS_INDEX_TABLE = tables.QUALIFIED_FILINGS_INDEX_TABLE

TOP_COUNTRY_LIMIT = 10

# Both per-filing download loops (facts + report-XHTML) log a progress line
# via `log_info` every this-many processed filings, plus once, unconditionally,
# at loop completion -- a multi-hour partition run otherwise produces no
# visible progress signal between the "N filings in scope" line at the start
# and the final materialization metadata.
_PROGRESS_LOG_INTERVAL = 100

# S3 bucket + key layout for downloaded fact JSON (Task 4). fxo_id is
# versioned upstream (a new filing version gets a new fxo_id), so the key is
# stable per filing version -- an existing object never needs re-fetching.
ESEF_FILINGS_FACTS_BUCKET = "source-esef-filings"
ESEF_FACT_JSON_PREFIX = "esef_filings/fact_json/"

# S3 key layout for archived rendered report XHTML (Task 9) -- same bucket as
# the fact JSON above, different prefix. fxo_id is version-stable upstream
# (verified live), so the key never needs re-fetching once an object exists.
ESEF_REPORT_XHTML_PREFIX = "esef_filings/report_xhtml/"

FACTS_TABLE = tables.FACTS_TABLE
QUALIFIED_FACTS_TABLE = tables.QUALIFIED_FACTS_TABLE

# Matches facts._period_end_year's accepted range and the
# StaticPartitionsDefinition below (str(y) for y in range(2019, 2028)).
# NOTE: once the real calendar year exceeds this ceiling (from 2028 on),
# `_esef_filings_refresh_run_request` finds no matching partition for
# `str(now.year)` and returns a `SkipReason` on every tick -- the weekly
# schedule will fire and skip, silently, forever, until this constant (and
# the partitions list below) is bumped. Nothing pages anyone when that
# happens; it just quietly stops refreshing. See design doc Sec 8.
ESEF_FACTS_PARTITION_YEAR_MIN = 2019
ESEF_FACTS_PARTITION_YEAR_MAX = 2027
ESEF_FILING_FACTS_PARTITIONS = dg.StaticPartitionsDefinition(
    [
        str(year)
        for year in range(
            ESEF_FACTS_PARTITION_YEAR_MIN, ESEF_FACTS_PARTITION_YEAR_MAX + 1
        )
    ]
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


def _fact_json_object_key(fxo_id: str) -> str:
    return f"{ESEF_FACT_JSON_PREFIX}fxo_id={fxo_id}/facts.json"


def _report_xhtml_object_key(fxo_id: str) -> str:
    return f"{ESEF_REPORT_XHTML_PREFIX}fxo_id={fxo_id}/report.xhtml"


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


# A crawl that captures fewer than this fraction of the API's own reported
# total filing count (`meta.count`, surfaced via
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
    API's own reported total filing count.

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
        f"total ({api_reported_total}) -- refusing to replace "
        f"{QUALIFIED_FILINGS_INDEX_TABLE} (possible truncated/incomplete "
        "crawl)."
    )


@dg.asset(
    name="esef_filings_index_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "esef_filings"},
    pool=ESEF_FILINGS_DUCKDB_POOL,
    description=(
        "Full crawl of the filings.xbrl.org filing index into DuckDB table "
        f"{QUALIFIED_FILINGS_INDEX_TABLE}. Full replace each run; refuses to "
        "replace the table on an empty crawl or on a crawl below "
        f"{int(CRAWL_COMPLETENESS_MIN_RATIO * 100)}% of the API's reported total."
    ),
)
def esef_filings_index_duckdb(
    context: dg.AssetExecutionContext,
    esef_filings_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    client = EsefFilingsClient()
    records = list(client.iter_filings())
    api_reported_total = client.last_reported_total
    context.log.info(
        "ESEF filings index crawl fetched %d filings (api_reported_total=%s)",
        len(records),
        api_reported_total,
    )

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

    _check_crawl_completeness(
        crawled_count=len(records), api_reported_total=api_reported_total
    )

    with esef_filings_duckdb.get_connection() as connection:
        summary = replace_esef_filings_index(
            connection=connection,
            records=records,
            source_url=ESEF_INDEX_URL,
            source_run_id=context.run.run_id,
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
            "api_reported_total": api_reported_total,
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
    """Parse the leading `YYYY` off an index `period_end` string.

    Returns None (never raises) for a missing/blank value, a value too
    short to hold a 4-digit year, a non-numeric prefix, or a year outside
    [ESEF_FACTS_PARTITION_YEAR_MIN, ESEF_FACTS_PARTITION_YEAR_MAX] -- all of
    these are counted as `index_filings_outside_partition_range` by the
    caller, never raised.
    """
    if not period_end or len(period_end) < 4:
        return None
    try:
        year = int(period_end[:4])
    except ValueError:
        return None
    if year < ESEF_FACTS_PARTITION_YEAR_MIN or year > ESEF_FACTS_PARTITION_YEAR_MAX:
        return None
    return year


_IndexFilingRow = tuple[str, str, str | None, str | None, bool]


def _split_filings_by_partition_year(
    rows: Sequence[tuple[Any, ...]], *, partition_year: int
) -> tuple[list[_IndexFilingRow], int]:
    """Bucket filings_index rows into (this partition's filings, out-of-range count).

    The out-of-range count is a whole-index count (every row with a NULL,
    unparseable, or out-of-[2019,2027] `period_end`), recomputed fresh on
    every partition run -- not scoped to `partition_year`. It's cheap (one
    pass over already-fetched rows) and doubles as an index-health signal;
    a row can never belong to more than one year bucket anyway, so scoping
    it further would just mean re-deriving the same total N times across
    the 9 backfill partitions for no benefit.

    The caller stores this under the `index_filings_outside_partition_range`
    metadata key -- renamed from `skipped_out_of_range`, which read as
    partition-scoped and was misinterpreted live (a 2019 partition run with
    `filings_in_scope=0, skipped_out_of_range=6` was reasonably read as "6
    filings out of range in 2019", when it was really the whole index's
    out-of-range count). Both partitioned assets also emit `partition_year`
    in their metadata now, so the two numbers can never be conflated again.
    """
    in_scope: list[_IndexFilingRow] = []
    outside_partition_range_count = 0
    for lei, fxo_id, period_end, json_url, has_json_facts in rows:
        year = _period_end_year(period_end)
        if year is None:
            outside_partition_range_count += 1
            continue
        if year != partition_year:
            continue
        in_scope.append((lei, fxo_id, period_end, json_url, bool(has_json_facts)))
    return in_scope, outside_partition_range_count


_ReportXhtmlFilingRow = tuple[str, str | None]


def _split_report_filings_by_partition_year(
    rows: Sequence[tuple[Any, ...]], *, partition_year: int
) -> tuple[list[_ReportXhtmlFilingRow], int]:
    """Bucket filings_index rows into (this partition's filings, out-of-range
    count) for the report XHTML archive asset.

    Same `period_end`-year bucketing rule as `_split_filings_by_partition_year`
    (and the same "recomputed fresh on every partition run" rationale for the
    `index_filings_outside_partition_range` metadata key -- see that
    function's docstring), but the row shape is narrower: (fxo_id,
    report_url) only -- this asset never parses content, so it has no use
    for `lei`/`json_url`/`has_json_facts`.
    """
    in_scope: list[_ReportXhtmlFilingRow] = []
    outside_partition_range_count = 0
    for fxo_id, period_end, report_url in rows:
        year = _period_end_year(period_end)
        if year is None:
            outside_partition_range_count += 1
            continue
        if year != partition_year:
            continue
        in_scope.append((fxo_id, report_url))
    return in_scope, outside_partition_range_count


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


def _is_permanently_missing_upstream_error(exc: dlt_requests.HTTPError) -> bool:
    """True for a 404/410 -- filings.xbrl.org's index advertised a `json_url`/
    `report_url` for this filing, but the file itself no longer exists
    upstream (observed 2026-07-21: several UA filings among others, which
    starved the 2020/2021/2022 backfill partitions before this guard
    existed -- see docs/esef_filings-design.md Sec 11).

    Every OTHER `HTTPError` (5xx after the dlt client's own retries are
    exhausted, or any other 4xx) must still propagate and fail the
    partition loudly -- only a confirmed-permanent 404/410 is safe to skip
    and count rather than raise.
    """
    response = exc.response
    return response is not None and response.status_code in (404, 410)


def _download_and_parse_filing_facts(
    *,
    client: EsefFilingsClient,
    object_store: ObjectStoreResource,
    temp_dir: Path,
    lei: str,
    fxo_id: str,
    period_end: str,
    json_url: str,
    log_warning: Callable[..., object],
) -> tuple[list[facts.EsefFact], bool, bool]:
    """Download-or-reuse one filing's fact JSON from S3, then parse it.

    Returns (parsed_facts, downloaded, parse_failed). `downloaded` is False
    when the S3 object already existed (skip-existing reuse) -- fxo_id is
    versioned upstream, so a stable key means an existing object is always
    the same bytes the filing would produce again. A malformed download
    (bad JSON) is reported via `parse_failed=True` rather than raised: one
    bad filing must not fail the whole partition (mirrors sweden_financial's
    parse_errors philosophy -- counts + logs suffice here in v1, no DuckDB
    error table).
    """
    object_key = _fact_json_object_key(fxo_id)
    local_path = temp_dir / f"{sha256(fxo_id.encode()).hexdigest()}.json"
    downloaded = not object_store.exists(object_key, bucket=ESEF_FILINGS_FACTS_BUCKET)
    if downloaded:
        client.download_json_facts(json_url, local_path)
        object_store.upload_file(
            object_key, local_path, bucket=ESEF_FILINGS_FACTS_BUCKET
        )
    else:
        object_store.download_file(
            object_key, local_path, bucket=ESEF_FILINGS_FACTS_BUCKET
        )

    try:
        payload = json.loads(local_path.read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        log_warning(
            "ESEF facts JSON parse failed: fxo_id=%s json_url=%s error=%s",
            fxo_id,
            json_url,
            exc,
        )
        return [], downloaded, True

    parsed = facts.parse_oim_facts(
        payload, lei=lei, fxo_id=fxo_id, period_end=period_end
    )
    return parsed, downloaded, False


# Bounds per-transaction uncommitted MVCC memory during the facts insert.
# Production incident: the 2020-2023 backfill partitions (millions of parsed
# XBRL facts each) failed with `_duckdb.OutOfMemoryException: failed to pin
# block ... (73.8 GiB/73.8 GiB used)` on a ~412 MB DuckDB file with 92 GB RAM
# free on the host -- not a data-volume problem. Root cause: a single
# `executemany` over the WHOLE year's rows inside ONE transaction holds every
# uncommitted row's MVCC version/undo data in memory for the entire insert;
# small years (2026/2027) passed only because they're tiny. Committing every
# `_FACTS_INSERT_CHUNK_SIZE` rows as its own transaction bounds the
# uncommitted-row memory to one chunk's worth, regardless of the year's total
# row count.
_FACTS_INSERT_CHUNK_SIZE = 25_000


def _replace_facts_partition(
    *,
    connection: Any,
    partition_year: int,
    fact_rows: Sequence[tuple[Any, ...]],
    log_info: Callable[..., object],
) -> None:
    """Delete-then-insert ONLY `period_end_year = partition_year` rows.

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
    then `fact_rows` is inserted in bounded `_FACTS_INSERT_CHUNK_SIZE`-row
    chunks, each its OWN transaction+commit -- uncommitted MVCC memory never
    exceeds one chunk's worth, regardless of the year's total row count.
    `preserve_insertion_order` is turned off first (insertion order is
    irrelevant here: the ClickHouse export orders on its own); leaving it on
    would make DuckDB buffer rows to preserve original insertion order across
    the chunked inserts, defeating the point of chunking. `memory_limit` is
    deliberately left at DuckDB's default -- this fix bounds *uncommitted*
    transaction memory, it isn't a total-memory cap.

    CONTRACT CHANGE from the prior single-transaction version: a mid-way
    failure can now leave a PARTIAL year committed (the DELETE, plus however
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
    connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
    connection.execute(
        f"create table if not exists {QUALIFIED_FACTS_TABLE} ({_FACTS_COLUMNS_SQL})"
    )
    connection.execute("set preserve_insertion_order = false")
    try:
        connection.execute("begin transaction")
        connection.execute(
            f"delete from {QUALIFIED_FACTS_TABLE} where period_end_year = ?",
            [partition_year],
        )
        connection.execute("commit")

        total = len(fact_rows)
        insert_sql = (
            f"insert into {QUALIFIED_FACTS_TABLE} values "
            f"({', '.join(['?'] * len(_FACTS_COLUMNS))})"
        )
        inserted = 0
        for start in range(0, total, _FACTS_INSERT_CHUNK_SIZE):
            chunk = fact_rows[start : start + _FACTS_INSERT_CHUNK_SIZE]
            connection.execute("begin transaction")
            connection.executemany(insert_sql, chunk)
            connection.execute("commit")
            inserted += len(chunk)
            log_info(
                "ESEF facts partition %s: inserted %d/%d fact rows",
                partition_year,
                inserted,
                total,
            )
        if total == 0:
            log_info("ESEF facts partition %s: inserted 0/0 fact rows", partition_year)
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
    partition_year: int,
    processed: int,
    total: int,
    downloaded: int,
    reused: int,
    skipped: int,
    force: bool = False,
) -> None:
    """Log one progress line for `run_esef_filing_facts_partition`'s loop.

    A no-op unless `processed` lands on `_PROGRESS_LOG_INTERVAL` or `force`
    is set -- `force=True` is used exactly once, after the loop ends, so the
    final tally is always logged even when `processed` isn't a multiple of
    the interval. `skipped` is `skipped_no_json + skipped_upstream_missing`
    (filings that never reached a download/reuse outcome) -- a filing whose
    parse failed after a successful download/reuse is not double-counted
    here, since it's already inside `downloaded`/`reused`.
    """
    if not force and processed % _PROGRESS_LOG_INTERVAL != 0:
        return
    log_info(
        "ESEF facts partition %s: %d/%d filings processed "
        "(%d downloaded, %d reused, %d skipped)",
        partition_year,
        processed,
        total,
        downloaded,
        reused,
        skipped,
    )


def run_esef_filing_facts_partition(
    *,
    esef_filings_duckdb: DuckDBResource,
    object_store: Any,
    client: Any,
    partition_year: int,
    source_run_id: str,
    log_info: Callable[..., object],
    log_warning: Callable[..., object],
) -> dict[str, int]:
    """Do the actual work of `esef_filing_facts_duckdb` for one partition year.

    Split out from the `@dg.asset` function (which only wires up
    context/resources and calls this) so tests can call it directly with
    plain duck-typed fakes for `object_store`/`client` -- mirroring
    sweden_financial's `extract_sweden_financial_report_xhtml_catalog`
    pattern. This also sidesteps a real Dagster behavior worth knowing: a
    `ConfigurableResource` instance built with an injected private
    attribute (e.g. `ObjectStoreResource(s3_client=fake)`) does NOT survive
    `dg.materialize`/`execute_in_process` -- Dagster reconstructs the
    resource from its resolved pydantic config fields alone (observed:
    `ObjectStoreResource.__init__` re-invoked with `s3_client=None`), so a
    fake client injected that way is silently replaced by a real
    `boto3.client(...)` inside the asset. Calling the plain function
    directly (as the tests do) avoids that reconstruction entirely.
    """
    with read_only_duckdb_connection(esef_filings_duckdb) as connection:
        index_rows = connection.execute(
            "select lei, fxo_id, period_end, json_url, has_json_facts "
            f"from {QUALIFIED_FILINGS_INDEX_TABLE} order by fxo_id"
        ).fetchall()

    in_scope, outside_partition_range_count = _split_filings_by_partition_year(
        index_rows, partition_year=partition_year
    )
    log_info(
        "ESEF facts partition %s: %d filings in scope, %d outside partition range",
        partition_year,
        len(in_scope),
        outside_partition_range_count,
    )

    object_store.ensure_bucket(ESEF_FILINGS_FACTS_BUCKET)

    processed_count = 0
    downloaded_count = 0
    reused_count = 0
    skipped_no_json = 0
    skipped_upstream_missing = 0
    parse_failed_count = 0
    fact_rows: list[tuple[Any, ...]] = []

    # All fallible work (S3 I/O, download, JSON parse) happens BEFORE the
    # writable DuckDB connection opens below -- see _replace_facts_partition's
    # docstring for why.
    with tempfile.TemporaryDirectory(prefix="esef_filings_facts_") as tmpdir:
        temp_dir = Path(tmpdir)
        for lei, fxo_id, period_end, json_url, has_json_facts in in_scope:
            processed_count += 1
            if not has_json_facts or not json_url:
                skipped_no_json += 1
                _log_facts_progress(
                    log_info,
                    partition_year=partition_year,
                    processed=processed_count,
                    total=len(in_scope),
                    downloaded=downloaded_count,
                    reused=reused_count,
                    skipped=skipped_no_json + skipped_upstream_missing,
                )
                continue
            try:
                parsed, downloaded, parse_failed = _download_and_parse_filing_facts(
                    client=client,
                    object_store=object_store,
                    temp_dir=temp_dir,
                    lei=lei,
                    fxo_id=fxo_id,
                    period_end=period_end,
                    json_url=json_url,
                    log_warning=log_warning,
                )
            except dlt_requests.HTTPError as exc:
                # A dead upstream link (Sec 11: 2020/2021/2022 backfill
                # incident) must not starve the whole partition -- but only
                # a confirmed-permanent 404/410 is safe to skip; anything
                # else (5xx, etc.) still propagates and fails loudly. This
                # skip happens BEFORE any S3 upload or fact row is produced
                # (_download_and_parse_filing_facts raises from inside its
                # `if downloaded:` branch, ahead of `object_store.upload_file`),
                # so a permanently-missing filing leaves no S3 object and no
                # facts rows behind.
                if not _is_permanently_missing_upstream_error(exc):
                    raise
                skipped_upstream_missing += 1
                log_warning(
                    "ESEF facts download permanently missing upstream "
                    "(status=%s): fxo_id=%s json_url=%s",
                    exc.response.status_code if exc.response is not None else None,
                    fxo_id,
                    json_url,
                )
                _log_facts_progress(
                    log_info,
                    partition_year=partition_year,
                    processed=processed_count,
                    total=len(in_scope),
                    downloaded=downloaded_count,
                    reused=reused_count,
                    skipped=skipped_no_json + skipped_upstream_missing,
                )
                continue
            if downloaded:
                downloaded_count += 1
            else:
                reused_count += 1
            if parse_failed:
                parse_failed_count += 1
                _log_facts_progress(
                    log_info,
                    partition_year=partition_year,
                    processed=processed_count,
                    total=len(in_scope),
                    downloaded=downloaded_count,
                    reused=reused_count,
                    skipped=skipped_no_json + skipped_upstream_missing,
                )
                continue
            fact_rows.extend(
                _row_from_fact(
                    fact, period_end_year=partition_year, source_run_id=source_run_id
                )
                for fact in parsed
            )
            _log_facts_progress(
                log_info,
                partition_year=partition_year,
                processed=processed_count,
                total=len(in_scope),
                downloaded=downloaded_count,
                reused=reused_count,
                skipped=skipped_no_json + skipped_upstream_missing,
            )

    # Unconditional completion log -- always fires exactly once with the
    # final tally, even when `processed_count` doesn't land on
    # `_PROGRESS_LOG_INTERVAL` (the periodic logging above is silent for the
    # remainder past the last interval boundary).
    if in_scope:
        _log_facts_progress(
            log_info,
            partition_year=partition_year,
            processed=processed_count,
            total=len(in_scope),
            downloaded=downloaded_count,
            reused=reused_count,
            skipped=skipped_no_json + skipped_upstream_missing,
            force=True,
        )

    with esef_filings_duckdb.get_connection() as connection:
        _replace_facts_partition(
            connection=connection,
            partition_year=partition_year,
            fact_rows=fact_rows,
            log_info=log_info,
        )

    return {
        "filings_in_scope": len(in_scope),
        "downloaded_count": downloaded_count,
        "reused_count": reused_count,
        "skipped_no_json": skipped_no_json,
        "skipped_upstream_missing": skipped_upstream_missing,
        "index_filings_outside_partition_range": outside_partition_range_count,
        "partition_year": partition_year,
        "parse_failed_count": parse_failed_count,
        "fact_row_count": len(fact_rows),
    }


@dg.asset(
    name="esef_filing_facts_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3", "xbrl", "esef_filings"},
    deps=["esef_filings_index_duckdb"],
    partitions_def=ESEF_FILING_FACTS_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ESEF_FILINGS_DUCKDB_POOL,
    description=(
        "Downloads each in-scope filing's OIM xBRL-JSON fact export to S3 "
        f"(bucket={ESEF_FILINGS_FACTS_BUCKET}, skip-existing) and parses it "
        f"into DuckDB table {QUALIFIED_FACTS_TABLE}, replacing only this "
        "partition's period_end_year scope."
    ),
)
def esef_filing_facts_duckdb(
    context: dg.AssetExecutionContext,
    esef_filings_duckdb: DuckDBResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    metadata = run_esef_filing_facts_partition(
        esef_filings_duckdb=esef_filings_duckdb,
        object_store=object_store,
        client=EsefFilingsClient(),
        partition_year=int(context.partition_key),
        source_run_id=context.run.run_id,
        log_info=context.log.info,
        log_warning=context.log.warning,
    )
    return dg.MaterializeResult(
        metadata={
            **metadata,
            "duckdb_path": str(esef_filings_source_duckdb_path()),
        }
    )


def _archive_filing_report_xhtml(
    *,
    client: Any,
    object_store: Any,
    temp_dir: Path,
    fxo_id: str,
    report_url: str,
) -> bool:
    """Download one filing's rendered report XHTML to S3, skipping when the
    object already exists.

    Returns True when the object was freshly downloaded, False when it was
    already present (skip-existing) -- fxo_id is version-stable upstream
    (verified live), so an existing object is always the same bytes the
    filing would produce again. Unlike `_download_and_parse_filing_facts`,
    there is no parsing here, so a reused object never needs downloading
    back locally -- an existence check alone suffices.
    """
    object_key = _report_xhtml_object_key(fxo_id)
    if object_store.exists(object_key, bucket=ESEF_FILINGS_FACTS_BUCKET):
        return False
    local_path = temp_dir / f"{sha256(fxo_id.encode()).hexdigest()}.xhtml"
    client.download_json_facts(report_url, local_path)
    object_store.upload_file(object_key, local_path, bucket=ESEF_FILINGS_FACTS_BUCKET)
    return True


def _log_report_xhtml_progress(
    log_info: Callable[..., object],
    *,
    partition_year: int,
    processed: int,
    total: int,
    downloaded: int,
    reused: int,
    skipped: int,
    force: bool = False,
) -> None:
    """Log one progress line for `run_esef_report_xhtml_partition`'s loop.

    Same interval/force semantics as `_log_facts_progress` (see its
    docstring). `skipped` is `skipped_no_report_url +
    skipped_upstream_missing` -- there is no parse step in this loop, so
    every filing lands in exactly one of downloaded/reused/skipped.
    """
    if not force and processed % _PROGRESS_LOG_INTERVAL != 0:
        return
    log_info(
        "ESEF report xhtml partition %s: %d/%d filings processed "
        "(%d downloaded, %d reused, %d skipped)",
        partition_year,
        processed,
        total,
        downloaded,
        reused,
        skipped,
    )


def run_esef_report_xhtml_partition(
    *,
    esef_filings_duckdb: DuckDBResource,
    object_store: Any,
    client: Any,
    partition_year: int,
    log_info: Callable[..., object],
    log_warning: Callable[..., object],
) -> dict[str, int]:
    """Do the actual work of `esef_report_xhtml_s3` for one partition year.

    Pure S3 archive layer (no parsing) -- the corpus for a future
    embeddings/LLM-search pass over annual reports. Unlike
    `run_esef_filing_facts_partition`, this function never opens a writable
    DuckDB connection at all: the local index is read once, read-only, for
    scope resolution, and everything else is S3 I/O. Split out from the
    `@dg.asset` function for the same reason as
    `run_esef_filing_facts_partition` (see its docstring): a
    `ConfigurableResource` built with an injected private attribute does not
    survive `dg.materialize`, so tests call this plain function directly with
    duck-typed fakes instead.
    """
    with read_only_duckdb_connection(esef_filings_duckdb) as connection:
        index_rows = connection.execute(
            "select fxo_id, period_end, report_url "
            f"from {QUALIFIED_FILINGS_INDEX_TABLE} order by fxo_id"
        ).fetchall()

    in_scope, outside_partition_range_count = _split_report_filings_by_partition_year(
        index_rows, partition_year=partition_year
    )
    log_info(
        "ESEF report XHTML partition %s: %d filings in scope, %d outside "
        "partition range",
        partition_year,
        len(in_scope),
        outside_partition_range_count,
    )

    object_store.ensure_bucket(ESEF_FILINGS_FACTS_BUCKET)

    processed_count = 0
    downloaded_count = 0
    reused_count = 0
    skipped_no_report_url = 0
    skipped_upstream_missing = 0

    with tempfile.TemporaryDirectory(prefix="esef_filings_report_xhtml_") as tmpdir:
        temp_dir = Path(tmpdir)
        for fxo_id, report_url in in_scope:
            processed_count += 1
            if not report_url:
                skipped_no_report_url += 1
                _log_report_xhtml_progress(
                    log_info,
                    partition_year=partition_year,
                    processed=processed_count,
                    total=len(in_scope),
                    downloaded=downloaded_count,
                    reused=reused_count,
                    skipped=skipped_no_report_url + skipped_upstream_missing,
                )
                continue
            try:
                downloaded = _archive_filing_report_xhtml(
                    client=client,
                    object_store=object_store,
                    temp_dir=temp_dir,
                    fxo_id=fxo_id,
                    report_url=report_url,
                )
            except dlt_requests.HTTPError as exc:
                # Same permanently-missing-upstream-file guard as
                # run_esef_filing_facts_partition (see that loop's comment) --
                # the skip happens before _archive_filing_report_xhtml's
                # `object_store.upload_file` call, so no phantom S3 object is
                # left for a filing whose report.html 404s/410s upstream.
                if not _is_permanently_missing_upstream_error(exc):
                    raise
                skipped_upstream_missing += 1
                log_warning(
                    "ESEF report XHTML download permanently missing upstream "
                    "(status=%s): fxo_id=%s report_url=%s",
                    exc.response.status_code if exc.response is not None else None,
                    fxo_id,
                    report_url,
                )
                _log_report_xhtml_progress(
                    log_info,
                    partition_year=partition_year,
                    processed=processed_count,
                    total=len(in_scope),
                    downloaded=downloaded_count,
                    reused=reused_count,
                    skipped=skipped_no_report_url + skipped_upstream_missing,
                )
                continue
            if downloaded:
                downloaded_count += 1
            else:
                reused_count += 1
            _log_report_xhtml_progress(
                log_info,
                partition_year=partition_year,
                processed=processed_count,
                total=len(in_scope),
                downloaded=downloaded_count,
                reused=reused_count,
                skipped=skipped_no_report_url + skipped_upstream_missing,
            )

    # Unconditional completion log -- see the analogous comment in
    # run_esef_filing_facts_partition.
    if in_scope:
        _log_report_xhtml_progress(
            log_info,
            partition_year=partition_year,
            processed=processed_count,
            total=len(in_scope),
            downloaded=downloaded_count,
            reused=reused_count,
            skipped=skipped_no_report_url + skipped_upstream_missing,
            force=True,
        )

    return {
        "filings_in_scope": len(in_scope),
        "downloaded_count": downloaded_count,
        "reused_count": reused_count,
        "skipped_no_report_url": skipped_no_report_url,
        "skipped_upstream_missing": skipped_upstream_missing,
        "index_filings_outside_partition_range": outside_partition_range_count,
        "partition_year": partition_year,
    }


@dg.asset(
    name="esef_report_xhtml_s3",
    group_name=GROUP_NAME,
    kinds={"python", "s3", "xhtml", "esef_filings"},
    deps=["esef_filings_index_duckdb"],
    partitions_def=ESEF_FILING_FACTS_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ESEF_FILINGS_DUCKDB_POOL,
    description=(
        "Archives each in-scope filing's rendered report XHTML to S3 "
        f"(bucket={ESEF_FILINGS_FACTS_BUCKET}, prefix={ESEF_REPORT_XHTML_PREFIX}, "
        "skip-existing). Pure archive layer -- no parsing -- the corpus for a "
        "future embeddings/LLM-search pass over annual reports."
    ),
)
def esef_report_xhtml_s3(
    context: dg.AssetExecutionContext,
    esef_filings_duckdb: DuckDBResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    metadata = run_esef_report_xhtml_partition(
        esef_filings_duckdb=esef_filings_duckdb,
        object_store=object_store,
        client=EsefFilingsClient(),
        partition_year=int(context.partition_key),
        log_info=context.log.info,
        log_warning=context.log.warning,
    )
    return dg.MaterializeResult(metadata=metadata)


class EsefFilingsClickhouseExportConfig(dg.Config):
    # Shrink-guard override (Finding M1; see publish.py's module docstring
    # and sweden_financial/clickhouse.py's guard_against_clickhouse_table_shrink)
    # -- MUST stay False by default. Only set True via explicit run config
    # for a confirmed-intentional shrink of a populated esef_filings table,
    # never as a standing default.
    allow_shrink: bool = False


@dg.asset(
    name="esef_filings_clickhouse",
    deps=["esef_filings_index_duckdb"],
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


def _all_partition_deps(*asset_keys: str) -> list[dg.AssetDep]:
    """Deps of an unpartitioned derived asset on a year-partitioned upstream
    (mirrors sweden_financial/assets.py's helper of the same name)."""
    return [
        dg.AssetDep(dg.AssetKey(asset_key), partition_mapping=dg.AllPartitionMapping())
        for asset_key in asset_keys
    ]


class EsefFactsClickhouseExportConfig(dg.Config):
    # Shrink-guard override (Finding M1) -- MUST stay False by default. Only
    # set True via explicit run config for a confirmed-intentional shrink of
    # a populated esef_facts table, never as a standing default.
    allow_shrink: bool = False


@dg.asset(
    name="esef_facts_clickhouse",
    deps=_all_partition_deps("esef_filing_facts_duckdb"),
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "esef_filings"},
    pool=ESEF_FILINGS_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_ESEF_FACTS_TABLE},
    description=(
        "Full-replaces corpscout.esef_facts from DuckDB "
        f"{QUALIFIED_FACTS_TABLE} (every year partition) via the shared "
        "stage+EXCHANGE ClickHouse exporter. Full replace is correct here -- "
        "one DuckDB file holds the entire dataset, no split-file hazard. "
        "Guarded against a replace that would shrink the table by more than "
        "half (see guard_against_clickhouse_table_shrink)."
    ),
)
def esef_facts_clickhouse(
    context: dg.AssetExecutionContext,
    config: EsefFactsClickhouseExportConfig,
    esef_filings_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(esef_filings_duckdb) as connection:
        rows = export_esef_facts_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
            allow_shrink=config.allow_shrink,
        )
    return dg.MaterializeResult(
        metadata={
            "rows_exported": rows,
            "clickhouse_table": tables.QUALIFIED_ESEF_FACTS_TABLE,
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
    deps=["esef_facts_clickhouse", "esef_filings_clickhouse"],
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
# esef_filings_refresh_job selects the index (unpartitioned), the two
# year-partitioned assets (facts + report-XHTML archive, both on
# ESEF_FILING_FACTS_PARTITIONS), and the three ClickHouse exports + metrics
# (all unpartitioned) as ONE job. This is safe to combine into a single job
# for a reason sweden_financial's history makes concrete: sweden's weekly
# chain needed a 2026-07-20 de-partitioning redesign because it keeps a
# SEPARATE DuckDB file PER YEAR, so a partition-scoped incremental ClickHouse
# export became order-dependent against the yearly backfill (the 2026-07-18
# incident -- see sweden_financial/docs/sweden_financial-design.md). ESEF
# keeps its ENTIRE dataset in ONE DuckDB file (esef_filings_source.duckdb):
# the year-partitioned facts/xhtml steps each delete-then-insert only their
# own `period_end_year` scope within that single file, and the unpartitioned
# exports downstream always full-replace ClickHouse from a read of the WHOLE
# file (every year materialized so far) -- there is no split-file
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
# `esef_filing_facts_duckdb` and `esef_report_xhtml_s3` share the exact same
# `ESEF_FILING_FACTS_PARTITIONS` object, so this job resolves to that single
# partitions_def; the other five assets in the selection are unpartitioned
# and simply run every time.
ESEF_FILINGS_TIMEZONE = "Europe/Belgrade"

ESEF_FILINGS_REFRESH_SELECTION = dg.AssetSelection.assets(
    "esef_filings_index_duckdb",
    "esef_filing_facts_duckdb",
    "esef_report_xhtml_s3",
    "esef_filings_clickhouse",
    "esef_facts_clickhouse",
    "esef_entity_registry_map_clickhouse",
    "esef_financial_metrics_clickhouse",
)

# UI-launched backfill: the two year-partitioned assets ONLY (2019-2027).
# Exports are deliberately excluded from the backfill job -- run
# esef_filings_refresh_job (or the individual export assets) once after all
# backfill partitions land, mirroring sweden_financial_backfill_job's
# exports-excluded shape (its exports run in their own separate job instead).
ESEF_FILINGS_BACKFILL_SELECTION = dg.AssetSelection.assets(
    "esef_filing_facts_duckdb",
    "esef_report_xhtml_s3",
)

esef_filings_refresh_job = dg.define_asset_job(
    "esef_filings_refresh_job",
    tags=HEAVY_BULK_RUN_TAGS,
    selection=ESEF_FILINGS_REFRESH_SELECTION,
)

esef_filings_backfill_job = dg.define_asset_job(
    "esef_filings_backfill_job",
    tags=HEAVY_BULK_RUN_TAGS,
    selection=ESEF_FILINGS_BACKFILL_SELECTION,
)


def _esef_filings_refresh_run_request(
    context: dg.ScheduleEvaluationContext,
) -> dg.RunRequest | dg.SkipReason:
    """Resolve the current year's partition key at schedule-evaluation time.

    Mirrors sweden_financial's (pre-de-partition) `_current_year_run_request`
    resolver: esef_filings_refresh_job's two partitioned assets are keyed by
    `str(year)` (ESEF_FILING_FACTS_PARTITIONS), not a Daily/Weekly/Monthly/
    Hourly cadence, so `build_schedule_from_partitioned_job` doesn't apply --
    the schedule must compute and hand the job a single partition_key itself.
    Falls back to "now" (in ESEF_FILINGS_TIMEZONE) when Dagster evaluates the
    schedule outside a real tick (`scheduled_execution_time is None`, e.g. a
    manual "test schedule" click in the UI).
    """
    if context.scheduled_execution_time is None:
        now = datetime.now(tz=ZoneInfo(ESEF_FILINGS_TIMEZONE))
    else:
        now = context.scheduled_execution_time.astimezone(
            ZoneInfo(ESEF_FILINGS_TIMEZONE)
        )
    partition_key = str(now.year)
    if partition_key not in ESEF_FILING_FACTS_PARTITIONS.get_partition_keys():
        return dg.SkipReason(
            f"No ESEF filings facts partition for schedule year {partition_key}"
        )
    return dg.RunRequest(partition_key=partition_key)


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
        esef_filing_facts_duckdb,
        esef_report_xhtml_s3,
        esef_filings_clickhouse,
        esef_facts_clickhouse,
        esef_entity_registry_map_clickhouse,
        esef_financial_metrics_clickhouse,
    ],
    asset_checks=[filings_index_non_empty],
    jobs=[esef_filings_refresh_job, esef_filings_backfill_job],
    schedules=[esef_filings_refresh_weekly],
    resources={
        "esef_filings_duckdb": duckdb_resource(esef_filings_source_duckdb_path()),
    },
)
