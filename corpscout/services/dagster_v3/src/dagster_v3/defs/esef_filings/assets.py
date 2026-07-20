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

No `from __future__ import annotations` -- this module defines `@dg.asset`s
and stringizing the `context: AssetExecutionContext` hint breaks Dagster's op
context-type validation (see CLAUDE.md).
"""

import json
import tempfile
from collections import Counter
from collections.abc import Callable, Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.esef_filings import facts, tables
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

# S3 bucket + key layout for downloaded fact JSON (Task 4). fxo_id is
# versioned upstream (a new filing version gets a new fxo_id), so the key is
# stable per filing version -- an existing object never needs re-fetching.
ESEF_FILINGS_FACTS_BUCKET = "source-esef-filings"
ESEF_FACT_JSON_PREFIX = "esef_filings/fact_json/"

FACTS_TABLE = "facts"
QUALIFIED_FACTS_TABLE = f"{tables.DLT_DATASET_NAME}.{FACTS_TABLE}"

# Matches facts._period_end_year's accepted range and the
# StaticPartitionsDefinition below (str(y) for y in range(2019, 2028)).
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


def _period_end_year(period_end: str | None) -> int | None:
    """Parse the leading `YYYY` off an index `period_end` string.

    Returns None (never raises) for a missing/blank value, a value too
    short to hold a 4-digit year, a non-numeric prefix, or a year outside
    [ESEF_FACTS_PARTITION_YEAR_MIN, ESEF_FACTS_PARTITION_YEAR_MAX] -- all of
    these are counted as `skipped_out_of_range` by the caller, never raised.
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

    `skipped_out_of_range` is a whole-index count (every row with a NULL,
    unparseable, or out-of-[2019,2027] `period_end`), recomputed fresh on
    every partition run -- not scoped to `partition_year`. It's cheap (one
    pass over already-fetched rows) and doubles as an index-health signal;
    a row can never belong to more than one year bucket anyway, so scoping
    it further would just mean re-deriving the same total N times across
    the 9 backfill partitions for no benefit.
    """
    in_scope: list[_IndexFilingRow] = []
    skipped_out_of_range = 0
    for lei, fxo_id, period_end, json_url, has_json_facts in rows:
        year = _period_end_year(period_end)
        if year is None:
            skipped_out_of_range += 1
            continue
        if year != partition_year:
            continue
        in_scope.append((lei, fxo_id, period_end, json_url, bool(has_json_facts)))
    return in_scope, skipped_out_of_range


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


def _replace_facts_partition(
    *,
    connection: Any,
    partition_year: int,
    fact_rows: Sequence[tuple[Any, ...]],
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
    """
    connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
    connection.execute(
        f"create table if not exists {QUALIFIED_FACTS_TABLE} ({_FACTS_COLUMNS_SQL})"
    )
    connection.execute("begin transaction")
    try:
        connection.execute(
            f"delete from {QUALIFIED_FACTS_TABLE} where period_end_year = ?",
            [partition_year],
        )
        if fact_rows:
            connection.executemany(
                f"insert into {QUALIFIED_FACTS_TABLE} values "
                f"({', '.join(['?'] * len(_FACTS_COLUMNS))})",
                fact_rows,
            )
        connection.execute("commit")
    except Exception:
        try:
            connection.execute("rollback")
        except Exception:
            pass
        connection.close()
        raise


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

    in_scope, skipped_out_of_range = _split_filings_by_partition_year(
        index_rows, partition_year=partition_year
    )
    log_info(
        "ESEF facts partition %s: %d filings in scope, %d skipped (out of range)",
        partition_year,
        len(in_scope),
        skipped_out_of_range,
    )

    object_store.ensure_bucket(ESEF_FILINGS_FACTS_BUCKET)

    downloaded_count = 0
    reused_count = 0
    skipped_no_json = 0
    parse_failed_count = 0
    fact_rows: list[tuple[Any, ...]] = []

    # All fallible work (S3 I/O, download, JSON parse) happens BEFORE the
    # writable DuckDB connection opens below -- see _replace_facts_partition's
    # docstring for why.
    with tempfile.TemporaryDirectory(prefix="esef_filings_facts_") as tmpdir:
        temp_dir = Path(tmpdir)
        for lei, fxo_id, period_end, json_url, has_json_facts in in_scope:
            if not has_json_facts or not json_url:
                skipped_no_json += 1
                continue
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
            if downloaded:
                downloaded_count += 1
            else:
                reused_count += 1
            if parse_failed:
                parse_failed_count += 1
                continue
            fact_rows.extend(
                _row_from_fact(
                    fact, period_end_year=partition_year, source_run_id=source_run_id
                )
                for fact in parsed
            )

    with esef_filings_duckdb.get_connection() as connection:
        _replace_facts_partition(
            connection=connection,
            partition_year=partition_year,
            fact_rows=fact_rows,
        )

    return {
        "filings_in_scope": len(in_scope),
        "downloaded_count": downloaded_count,
        "reused_count": reused_count,
        "skipped_no_json": skipped_no_json,
        "skipped_out_of_range": skipped_out_of_range,
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


defs = dg.Definitions(
    assets=[esef_filings_index_duckdb, esef_filing_facts_duckdb],
    asset_checks=[filings_index_non_empty],
    resources={
        "esef_filings_duckdb": duckdb_resource(esef_filings_source_duckdb_path()),
    },
)
