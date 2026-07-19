import re
import uuid
from collections.abc import Callable, Sequence
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.sweden_financial.parsing import SWEDEN_FINANCIAL_DATASET_NAME

SWEDEN_FINANCIAL_DATABASE = "corpscout"
SE_FINANCIAL_REPORTS_TABLE = "se_financial_reports"
SE_FINANCIAL_FACTS_TABLE = "se_financial_facts"
QUALIFIED_SE_FINANCIAL_REPORTS_TABLE = (
    f"{SWEDEN_FINANCIAL_DATABASE}.{SE_FINANCIAL_REPORTS_TABLE}"
)
QUALIFIED_SE_FINANCIAL_FACTS_TABLE = (
    f"{SWEDEN_FINANCIAL_DATABASE}.{SE_FINANCIAL_FACTS_TABLE}"
)

SE_FINANCIAL_REPORTS_EXPORT_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "statement_key",
    "company_id",
    "report_period_start",
    "report_period_end",
    "fiscal_year",
    "reported_company_name",
    "report_language",
    "source_archive_key",
    "source_archive_name",
    "nested_zip_name",
    "xhtml_object_key",
    "xhtml_sha256",
    "xhtml_size_bytes",
    "taxonomy_entrypoint",
    "schema_refs",
    "contexts_count",
    "units_count",
    "facts_count",
    "parser_version",
    "source_payload_hash",
    "resolved_at",
)

SE_FINANCIAL_FACTS_EXPORT_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "statement_key",
    "company_id",
    "report_period_end",
    "fact_ordinal",
    "concept_qname",
    "concept_namespace",
    "concept_local_name",
    "context_id",
    "unit_id",
    "decimals",
    "precision",
    "value_kind",
    "raw_value",
    "amount_original",
    "amount_usd",
    "date_value",
    "text_value",
    "currency",
    "dimensions",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "parser_version",
    "source_payload_hash",
    "resolved_at",
)

# A staged full-table replace that would leave a target with less than this
# fraction of its CURRENT row count is refused by default (see
# guard_against_clickhouse_table_shrink below). Added 2026-07-19 after a live
# regression: sweden_financial_reports_clickhouse/facts_clickhouse
# full-replaced corpscout.se_financial_reports/se_financial_facts from a
# local DuckDB holding only one source-year partition, and the pre-existing
# empty-input guard (0 staged rows -> refuse) never tripped because the
# lone partition still had *some* rows -- it dropped 2020-2025 silently
# (se_financial_reports 1.85M -> 396,877 rows). This guard catches any
# replace that would shrink a populated table by more than half, not just
# an all-the-way-to-zero replace.
#
# 2026-07-19 follow-up: reports/facts no longer do a full replace at all --
# they were converted to partition-scoped delete+insert
# (upsert_sweden_financial_reports_partition /
# upsert_sweden_financial_facts_partition below), which structurally cannot
# touch rows beyond the running partition's own scope. This guard now only
# guards the remaining Sweden full-replaces (the derived
# se_financial_metrics rebuild plus the officers/audits publishes).
SHRINK_GUARD_MIN_RATIO = 0.5


def guard_against_clickhouse_table_shrink(
    *,
    qualified_table: str,
    existing_row_count: int,
    staged_row_count: int,
    allow_shrink: bool,
) -> None:
    """Refuse a full-table replace that would shrink ``qualified_table``.

    Applies to the remaining Sweden CH full-replaces
    (``sweden_financial_metrics_clickhouse`` and the derived
    officers/audits publishes): each stages its replacement data, then --
    before the atomic ``EXCHANGE TABLES`` swap -- must compare the staged
    row count against the table's CURRENT (pre-swap) row count. If the
    target already holds rows and the staged replacement would leave it
    with less than ``SHRINK_GUARD_MIN_RATIO`` (50%) of that count, refuse
    unless the caller explicitly passes ``allow_shrink=True``.
    ``allow_shrink`` must never default to ``True`` anywhere -- it exists
    solely as an explicit, deliberate override for an operator who has
    confirmed the shrink is intentional (e.g. a genuine upstream
    retirement of data), threaded through from the asset's own run
    config, never hardcoded on.
    """
    if allow_shrink:
        return
    if existing_row_count <= 0:
        return
    if staged_row_count >= existing_row_count * SHRINK_GUARD_MIN_RATIO:
        return
    raise ValueError(
        f"Refusing to replace ClickHouse table {qualified_table}: staged "
        f"row count {staged_row_count} is less than "
        f"{int(SHRINK_GUARD_MIN_RATIO * 100)}% of the existing "
        f"{existing_row_count} rows. If this shrink is intentional, pass "
        "allow_shrink=True explicitly to override this guard."
    )


def clickhouse_table_row_count(client: Any, qualified_table: str) -> int:
    rows = client.execute(f"SELECT count() FROM {qualified_table}")
    return int(rows[0][0])


_BACKFILL_PARTITION_KEY_PATTERN = re.compile(r"^\d{4}$")


def _is_backfill_partition_key(partition_key: str) -> bool:
    """Distinguish yearly backfill partitions from weekly current partitions.

    Backfill partitions are bare 4-digit years (``"2020"``..``"2026"``);
    current partitions are ISO week-start dates (``"2026-07-11"``). The two
    partition families never collide in format, so this is enough to pick a
    scope-resolution strategy without a separate "kind" parameter.
    """
    return bool(_BACKFILL_PARTITION_KEY_PATTERN.fullmatch(partition_key))


def resolve_sweden_financial_partition_archive_keys(
    duckdb_connection: Any,
    partition_key: str,
) -> list[str]:
    """Resolve this partition's ``source_archive_key`` scope from the local
    per-year DuckDB file already open on ``duckdb_connection``.

    - **Yearly backfill partitions** (bare 4-digit year): the parse layer
      replaces the ENTIRE year file for these
      (``replace_scope="partition"`` in ``parsing.py``), so the export scope
      is every distinct ``source_archive_key`` currently in that file's
      ``reports`` table -- "the whole file".
    - **Weekly current partitions** (ISO date): the parse layer only
      replaces the archives it newly downloaded THIS run
      (``replace_scope="archive"``), correlated by ``source_run_id`` within
      one job run. The export runs later, in a *different* run (a different
      ``source_run_id``), so it cannot use that correlation; instead it
      re-derives the same archive set from ``archive_sync_catalog`` via the
      ``load_partition_key`` column, which is set to the Dagster partition
      key itself at sync time (see
      ``archive_state.record_sweden_financial_archive_sync``) and so is
      stable across runs. Filtered to ``downloaded`` rows because every
      weekly sync re-lists the FULL year's upstream archives (see
      ``resources.py:sync_raw_archives``) -- only rows with
      ``downloaded = true`` are the archives that were actually new (and
      therefore (re-)parsed) for this specific partition.

    Raises ``ValueError`` if the resolved scope is empty -- either this
    host's local DuckDB file holds no rows for this partition at all, or
    (for a weekly partition) this exact partition was never synced/parsed
    locally. Never returns an empty list silently (see the 2026-07-19
    incident this design replaces).
    """
    if _is_backfill_partition_key(partition_key):
        rows = duckdb_connection.execute(
            f"select distinct source_archive_key "
            f"from {SWEDEN_FINANCIAL_DATASET_NAME}.reports"
        ).fetchall()
    else:
        rows = duckdb_connection.execute(
            f"""
            select distinct s3_key
            from {SWEDEN_FINANCIAL_DATASET_NAME}.archive_sync_catalog
            where load_partition_key = ?
              and sync_kind = 'current'
              and downloaded
            """,
            [partition_key],
        ).fetchall()
    archive_keys = sorted({str(row[0]) for row in rows})
    if not archive_keys:
        raise ValueError(
            "No Sweden financial archive keys found for partition "
            f"{partition_key!r}. The local DuckDB file holds no rows for "
            "this partition's scope -- materialize the matching parse asset "
            "for this exact partition on this host before exporting it (this "
            "host may simply not hold this partition's data, e.g. a "
            "different host completed this week's sync)."
        )
    return archive_keys


def _statement_keys_for_archive_keys(
    duckdb_connection: Any,
    archive_keys: Sequence[str],
) -> list[str]:
    """Map this partition's archive keys to their ``statement_key`` values.

    ``se_financial_facts`` has no ``source_archive_key`` column (only
    ``se_financial_reports`` and the derived ``se_financial_metrics`` do --
    see migrations 000090/000134); facts link back to their report via
    ``statement_key`` only (``se_financial_facts_with_source`` joins on it).
    So the facts export's delete/insert scope is expressed in
    ``statement_key``, derived here from the same local ``reports`` table
    used to resolve the archive scope.
    """
    if not archive_keys:
        return []
    placeholders = ", ".join("?" for _ in archive_keys)
    rows = duckdb_connection.execute(
        f"""
        select distinct statement_key
        from {SWEDEN_FINANCIAL_DATASET_NAME}.reports
        where source_archive_key in ({placeholders})
        """,
        list(archive_keys),
    ).fetchall()
    return sorted({str(row[0]) for row in rows})


def _count_and_delete_clickhouse_rows_by_key(
    client: Any,
    *,
    qualified_table: str,
    column: str,
    keys: Sequence[str],
) -> int:
    """Delete every row in ``qualified_table`` whose ``column`` is in ``keys``.

    Binds ``keys`` as a single ClickHouse ``Array(String)`` parameter
    (``IN %(keys)s``) -- never interpolated into the SQL text, per the
    Norway precedent (``financial_statements.py`` delete-by-key-set) and the
    ClickHouse ``IN``-list-size constraint. Uses ``mutations_sync = 1`` so
    the delete is visible before the subsequent insert. Returns the
    pre-delete row count in scope (the "deleted" metadata field) -- always
    0 for a brand-new archive, which is what makes re-running a partition
    idempotent.
    """
    if not keys:
        return 0
    key_array = tuple(keys)
    pre_count_rows = client.execute(
        f"SELECT count() FROM {qualified_table} WHERE {column} IN %(keys)s",
        {"keys": key_array},
    )
    pre_count = int(pre_count_rows[0][0])
    client.execute(
        f"ALTER TABLE {qualified_table} DELETE WHERE {column} IN %(keys)s "
        "SETTINGS mutations_sync = 1",
        {"keys": key_array},
    )
    return pre_count


def _quote_duckdb_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _duckdb_string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _insert_partition_scope_rows(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    duckdb_table: str,
    key_column: str,
    keys: Sequence[str],
    clickhouse_table: str,
    columns: Sequence[str],
    log: Callable[..., object] | None,
) -> int:
    """Insert only the rows of ``duckdb_table`` whose ``key_column`` is in
    ``keys`` into ``clickhouse_table``, reusing the shared batched-insert
    machinery (Arrow fast path with a row-based fallback) via a scoped
    DuckDB view rather than duplicating that logic.

    The view is created in DuckDB's ``temp`` schema so this works against a
    READ-ONLY connection (DuckDB's temporary catalog is independent of the
    on-disk file's access mode). ``CREATE VIEW`` cannot be prepared with
    bound parameters, so the key list is inlined as escaped SQL string
    literals -- safe here because the values are our own S3-object-key /
    hash strings, not external input.
    """
    if not keys:
        return 0
    view_name = f"_sweden_financial_export_scope_{uuid.uuid4().hex}"
    quoted_view_name = _quote_duckdb_identifier(view_name)
    values_sql = ", ".join(_duckdb_string_literal(key) for key in keys)
    duckdb_connection.execute(
        f"""
        create temp view {quoted_view_name} as
        select * from {SWEDEN_FINANCIAL_DATASET_NAME}.{duckdb_table}
        where {key_column} in ({values_sql})
        """
    )
    try:
        return export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema="temp",
            duckdb_table=view_name,
            clickhouse_database=SWEDEN_FINANCIAL_DATABASE,
            clickhouse_table=clickhouse_table,
            columns=columns,
            truncate=False,
            log=log,
        )
    finally:
        duckdb_connection.execute(f"drop view if exists {quoted_view_name}")


def upsert_sweden_financial_reports_partition(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    partition_key: str,
    log: Callable[..., object] | None = None,
) -> dict[str, str | int]:
    """Delete-and-insert-scoped export of one partition's Sweden financial
    report rows into ``corpscout.se_financial_reports``.

    Never touches rows outside this partition's own ``source_archive_key``
    scope -- cross-host and cross-partition safe by construction (the
    never-full-replace design that closes the 2026-07-19 wipe incident).
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(SE_FINANCIAL_REPORTS_TABLE,),
    )
    archive_keys = resolve_sweden_financial_partition_archive_keys(
        duckdb_connection, partition_key
    )
    if log is not None:
        log(
            "Upserting Sweden financial reports partition: partition=%s archives=%s",
            partition_key,
            len(archive_keys),
        )
    with clickhouse.get_connection() as client:
        deleted = _count_and_delete_clickhouse_rows_by_key(
            client,
            qualified_table=QUALIFIED_SE_FINANCIAL_REPORTS_TABLE,
            column="source_archive_key",
            keys=archive_keys,
        )
        inserted = _insert_partition_scope_rows(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_table="reports",
            key_column="source_archive_key",
            keys=archive_keys,
            clickhouse_table=SE_FINANCIAL_REPORTS_TABLE,
            columns=SE_FINANCIAL_REPORTS_EXPORT_COLUMNS,
            log=log,
        )
    if log is not None:
        log(
            "Finished Sweden financial reports partition upsert: partition=%s "
            "archives=%s deleted=%s inserted=%s",
            partition_key,
            len(archive_keys),
            deleted,
            inserted,
        )
    return {
        "partition": partition_key,
        "archives": len(archive_keys),
        "deleted": deleted,
        "inserted": inserted,
    }


def upsert_sweden_financial_facts_partition(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    partition_key: str,
    log: Callable[..., object] | None = None,
) -> dict[str, str | int]:
    """Delete-and-insert-scoped export of one partition's Sweden financial
    fact rows into ``corpscout.se_financial_facts``.

    Scoped by ``statement_key`` (derived from the partition's archive keys
    via the local ``reports`` table) rather than ``source_archive_key``
    directly -- ``se_financial_facts`` has no ``source_archive_key`` column.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(SE_FINANCIAL_FACTS_TABLE,),
    )
    archive_keys = resolve_sweden_financial_partition_archive_keys(
        duckdb_connection, partition_key
    )
    statement_keys = _statement_keys_for_archive_keys(duckdb_connection, archive_keys)
    if log is not None:
        log(
            "Upserting Sweden financial facts partition: partition=%s archives=%s "
            "statements=%s",
            partition_key,
            len(archive_keys),
            len(statement_keys),
        )
    with clickhouse.get_connection() as client:
        deleted = _count_and_delete_clickhouse_rows_by_key(
            client,
            qualified_table=QUALIFIED_SE_FINANCIAL_FACTS_TABLE,
            column="statement_key",
            keys=statement_keys,
        )
        inserted = _insert_partition_scope_rows(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_table="facts",
            key_column="statement_key",
            keys=statement_keys,
            clickhouse_table=SE_FINANCIAL_FACTS_TABLE,
            columns=SE_FINANCIAL_FACTS_EXPORT_COLUMNS,
            log=log,
        )
    if log is not None:
        log(
            "Finished Sweden financial facts partition upsert: partition=%s "
            "archives=%s statements=%s deleted=%s inserted=%s",
            partition_key,
            len(archive_keys),
            len(statement_keys),
            deleted,
            inserted,
        )
    return {
        "partition": partition_key,
        "archives": len(archive_keys),
        "deleted": deleted,
        "inserted": inserted,
    }
