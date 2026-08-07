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
    "context_period_start",
    "context_period_end",
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


def resolve_sweden_financial_year_archive_keys(
    duckdb_connection: Any,
) -> list[str]:
    """Every distinct ``source_archive_key`` in the local year file's
    ``reports`` table -- the backfill export scope ("the whole file"; the
    parse layer replaces the entire year file for backfill partitions,
    ``replace_scope="partition"`` in ``parsing.py``).

    The weekly current exports do NOT use this: they are reconcilers
    (``reconcile_sweden_financial_reports_clickhouse`` /
    ``..._facts_...``) that diff the local file against ClickHouse and
    need no partition-scope bookkeeping at all (the 2026-07-20
    order-independence design).

    Raises ``ValueError`` if the file holds no rows at all (corruption /
    parse never ran -- never export nothing silently).
    """
    rows = duckdb_connection.execute(
        f"select distinct source_archive_key "
        f"from {SWEDEN_FINANCIAL_DATASET_NAME}.reports"
    ).fetchall()
    archive_keys = sorted({str(row[0]) for row in rows})
    if not archive_keys:
        raise ValueError(
            "No Sweden financial archive keys found in the local year file. "
            "The reports table is empty -- materialize the matching parse "
            "asset on this host before exporting."
        )
    return archive_keys


def _local_report_counts_by_archive(duckdb_connection: Any) -> dict[str, int]:
    rows = duckdb_connection.execute(
        f"select source_archive_key, count(*) "
        f"from {SWEDEN_FINANCIAL_DATASET_NAME}.reports group by 1"
    ).fetchall()
    return {str(key): int(count) for key, count in rows}


def _local_facts_counts_by_archive(
    duckdb_connection: Any,
) -> dict[str, tuple[int, int, int]]:
    rows = duckdb_connection.execute(
        f"""
        select r.source_archive_key,
               count(*),
               count(f.amount_usd),
               count(f.context_period_end)
        from {SWEDEN_FINANCIAL_DATASET_NAME}.facts f
        join {SWEDEN_FINANCIAL_DATASET_NAME}.reports r using (statement_key)
        group by 1
        """
    ).fetchall()
    return {
        str(key): (int(fact_count), int(usd_count), int(context_period_count))
        for key, fact_count, usd_count, context_period_count in rows
    }


def _diff_against_clickhouse_counts(
    *,
    local_counts: dict[str, int],
    clickhouse_rows: Sequence[tuple[Any, Any]],
) -> list[str]:
    clickhouse_counts = {str(key): int(count) for key, count in clickhouse_rows}
    return sorted(
        key
        for key, count in local_counts.items()
        if clickhouse_counts.get(key, 0) != count
    )


def _diff_facts_against_clickhouse_counts(
    *,
    local_counts: dict[str, tuple[int, int, int]],
    clickhouse_rows: Sequence[tuple[Any, Any, Any, Any]],
) -> list[str]:
    clickhouse_counts = {
        str(key): (
            int(fact_count),
            int(usd_count),
            int(context_period_count),
        )
        for key, fact_count, usd_count, context_period_count in clickhouse_rows
    }
    return sorted(
        key
        for key, counts in local_counts.items()
        if clickhouse_counts.get(key, (0, 0, 0)) != counts
    )


def _require_nonempty_local_reports(local_counts: dict[str, int]) -> None:
    if not local_counts:
        raise ValueError(
            "Sweden financial year file has zero report rows; refusing to "
            "reconcile from an empty local file -- re-materialize the parse "
            "assets on this host first."
        )


def resolve_unreconciled_report_archive_keys(
    duckdb_connection: Any,
    clickhouse_client: Any,
    *,
    log: Callable[..., object] | None = None,
) -> list[str]:
    """Archives whose local ``reports`` row count differs from ClickHouse.

    The order-independence mechanism (2026-07-20 design): the weekly export
    carries NO bookkeeping of its own -- it diffs the local year file
    against the target table and upserts exactly the difference, so a
    yearly rebuild (which replaces the whole year file) can never strand
    it. An empty diff is the legitimate "ClickHouse already has
    everything" no-op.
    """
    local_counts = _local_report_counts_by_archive(duckdb_connection)
    _require_nonempty_local_reports(local_counts)
    clickhouse_rows = clickhouse_client.execute(
        f"SELECT source_archive_key, count() "
        f"FROM {QUALIFIED_SE_FINANCIAL_REPORTS_TABLE} "
        f"WHERE source_archive_key IN %(keys)s GROUP BY source_archive_key",
        {"keys": tuple(sorted(local_counts))},
    )
    keys = _diff_against_clickhouse_counts(
        local_counts=local_counts, clickhouse_rows=clickhouse_rows
    )
    if log is not None:
        log(
            "Sweden reports reconcile scope: local_archives=%s unreconciled=%s",
            len(local_counts),
            len(keys),
        )
    return keys


def resolve_unreconciled_facts_archive_keys(
    duckdb_connection: Any,
    clickhouse_client: Any,
    *,
    log: Callable[..., object] | None = None,
) -> list[str]:
    """Archives whose per-archive fact count or USD-populated count differs
    from ClickHouse, joined through ``statement_key`` on both sides. The USD
    count makes the first currency-enrichment rollout republish active-year
    rows whose raw fact count already matched but whose FX columns were null.
    This is computed independently of the reports diff so a facts-only gap
    is still found.

    Diffs over the full local REPORT archive universe with the facts count
    defaulted to 0: an archive with reports but zero extracted facts is a
    clean 0 == 0 no-op when ClickHouse agrees, but ENTERS the scope when
    ClickHouse still holds stale facts for it (e.g. a re-parse now yields
    zero facts where an earlier export inserted some) -- the upsert then
    deletes those stale rows and inserts nothing. Facts whose statement
    keys vanished from the local file entirely remain unreachable
    (pre-existing limitation shared with the backfill path; a backfill
    export of the year cleans them).
    """
    report_counts = _local_report_counts_by_archive(duckdb_connection)
    _require_nonempty_local_reports(report_counts)
    facts_counts = _local_facts_counts_by_archive(duckdb_connection)
    local_counts = {key: facts_counts.get(key, (0, 0, 0)) for key in report_counts}
    clickhouse_rows = clickhouse_client.execute(
        f"SELECT r.source_archive_key, count(), "
        f"countIf(f.amount_usd IS NOT NULL), "
        f"countIf(f.context_period_end IS NOT NULL) "
        f"FROM {QUALIFIED_SE_FINANCIAL_FACTS_TABLE} AS f "
        f"INNER JOIN {QUALIFIED_SE_FINANCIAL_REPORTS_TABLE} AS r "
        f"ON f.statement_key = r.statement_key "
        f"WHERE r.source_archive_key IN %(keys)s "
        f"GROUP BY r.source_archive_key",
        {"keys": tuple(sorted(report_counts))},
    )
    keys = _diff_facts_against_clickhouse_counts(
        local_counts=local_counts, clickhouse_rows=clickhouse_rows
    )
    if log is not None:
        log(
            "Sweden facts reconcile scope: local_archives=%s unreconciled=%s",
            len(local_counts),
            len(keys),
        )
    return keys


def _statement_keys_for_archive_keys(
    duckdb_connection: Any,
    archive_keys: Sequence[str],
) -> list[str]:
    """Map this partition's archive keys to their ``statement_key`` values.

    ``se_financial_facts`` has no ``source_archive_key`` column (only
    ``se_financial_reports`` and the derived ``se_financial_metrics`` do --
    see migrations 000090/000134); facts link back to their report via
    ``statement_key`` only (``se_financial_facts_with_source`` joins on it).
    So the facts export's ClickHouse delete scope is expressed in
    ``statement_key``, derived here from the same local ``reports`` table
    used to resolve the archive scope. ``archive_keys`` is never empty
    (``resolve_sweden_financial_year_archive_keys`` raises first, and the
    reconcilers return early on an empty diff before calling this).
    """
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

    Reports-scale scopes ONLY (a partition's archive keys, at most a few
    hundred strings): binds ``keys`` as a single ClickHouse
    ``Array(String)`` parameter (``IN %(keys)s``) -- never interpolated into
    the SQL text by us. Note clickhouse-driver substitutes params
    CLIENT-side, so the key set still travels inside the query text and is
    subject to the server's ``max_query_size`` (256 KiB default) -- fine for
    archive-key scopes (~20 KB), which is exactly why the facts path uses
    the stage-table mechanism instead
    (``_count_and_delete_facts_rows_via_scope_stage``).

    Skips the ALTER DELETE mutation entirely when the pre-count is 0 (the
    steady-state brand-new-archive case: a pure insert). Uses
    ``mutations_sync = 1`` so the delete is visible before the subsequent
    insert. Returns the pre-delete row count in scope (the "deleted"
    metadata field), which is what makes re-running a partition idempotent.
    """
    key_array = tuple(keys)
    pre_count_rows = client.execute(
        f"SELECT count() FROM {qualified_table} WHERE {column} IN %(keys)s",
        {"keys": key_array},
    )
    pre_count = int(pre_count_rows[0][0])
    if pre_count > 0:
        client.execute(
            f"ALTER TABLE {qualified_table} DELETE WHERE {column} IN %(keys)s "
            "SETTINGS mutations_sync = 1",
            {"keys": key_array},
        )
    return pre_count


SCOPE_STAGE_INSERT_BATCH_SIZE = 50_000


def _count_and_delete_facts_rows_via_scope_stage(
    client: Any,
    statement_keys: Sequence[str],
) -> int:
    """Delete this partition's ``corpscout.se_financial_facts`` rows by
    staging the ``statement_key`` scope in a per-run Memory table.

    A backfill year's facts scope is ~98k-498k 64-char statement keys.
    Binding that as an ``Array(String)`` param (the reports approach) does
    NOT keep it out of the query text -- clickhouse-driver substitutes
    params client-side, producing 6-33 MB of SQL vs the server's default
    ``max_query_size`` of 262,144 bytes. So, per the Norway precedent
    (``norway_brreg_financial/assets/financial_statements.py`` update
    path), the keys are batch-INSERTed into a per-run ``ENGINE = Memory``
    stage table as native-protocol data blocks (data blocks bypass
    ``max_query_size`` entirely), and the pre-count and DELETE run as
    ``statement_key IN (SELECT statement_key FROM <stage>)``.

    Skips the ALTER DELETE mutation entirely when the pre-count is 0 (the
    steady-state brand-new-archive case: a pure insert). Uses
    ``mutations_sync = 1`` so the delete is visible before the subsequent
    insert. The stage table is dropped in all cases. Returns the pre-delete
    in-scope row count (the "deleted" metadata field).
    """
    stage_table = f"_tmp_se_facts_scope_{uuid.uuid4().hex}"
    qualified_stage_table = f"{SWEDEN_FINANCIAL_DATABASE}.{stage_table}"
    client.execute(
        f"CREATE TABLE {qualified_stage_table} (statement_key String) ENGINE = Memory"
    )
    primary_error: Exception | None = None
    try:
        rows = [(key,) for key in statement_keys]
        for start in range(0, len(rows), SCOPE_STAGE_INSERT_BATCH_SIZE):
            client.execute(
                f"INSERT INTO {qualified_stage_table} (statement_key) VALUES",
                rows[start : start + SCOPE_STAGE_INSERT_BATCH_SIZE],
            )
        scope_subquery = f"(SELECT statement_key FROM {qualified_stage_table})"
        pre_count_rows = client.execute(
            f"SELECT count() FROM {QUALIFIED_SE_FINANCIAL_FACTS_TABLE} "
            f"WHERE statement_key IN {scope_subquery}"
        )
        pre_count = int(pre_count_rows[0][0])
        if pre_count > 0:
            client.execute(
                f"ALTER TABLE {QUALIFIED_SE_FINANCIAL_FACTS_TABLE} "
                f"DELETE WHERE statement_key IN {scope_subquery} "
                "SETTINGS mutations_sync = 1"
            )
        return pre_count
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        try:
            client.execute(f"DROP TABLE IF EXISTS {qualified_stage_table}")
        except Exception:
            if primary_error is None:
                raise


def _quote_duckdb_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _duckdb_string_literal(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _archive_keys_in_sql(archive_keys: Sequence[str]) -> str:
    return ", ".join(_duckdb_string_literal(key) for key in archive_keys)


def _reports_scope_where_sql(archive_keys: Sequence[str]) -> str:
    return f"source_archive_key in ({_archive_keys_in_sql(archive_keys)})"


def _facts_scope_where_sql(archive_keys: Sequence[str]) -> str:
    """DuckDB-side facts filter for an archive-scoped (reconcile) export:
    facts joined through the local ``reports`` table by archive keys (at
    most a few hundred inlined literals) -- never by inlined statement
    keys (a year's ~98k-498k statement keys as literals would recreate
    the oversized-query problem on the DuckDB side). The backfill upsert
    passes ``where_sql=None`` instead: its scope IS the whole local
    ``facts`` table."""
    return (
        "statement_key in (select statement_key "
        f"from {SWEDEN_FINANCIAL_DATASET_NAME}.reports "
        f"where source_archive_key in ({_archive_keys_in_sql(archive_keys)}))"
    )


def _insert_partition_scope_rows(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    duckdb_table: str,
    where_sql: str | None,
    clickhouse_table: str,
    columns: Sequence[str],
    log: Callable[..., object] | None,
) -> int:
    """Insert the rows of ``duckdb_table`` matching ``where_sql`` (the whole
    table when ``None``) into ``clickhouse_table``, reusing the shared
    batched-insert machinery (Arrow fast path with a row-based fallback) via
    a scoped DuckDB view rather than duplicating that logic.

    The view is created in DuckDB's ``temp`` schema so this works against a
    READ-ONLY connection (DuckDB's temporary catalog is independent of the
    on-disk file's access mode). ``CREATE VIEW`` cannot be prepared with
    bound parameters, so ``where_sql`` inlines its archive keys as escaped
    SQL string literals -- safe here because the values are our own
    S3-object-key strings, not external input, and archive-key-scale only
    (statement keys are never inlined; see ``_facts_scope_where_sql``).
    """
    if where_sql is None:
        return export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=SWEDEN_FINANCIAL_DATASET_NAME,
            duckdb_table=duckdb_table,
            clickhouse_database=SWEDEN_FINANCIAL_DATABASE,
            clickhouse_table=clickhouse_table,
            columns=columns,
            truncate=False,
            log=log,
        )
    view_name = f"_sweden_financial_export_scope_{uuid.uuid4().hex}"
    quoted_view_name = _quote_duckdb_identifier(view_name)
    duckdb_connection.execute(
        f"""
        create temp view {quoted_view_name} as
        select * from {SWEDEN_FINANCIAL_DATASET_NAME}.{duckdb_table}
        where {where_sql}
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
    """Delete-and-insert-scoped export of one backfill year's Sweden
    financial report rows into ``corpscout.se_financial_reports``.

    The scope is the year file's full archive set. Never touches rows
    outside that scope -- cross-host and cross-year safe by construction
    (the never-full-replace design that closes the 2026-07-19 wipe
    incident).
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(SE_FINANCIAL_REPORTS_TABLE,),
    )
    archive_keys = resolve_sweden_financial_year_archive_keys(duckdb_connection)
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
            where_sql=_reports_scope_where_sql(archive_keys),
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
    """Delete-and-insert-scoped export of one backfill year's Sweden
    financial fact rows into ``corpscout.se_financial_facts``.

    Scoped by ``statement_key`` (derived from the year's archive keys via
    the local ``reports`` table) rather than ``source_archive_key``
    directly -- ``se_financial_facts`` has no ``source_archive_key`` column.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(SE_FINANCIAL_FACTS_TABLE,),
    )
    archive_keys = resolve_sweden_financial_year_archive_keys(duckdb_connection)
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
        deleted = _count_and_delete_facts_rows_via_scope_stage(
            client,
            statement_keys,
        )
        inserted = _insert_partition_scope_rows(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_table="facts",
            where_sql=None,
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


_RECONCILE_NOOP_METADATA: dict[str, str | int] = {
    "archives": 0,
    "deleted": 0,
    "inserted": 0,
    "skipped_reason": "clickhouse already matches the local year file",
}


def reconcile_sweden_financial_reports_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, str | int]:
    """Order-independent weekly export: upsert exactly the archives whose
    local ``reports`` rows are missing or count-mismatched in ClickHouse.

    A yearly rebuild cannot strand this -- there is no bookkeeping to
    lose; the diff is recomputed from data every run (the 2026-07-20
    design closing the 2026-07-18 wiped-ledger incident).
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(SE_FINANCIAL_REPORTS_TABLE,),
    )
    with clickhouse.get_connection() as client:
        archive_keys = resolve_unreconciled_report_archive_keys(
            duckdb_connection, client, log=log
        )
        if not archive_keys:
            return dict(_RECONCILE_NOOP_METADATA)
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
            where_sql=_reports_scope_where_sql(archive_keys),
            clickhouse_table=SE_FINANCIAL_REPORTS_TABLE,
            columns=SE_FINANCIAL_REPORTS_EXPORT_COLUMNS,
            log=log,
        )
    if log is not None:
        log(
            "Reconciled Sweden financial reports: archives=%s deleted=%s inserted=%s",
            len(archive_keys),
            deleted,
            inserted,
        )
    return {"archives": len(archive_keys), "deleted": deleted, "inserted": inserted}


def reconcile_sweden_financial_facts_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, str | int]:
    """Facts twin of ``reconcile_sweden_financial_reports_clickhouse``,
    diffing fact and USD-populated counts per archive (via ``statement_key``)
    and deleting through the Memory stage-table mechanism (statement-key
    scopes can exceed ``max_query_size`` as Array params)."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(SE_FINANCIAL_FACTS_TABLE,),
    )
    with clickhouse.get_connection() as client:
        archive_keys = resolve_unreconciled_facts_archive_keys(
            duckdb_connection, client, log=log
        )
        if not archive_keys:
            return dict(_RECONCILE_NOOP_METADATA)
        statement_keys = _statement_keys_for_archive_keys(
            duckdb_connection, archive_keys
        )
        deleted = _count_and_delete_facts_rows_via_scope_stage(
            client,
            statement_keys,
        )
        inserted = _insert_partition_scope_rows(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_table="facts",
            where_sql=_facts_scope_where_sql(archive_keys),
            clickhouse_table=SE_FINANCIAL_FACTS_TABLE,
            columns=SE_FINANCIAL_FACTS_EXPORT_COLUMNS,
            log=log,
        )
    if log is not None:
        log(
            "Reconciled Sweden financial facts: archives=%s deleted=%s inserted=%s",
            len(archive_keys),
            deleted,
            inserted,
        )
    return {"archives": len(archive_keys), "deleted": deleted, "inserted": inserted}
