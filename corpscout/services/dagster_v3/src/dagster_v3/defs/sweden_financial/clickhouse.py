from collections.abc import Callable
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
SHRINK_GUARD_MIN_RATIO = 0.5


def guard_against_clickhouse_table_shrink(
    *,
    qualified_table: str,
    existing_row_count: int,
    staged_row_count: int,
    allow_shrink: bool,
) -> None:
    """Refuse a full-table replace that would shrink ``qualified_table``.

    Applies to all three Sweden CH exporters (reports/facts/metrics): each
    stages its replacement data, then -- before the atomic
    ``EXCHANGE TABLES`` swap -- must compare the staged row count against
    the table's CURRENT (pre-swap) row count. If the target already holds
    rows and the staged replacement would leave it with less than
    ``SHRINK_GUARD_MIN_RATIO`` (50%) of that count, refuse unless the
    caller explicitly passes ``allow_shrink=True``. ``allow_shrink`` must
    never default to ``True`` anywhere -- it exists solely as an explicit,
    deliberate override for an operator who has confirmed the shrink is
    intentional (e.g. a genuine upstream retirement of data), threaded
    through from the asset's own run config, never hardcoded on.
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


def _duckdb_table_row_count(
    duckdb_connection: Any,
    duckdb_schema: str,
    duckdb_table: str,
) -> int:
    row = duckdb_connection.execute(
        f'select count(*) from "{duckdb_schema}"."{duckdb_table}"'
    ).fetchone()
    return int(row[0])


def export_sweden_financial_reports_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
    allow_shrink: bool = False,
) -> int:
    """Replace corpscout.se_financial_reports from parsed Sweden financial reports."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(SE_FINANCIAL_REPORTS_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Sweden financial reports to ClickHouse: table=%s",
            QUALIFIED_SE_FINANCIAL_REPORTS_TABLE,
        )
    with clickhouse.get_connection() as client:
        existing_row_count = clickhouse_table_row_count(
            client, f"`{SWEDEN_FINANCIAL_DATABASE}`.`{SE_FINANCIAL_REPORTS_TABLE}`"
        )
        staged_row_count = _duckdb_table_row_count(
            duckdb_connection, SWEDEN_FINANCIAL_DATASET_NAME, "reports"
        )
        guard_against_clickhouse_table_shrink(
            qualified_table=QUALIFIED_SE_FINANCIAL_REPORTS_TABLE,
            existing_row_count=existing_row_count,
            staged_row_count=staged_row_count,
            allow_shrink=allow_shrink,
        )
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=SWEDEN_FINANCIAL_DATASET_NAME,
            duckdb_table="reports",
            clickhouse_database=SWEDEN_FINANCIAL_DATABASE,
            clickhouse_table=SE_FINANCIAL_REPORTS_TABLE,
            columns=SE_FINANCIAL_REPORTS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Sweden financial reports ClickHouse export: rows=%s", rows)
    return rows


def export_sweden_financial_facts_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
    allow_shrink: bool = False,
) -> int:
    """Replace corpscout.se_financial_facts from parsed Sweden financial facts."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(SE_FINANCIAL_FACTS_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Sweden financial facts to ClickHouse: table=%s",
            QUALIFIED_SE_FINANCIAL_FACTS_TABLE,
        )
    with clickhouse.get_connection() as client:
        existing_row_count = clickhouse_table_row_count(
            client, f"`{SWEDEN_FINANCIAL_DATABASE}`.`{SE_FINANCIAL_FACTS_TABLE}`"
        )
        staged_row_count = _duckdb_table_row_count(
            duckdb_connection, SWEDEN_FINANCIAL_DATASET_NAME, "facts"
        )
        guard_against_clickhouse_table_shrink(
            qualified_table=QUALIFIED_SE_FINANCIAL_FACTS_TABLE,
            existing_row_count=existing_row_count,
            staged_row_count=staged_row_count,
            allow_shrink=allow_shrink,
        )
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=SWEDEN_FINANCIAL_DATASET_NAME,
            duckdb_table="facts",
            clickhouse_database=SWEDEN_FINANCIAL_DATABASE,
            clickhouse_table=SE_FINANCIAL_FACTS_TABLE,
            columns=SE_FINANCIAL_FACTS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Sweden financial facts ClickHouse export: rows=%s", rows)
    return rows
