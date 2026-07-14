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


def export_sweden_financial_reports_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
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
