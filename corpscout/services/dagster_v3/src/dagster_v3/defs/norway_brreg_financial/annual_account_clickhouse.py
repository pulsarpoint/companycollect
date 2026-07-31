from __future__ import annotations

from collections.abc import Callable, Sequence
import re
from typing import Any
import uuid

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.norway_brreg_financial.annual_account_financials import (
    ANNUAL_ACCOUNT_DATASET,
    METRIC_NAMES,
)

CLICKHOUSE_DATABASE = "corpscout"
REPORTS_TABLE = "no_financial_reports"
FACTS_TABLE = "no_financial_facts"
METRICS_TABLE = "no_financial_metrics"

REPORT_COLUMNS = (
    "document_id",
    "country_iso2",
    "source_slug",
    "source_run_id",
    "org_number",
    "legal_name",
    "source_filing_year",
    "source_chunk",
    "source_json_object_key",
    "source_json_uri",
    "source_json_sha256",
    "source_file_name",
    "source_pdf_url",
    "source_pdf_sha256",
    "source_pdf_size_bytes",
    "retrieved_at",
    "pdf_page_count",
    "native_text_page_count",
    "ocr_page_count",
    "parse_status",
    "parse_warnings",
    "fact_count",
    "parser_version",
    "resolved_at",
)

FACT_COLUMNS = (
    "fact_id",
    "document_id",
    "country_iso2",
    "source_slug",
    "source_file_name",
    "source_url",
    "source_run_id",
    "org_number",
    "source_filing_year",
    "source_chunk",
    "fact_ordinal",
    "page_number",
    "line_number",
    "statement_type",
    "table_title",
    "raw_label",
    "normalized_label",
    "canonical_concept",
    "column_label",
    "fiscal_year",
    "period_end_date",
    "is_comparative",
    "value_kind",
    "raw_value",
    "numeric_value",
    "currency",
    "unit_scale",
    "amount_original",
    "amount_usd",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "bbox",
    "evidence",
    "ocr_confidence",
    "extraction_method",
    "mapping_method",
    "mapping_confidence",
    "quality_flags",
    "source_json_sha256",
    "parser_version",
    "resolved_at",
)

METRIC_AMOUNT_COLUMNS = tuple(
    column
    for metric in METRIC_NAMES
    for column in (f"{metric}_amount_original", f"{metric}_amount_usd")
)
METRIC_COLUMNS = (
    "metric_id",
    "document_id",
    "country_iso2",
    "source_slug",
    "source_file_name",
    "source_run_id",
    "org_number",
    "legal_name",
    "source_filing_year",
    "source_chunk",
    "fiscal_year",
    "period_end_date",
    "is_comparative",
    "currency",
    *METRIC_AMOUNT_COLUMNS,
    "source_fact_count",
    "mapped_fact_count",
    "unmapped_numeric_fact_count",
    "validation_status",
    "metric_warnings",
    "source_fact_ids",
    "mapping_version",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "source_pdf_url",
    "source_json_uri",
    "source_json_sha256",
    "resolved_at",
)


def publish_annual_account_partition(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    duckdb_table: str,
    clickhouse_table: str,
    columns: Sequence[str],
    filing_year: int,
    chunk_key: str,
    log: Callable[..., object] | None,
) -> int:
    if not re.fullmatch(r"bucket_\d{2}", chunk_key):
        raise ValueError(f"Invalid annual-account chunk key: {chunk_key}")
    assert_clickhouse_tables_exist(
        clickhouse,
        database=CLICKHOUSE_DATABASE,
        tables=(clickhouse_table,),
    )
    export_table = f"_annual_account_{duckdb_table}_export"
    duckdb_connection.execute(
        f"""
        create or replace temp table {export_table} as
        select {", ".join(columns)}
        from {ANNUAL_ACCOUNT_DATASET}.{duckdb_table}
        where source_filing_year = ? and source_chunk = ?
        """,
        [filing_year, chunk_key],
    )
    row_count = int(
        duckdb_connection.execute(f"select count(*) from {export_table}").fetchone()[0]
    )
    if duckdb_table == "documents" and row_count == 0:
        raise ValueError(
            f"No Norway annual-account documents for {filing_year}/{chunk_key}; "
            "refusing ClickHouse publish"
        )

    stage_table = f"{clickhouse_table}__stage_{uuid.uuid4().hex}"
    qualified_target = f"`{CLICKHOUSE_DATABASE}`.`{clickhouse_table}`"
    qualified_stage = f"`{CLICKHOUSE_DATABASE}`.`{stage_table}`"
    partition_expression = f"tuple({filing_year}, '{chunk_key}')"
    with clickhouse.get_connection() as client:
        if row_count == 0:
            existing = int(
                client.execute(
                    f"select count() from {qualified_target} "
                    "where source_filing_year = %(filing_year)s "
                    "and source_chunk = %(chunk_key)s",
                    {"filing_year": filing_year, "chunk_key": chunk_key},
                )[0][0]
            )
            if existing:
                client.execute(
                    f"alter table {qualified_target} delete where "
                    "source_filing_year = %(filing_year)s and source_chunk = %(chunk_key)s "
                    "settings mutations_sync = 2",
                    {"filing_year": filing_year, "chunk_key": chunk_key},
                )
            return 0

        client.execute(f"create table {qualified_stage} as {qualified_target}")
        primary_error: Exception | None = None
        try:
            inserted = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=client,
                duckdb_schema="temp",
                duckdb_table=export_table,
                clickhouse_database=CLICKHOUSE_DATABASE,
                clickhouse_table=stage_table,
                columns=columns,
                truncate=False,
                log=log,
            )
            if inserted != row_count:
                raise RuntimeError(
                    f"ClickHouse stage row-count mismatch for {clickhouse_table}: "
                    f"expected={row_count} inserted={inserted}"
                )
            client.execute(
                f"alter table {qualified_target} replace partition "
                f"{partition_expression} from {qualified_stage}"
            )
        except Exception as error:
            primary_error = error
            raise
        finally:
            try:
                client.execute(f"drop table if exists {qualified_stage}")
            except Exception:
                if primary_error is None:
                    raise
    return row_count
