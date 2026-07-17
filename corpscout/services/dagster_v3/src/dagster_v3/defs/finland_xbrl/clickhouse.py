from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
import json
from typing import Any
import uuid

import pyarrow as pa

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.finland_xbrl.resources import XbrlParquetStorageResource

CLICKHOUSE_DATABASE = "corpscout"
FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE = "fi_financial_statements"
FINANCIAL_METRICS_CLICKHOUSE_TABLE = "fi_financial_metrics"
XBRL_CONTEXTS_CLICKHOUSE_TABLE = "fi_xbrl_contexts"
XBRL_UNITS_CLICKHOUSE_TABLE = "fi_xbrl_units"
XBRL_FACTS_CLICKHOUSE_TABLE = "fi_xbrl_facts_raw"
XBRL_TAXONOMY_CLICKHOUSE_TABLE = "fi_xbrl_taxonomy_codes"
SOURCE_SYSTEM = "finland_prh_xbrl"
EUR_CURRENCY = "EUR"
DECIMAL_SCALE = Decimal("0.000001")
RATE_DECIMAL_SCALE = Decimal("0.000000000001")
EMPTY_SHA256 = "0" * 64

FINANCIAL_STATEMENTS_CLICKHOUSE_COLUMNS = (
    "statement_key",
    "business_id",
    "financial_date",
    "registration_date",
    "source_url",
    "xml_object_key",
    "xml_sha256",
    "xml_size_bytes",
    "root_name",
    "schema_refs",
    "taxonomy_entrypoint",
    "reported_business_id",
    "reported_company_name",
    "period_start",
    "period_end",
    "contexts_count",
    "units_count",
    "facts_count",
    "validation_warnings",
    "parser_version",
    "parsed_at",
    "source_system",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "resolved_at",
)

FINANCIAL_METRICS_CLICKHOUSE_COLUMNS = (
    "statement_key",
    "business_id",
    "financial_date",
    "registration_date",
    "period_start",
    "period_end",
    "reported_company_name",
    "source_url",
    "xml_object_key",
    "xml_sha256",
    "xml_size_bytes",
    "currency_original",
    "revenue_amount_original",
    "revenue_amount_usd",
    "operating_profit_loss_amount_original",
    "operating_profit_loss_amount_usd",
    "profit_loss_amount_original",
    "profit_loss_amount_usd",
    "total_assets_amount_original",
    "total_assets_amount_usd",
    "equity_amount_original",
    "equity_amount_usd",
    "liabilities_amount_original",
    "liabilities_amount_usd",
    "cash_and_bank_amount_original",
    "cash_and_bank_amount_usd",
    "current_assets_amount_original",
    "current_assets_amount_usd",
    "current_receivables_amount_original",
    "current_receivables_amount_usd",
    "current_liabilities_amount_original",
    "current_liabilities_amount_usd",
    "personnel_expenses_amount_original",
    "personnel_expenses_amount_usd",
    "wages_and_salaries_amount_original",
    "wages_and_salaries_amount_usd",
    "employees",
    "source_fact_count",
    "mapped_fact_count",
    "unmapped_numeric_fact_count",
    "metric_warnings",
    "mapping_version",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_converted_at",
    "source_system",
    "source_run_id",
    "source_record_id",
    "source_payload_hash",
    "resolved_at",
)

XBRL_CONTEXTS_CLICKHOUSE_COLUMNS = (
    "statement_key",
    "context_id",
    "entity_identifier",
    "entity_scheme",
    "period_type",
    "instant_date",
    "period_start",
    "period_end",
    "dimensions",
    "mcy_member_code",
    "mcy_member_label_fi",
    "ref_member_code",
    "ref_member_label_fi",
    "is_comparative",
    "raw_xml",
    "parser_version",
    "parsed_at",
)

XBRL_UNITS_CLICKHOUSE_COLUMNS = (
    "statement_key",
    "unit_id",
    "measures",
    "numerator_measures",
    "denominator_measures",
    "is_divide",
    "raw_xml",
    "parser_version",
    "parsed_at",
)

XBRL_FACTS_CLICKHOUSE_COLUMNS = (
    "statement_key",
    "business_id",
    "financial_date",
    "fact_ordinal",
    "concept_qname",
    "concept_namespace",
    "concept_local_name",
    "context_id",
    "unit_id",
    "currency",
    "decimals",
    "precision",
    "is_nil",
    "xml_lang",
    "value_kind",
    "raw_value",
    "numeric_value",
    "date_value",
    "text_value",
    "mcy_member_code",
    "mcy_member_label_fi",
    "ref_member_code",
    "ref_member_label_fi",
    "is_comparative",
    "dimensions",
    "parser_version",
    "parsed_at",
)

XBRL_TAXONOMY_CLICKHOUSE_COLUMNS = (
    "taxonomy_version",
    "code",
    "code_kind",
    "namespace_hint",
    "label_fi",
    "label_en",
    "label_sv",
    "metric_name_hint",
    "source_artifact",
    "source_url",
    "loaded_at",
)

FINANCIAL_METRICS_ARROW_SCHEMA = pa.schema(
    [
        pa.field("statement_key", pa.string()),
        pa.field("business_id", pa.string()),
        pa.field("financial_date", pa.date32()),
        pa.field("registration_date", pa.date32()),
        pa.field("period_start", pa.date32()),
        pa.field("period_end", pa.date32()),
        pa.field("reported_company_name", pa.string()),
        pa.field("source_url", pa.string()),
        pa.field("xml_object_key", pa.string()),
        pa.field("xml_sha256", pa.string()),
        pa.field("xml_size_bytes", pa.uint64()),
        pa.field("currency_original", pa.string()),
        pa.field("revenue_amount_original", pa.decimal128(38, 6)),
        pa.field("revenue_amount_usd", pa.decimal128(38, 6)),
        pa.field("operating_profit_loss_amount_original", pa.decimal128(38, 6)),
        pa.field("operating_profit_loss_amount_usd", pa.decimal128(38, 6)),
        pa.field("profit_loss_amount_original", pa.decimal128(38, 6)),
        pa.field("profit_loss_amount_usd", pa.decimal128(38, 6)),
        pa.field("total_assets_amount_original", pa.decimal128(38, 6)),
        pa.field("total_assets_amount_usd", pa.decimal128(38, 6)),
        pa.field("equity_amount_original", pa.decimal128(38, 6)),
        pa.field("equity_amount_usd", pa.decimal128(38, 6)),
        pa.field("liabilities_amount_original", pa.decimal128(38, 6)),
        pa.field("liabilities_amount_usd", pa.decimal128(38, 6)),
        pa.field("cash_and_bank_amount_original", pa.decimal128(38, 6)),
        pa.field("cash_and_bank_amount_usd", pa.decimal128(38, 6)),
        pa.field("current_assets_amount_original", pa.decimal128(38, 6)),
        pa.field("current_assets_amount_usd", pa.decimal128(38, 6)),
        pa.field("current_receivables_amount_original", pa.decimal128(38, 6)),
        pa.field("current_receivables_amount_usd", pa.decimal128(38, 6)),
        pa.field("current_liabilities_amount_original", pa.decimal128(38, 6)),
        pa.field("current_liabilities_amount_usd", pa.decimal128(38, 6)),
        pa.field("personnel_expenses_amount_original", pa.decimal128(38, 6)),
        pa.field("personnel_expenses_amount_usd", pa.decimal128(38, 6)),
        pa.field("wages_and_salaries_amount_original", pa.decimal128(38, 6)),
        pa.field("wages_and_salaries_amount_usd", pa.decimal128(38, 6)),
        pa.field("employees", pa.uint64()),
        pa.field("source_fact_count", pa.uint64()),
        pa.field("mapped_fact_count", pa.uint64()),
        pa.field("unmapped_numeric_fact_count", pa.uint64()),
        pa.field("metric_warnings", pa.string()),
        pa.field("mapping_version", pa.string()),
        pa.field("fx_rate_to_usd", pa.decimal128(38, 12)),
        pa.field("fx_rate_date", pa.date32()),
        pa.field("fx_converted_at", pa.timestamp("ms", tz="UTC")),
        pa.field("source_system", pa.string()),
        pa.field("source_run_id", pa.string()),
        pa.field("source_record_id", pa.string()),
        pa.field("source_payload_hash", pa.string()),
        pa.field("resolved_at", pa.timestamp("ms", tz="UTC")),
    ]
)

MONEY_METRIC_TO_CLICKHOUSE_COLUMN = {
    "revenue": "revenue_amount_original",
    "operating_profit_loss": "operating_profit_loss_amount_original",
    "profit_loss": "profit_loss_amount_original",
    "total_assets": "total_assets_amount_original",
    "equity": "equity_amount_original",
    "liabilities": "liabilities_amount_original",
    "cash_and_bank": "cash_and_bank_amount_original",
    "current_assets": "current_assets_amount_original",
    "current_receivables": "current_receivables_amount_original",
    "current_liabilities": "current_liabilities_amount_original",
    "personnel_expenses": "personnel_expenses_amount_original",
    "wages_and_salaries": "wages_and_salaries_amount_original",
}


def export_finland_xbrl_financial_statements_clickhouse(
    *,
    statement_documents: list[dict[str, Any]],
    clickhouse: Any,
    log: Callable[..., object] | None = None,
) -> int:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=CLICKHOUSE_DATABASE,
        tables=(FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE,),
    )
    rows = [_clickhouse_financial_statement_row(row) for row in statement_documents]

    if log is not None:
        log(
            "Exporting Finland XBRL financial statements to ClickHouse: table=%s.%s rows=%d",
            CLICKHOUSE_DATABASE,
            FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE,
            len(rows),
        )

    with clickhouse.get_connection() as client:
        _replace_clickhouse_table_with_rows(
            clickhouse_client=client,
            rows=rows,
            database=CLICKHOUSE_DATABASE,
            table=FINANCIAL_STATEMENTS_CLICKHOUSE_TABLE,
            columns=FINANCIAL_STATEMENTS_CLICKHOUSE_COLUMNS,
        )

    if log is not None:
        log(
            "Finished Finland XBRL financial statements ClickHouse export: rows=%d",
            len(rows),
        )
    return len(rows)


def _clickhouse_financial_statement_row(row: dict[str, Any]) -> dict[str, Any]:
    parsed_at = _datetime_value(row.get("parsed_at")) or datetime.now(UTC)
    xml_sha256 = _sha256_value(row.get("xml_sha256"))
    statement_key = str(row.get("statement_key") or "")
    return {
        "statement_key": statement_key,
        "business_id": str(row.get("business_id") or ""),
        "financial_date": _date_value(row.get("financial_date")),
        "registration_date": _date_value(row.get("registration_date")),
        "source_url": str(row.get("source_url") or ""),
        "xml_object_key": str(row.get("xml_object_key") or ""),
        "xml_sha256": xml_sha256,
        "xml_size_bytes": _uint_value(row.get("xml_size_bytes")) or 0,
        "root_name": str(row.get("root_name") or ""),
        "schema_refs": _json_string_list(row.get("schema_refs")),
        "taxonomy_entrypoint": str(row.get("taxonomy_entrypoint") or ""),
        "reported_business_id": _optional_string(row.get("reported_business_id")),
        "reported_company_name": _optional_string(row.get("reported_company_name")),
        "period_start": _date_value(row.get("reported_period_start")),
        "period_end": _date_value(row.get("reported_period_end")),
        "contexts_count": _uint_value(row.get("contexts_count")) or 0,
        "units_count": _uint_value(row.get("units_count")) or 0,
        "facts_count": _uint_value(row.get("facts_count")) or 0,
        "validation_warnings": str(row.get("validation_warnings") or "[]"),
        "parser_version": str(row.get("parser_version") or ""),
        "parsed_at": parsed_at,
        "source_system": SOURCE_SYSTEM,
        "source_run_id": str(row.get("source_run_id") or ""),
        "source_record_id": statement_key,
        "source_payload_hash": xml_sha256,
        "resolved_at": parsed_at,
    }


def clickhouse_financial_statement_row(row: dict[str, Any]) -> dict[str, Any]:
    return _clickhouse_financial_statement_row(row)


def clickhouse_context_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "statement_key": str(row.get("statement_key") or ""),
        "context_id": str(row.get("context_id") or ""),
        "entity_identifier": _optional_string(row.get("entity_identifier")),
        "entity_scheme": _optional_string(row.get("entity_scheme")),
        "period_type": str(row.get("period_type") or "none"),
        "instant_date": _date_value(row.get("instant_date")),
        "period_start": _date_value(row.get("period_start")),
        "period_end": _date_value(row.get("period_end")),
        "dimensions": _json_dimensions(row.get("dimensions")),
        "mcy_member_code": _optional_string(row.get("mcy_member_code")),
        "mcy_member_label_fi": _optional_string(row.get("mcy_member_label_fi")),
        "ref_member_code": _optional_string(row.get("ref_member_code")),
        "ref_member_label_fi": _optional_string(row.get("ref_member_label_fi")),
        "is_comparative": int(bool(row.get("is_comparative"))),
        "raw_xml": str(row.get("raw_xml") or ""),
        "parser_version": str(row.get("parser_version") or ""),
        "parsed_at": _datetime_value(row.get("parsed_at")) or datetime.now(UTC),
    }


def clickhouse_unit_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "statement_key": str(row.get("statement_key") or ""),
        "unit_id": str(row.get("unit_id") or ""),
        "measures": _json_string_list(row.get("measures")),
        "numerator_measures": _json_string_list(row.get("numerator_measures")),
        "denominator_measures": _json_string_list(row.get("denominator_measures")),
        "is_divide": int(bool(row.get("is_divide"))),
        "raw_xml": str(row.get("raw_xml") or ""),
        "parser_version": str(row.get("parser_version") or ""),
        "parsed_at": _datetime_value(row.get("parsed_at")) or datetime.now(UTC),
    }


def clickhouse_fact_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "statement_key": str(row.get("statement_key") or ""),
        "business_id": str(row.get("business_id") or ""),
        "financial_date": _date_value(row.get("financial_date")),
        "fact_ordinal": _uint_value(row.get("fact_ordinal")) or 0,
        "concept_qname": str(row.get("concept_qname") or ""),
        "concept_namespace": str(row.get("concept_namespace") or ""),
        "concept_local_name": str(row.get("concept_local_name") or ""),
        "context_id": str(row.get("context_id") or ""),
        "unit_id": _optional_string(row.get("unit_id")),
        "currency": _optional_string(row.get("currency")),
        "decimals": _optional_string(row.get("decimals")),
        "precision": _optional_string(row.get("precision")),
        "is_nil": int(bool(row.get("is_nil"))),
        "xml_lang": _optional_string(row.get("xml_lang")),
        "value_kind": str(row.get("value_kind") or "empty"),
        "raw_value": str(row.get("raw_value") or ""),
        "numeric_value": _unscaled_decimal_value(row.get("numeric_value")),
        "date_value": _date_value(row.get("date_value")),
        "text_value": _optional_string(row.get("text_value")),
        "mcy_member_code": _optional_string(row.get("mcy_member_code")),
        "mcy_member_label_fi": _optional_string(row.get("mcy_member_label_fi")),
        "ref_member_code": _optional_string(row.get("ref_member_code")),
        "ref_member_label_fi": _optional_string(row.get("ref_member_label_fi")),
        "is_comparative": int(bool(row.get("is_comparative"))),
        "dimensions": _json_dimensions(row.get("dimensions")),
        "parser_version": str(row.get("parser_version") or ""),
        "parsed_at": _datetime_value(row.get("parsed_at")) or datetime.now(UTC),
    }


def _replace_clickhouse_table_with_rows(
    *,
    clickhouse_client: Any,
    rows: list[dict[str, Any]],
    database: str,
    table: str,
    columns: tuple[str, ...],
) -> None:
    stage_table = f"{table}__stage_{uuid.uuid4().hex}"
    qualified_table = f"{database}.{table}"
    qualified_stage_table = f"{database}.{stage_table}"
    primary_error: Exception | None = None
    try:
        clickhouse_client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_table}"
        )
        if rows:
            clickhouse_client.execute(
                _insert_sql(qualified_stage_table, columns),
                [tuple(row[column] for column in columns) for row in rows],
            )
        clickhouse_client.execute(
            f"EXCHANGE TABLES {qualified_stage_table} AND {qualified_table}"
        )
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        try:
            clickhouse_client.execute(f"DROP TABLE IF EXISTS {qualified_stage_table}")
        except Exception:
            if primary_error is None:
                raise


def _insert_sql(qualified_table: str, columns: tuple[str, ...]) -> str:
    column_lines = "\n".join(f"            {column}," for column in columns).rstrip(",")
    return f"""
        INSERT INTO {qualified_table} (
{column_lines}
        ) VALUES
    """


def export_finland_xbrl_financial_metrics_clickhouse(
    *,
    xbrl_parquet_storage: XbrlParquetStorageResource,
    clickhouse: Any,
    log: Callable[..., object] | None = None,
) -> int:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=CLICKHOUSE_DATABASE,
        tables=(FINANCIAL_METRICS_CLICKHOUSE_TABLE,),
    )
    rows = xbrl_parquet_storage.read_financial_metrics_usd()
    arrow_table = financial_metrics_arrow_table(rows)

    if log is not None:
        log(
            "Exporting Finland XBRL financial metrics to ClickHouse: table=%s.%s rows=%d",
            CLICKHOUSE_DATABASE,
            FINANCIAL_METRICS_CLICKHOUSE_TABLE,
            arrow_table.num_rows,
        )

    with clickhouse.get_connection() as client:
        _replace_clickhouse_table_with_arrow(
            clickhouse_client=client,
            arrow_table=arrow_table,
            database=CLICKHOUSE_DATABASE,
            table=FINANCIAL_METRICS_CLICKHOUSE_TABLE,
        )

    if log is not None:
        log(
            "Finished Finland XBRL financial metrics ClickHouse export: rows=%d",
            arrow_table.num_rows,
        )
    return arrow_table.num_rows


def financial_metrics_arrow_table(rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(
        [_clickhouse_financial_metric_row(row) for row in rows],
        schema=FINANCIAL_METRICS_ARROW_SCHEMA,
    )


def _clickhouse_financial_metric_row(row: dict[str, Any]) -> dict[str, Any]:
    resolved_at = _datetime_value(row.get("resolved_at")) or datetime.now(UTC)
    clickhouse_row = {
        "statement_key": str(row.get("statement_key") or ""),
        "business_id": str(row.get("business_id") or ""),
        "financial_date": _date_value(row.get("financial_date")),
        "registration_date": _date_value(row.get("registration_date")),
        "period_start": _date_value(row.get("period_start")),
        "period_end": _date_value(row.get("period_end")),
        "reported_company_name": str(row.get("reported_company_name") or ""),
        "source_url": str(row.get("source_url") or ""),
        "xml_object_key": str(row.get("xml_object_key") or ""),
        "xml_sha256": _sha256_value(row.get("xml_sha256")),
        "xml_size_bytes": _uint_value(row.get("xml_size_bytes")),
        "currency_original": str(row.get("currency_original") or EUR_CURRENCY),
        "employees": _uint_value(row.get("employees")),
        "source_fact_count": _uint_value(row.get("source_fact_count")) or 0,
        "mapped_fact_count": _uint_value(row.get("mapped_fact_count")) or 0,
        "unmapped_numeric_fact_count": _uint_value(
            row.get("unmapped_numeric_fact_count")
        )
        or 0,
        "metric_warnings": str(row.get("metric_warnings") or "[]"),
        "mapping_version": str(row.get("mapping_version") or ""),
        "fx_rate_to_usd": _rate_decimal_value(row.get("fx_rate_to_usd")),
        "fx_rate_date": _date_value(row.get("fx_rate_date")),
        "fx_converted_at": _datetime_value(row.get("fx_converted_at")),
        "source_system": str(row.get("source_system") or SOURCE_SYSTEM),
        "source_run_id": str(row.get("source_run_id") or ""),
        "source_record_id": str(row.get("source_record_id") or row.get("statement_key") or ""),
        "source_payload_hash": _sha256_value(row.get("source_payload_hash")),
        "resolved_at": resolved_at,
    }
    for metric_name, clickhouse_column in MONEY_METRIC_TO_CLICKHOUSE_COLUMN.items():
        clickhouse_row[clickhouse_column] = _decimal_value(row.get(clickhouse_column))
        clickhouse_row[clickhouse_column.replace("_original", "_usd")] = _decimal_value(
            row.get(clickhouse_column.replace("_original", "_usd"))
        )
    return {
        column: clickhouse_row.get(column)
        for column in FINANCIAL_METRICS_CLICKHOUSE_COLUMNS
    }


def _replace_clickhouse_table_with_arrow(
    *,
    clickhouse_client: Any,
    arrow_table: pa.Table,
    database: str,
    table: str,
) -> None:
    stage_table = f"{table}__stage_{uuid.uuid4().hex}"
    qualified_table = f"{database}.{table}"
    qualified_stage_table = f"{database}.{stage_table}"
    primary_error: Exception | None = None
    try:
        clickhouse_client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_table}"
        )
        rows = _financial_metrics_insert_rows(arrow_table)
        if rows:
            clickhouse_client.execute(
                _financial_metrics_insert_sql(qualified_stage_table),
                rows,
            )
        clickhouse_client.execute(
            f"EXCHANGE TABLES {qualified_stage_table} AND {qualified_table}"
        )
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        try:
            clickhouse_client.execute(f"DROP TABLE IF EXISTS {qualified_stage_table}")
        except Exception:
            if primary_error is None:
                raise


def _financial_metrics_insert_sql(qualified_table: str) -> str:
    return f"""
        INSERT INTO {qualified_table} (
            statement_key,
            business_id,
            financial_date,
            registration_date,
            period_start,
            period_end,
            reported_company_name,
            source_url,
            xml_object_key,
            xml_sha256,
            xml_size_bytes,
            currency_original,
            revenue_amount_original,
            revenue_amount_usd,
            operating_profit_loss_amount_original,
            operating_profit_loss_amount_usd,
            profit_loss_amount_original,
            profit_loss_amount_usd,
            total_assets_amount_original,
            total_assets_amount_usd,
            equity_amount_original,
            equity_amount_usd,
            liabilities_amount_original,
            liabilities_amount_usd,
            cash_and_bank_amount_original,
            cash_and_bank_amount_usd,
            current_assets_amount_original,
            current_assets_amount_usd,
            current_receivables_amount_original,
            current_receivables_amount_usd,
            current_liabilities_amount_original,
            current_liabilities_amount_usd,
            personnel_expenses_amount_original,
            personnel_expenses_amount_usd,
            wages_and_salaries_amount_original,
            wages_and_salaries_amount_usd,
            employees,
            source_fact_count,
            mapped_fact_count,
            unmapped_numeric_fact_count,
            metric_warnings,
            mapping_version,
            fx_rate_to_usd,
            fx_rate_date,
            fx_converted_at,
            source_system,
            source_run_id,
            source_record_id,
            source_payload_hash,
            resolved_at
        ) VALUES
    """


def _financial_metrics_insert_rows(arrow_table: pa.Table) -> list[tuple[object, ...]]:
    return [
        (
            row["statement_key"],
            row["business_id"],
            row["financial_date"],
            row["registration_date"],
            row["period_start"],
            row["period_end"],
            row["reported_company_name"],
            row["source_url"],
            row["xml_object_key"],
            row["xml_sha256"],
            row["xml_size_bytes"],
            row["currency_original"],
            row["revenue_amount_original"],
            row["revenue_amount_usd"],
            row["operating_profit_loss_amount_original"],
            row["operating_profit_loss_amount_usd"],
            row["profit_loss_amount_original"],
            row["profit_loss_amount_usd"],
            row["total_assets_amount_original"],
            row["total_assets_amount_usd"],
            row["equity_amount_original"],
            row["equity_amount_usd"],
            row["liabilities_amount_original"],
            row["liabilities_amount_usd"],
            row["cash_and_bank_amount_original"],
            row["cash_and_bank_amount_usd"],
            row["current_assets_amount_original"],
            row["current_assets_amount_usd"],
            row["current_receivables_amount_original"],
            row["current_receivables_amount_usd"],
            row["current_liabilities_amount_original"],
            row["current_liabilities_amount_usd"],
            row["personnel_expenses_amount_original"],
            row["personnel_expenses_amount_usd"],
            row["wages_and_salaries_amount_original"],
            row["wages_and_salaries_amount_usd"],
            row["employees"],
            row["source_fact_count"],
            row["mapped_fact_count"],
            row["unmapped_numeric_fact_count"],
            row["metric_warnings"],
            row["mapping_version"],
            row["fx_rate_to_usd"],
            row["fx_rate_date"],
            row["fx_converted_at"],
            row["source_system"],
            row["source_run_id"],
            row["source_record_id"],
            row["source_payload_hash"],
            row["resolved_at"],
        )
        for row in arrow_table.to_pylist()
    ]


def _decimal_value(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value)).quantize(DECIMAL_SCALE)


def _unscaled_decimal_value(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _rate_decimal_value(value: object) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value)).quantize(RATE_DECIMAL_SCALE)


def _uint_value(value: object) -> int | None:
    if value is None or value == "":
        return None
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"Expected non-negative integer, got {value!r}")
    return parsed


def _date_value(value: object) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value))


def _datetime_value(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return _utc_datetime(value)
    return _utc_datetime(datetime.fromisoformat(str(value).replace("Z", "+00:00")))


def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _sha256_value(value: object) -> str:
    text = str(value or "")
    if len(text) == 64:
        return text
    return EMPTY_SHA256


def _optional_string(value: object) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def _json_string_list(value: object) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    parsed = json.loads(str(value))
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed]


def _json_dimensions(value: object) -> list[tuple[str, str, str | None]]:
    if value is None or value == "":
        return []
    parsed = value if isinstance(value, list) else json.loads(str(value))
    if not isinstance(parsed, list):
        return []
    return [
        (
            str(item[0]),
            str(item[1]),
            _optional_string(item[2]) if len(item) > 2 else None,
        )
        for item in parsed
        if isinstance(item, (list, tuple)) and len(item) >= 2
    ]
