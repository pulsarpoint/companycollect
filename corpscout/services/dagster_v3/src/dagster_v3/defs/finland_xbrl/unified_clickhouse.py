"""ClickHouse export for the Finland unified XBRL `_next` tables.

Modeled on `finland_xbrl/clickhouse.py`'s row-converter + stage/`EXCHANGE
TABLES` replace pattern, adapted for the unified `XbrlRowContract` column
order (see `unified_adapter.FINLAND_UNIFIED_CONTRACT`) and batched inserts
(50_000 rows/batch) for the larger unified fact volume — the legacy helper
inserts all rows in a single `execute` call, which does not scale here.
"""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
import uuid

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.finland_xbrl.clickhouse import CLICKHOUSE_DATABASE

INSERT_BATCH_SIZE = 50_000
DECIMAL_SCALE = Decimal("0.000001")

UNIFIED_DOCUMENTS_CLICKHOUSE_TABLE = "fi_xbrl_documents_next"
UNIFIED_CONTEXTS_CLICKHOUSE_TABLE = "fi_xbrl_contexts_next"
UNIFIED_UNITS_CLICKHOUSE_TABLE = "fi_xbrl_units_next"
UNIFIED_FACTS_CLICKHOUSE_TABLE = "fi_xbrl_facts_next"

UNIFIED_DOCUMENTS_CLICKHOUSE_COLUMNS = (
    "statement_key",
    "source_run_id",
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
    "reported_entity_id",
    "reported_company_name",
    "reported_period_start",
    "reported_period_end",
    "contexts_count",
    "units_count",
    "facts_count",
    "validation_warnings",
    "parser_version",
    "parsed_at",
)

UNIFIED_CONTEXTS_CLICKHOUSE_COLUMNS = (
    "statement_key",
    "context_id",
    "entity_identifier",
    "entity_scheme",
    "period_type",
    "instant_date",
    "period_start",
    "period_end",
    "dimensions",
    "is_comparative",
    "parser_version",
    "parsed_at",
    "mcy_member_code",
    "ref_member_code",
)

UNIFIED_UNITS_CLICKHOUSE_COLUMNS = (
    "statement_key",
    "unit_id",
    "measures",
    "numerator_measures",
    "denominator_measures",
    "is_divide",
    "currency",
    "parser_version",
    "parsed_at",
)

UNIFIED_FACTS_CLICKHOUSE_COLUMNS = (
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
    "dimensions",
    "is_comparative",
    "parser_version",
    "parsed_at",
    "mcy_member_code",
    "ref_member_code",
)


def unified_document_row(row: dict[str, Any]) -> tuple[Any, ...]:
    converted = {
        "statement_key": _text(row.get("statement_key")),
        "source_run_id": _text(row.get("source_run_id")),
        "business_id": _text(row.get("business_id")),
        "financial_date": _required_date(row.get("financial_date")),
        "registration_date": _date(row.get("registration_date")),
        "source_url": _text(row.get("source_url")),
        "xml_object_key": _text(row.get("xml_object_key")),
        "xml_sha256": _text(row.get("xml_sha256")),
        "xml_size_bytes": _uint(row.get("xml_size_bytes")),
        "root_name": _text(row.get("root_name")),
        "schema_refs": _text(row.get("schema_refs")),
        "taxonomy_entrypoint": _text(row.get("taxonomy_entrypoint")),
        "reported_entity_id": _text(row.get("reported_entity_id")),
        "reported_company_name": _text(row.get("reported_company_name")),
        "reported_period_start": _date(row.get("reported_period_start")),
        "reported_period_end": _date(row.get("reported_period_end")),
        "contexts_count": _uint(row.get("contexts_count")),
        "units_count": _uint(row.get("units_count")),
        "facts_count": _uint(row.get("facts_count")),
        "validation_warnings": _text(row.get("validation_warnings")),
        "parser_version": _text(row.get("parser_version")),
        "parsed_at": _required_datetime(row.get("parsed_at")),
    }
    return tuple(converted[column] for column in UNIFIED_DOCUMENTS_CLICKHOUSE_COLUMNS)


def unified_context_row(row: dict[str, Any]) -> tuple[Any, ...]:
    converted = {
        "statement_key": _text(row.get("statement_key")),
        "context_id": _text(row.get("context_id")),
        "entity_identifier": _text(row.get("entity_identifier")),
        "entity_scheme": _text(row.get("entity_scheme")),
        "period_type": _text(row.get("period_type")) or "none",
        "instant_date": _date(row.get("instant_date")),
        "period_start": _date(row.get("period_start")),
        "period_end": _date(row.get("period_end")),
        "dimensions": _text(row.get("dimensions")),
        "is_comparative": _bool(row.get("is_comparative")),
        "parser_version": _text(row.get("parser_version")),
        "parsed_at": _required_datetime(row.get("parsed_at")),
        "mcy_member_code": _text(row.get("mcy_member_code")),
        "ref_member_code": _text(row.get("ref_member_code")),
    }
    return tuple(converted[column] for column in UNIFIED_CONTEXTS_CLICKHOUSE_COLUMNS)


def unified_unit_row(row: dict[str, Any]) -> tuple[Any, ...]:
    converted = {
        "statement_key": _text(row.get("statement_key")),
        "unit_id": _text(row.get("unit_id")),
        "measures": _text(row.get("measures")),
        "numerator_measures": _text(row.get("numerator_measures")),
        "denominator_measures": _text(row.get("denominator_measures")),
        "is_divide": _bool(row.get("is_divide")),
        "currency": _text(row.get("currency")),
        "parser_version": _text(row.get("parser_version")),
        "parsed_at": _required_datetime(row.get("parsed_at")),
    }
    return tuple(converted[column] for column in UNIFIED_UNITS_CLICKHOUSE_COLUMNS)


def unified_fact_row(row: dict[str, Any]) -> tuple[Any, ...]:
    converted = {
        "statement_key": _text(row.get("statement_key")),
        "business_id": _text(row.get("business_id")),
        "financial_date": _required_date(row.get("financial_date")),
        "fact_ordinal": _uint(row.get("fact_ordinal")),
        "concept_qname": _text(row.get("concept_qname")),
        "concept_namespace": _text(row.get("concept_namespace")),
        "concept_local_name": _text(row.get("concept_local_name")),
        "context_id": _text(row.get("context_id")),
        "unit_id": _text(row.get("unit_id")),
        "currency": _text(row.get("currency")),
        "decimals": _text(row.get("decimals")),
        "precision": _text(row.get("precision")),
        "is_nil": _bool(row.get("is_nil")),
        "xml_lang": _text(row.get("xml_lang")),
        "value_kind": _text(row.get("value_kind")),
        "raw_value": _text(row.get("raw_value")),
        "numeric_value": _decimal(row.get("numeric_value")),
        "date_value": _date(row.get("date_value")),
        "text_value": _text(row.get("text_value")),
        "dimensions": _text(row.get("dimensions")),
        "is_comparative": _bool(row.get("is_comparative")),
        "parser_version": _text(row.get("parser_version")),
        "parsed_at": _required_datetime(row.get("parsed_at")),
        "mcy_member_code": _text(row.get("mcy_member_code")),
        "ref_member_code": _text(row.get("ref_member_code")),
    }
    return tuple(converted[column] for column in UNIFIED_FACTS_CLICKHOUSE_COLUMNS)


def _text(value: Any) -> str:
    if value is None or value == "":
        return ""
    return str(value)


def _uint(value: Any) -> int:
    if value is None or value == "":
        return 0
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"Expected non-negative integer, got {value!r}")
    return parsed


def _bool(value: Any) -> int:
    return int(bool(value))


def _date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return date.fromisoformat(str(value))


def _required_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if value is None or value == "":
        raise ValueError("financial_date is required and cannot be empty")
    return date.fromisoformat(str(value))


def _required_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _utc_datetime(value)
    if value is None or value == "":
        raise ValueError("parsed_at is required and cannot be empty")
    return _utc_datetime(datetime.fromisoformat(str(value)))


def _utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value)).quantize(DECIMAL_SCALE)


def export_finland_unified_clickhouse(
    *,
    clickhouse: Any,
    documents: list[dict[str, Any]],
    contexts: list[dict[str, Any]],
    units: list[dict[str, Any]],
    facts: list[dict[str, Any]],
) -> dict[str, int]:
    if not facts:
        raise ValueError(
            "Refusing to publish Finland unified XBRL export: facts list is empty "
            "(would blank a populated table)"
        )

    tables_spec = (
        (
            UNIFIED_DOCUMENTS_CLICKHOUSE_TABLE,
            UNIFIED_DOCUMENTS_CLICKHOUSE_COLUMNS,
            documents,
            unified_document_row,
        ),
        (
            UNIFIED_CONTEXTS_CLICKHOUSE_TABLE,
            UNIFIED_CONTEXTS_CLICKHOUSE_COLUMNS,
            contexts,
            unified_context_row,
        ),
        (
            UNIFIED_UNITS_CLICKHOUSE_TABLE,
            UNIFIED_UNITS_CLICKHOUSE_COLUMNS,
            units,
            unified_unit_row,
        ),
        (
            UNIFIED_FACTS_CLICKHOUSE_TABLE,
            UNIFIED_FACTS_CLICKHOUSE_COLUMNS,
            facts,
            unified_fact_row,
        ),
    )

    assert_clickhouse_tables_exist(
        clickhouse,
        database=CLICKHOUSE_DATABASE,
        tables=tuple(spec[0] for spec in tables_spec),
    )

    counts: dict[str, int] = {}
    with clickhouse.get_connection() as client:
        for table, columns, rows, converter in tables_spec:
            counts[table] = replace_clickhouse_table_with_rows(
                clickhouse_client=client,
                table=table,
                columns=columns,
                rows=rows,
                converter=converter,
            )
    return counts


def replace_clickhouse_table_with_rows(
    *,
    clickhouse_client: Any,
    table: str,
    columns: tuple[str, ...],
    rows: list[dict[str, Any]],
    converter: Any,
) -> int:
    stage_table = f"{table}__stage_{uuid.uuid4().hex}"
    qualified_table = f"{CLICKHOUSE_DATABASE}.{table}"
    qualified_stage_table = f"{CLICKHOUSE_DATABASE}.{stage_table}"
    inserted = 0
    primary_error: Exception | None = None
    try:
        clickhouse_client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_table}"
        )
        insert_sql = _insert_sql(qualified_stage_table, columns)
        batch: list[tuple[Any, ...]] = []
        for row in rows:
            batch.append(converter(row))
            if len(batch) >= INSERT_BATCH_SIZE:
                clickhouse_client.execute(insert_sql, batch)
                inserted += len(batch)
                batch = []
        if batch:
            clickhouse_client.execute(insert_sql, batch)
            inserted += len(batch)
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
    return inserted


def _insert_sql(qualified_table: str, columns: tuple[str, ...]) -> str:
    insert_columns = ", ".join(columns)
    return f"INSERT INTO {qualified_table} ({insert_columns}) VALUES"
