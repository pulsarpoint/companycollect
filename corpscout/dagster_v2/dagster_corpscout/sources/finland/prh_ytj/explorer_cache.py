"""Refresh the Finland PRH YTJ company explorer cache table."""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

COMPANY_EXPLORER_VIEW_TABLE = "fi_prhytj_company_explorer"
COMPANY_EXPLORER_CACHE_TABLE = "fi_prhytj_company_explorer_cache"
INDUSTRY_NACE_MAPPING_TABLE = "fi_prhytj_industry_nace_mappings"

COMPANY_EXPLORER_CACHE_COLUMNS = [
    "business_id",
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "name",
    "registration_date",
    "end_date",
    "status_code",
    "status_description",
    "trade_register_status_code",
    "trade_register_status_description",
    "lifecycle_status",
    "is_active",
    "main_business_line_code",
    "main_business_line_code_set",
    "main_business_line_description_en",
    "company_form_code",
    "company_form_description_en",
    "company_situation_code",
    "company_situation_description_en",
    "website",
    "name_history",
    "registered_entries",
    "company_forms",
    "company_situations",
    "websites",
    "addresses",
    "source_payload_hash",
    "latest_ingested_at",
    "nace_revision",
    "nace_code",
    "nace_normalized_code",
    "nace_section_code",
    "nace_division_code",
    "nace_group_code",
    "nace_class_code",
    "nace_title_en",
    "nace_mapping_method",
    "nace_mapping_status",
    "refreshed_at",
]

_NACE_COLUMNS = {
    "nace_revision",
    "nace_code",
    "nace_normalized_code",
    "nace_section_code",
    "nace_division_code",
    "nace_group_code",
    "nace_class_code",
    "nace_title_en",
}


@dataclass(frozen=True)
class CompanyExplorerCacheRefreshResult:
    cache_table: str
    rows: int
    refreshed_at: datetime


def refresh_company_explorer_cache(
    clickhouse,
    *,
    refreshed_at: datetime | None = None,
) -> CompanyExplorerCacheRefreshResult:
    database = clickhouse.database
    client = clickhouse.client()
    refreshed_at = refreshed_at or datetime.now(timezone.utc)
    refreshed_at = refreshed_at.astimezone(timezone.utc).replace(
        microsecond=(refreshed_at.microsecond // 1000) * 1000
    )

    temp_table = _temp_table_name()
    temp_qualified = _qualified_table(database, temp_table)
    cache_qualified = _qualified_table(database, COMPANY_EXPLORER_CACHE_TABLE)

    client.command(f"DROP TABLE IF EXISTS {temp_qualified}")
    temp_created = False
    try:
        client.command(f"CREATE TABLE {temp_qualified} AS {cache_qualified}")
        temp_created = True
        client.command(f"TRUNCATE TABLE IF EXISTS {temp_qualified}")
        client.command(
            build_company_explorer_cache_insert_query(
                database=database,
                target_table=temp_table,
                refreshed_at=refreshed_at,
            )
        )
        rows = int(client.query(f"SELECT count() FROM {temp_qualified}").result_rows[0][0])
        client.command(f"EXCHANGE TABLES {cache_qualified} AND {temp_qualified}")
    finally:
        if temp_created:
            client.command(f"DROP TABLE IF EXISTS {temp_qualified}")

    return CompanyExplorerCacheRefreshResult(
        cache_table=f"{database}.{COMPANY_EXPLORER_CACHE_TABLE}",
        rows=rows,
        refreshed_at=refreshed_at,
    )


def build_company_explorer_cache_insert_query(
    *,
    database: str,
    target_table: str,
    refreshed_at: datetime,
) -> str:
    columns = ", ".join(_quote_ident(column) for column in COMPANY_EXPLORER_CACHE_COLUMNS)
    select_columns = ", ".join(
        _select_expression(column, refreshed_at) for column in COMPANY_EXPLORER_CACHE_COLUMNS
    )
    return (
        f"INSERT INTO {_qualified_table(database, target_table)} ({columns})\n"
        f"SELECT {select_columns}\n"
        f"FROM {_qualified_table(database, COMPANY_EXPLORER_VIEW_TABLE)} AS explorer\n"
        f"LEFT JOIN {_qualified_table(database, INDUSTRY_NACE_MAPPING_TABLE)} AS industry_mapping\n"
        "  ON ifNull(industry_mapping.`source_code_set`, '') = "
        "ifNull(explorer.`main_business_line_code_set`, '')\n"
        " AND ifNull(industry_mapping.`source_code`, '') = "
        "ifNull(explorer.`main_business_line_code`, '')"
    )


def _select_expression(column: str, refreshed_at: datetime) -> str:
    quoted = _quote_ident(column)
    if column == "refreshed_at":
        return f"{_datetime64_literal(refreshed_at)} AS {quoted}"
    if column in _NACE_COLUMNS:
        return f"industry_mapping.{quoted} AS {quoted}"
    if column == "nace_mapping_method":
        return f"industry_mapping.`mapping_method` AS {quoted}"
    if column == "nace_mapping_status":
        return f"industry_mapping.`mapping_status` AS {quoted}"
    return f"explorer.{quoted} AS {quoted}"


def _temp_table_name() -> str:
    return f"{COMPANY_EXPLORER_CACHE_TABLE}_refresh_{uuid.uuid4().hex}"


def _qualified_table(database: str, table: str) -> str:
    return f"{_quote_ident(database)}.{_quote_ident(table)}"


def _quote_ident(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _datetime64_literal(value: datetime) -> str:
    formatted = value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:23]
    return f"toDateTime64('{formatted}', 3, 'UTC')"
