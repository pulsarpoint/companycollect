from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.czech_ares import contacts
from dagster_v3.defs.czech_ares import tables

DUCKDB_SCHEMA = tables.DUCKDB_SCHEMA


def export_czech_ares_clickhouse_companies(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.cz_companies with the DuckDB companies table."""
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.CZECH_DATABASE, tables=(tables.COMPANIES_TABLE_CH,)
    )
    if log is not None:
        log("Exporting Czech ARES companies: table=%s", tables.QUALIFIED_COMPANIES_TABLE)
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DUCKDB_SCHEMA,
            duckdb_table=tables.COMPANIES_TABLE,
            clickhouse_database=tables.CZECH_DATABASE,
            clickhouse_table=tables.COMPANIES_TABLE_CH,
            columns=tables.CZ_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Czech ARES companies ClickHouse export: rows=%s", rows)
    return rows


def export_czech_ares_clickhouse_industries(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.cz_industries with the DuckDB industries table."""
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.CZECH_DATABASE, tables=(tables.INDUSTRIES_TABLE_CH,)
    )
    if log is not None:
        log("Exporting Czech ARES industries: table=%s", tables.QUALIFIED_INDUSTRIES_TABLE)
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DUCKDB_SCHEMA,
            duckdb_table=tables.INDUSTRIES_RAW_TABLE,
            clickhouse_database=tables.CZECH_DATABASE,
            clickhouse_table=tables.INDUSTRIES_TABLE_CH,
            columns=tables.CZ_INDUSTRIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Czech ARES industries ClickHouse export: rows=%s", rows)
    return rows


def export_czech_ares_clickhouse_company_contacts(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Replace corpscout.cz_company_contacts from contacts embedded in cz_companies.name."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CZECH_DATABASE,
        tables=(
            tables.COMPANIES_TABLE_CH,
            "commoncrawl_domains",
            tables.COMPANY_CONTACTS_TABLE_CH,
        ),
    )
    if log is not None:
        log(
            "Exporting Czech ARES company contacts: table=%s",
            tables.QUALIFIED_COMPANY_CONTACTS_TABLE,
        )
    with clickhouse.get_connection() as client:
        counts = contacts.replace_czech_company_contacts_clickhouse(
            clickhouse_client=client,
            source_run_id=source_run_id,
            log=log,
        )
    if log is not None:
        log("Finished Czech ARES company contacts ClickHouse export: counts=%s", counts)
    return counts
