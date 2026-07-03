from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.estonia_ar import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
ENTITIES_TABLE = tables.ENTITIES_TABLE


def export_estonia_ar_clickhouse_companies(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.ee_companies with the DuckDB register entities table.

    The ClickHouse schema is owned by the migration; this only asserts the table
    exists and atomically swaps in the freshly loaded rows.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESTONIA_AR_DATABASE,
        tables=(tables.EE_COMPANIES_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Estonia AR companies to ClickHouse: table=%s",
            tables.QUALIFIED_EE_COMPANIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=ENTITIES_TABLE,
            clickhouse_database=tables.ESTONIA_AR_DATABASE,
            clickhouse_table=tables.EE_COMPANIES_TABLE,
            columns=tables.EE_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Estonia AR companies ClickHouse export: rows=%s", rows)
    return rows


def export_estonia_ar_clickhouse_company_contacts(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.ee_company_contacts with the DuckDB contacts table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESTONIA_AR_DATABASE,
        tables=(tables.EE_COMPANY_CONTACTS_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Estonia AR company contacts to ClickHouse: table=%s",
            tables.QUALIFIED_EE_COMPANY_CONTACTS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.GENERAL_DATA_RAW_TABLE,
            clickhouse_database=tables.ESTONIA_AR_DATABASE,
            clickhouse_table=tables.EE_COMPANY_CONTACTS_TABLE,
            columns=tables.EE_COMPANY_CONTACTS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Estonia AR company contacts ClickHouse export: rows=%s", rows)
    return rows


def export_estonia_ar_clickhouse_company_domains(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.ee_company_domains with the DuckDB feeder table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESTONIA_AR_DATABASE,
        tables=(tables.EE_COMPANY_DOMAINS_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Estonia AR company domains to ClickHouse: table=%s",
            tables.QUALIFIED_EE_COMPANY_DOMAINS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.COMPANY_DOMAINS_TABLE,
            clickhouse_database=tables.ESTONIA_AR_DATABASE,
            clickhouse_table=tables.EE_COMPANY_DOMAINS_TABLE,
            columns=tables.EE_COMPANY_DOMAINS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Estonia AR company domains ClickHouse export: rows=%s", rows)
    return rows


def export_estonia_ar_clickhouse_industries(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.ee_industries with the DuckDB industries table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESTONIA_AR_DATABASE,
        tables=(tables.EE_INDUSTRIES_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Estonia AR industries to ClickHouse: table=%s",
            tables.QUALIFIED_EE_INDUSTRIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.INDUSTRIES_RAW_TABLE,
            clickhouse_database=tables.ESTONIA_AR_DATABASE,
            clickhouse_table=tables.EE_INDUSTRIES_TABLE,
            columns=tables.EE_INDUSTRIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Estonia AR industries ClickHouse export: rows=%s", rows)
    return rows
