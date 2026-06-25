from collections.abc import Callable
from pathlib import Path

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_rfb import tables
from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_table_to_clickhouse,
)

DLT_DATASET_NAME = tables.DLT_DATASET_NAME


def export_brazil_rfb_clickhouse_companies(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.br_companies with the normalized DuckDB companies table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_RFB_DATABASE,
        tables=(tables.BR_COMPANIES_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting Brazil RFB companies to ClickHouse: duckdb_path=%s, table=%s",
            database_path,
            tables.QUALIFIED_BR_COMPANIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.COMPANIES_TABLE,
            clickhouse_database=tables.BRAZIL_RFB_DATABASE,
            clickhouse_table=tables.BR_COMPANIES_TABLE_CH,
            columns=tables.BR_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Brazil RFB companies ClickHouse export: rows=%s", rows)
    return rows


def export_brazil_rfb_clickhouse_establishments(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.br_establishments with the normalized DuckDB establishments table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_RFB_DATABASE,
        tables=(tables.BR_ESTABLISHMENTS_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting Brazil RFB establishments to ClickHouse: duckdb_path=%s, table=%s",
            database_path,
            tables.QUALIFIED_BR_ESTABLISHMENTS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.ESTABLISHMENTS_TABLE,
            clickhouse_database=tables.BRAZIL_RFB_DATABASE,
            clickhouse_table=tables.BR_ESTABLISHMENTS_TABLE_CH,
            columns=tables.BR_ESTABLISHMENTS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Brazil RFB establishments ClickHouse export: rows=%s", rows)
    return rows


def export_brazil_rfb_clickhouse_contact_info(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.br_company_contact_info with the DuckDB contact table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_RFB_DATABASE,
        tables=(tables.BR_COMPANY_CONTACT_INFO_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting Brazil RFB contact info to ClickHouse: duckdb_path=%s, table=%s",
            database_path,
            tables.QUALIFIED_BR_COMPANY_CONTACT_INFO_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.COMPANY_CONTACT_INFO_TABLE,
            clickhouse_database=tables.BRAZIL_RFB_DATABASE,
            clickhouse_table=tables.BR_COMPANY_CONTACT_INFO_TABLE_CH,
            columns=tables.BR_COMPANY_CONTACT_INFO_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Brazil RFB contact info ClickHouse export: rows=%s", rows)
    return rows


def export_brazil_rfb_clickhouse_websites(
    *,
    database_path: str | Path,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.br_websites with the DuckDB domain feeder table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_RFB_DATABASE,
        tables=(tables.BR_WEBSITES_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting Brazil RFB websites to ClickHouse: duckdb_path=%s, table=%s",
            database_path,
            tables.QUALIFIED_BR_WEBSITES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_table_to_clickhouse(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.WEBSITES_TABLE,
            clickhouse_database=tables.BRAZIL_RFB_DATABASE,
            clickhouse_table=tables.BR_WEBSITES_TABLE_CH,
            columns=tables.BR_WEBSITES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Brazil RFB websites ClickHouse export: rows=%s", rows)
    return rows
