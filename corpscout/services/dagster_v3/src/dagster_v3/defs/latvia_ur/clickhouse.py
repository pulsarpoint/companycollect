from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.latvia_ur import tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
ENTITIES_TABLE = tables.ENTITIES_TABLE
LV_COMPANIES_EXPORT_VIEW = "lv_companies_export"


def export_latvia_ur_clickhouse_companies(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.lv_companies with the DuckDB register entities table.

    The ClickHouse schema is owned by the migration; this only asserts the table
    exists and atomically swaps in the freshly loaded rows.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.LATVIA_UR_DATABASE,
        tables=(tables.LV_COMPANIES_TABLE,),
    )
    if log is not None:
        log(
            "Exporting Latvia UR companies to ClickHouse: table=%s",
            tables.QUALIFIED_LV_COMPANIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        _create_lv_companies_export_view(duckdb_connection)
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=LV_COMPANIES_EXPORT_VIEW,
            clickhouse_database=tables.LATVIA_UR_DATABASE,
            clickhouse_table=tables.LV_COMPANIES_TABLE,
            columns=tables.LV_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Latvia UR companies ClickHouse export: rows=%s", rows)
    return rows


def _create_lv_companies_export_view(duckdb_connection: Any) -> None:
    entity_columns = ",\n        ".join(f"e.{column}" for column in tables.LATVIA_UR_ENTITIES_COLUMNS)
    duckdb_connection.execute(
        f"""
        create or replace view {DLT_DATASET_NAME}.{LV_COMPANIES_EXPORT_VIEW} as
        select
            {entity_columns},
            a.activity_text_original,
            b.full_address as vzd_address_text,
            b.postal_code as vzd_address_postal_code,
            b.status as vzd_address_status,
            c.name as address_city_name,
            coalesce(m_atvk.name, m_parent.name) as address_municipality_name,
            b.latitude as address_latitude,
            b.longitude as address_longitude
        from {DLT_DATASET_NAME}.{ENTITIES_TABLE} e
        left join (
            select
                regcode,
                any_value(activity_text_original) as activity_text_original
            from {DLT_DATASET_NAME}.{tables.COMPANY_ACTIVITY_TABLE}
            group by regcode
        ) a on a.regcode = e.regcode
        left join (
            select *
            from {DLT_DATASET_NAME}.{tables.ADDRESS_BUILDINGS_TABLE}
            where status = 'EKS'
        ) b on b.address_code = e.address_id
        left join (
            select *
            from {DLT_DATASET_NAME}.{tables.ADDRESS_CITIES_TABLE}
            where status = 'EKS'
        ) c on c.address_code = coalesce(nullif(e.city_code, '0'), nullif(e.region_code, '0'))
        left join (
            select *
            from {DLT_DATASET_NAME}.{tables.ADDRESS_MUNICIPALITIES_TABLE}
            where status = 'EKS'
        ) m_atvk on m_atvk.atvk_code = e.atvk_code
        left join (
            select *
            from {DLT_DATASET_NAME}.{tables.ADDRESS_MUNICIPALITIES_TABLE}
            where status = 'EKS'
        ) m_parent on m_parent.address_code = c.parent_address_code
        """
    )
