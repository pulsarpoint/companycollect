"""ClickHouse export for France Annuaire enrichments."""

from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.france_annuaire import tables


def export_france_company_enrichments_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Atomically replace corpscout.fr_company_enrichments."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.COMPANY_ENRICHMENTS_TABLE,),
    )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DLT_DATASET_NAME,
            duckdb_table=tables.ENRICHMENTS_TABLE,
            clickhouse_database=tables.CLICKHOUSE_DATABASE,
            clickhouse_table=tables.COMPANY_ENRICHMENTS_TABLE,
            columns=tables.FR_COMPANY_ENRICHMENTS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Exported France Annuaire enrichments to ClickHouse: rows=%s", rows)
    return rows
