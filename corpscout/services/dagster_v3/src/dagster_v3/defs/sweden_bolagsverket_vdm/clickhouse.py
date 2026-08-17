from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.sweden_bolagsverket_vdm import tables


def append_observations_to_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Append the bounded run's company and document observations."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(
            tables.COMPANY_OBSERVATIONS_TABLE_CH,
            tables.DOCUMENT_OBSERVATIONS_TABLE_CH,
        ),
    )
    with clickhouse.get_connection() as client:
        company_rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DUCKDB_SCHEMA,
            duckdb_table=tables.COMPANY_OBSERVATIONS_TABLE,
            clickhouse_database=tables.CLICKHOUSE_DATABASE,
            clickhouse_table=tables.COMPANY_OBSERVATIONS_TABLE_CH,
            columns=tables.COMPANY_OBSERVATION_COLUMNS,
            truncate=False,
        )
        document_rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DUCKDB_SCHEMA,
            duckdb_table=tables.DOCUMENT_OBSERVATIONS_TABLE,
            clickhouse_database=tables.CLICKHOUSE_DATABASE,
            clickhouse_table=tables.DOCUMENT_OBSERVATIONS_TABLE_CH,
            columns=tables.DOCUMENT_OBSERVATION_COLUMNS,
            truncate=False,
        )
    if log is not None:
        log(
            "Appended Bolagsverket VDM targeted observations: companies=%s documents=%s",
            company_rows,
            document_rows,
        )
    return {
        "company_observations": company_rows,
        "document_observations": document_rows,
    }
