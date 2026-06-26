from collections.abc import Iterator
from datetime import date
import hashlib
from pathlib import Path
from typing import Any

import dagster as dg
import dlt as dlt_lib
import duckdb
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.clickhouse.resolved import replace_duckdb_connection_tables_in_clickhouse
from dagster_v3.defs.clickhouse.resources import clickhouse_resource_from_env
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.duckdb.schema_contract import (
    create_duckdb_table_from_contract,
    dagster_table_schema_from_contract,
    validate_duckdb_table_contract,
)
from dagster_v3.defs.nace import tables
from dagster_v3.defs.nace.source import (
    NACE_DUCKDB_DATASET_NAME,
    NACE_DUCKDB_PIPELINE_NAME,
    NACE_RAW_DLT_TABLE,
    NaceScheme,
    build_nace_rows,
    nace_raw_source,
    parse_sparql_csv,
)

GROUP_NAME = "nace"
NACE_DUCKDB_PATH = Path("data/nace_source.duckdb")


class NaceDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.table_name != NACE_RAW_DLT_TABLE:
            return spec
        return spec.replace_attributes(
            key="nace_raw_duckdb",
            deps=[],
            group_name=GROUP_NAME,
            description="Raw official NACE SPARQL CSV payloads stored in DuckDB.",
            kinds={"dlt", "duckdb", "reference"},
        )


# dagster-dlt needs a concrete source and pipeline when definitions are loaded.
# The empty scheme tuple is only the definition-time shape; the asset function
# supplies the real schemes and run id to dlt.run at materialization time.
@dlt_assets(
    dlt_source=nace_raw_source(schemes=()),
    dlt_pipeline=dlt_lib.pipeline(
        pipeline_name=NACE_DUCKDB_PIPELINE_NAME,
        destination=dlt_lib.destinations.duckdb(str(NACE_DUCKDB_PATH)),
        dataset_name=NACE_DUCKDB_DATASET_NAME,
        dev_mode=False,
    ),
    name="nace_raw_duckdb",
    dagster_dlt_translator=NaceDltTranslator(),
)
def nace_raw_duckdb_asset(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    NACE_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(
        "Loading raw NACE SPARQL payloads to DuckDB: duckdb_path=%s, schema=%s, table=%s",
        NACE_DUCKDB_PATH,
        NACE_DUCKDB_DATASET_NAME,
        NACE_RAW_DLT_TABLE,
    )
    yield from dlt.run(
        context=context,
        dlt_source=nace_raw_source(source_run_id=context.run_id),
        dlt_pipeline=dlt_lib.pipeline(
            pipeline_name=NACE_DUCKDB_PIPELINE_NAME,
            destination=dlt_lib.destinations.duckdb(str(NACE_DUCKDB_PATH)),
            dataset_name=NACE_DUCKDB_DATASET_NAME,
            dev_mode=False,
        ),
    )


@dg.asset(
    deps=[dg.AssetKey("nace_raw_duckdb")],
    group_name=GROUP_NAME,
    kinds={"duckdb", "reference"},
    metadata={
        "dagster/column_schema": dagster_table_schema_from_contract(
            tables.NACE_CATEGORIES_DUCKDB_CONTRACT
        )
    },
    description="Normalized official NACE category rows parsed from raw SPARQL CSV payloads.",
)
def nace_categories_duckdb(
    context: AssetExecutionContext,
    nace_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with nace_duckdb.get_connection() as connection:
        counts = normalize_nace_categories_duckdb(connection=connection)
    context.log.info(
        "Normalized NACE categories in DuckDB: raw_payloads=%s, rows=%s",
        counts["raw_payloads"],
        counts["rows"],
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("nace_categories_duckdb")],
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse", "reference"},
    description="NACE category reference rows exported from DuckDB to migrated ClickHouse table.",
)
def nace_categories_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    nace_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with nace_duckdb.get_connection() as connection:
        counts = export_nace_categories_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
        )
    context.log.info(
        "Exported NACE categories to ClickHouse: rows=%s, table=%s",
        counts["rows"],
        counts["table"],
    )
    return dg.MaterializeResult(metadata=counts)


def normalize_nace_categories_duckdb(
    *, connection: duckdb.DuckDBPyConnection
) -> dict[str, int]:
    _ensure_nace_duckdb_schema(connection)
    raw_rows = connection.execute(
        f"""
        select
          classification_version,
          source_scheme_uri,
          source_url,
          response_csv,
          source_payload_hash,
          valid_from,
          valid_to,
          is_current,
          source_run_id,
          pulled_at
        from {NACE_DUCKDB_DATASET_NAME}.{NACE_RAW_DLT_TABLE}
        order by classification_version
        """
    ).fetchall()
    connection.execute(
        f"delete from {NACE_DUCKDB_DATASET_NAME}.{tables.NACE_CATEGORIES_TABLE}"
    )

    normalized_rows: list[tuple[Any, ...]] = []
    for raw_row in raw_rows:
        (
            classification_version,
            source_scheme_uri,
            source_url,
            response_csv,
            source_payload_hash,
            valid_from,
            valid_to,
            is_current,
            source_run_id,
            pulled_at,
        ) = raw_row
        scheme = NaceScheme(
            classification_version=str(classification_version),
            scheme_uri=str(source_scheme_uri),
            valid_from=_date_string(valid_from),
            valid_to=_date_string(valid_to) if valid_to is not None else None,
            is_current=int(is_current),
        )
        rows = build_nace_rows(
            scheme=scheme,
            source_rows=parse_sparql_csv(str(response_csv)),
            source_url=str(source_url),
            source_payload_hash=str(source_payload_hash),
            source_run_id=str(source_run_id),
            pulled_at=str(pulled_at),
        )
        normalized_rows.extend(_nace_row_tuple(row) for row in rows)

    if normalized_rows:
        connection.executemany(
            f"""
            insert into {NACE_DUCKDB_DATASET_NAME}.{tables.NACE_CATEGORIES_TABLE}
            ({", ".join(tables.NACE_CATEGORIES_COLUMNS)})
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            normalized_rows,
        )
    return {"raw_payloads": len(raw_rows), "rows": len(normalized_rows)}


def export_nace_categories_clickhouse(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    clickhouse: ClickhouseResource,
) -> dict[str, int | str]:
    _validate_nace_duckdb_schema(duckdb_connection)

    with clickhouse.get_connection() as client:
        counts = replace_duckdb_connection_tables_in_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=NACE_DUCKDB_DATASET_NAME,
            clickhouse_database=tables.NACE_DATABASE,
            tables=[(tables.NACE_CATEGORIES_TABLE, tables.NACE_CATEGORIES_COLUMNS)],
        )
    return {
        "rows": counts[tables.NACE_CATEGORIES_TABLE],
        "table": tables.QUALIFIED_NACE_CATEGORIES_TABLE,
    }


nace_selection = dg.AssetSelection.assets("nace_categories_clickhouse").upstream()

nace_refresh_job = dg.define_asset_job(
    "nace_refresh_job",
    selection=nace_selection,
)


@dg.schedule(
    name="nace_weekly_schedule",
    cron_schedule="0 3 * * 1",
    execution_timezone="Europe/Belgrade",
    job=nace_refresh_job,
)
def nace_weekly_schedule() -> dg.RunRequest:
    return dg.RunRequest()


def _ensure_nace_duckdb_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema if not exists {NACE_DUCKDB_DATASET_NAME}")
    create_duckdb_table_from_contract(
        connection,
        schema=NACE_DUCKDB_DATASET_NAME,
        table=tables.NACE_CATEGORIES_TABLE,
        contract=tables.NACE_CATEGORIES_DUCKDB_CONTRACT,
    )


def _validate_nace_duckdb_schema(connection: duckdb.DuckDBPyConnection) -> None:
    validate_duckdb_table_contract(
        connection,
        schema=NACE_DUCKDB_DATASET_NAME,
        table=tables.NACE_CATEGORIES_TABLE,
        contract=tables.NACE_CATEGORIES_DUCKDB_CONTRACT,
    )


def _nace_row_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
    dlt_id_source = "|".join(
        "" if row.get(column) is None else str(row.get(column))
        for column in tables.NACE_CATEGORIES_COLUMNS[:-2]
    )
    row_with_dlt = {
        **row,
        "_dlt_load_id": "",
        "_dlt_id": hashlib.sha256(dlt_id_source.encode("utf-8")).hexdigest(),
    }
    return tuple(row_with_dlt.get(column) for column in tables.NACE_CATEGORIES_COLUMNS)


def _date_string(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


defs = dg.Definitions(
    assets=[
        nace_raw_duckdb_asset,
        nace_categories_duckdb,
        nace_categories_clickhouse,
    ],
    jobs=[nace_refresh_job],
    schedules=[nace_weekly_schedule],
    resources={"nace_duckdb": duckdb_resource(NACE_DUCKDB_PATH)},
)
