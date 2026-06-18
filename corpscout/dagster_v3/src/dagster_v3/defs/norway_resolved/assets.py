import os
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    DbtProject,
    dbt_assets,
    get_asset_key_for_model,
)

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    replace_duckdb_tables_in_clickhouse,
)
from dagster_v3.defs.norway_resolved import tables

GROUP_NAME = "norway_resolved"
NORWAY_RESOLVED_DUCKDB_SCHEMA = "norway_resolved"
NORWAY_BRREG_DUCKDB_PATH = Path("data/norway_brreg_source.duckdb")
NORWAY_RESOLVED_DBT_PROJECT_DIR = Path(__file__).parent / "dbt"
NORWAY_BRREG_DUCKDB_POOL = "norway_brreg_duckdb"

_DEFAULT_DUCKDB_PATH = NORWAY_BRREG_DUCKDB_PATH.expanduser()
if not _DEFAULT_DUCKDB_PATH.is_absolute():
    _DEFAULT_DUCKDB_PATH = _DEFAULT_DUCKDB_PATH.resolve()
os.environ["NORWAY_BRREG_DUCKDB_PATH"] = str(_DEFAULT_DUCKDB_PATH)

norway_resolved_dbt_project = DbtProject(
    project_dir=NORWAY_RESOLVED_DBT_PROJECT_DIR,
    profiles_dir=NORWAY_RESOLVED_DBT_PROJECT_DIR,
)
norway_resolved_dbt_project.prepare_if_dev()


class NorwayResolvedDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return super().get_asset_key(dbt_resource_props)
        return dg.AssetKey(f"norway_resolved_{dbt_resource_props['name']}")

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        return GROUP_NAME


@dbt_assets(
    manifest=norway_resolved_dbt_project.manifest_path,
    project=norway_resolved_dbt_project,
    dagster_dbt_translator=NorwayResolvedDbtTranslator(),
    pool=NORWAY_BRREG_DUCKDB_POOL,
)
def norway_resolved_dbt_assets(
    context: AssetExecutionContext,
    norway_resolved_dbt: DbtCliResource,
) -> Iterator[Any]:
    yield from norway_resolved_dbt.cli(["build"], context=context).stream()


@dg.asset(
    deps=[
        get_asset_key_for_model([norway_resolved_dbt_assets], "no_companies"),
        get_asset_key_for_model([norway_resolved_dbt_assets], "no_websites"),
        get_asset_key_for_model([norway_resolved_dbt_assets], "no_industries"),
        get_asset_key_for_model([norway_resolved_dbt_assets], "no_financial_statements"),
    ],
    group_name=GROUP_NAME,
    kinds={"duckdb", "clickhouse"},
    pool=NORWAY_BRREG_DUCKDB_POOL,
    description="Exports normalized Norway resolved DuckDB tables to migrated ClickHouse tables.",
)
def norway_resolved_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Norway resolved ClickHouse export: duckdb_path=%s, tables=%s",
        NORWAY_BRREG_DUCKDB_PATH,
        tables.NORWAY_RESOLVED_TABLES,
    )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.NORWAY_RESOLVED_TABLES,
    )
    with clickhouse.get_connection() as client:
        row_counts = replace_duckdb_tables_in_clickhouse(
            duckdb_path=NORWAY_BRREG_DUCKDB_PATH,
            clickhouse_client=client,
            duckdb_schema=NORWAY_RESOLVED_DUCKDB_SCHEMA,
            clickhouse_database=RESOLVED_DATABASE,
            tables=tuple(
                (table, tables.RESOLVED_TABLE_COLUMNS[table])
                for table in tables.NORWAY_RESOLVED_TABLES
            ),
        )
    context.log.info("Completed Norway resolved ClickHouse export: row_counts=%s", row_counts)
    return dg.MaterializeResult(
        metadata={f"{table}_row_count": count for table, count in row_counts.items()}
    )


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[
            norway_resolved_dbt_assets,
            norway_resolved_clickhouse,
        ],
        resources={
            "norway_resolved_dbt": DbtCliResource(
                project_dir=norway_resolved_dbt_project,
                profiles_dir=NORWAY_RESOLVED_DBT_PROJECT_DIR,
            ),
        },
    )
