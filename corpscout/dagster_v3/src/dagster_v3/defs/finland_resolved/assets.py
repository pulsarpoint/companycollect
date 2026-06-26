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
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    replace_duckdb_connection_tables_in_clickhouse,
)
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.finland_resolved import tables

GROUP_NAME = "finland_resolved"
RESOLVED_DUCKDB_SCHEMA = "finland_resolved"
FINLAND_RESOLVED_DBT_PROJECT_DIR = Path(__file__).parent / "dbt"

_DEFAULT_DUCKDB_PATH = Path("data/finland_ytj.duckdb").expanduser()
if not _DEFAULT_DUCKDB_PATH.is_absolute():
    _DEFAULT_DUCKDB_PATH = _DEFAULT_DUCKDB_PATH.resolve()
# dbt reads FINLAND_YTJ_DUCKDB_PATH (profiles.yml) while finland_ytj_resolved_clickhouse
# reads ytj_duckdb.database. Set it unconditionally from the resource default so the two
# never disagree (a stale env var must not silently point dbt at a different file).
os.environ["FINLAND_YTJ_DUCKDB_PATH"] = str(_DEFAULT_DUCKDB_PATH)

finland_resolved_dbt_project = DbtProject(
    project_dir=FINLAND_RESOLVED_DBT_PROJECT_DIR,
    profiles_dir=FINLAND_RESOLVED_DBT_PROJECT_DIR,
)
finland_resolved_dbt_project.prepare_if_dev()


class FinlandResolvedDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return super().get_asset_key(dbt_resource_props)
        return dg.AssetKey(f"finland_ytj_resolved_{dbt_resource_props['name']}")

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        return GROUP_NAME


@dbt_assets(
    manifest=finland_resolved_dbt_project.manifest_path,
    project=finland_resolved_dbt_project,
    dagster_dbt_translator=FinlandResolvedDbtTranslator(),
    pool="finland_ytj_duckdb",
)
def finland_resolved_dbt_assets(
    context: AssetExecutionContext,
    finland_resolved_dbt: DbtCliResource,
) -> Iterator[Any]:
    yield from finland_resolved_dbt.cli(["build"], context=context).stream()


@dg.asset(
    deps=[
        get_asset_key_for_model([finland_resolved_dbt_assets], "fi_companies"),
        get_asset_key_for_model([finland_resolved_dbt_assets], "fi_names"),
        get_asset_key_for_model([finland_resolved_dbt_assets], "fi_websites"),
        get_asset_key_for_model([finland_resolved_dbt_assets], "fi_industries"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool="finland_ytj_duckdb",
    description="Exports resolved Finland YTJ DuckDB tables to migrated ClickHouse tables.",
)
def finland_ytj_resolved_clickhouse(
    clickhouse: ClickhouseResource,
    ytj_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.FINLAND_YTJ_RESOLVED_TABLES,
    )
    with ytj_duckdb.get_connection() as connection:
        with clickhouse.get_connection() as client:
            row_counts = replace_duckdb_connection_tables_in_clickhouse(
                duckdb_connection=connection,
                clickhouse_client=client,
                duckdb_schema=RESOLVED_DUCKDB_SCHEMA,
                clickhouse_database=RESOLVED_DATABASE,
                tables=tuple(
                    (table, tables.RESOLVED_EXPORT_COLUMNS[table])
                    for table in tables.FINLAND_YTJ_RESOLVED_TABLES
                ),
            )
    return dg.MaterializeResult(
        metadata={f"{table}_row_count": count for table, count in row_counts.items()}
    )


# Daily register+resolve refresh (see dagster_v3/CLAUDE.md "Scheduling"). YTJ is a
# full-snapshot register and the resolved dbt + ClickHouse export are fast.
# .upstream() pulls the finland_ytj register load (via the dbt source dep) + the
# dbt models + the export — no translation/Temporal gating. (finland_xbrl
# financials are a separate, partitioned pipeline with no ClickHouse export yet;
# scheduled separately once that export path lands.)
finland_ytj_resolved_job = dg.define_asset_job(
    "finland_ytj_resolved_job",
    selection=dg.AssetSelection.assets("finland_ytj_resolved_clickhouse").upstream(),
)
finland_ytj_resolved_schedule = dg.ScheduleDefinition(
    name="finland_ytj_resolved_schedule",
    job=finland_ytj_resolved_job,
    cron_schedule="45 4 * * *",  # daily 04:45 (staggered vs other sources)
    execution_timezone="Europe/Belgrade",
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[
            finland_resolved_dbt_assets,
            finland_ytj_resolved_clickhouse,
        ],
        jobs=[finland_ytj_resolved_job],
        schedules=[finland_ytj_resolved_schedule],
        resources={
            "ytj_duckdb": duckdb_resource(_DEFAULT_DUCKDB_PATH),
            "finland_resolved_dbt": DbtCliResource(
                project_dir=finland_resolved_dbt_project,
                profiles_dir=FINLAND_RESOLVED_DBT_PROJECT_DIR,
            ),
        },
    )
