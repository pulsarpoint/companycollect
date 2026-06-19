from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
import dlt
import duckdb
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.latvia_ur import resources, tables
from dagster_v3.defs.latvia_ur.clickhouse import export_latvia_ur_clickhouse_companies

GROUP_NAME = "latvia_ur"
LATVIA_UR_DUCKDB_POOL = "latvia_ur_duckdb"
# File stem (catalog) must differ from DLT_DATASET_NAME or DuckDB's binder
# cannot disambiguate "<dataset>.<table>" (mirrors norway_brreg_source.duckdb).
LATVIA_UR_DUCKDB_PATH = Path("data/latvia_ur_source.duckdb")
DLT_DATASET_NAME = tables.DLT_DATASET_NAME
ENTITIES_TABLE = tables.ENTITIES_TABLE
ENTITIES_ASSET_KEY = "latvia_ur_entities_duckdb"


class LatviaUrDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.name != ENTITIES_TABLE:
            return spec
        return spec.replace_attributes(
            key=dg.AssetKey(ENTITIES_ASSET_KEY),
            deps=[],
            group_name=GROUP_NAME,
            description="Latvia UR register entity bulk data loaded to local DuckDB with dlt.",
            kinds={"python", "dlt", "duckdb"},
        )


def latvia_ur_pipeline(
    database_path: str | Path,
    *,
    pipelines_dir: str | Path | None = None,
) -> Any:
    kwargs: dict[str, Any] = {
        "pipeline_name": "latvia_ur_register",
        "destination": dlt.destinations.duckdb(str(database_path)),
        "dataset_name": DLT_DATASET_NAME,
        "dev_mode": False,
    }
    if pipelines_dir is not None:
        kwargs["pipelines_dir"] = str(pipelines_dir)
    return dlt.pipeline(**kwargs)


def run_latvia_ur_dlt_pipeline(
    *,
    database_path: str | Path,
    run_id: str,
    session: resources.HttpSession | None = None,
    pipelines_dir: str | Path | None = None,
) -> Any:
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    return latvia_ur_pipeline(database_path, pipelines_dir=pipelines_dir).run(
        resources.latvia_ur_source(run_id=run_id, session=session)
    )


@dlt_assets(
    dlt_source=resources.latvia_ur_source(),
    dlt_pipeline=latvia_ur_pipeline(LATVIA_UR_DUCKDB_PATH),
    name=ENTITIES_ASSET_KEY,
    dagster_dlt_translator=LatviaUrDltTranslator(),
    pool=LATVIA_UR_DUCKDB_POOL,
)
def latvia_ur_entities_duckdb_asset(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
) -> Iterator[Any]:
    """Load Latvia UR register entity bulk data to local DuckDB with dlt."""
    LATVIA_UR_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    context.log.info(
        "Starting Latvia UR register dlt load: source_url=%s, duckdb_path=%s, "
        "dataset=%s, table=%s",
        resources.REGISTER_DOWNLOAD_URL,
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
    )
    yield from dlt.run(context=context)
    row_count = _duckdb_table_count(
        database_path=LATVIA_UR_DUCKDB_PATH,
        table_name=f"{DLT_DATASET_NAME}.{ENTITIES_TABLE}",
    )
    context.log.info(
        "Completed Latvia UR register dlt load: duckdb_path=%s, dataset=%s, table=%s, rows=%s",
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        ENTITIES_TABLE,
        row_count,
    )


@dg.asset(
    deps=[dg.AssetKey(ENTITIES_ASSET_KEY)],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=LATVIA_UR_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_LV_COMPANIES_TABLE},
    description="Latvia UR register companies exported to ClickHouse corpscout.lv_companies.",
)
def latvia_ur_clickhouse_companies(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Latvia UR companies ClickHouse export: duckdb_path=%s, table=%s",
        LATVIA_UR_DUCKDB_PATH,
        tables.QUALIFIED_LV_COMPANIES_TABLE,
    )
    rows = export_latvia_ur_clickhouse_companies(
        database_path=LATVIA_UR_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    context.log.info("Completed Latvia UR companies ClickHouse export: rows=%s", rows)
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_LV_COMPANIES_TABLE},
    )


def _duckdb_table_count(*, database_path: str | Path, table_name: str) -> int:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        value = connection.execute(f"select count(*) from {table_name}").fetchone()[0]
    return int(value)
