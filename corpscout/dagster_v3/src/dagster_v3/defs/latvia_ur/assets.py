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
from dagster_v3.defs.latvia_ur.clickhouse import (
    export_latvia_ur_clickhouse_companies,
    export_latvia_ur_clickhouse_financial_statements,
)
from dagster_v3.defs.latvia_ur.financials import (
    build_latvia_ur_financial_statements,
    load_latvia_ur_financial_csv,
)

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


def _load_financial_raw(
    context: AssetExecutionContext,
    *,
    raw_table: str,
    download_url: str,
) -> dg.MaterializeResult:
    context.log.info(
        "Loading Latvia UR financial CSV: url=%s, duckdb_path=%s, table=%s.%s",
        download_url,
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        raw_table,
    )
    rows = load_latvia_ur_financial_csv(
        database_path=LATVIA_UR_DUCKDB_PATH,
        download_url=download_url,
        raw_table=raw_table,
    )
    context.log.info(
        "Loaded Latvia UR financial CSV: table=%s.%s, rows=%s",
        DLT_DATASET_NAME,
        raw_table,
        rows,
    )
    return dg.MaterializeResult(metadata={"rows": rows, "table": f"{DLT_DATASET_NAME}.{raw_table}"})


@dg.asset(
    name="latvia_ur_financial_statements_raw_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="Latvia UR annual-report metadata CSV loaded raw into DuckDB (checkpoint).",
)
def latvia_ur_financial_statements_raw_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    return _load_financial_raw(
        context,
        raw_table=tables.FINANCIAL_STATEMENTS_RAW_TABLE,
        download_url=tables.FINANCIAL_STATEMENTS_URL,
    )


@dg.asset(
    name="latvia_ur_balance_sheets_raw_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="Latvia UR balance-sheet CSV loaded raw into DuckDB (checkpoint).",
)
def latvia_ur_balance_sheets_raw_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    return _load_financial_raw(
        context,
        raw_table=tables.BALANCE_SHEETS_RAW_TABLE,
        download_url=tables.BALANCE_SHEETS_URL,
    )


@dg.asset(
    name="latvia_ur_income_statements_raw_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="Latvia UR income-statement CSV loaded raw into DuckDB (checkpoint).",
)
def latvia_ur_income_statements_raw_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    return _load_financial_raw(
        context,
        raw_table=tables.INCOME_STATEMENTS_RAW_TABLE,
        download_url=tables.INCOME_STATEMENTS_URL,
    )


@dg.asset(
    name="latvia_ur_cash_flow_statements_raw_duckdb",
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="Latvia UR cash-flow CSV loaded raw into DuckDB (checkpoint).",
)
def latvia_ur_cash_flow_statements_raw_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    return _load_financial_raw(
        context,
        raw_table=tables.CASH_FLOW_STATEMENTS_RAW_TABLE,
        download_url=tables.CASH_FLOW_STATEMENTS_URL,
    )


@dg.asset(
    name="latvia_ur_financial_statements_duckdb",
    deps=[
        dg.AssetKey("latvia_ur_financial_statements_raw_duckdb"),
        dg.AssetKey("latvia_ur_balance_sheets_raw_duckdb"),
        dg.AssetKey("latvia_ur_income_statements_raw_duckdb"),
        dg.AssetKey("latvia_ur_cash_flow_statements_raw_duckdb"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=LATVIA_UR_DUCKDB_POOL,
    description="Latvia UR wide financial statements pivoted from the four raw tables on statement_id.",
)
def latvia_ur_financial_statements_duckdb(
    context: AssetExecutionContext,
) -> dg.MaterializeResult:
    context.log.info(
        "Building Latvia UR wide financial statements: duckdb_path=%s, table=%s.%s",
        LATVIA_UR_DUCKDB_PATH,
        DLT_DATASET_NAME,
        tables.FINANCIAL_STATEMENTS_WIDE_TABLE,
    )
    counts = build_latvia_ur_financial_statements(
        database_path=LATVIA_UR_DUCKDB_PATH,
        source_run_id=context.run_id,
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    deps=[dg.AssetKey("latvia_ur_financial_statements_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=LATVIA_UR_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_LV_FINANCIAL_STATEMENTS_TABLE},
    description=(
        "Latvia UR financial statements exported to ClickHouse "
        "corpscout.lv_financial_statements."
    ),
)
def latvia_ur_clickhouse_financial_statements(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    context.log.info(
        "Starting Latvia UR financial statements ClickHouse export: duckdb_path=%s, table=%s",
        LATVIA_UR_DUCKDB_PATH,
        tables.QUALIFIED_LV_FINANCIAL_STATEMENTS_TABLE,
    )
    rows = export_latvia_ur_clickhouse_financial_statements(
        database_path=LATVIA_UR_DUCKDB_PATH,
        clickhouse=clickhouse,
        log=context.log.info,
    )
    context.log.info(
        "Completed Latvia UR financial statements ClickHouse export: rows=%s", rows
    )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_LV_FINANCIAL_STATEMENTS_TABLE},
    )


def _duckdb_table_count(*, database_path: str | Path, table_name: str) -> int:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        value = connection.execute(f"select count(*) from {table_name}").fetchone()[0]
    return int(value)
