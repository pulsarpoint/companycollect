"""Publish the unified Finland XBRL row contract into ClickHouse `_next` tables."""

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.finland_xbrl.assets.common import FINLAND_XBRL_DUCKDB_POOL
from dagster_v3.defs.finland_xbrl.assets.data_daily_xml_duckdb import (
    data_daily_xml_unified_duckdb,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml_duckdb import (
    data_snapshot_xml_unified_duckdb,
    list_xml_unified_duckdb_paths,
    read_xml_unified_duckdb_rows,
)
from dagster_v3.defs.finland_xbrl.unified_clickhouse import (
    CLICKHOUSE_DATABASE,
    UNIFIED_CONTEXTS_CLICKHOUSE_TABLE,
    UNIFIED_DOCUMENTS_CLICKHOUSE_TABLE,
    UNIFIED_FACTS_CLICKHOUSE_TABLE,
    UNIFIED_UNITS_CLICKHOUSE_TABLE,
    export_finland_unified_clickhouse,
)


@dg.asset(
    name="fi_xbrl_unified_clickhouse",
    group_name="finland_xbrl",
    pool=FINLAND_XBRL_DUCKDB_POOL,
    deps=[data_snapshot_xml_unified_duckdb, data_daily_xml_unified_duckdb],
    kinds={"python", "duckdb", "clickhouse", "xbrl"},
    description=(
        "Publishes the unified Finland XBRL row contract (documents, contexts, "
        "units, facts) from every unified parse DuckDB file into the ClickHouse "
        "`_next` staging tables."
    ),
)
def fi_xbrl_unified_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    paths = list_xml_unified_duckdb_paths()
    if not paths:
        raise FileNotFoundError(
            "No Finland unified XBRL parsed DuckDB files were found"
        )
    parsed = read_xml_unified_duckdb_rows(duckdb_paths=paths)
    counts = export_finland_unified_clickhouse(
        clickhouse=clickhouse,
        documents=parsed.statement_documents,
        contexts=parsed.contexts,
        units=parsed.units,
        facts=parsed.facts,
    )
    context.log.info(
        "Published Finland unified XBRL to ClickHouse: "
        "documents=%d contexts=%d units=%d facts=%d",
        counts[UNIFIED_DOCUMENTS_CLICKHOUSE_TABLE],
        counts[UNIFIED_CONTEXTS_CLICKHOUSE_TABLE],
        counts[UNIFIED_UNITS_CLICKHOUSE_TABLE],
        counts[UNIFIED_FACTS_CLICKHOUSE_TABLE],
    )
    return dg.MaterializeResult(
        metadata={
            "duckdb_path_count": parsed.duckdb_path_count,
            "documents_row_count": counts[UNIFIED_DOCUMENTS_CLICKHOUSE_TABLE],
            "contexts_row_count": counts[UNIFIED_CONTEXTS_CLICKHOUSE_TABLE],
            "units_row_count": counts[UNIFIED_UNITS_CLICKHOUSE_TABLE],
            "facts_row_count": counts[UNIFIED_FACTS_CLICKHOUSE_TABLE],
            "clickhouse_database": CLICKHOUSE_DATABASE,
        }
    )
