from collections.abc import Iterator
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.serbia_apr_company_people import tables
from dagster_v3.defs.serbia_apr_company_people.clickhouse import (
    replace_serbia_apr_beneficial_owners_clickhouse,
    replace_serbia_apr_representatives_clickhouse,
)

REPRESENTATIVES_DUCKDB_PATH = Path("data") / tables.REPRESENTATIVES_DUCKDB_FILE_NAME
BENEFICIAL_OWNERS_DUCKDB_PATH = Path("data") / tables.BENEFICIAL_OWNERS_DUCKDB_FILE_NAME


@dg.multi_asset(
    name="serbia_apr_representatives_clickhouse_publish",
    specs=[
        dg.AssetSpec(
            tables.REPRESENTATIVE_OBSERVATIONS_ASSET,
            deps=[tables.REPRESENTATIVE_OBSERVATIONS_DUCKDB_ASSET],
            group_name=tables.GROUP_NAME,
            kinds={"python", "duckdb", "clickhouse"},
            tags=tables.REPRESENTATIVE_ASSET_TAGS,
            metadata={
                "table": (
                    f"{tables.CLICKHOUSE_DATABASE}."
                    f"{tables.REPRESENTATIVE_OBSERVATIONS_TABLE}"
                )
            },
            description=(
                "Complete APR SP3/SP4 representative observation history "
                "published from DuckDB to ClickHouse."
            ),
        ),
        dg.AssetSpec(
            tables.REPRESENTATIVES_CURRENT_ASSET,
            deps=[tables.REPRESENTATIVES_CURRENT_DUCKDB_ASSET],
            group_name=tables.GROUP_NAME,
            kinds={"python", "duckdb", "clickhouse"},
            tags=tables.REPRESENTATIVE_ASSET_TAGS,
            metadata={
                "table": (
                    f"{tables.CLICKHOUSE_DATABASE}."
                    f"{tables.REPRESENTATIVES_CURRENT_TABLE}"
                )
            },
            description=(
                "Current APR SP3/SP4 company representatives published from "
                "DuckDB to ClickHouse."
            ),
        ),
    ],
    pool=tables.REPRESENTATIVES_DUCKDB_POOL,
    can_subset=False,
)
def serbia_apr_representatives_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    serbia_apr_representatives_duckdb: DuckDBResource,
) -> Iterator[dg.MaterializeResult]:
    with read_only_duckdb_connection(serbia_apr_representatives_duckdb) as connection:
        counts = replace_serbia_apr_representatives_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )

    for asset_name, table_name in (
        (
            tables.REPRESENTATIVE_OBSERVATIONS_ASSET,
            tables.REPRESENTATIVE_OBSERVATIONS_TABLE,
        ),
        (
            tables.REPRESENTATIVES_CURRENT_ASSET,
            tables.REPRESENTATIVES_CURRENT_TABLE,
        ),
    ):
        yield dg.MaterializeResult(
            asset_key=asset_name,
            metadata={
                "rows": counts[table_name],
                "table": f"{tables.CLICKHOUSE_DATABASE}.{table_name}",
                "duckdb_table": (
                    f"{tables.REPRESENTATIVES_DUCKDB_SCHEMA}.{table_name}"
                ),
            },
        )


@dg.multi_asset(
    name="serbia_apr_beneficial_owners_clickhouse_publish",
    specs=[
        dg.AssetSpec(
            tables.BENEFICIAL_OWNER_OBSERVATIONS_ASSET,
            deps=[tables.BENEFICIAL_OWNER_OBSERVATIONS_DUCKDB_ASSET],
            group_name=tables.GROUP_NAME,
            kinds={"python", "duckdb", "clickhouse"},
            tags=tables.BENEFICIAL_OWNER_ASSET_TAGS,
            metadata={
                "table": (
                    f"{tables.CLICKHOUSE_DATABASE}."
                    f"{tables.BENEFICIAL_OWNER_OBSERVATIONS_TABLE}"
                )
            },
            description=(
                "Complete APR CEV beneficial-owner observation history "
                "published from DuckDB to ClickHouse."
            ),
        ),
        dg.AssetSpec(
            tables.BENEFICIAL_OWNERS_CURRENT_ASSET,
            deps=[tables.BENEFICIAL_OWNERS_CURRENT_DUCKDB_ASSET],
            group_name=tables.GROUP_NAME,
            kinds={"python", "duckdb", "clickhouse"},
            tags=tables.BENEFICIAL_OWNER_ASSET_TAGS,
            metadata={
                "table": (
                    f"{tables.CLICKHOUSE_DATABASE}."
                    f"{tables.BENEFICIAL_OWNERS_CURRENT_TABLE}"
                )
            },
            description=(
                "Current APR CEV beneficial owners published from DuckDB to ClickHouse."
            ),
        ),
    ],
    pool=tables.BENEFICIAL_OWNERS_DUCKDB_POOL,
    can_subset=False,
)
def serbia_apr_beneficial_owners_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    serbia_apr_beneficial_owners_duckdb: DuckDBResource,
) -> Iterator[dg.MaterializeResult]:
    with read_only_duckdb_connection(serbia_apr_beneficial_owners_duckdb) as connection:
        counts = replace_serbia_apr_beneficial_owners_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )

    for asset_name, table_name in (
        (
            tables.BENEFICIAL_OWNER_OBSERVATIONS_ASSET,
            tables.BENEFICIAL_OWNER_OBSERVATIONS_TABLE,
        ),
        (
            tables.BENEFICIAL_OWNERS_CURRENT_ASSET,
            tables.BENEFICIAL_OWNERS_CURRENT_TABLE,
        ),
    ):
        yield dg.MaterializeResult(
            asset_key=asset_name,
            metadata={
                "rows": counts[table_name],
                "table": f"{tables.CLICKHOUSE_DATABASE}.{table_name}",
                "duckdb_table": (
                    f"{tables.BENEFICIAL_OWNERS_DUCKDB_SCHEMA}.{table_name}"
                ),
            },
        )


defs = dg.Definitions(
    assets=[
        serbia_apr_representatives_clickhouse,
        serbia_apr_beneficial_owners_clickhouse,
    ],
    resources={
        "serbia_apr_representatives_duckdb": duckdb_resource(
            REPRESENTATIVES_DUCKDB_PATH
        ),
        "serbia_apr_beneficial_owners_duckdb": duckdb_resource(
            BENEFICIAL_OWNERS_DUCKDB_PATH
        ),
    },
)
