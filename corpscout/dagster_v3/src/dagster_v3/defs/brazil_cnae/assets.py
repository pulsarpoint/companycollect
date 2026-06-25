from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_cnae import tables
from dagster_v3.defs.brazil_cnae.mapping import (
    replace_br_cnae_to_nace_clickhouse,
)
from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist

GROUP_NAME = "brazil_cnae"
BR_CNAE_TO_NACE_FIXTURE = Path(__file__).parent / "fixtures" / "br_cnae_to_nace.csv"


@dg.asset(
    deps=[dg.AssetKey("nace_categories_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "reference"},
    description=(
        "Brazil CNAE to NACE many-to-many mapping edges from curated fixture data."
    ),
)
def brazil_cnae_to_nace_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_CNAE_DATABASE,
        tables=(tables.BR_CNAE_TO_NACE_TABLE,),
    )
    with clickhouse.get_connection() as client:
        counts = replace_br_cnae_to_nace_clickhouse(
            clickhouse_client=client,
            fixture_path=BR_CNAE_TO_NACE_FIXTURE,
            source_run_id=context.run.run_id,
        )

    context.log.info("Published Brazil CNAE to NACE mapping: counts=%s", counts)
    return dg.MaterializeResult(metadata=counts)
