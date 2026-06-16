from collections.abc import Iterator
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.nace.clickhouse import prepare_nace_categories_table
from dagster_v3.defs.nace.source import (
    NACE_CATEGORIES_DLT_TABLE,
    nace_categories_source,
    nace_clickhouse_pipeline,
)


class NaceDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        if data.resource.name != NACE_CATEGORIES_DLT_TABLE:
            return spec
        return spec.replace_attributes(
            key="nace_categories",
            deps=[],
            group_name="nace",
            description=(
                "Official NACE Rev. 2 and Rev. 2.1 category reference table "
                "loaded to ClickHouse."
            ),
            kinds={"python", "dlt", "clickhouse", "reference"},
        )


@dlt_assets(
    dlt_source=nace_categories_source(),
    dlt_pipeline=nace_clickhouse_pipeline(),
    name="nace_categories",
    dagster_dlt_translator=NaceDltTranslator(),
)
def nace_categories_asset(
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
    clickhouse: ClickhouseResource,
) -> Iterator[Any]:
    prepare_nace_categories_table(clickhouse)
    yield from dlt.run(
        context=context,
        dlt_source=nace_categories_source(source_run_id=context.run_id),
        dlt_pipeline=nace_clickhouse_pipeline(),
    )


defs = dg.Definitions(
    assets=[nace_categories_asset],
    resources={
        "clickhouse": ClickhouseResource(
            host=dg.EnvVar("CLICKHOUSE_HOST"),
            user=dg.EnvVar("CLICKHOUSE_USER"),
            password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
            database=dg.EnvVar("CLICKHOUSE_DATABASE"),
        ),
    },
)
