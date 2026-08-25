import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_serving import tables
from dagster_v3.defs.company_serving.publish import publish_company_serving_country

COUNTRY_PARTITIONS = dg.StaticPartitionsDefinition([tables.COUNTRY_CODE])


class PublishCompanyServingConfig(dg.Config):
    allow_empty: bool = False


@dg.asset(
    deps=[contract.build_model for contract in tables.CURRENT_TABLES],
    group_name="company_serving",
    kinds={"dbt", "clickhouse"},
    partitions_def=COUNTRY_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=tables.PUBLISH_POOL,
    metadata={"table": f"{tables.CLICKHOUSE_DATABASE}.{tables.PRESENCE.name}"},
    description=(
        "Validates and atomically publishes one country of company-keyed application "
        "serving tables. Section presence is published last."
    ),
)
def company_serving_current(
    context: dg.AssetExecutionContext,
    config: PublishCompanyServingConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    if context.partition_key != tables.COUNTRY_CODE:
        raise ValueError(
            "The first company-serving implementation supports Sweden only"
        )
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=tuple(contract.name for contract in tables.CURRENT_TABLES)
        + tuple(tables.HISTORY_TABLES.values()),
    )
    with clickhouse.get_connection() as client:
        metrics = publish_company_serving_country(
            client,
            country_code=context.partition_key,
            source_run_id=context.run.run_id,
            allow_empty=config.allow_empty,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"country_code": context.partition_key, **metrics}
    )
