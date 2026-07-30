import json
from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.partition_duckdb import (
    open_partition_duckdb,
    require_partition_duckdb,
)
from dagster_v3.defs.common.eur_usd import apply_eur_usd_conversion
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.estonia_rhr_procurement import tables
from dagster_v3.defs.estonia_rhr_procurement.clickhouse import export_rhr_partition
from dagster_v3.defs.estonia_rhr_procurement.duckdb import write_parsed_month
from dagster_v3.defs.estonia_rhr_procurement.parser import parse_monthly_awards
from dagster_v3.defs.estonia_rhr_procurement.resources import (
    latest_month_manifest,
    sync_rhr_month,
)

# No shared DuckDB path: every partitioned asset opens its own file, so an
# export cannot read another month's rows. See
# defs/common/partition_duckdb.py.
SOURCE_NAME = "estonia_rhr_procurement"
MONTHLY_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date=tables.PARTITION_START_DATE,
    end_offset=1,
)
BACKFILL_POLICY = dg.BackfillPolicy.multi_run(max_partitions_per_run=1)


@dg.asset(
    name="estonia_rhr_procurement_xml_s3",
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "xml", "s3"},
    description=(
        "Snapshots one official RHR monthly contract-award XML bundle plus a "
        "TED notice-identifier index used to distinguish national-only awards."
    ),
)
def estonia_rhr_procurement_xml_s3(
    context: dg.AssetExecutionContext,
    estonia_rhr_procurement_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    snapshot = sync_rhr_month(
        object_store=estonia_rhr_procurement_object_store,
        partition_key=context.partition_key,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
    )
    return dg.MaterializeResult(
        metadata={
            "xml_bytes": snapshot.xml_bytes,
            "ted_notices": snapshot.ted_notices,
            "reused_objects": snapshot.reused_objects,
            "xml_object_key": snapshot.xml_object_key,
            "ted_index_object_key": snapshot.ted_index_object_key,
            "manifest_key": snapshot.manifest_key,
        }
    )


@dg.asset(
    name="estonia_rhr_procurement_normalized_duckdb",
    deps=[dg.AssetKey("estonia_rhr_procurement_xml_s3")],
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "xml", "s3", "duckdb"},
    description=(
        "Parses RHR eForms UBL into notice, lot, and winning-tenderer grains. "
        "Notice changes remain versioned and consortium values stay shared."
    ),
)
def estonia_rhr_procurement_normalized_duckdb(
    context: dg.AssetExecutionContext,
    estonia_rhr_procurement_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    manifest = latest_month_manifest(
        estonia_rhr_procurement_object_store, context.partition_key
    )
    parsed = parse_monthly_awards(
        estonia_rhr_procurement_object_store.read_bytes(
            str(manifest["xml_object_key"]), bucket=tables.S3_BUCKET
        ),
        ted_index=json.loads(
            estonia_rhr_procurement_object_store.read_bytes(
                str(manifest["ted_index_object_key"]), bucket=tables.S3_BUCKET
            )
        ),
        partition_key=context.partition_key,
        source_run_id=str(manifest["source_run_id"]),
        source_object_key=str(manifest["xml_object_key"]),
        source_retrieved_at=datetime.fromisoformat(str(manifest["retrieved_at"])),
        resolved_at=datetime.now(UTC),
    )
    with open_partition_duckdb(
        source=SOURCE_NAME, partition=context.partition_key
    ) as connection:
        counts = write_parsed_month(connection, parsed)
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="estonia_rhr_procurement_winners_usd",
    deps=[dg.AssetKey("estonia_rhr_procurement_normalized_duckdb")],
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb"},
    description=(
        "Converts RHR winner-grain EUR award and subcontracting amounts to USD "
        "using the notice publication date."
    ),
)
def estonia_rhr_procurement_winners_usd(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with require_partition_duckdb(
        source=SOURCE_NAME, partition=context.partition_key
    ) as connection:
        counts = apply_eur_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            qualified_table=(
                f"{tables.DUCKDB_SCHEMA}.{tables.WINNER_CANDIDATES_TABLE}"
            ),
            rate_date_columns=("publication_date",),
            amount_columns=(
                ("awarded_amount_eur", "awarded_amount_usd"),
                ("subcontracting_amount_eur", "subcontracting_amount_usd"),
            ),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="estonia_rhr_procurement_clickhouse",
    deps=[
        dg.AssetKey("estonia_rhr_procurement_winners_usd"),
        dg.AssetKey("estonia_ar_clickhouse_companies"),
    ],
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    metadata={
        "tables": [
            f"{tables.CLICKHOUSE_DATABASE}.{tables.NOTICES_TABLE}",
            f"{tables.CLICKHOUSE_DATABASE}.{tables.LOTS_TABLE}",
            f"{tables.CLICKHOUSE_DATABASE}.{tables.WINNERS_TABLE}",
        ]
    },
    description=(
        "Idempotently replaces one RHR publication month and resolves eligible "
        "eight-digit winner codes against ee_companies.reg_code."
    ),
)
def estonia_rhr_procurement_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(
            tables.NOTICES_TABLE,
            tables.LOTS_TABLE,
            tables.WINNERS_TABLE,
            "ee_companies",
        ),
    )
    with (
        require_partition_duckdb(
            source=SOURCE_NAME, partition=context.partition_key
        ) as connection,
        clickhouse.get_connection() as client,
    ):
        counts = export_rhr_partition(
            duckdb_connection=connection,
            clickhouse_client=client,
            partition_key=context.partition_key,
        )
    return dg.MaterializeResult(metadata=counts)


estonia_rhr_procurement_backfill_job = dg.define_asset_job(
    "estonia_rhr_procurement_backfill_job",
    selection=dg.AssetSelection.assets("estonia_rhr_procurement_clickhouse").upstream(),
)

defs = dg.Definitions(
    assets=[
        estonia_rhr_procurement_xml_s3,
        estonia_rhr_procurement_normalized_duckdb,
        estonia_rhr_procurement_winners_usd,
        estonia_rhr_procurement_clickhouse,
    ],
    jobs=[estonia_rhr_procurement_backfill_job],
    resources={
        "estonia_rhr_procurement_object_store": ObjectStoreResource(
            bucket=tables.S3_BUCKET
        ),
    },
)
