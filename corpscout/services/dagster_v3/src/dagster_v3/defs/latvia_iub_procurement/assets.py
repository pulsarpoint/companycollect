from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.eur_usd import apply_eur_usd_conversion
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.latvia_iub_procurement import tables
from dagster_v3.defs.latvia_iub_procurement.clickhouse import export_iub_partition
from dagster_v3.defs.latvia_iub_procurement.duckdb import write_parsed_month
from dagster_v3.defs.latvia_iub_procurement.parser import parse_daily_payload
from dagster_v3.defs.latvia_iub_procurement.resources import (
    latest_month_manifest,
    sync_iub_month,
)

DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME
MONTHLY_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date=tables.PARTITION_START_DATE,
    end_offset=1,
)
BACKFILL_POLICY = dg.BackfillPolicy.multi_run(max_partitions_per_run=1)


@dg.asset(
    name="latvia_iub_procurement_daily_json_s3",
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "s3"},
    description=(
        "Snapshots every official IUB daily JSON file in a month to S3. Missing "
        "non-publication days are recorded in the manifest rather than treated "
        "as failed notices."
    ),
)
def latvia_iub_procurement_daily_json_s3(
    context: dg.AssetExecutionContext,
    latvia_iub_procurement_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    snapshot = sync_iub_month(
        object_store=latvia_iub_procurement_object_store,
        partition_key=context.partition_key,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
    )
    return dg.MaterializeResult(
        metadata={
            "files": len(snapshot.object_keys),
            "downloaded": snapshot.downloaded,
            "reused": snapshot.reused,
            "absent_days": snapshot.absent,
            "absent_dates": list(snapshot.absent_dates),
            "bytes_written": snapshot.bytes_written,
            "manifest_key": snapshot.manifest_key,
        }
    )


@dg.asset(
    name="latvia_iub_procurement_normalized_duckdb",
    deps=[dg.AssetKey("latvia_iub_procurement_daily_json_s3")],
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "s3", "duckdb"},
    description=(
        "Parses one month of IUB notice versions into notice, lot, award-winner, "
        "and contract-execution grains. Raw versions are retained so clonedFrom "
        "can identify the latest published version in ClickHouse."
    ),
)
def latvia_iub_procurement_normalized_duckdb(
    context: dg.AssetExecutionContext,
    latvia_iub_procurement_duckdb: DuckDBResource,
    latvia_iub_procurement_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    manifest = latest_month_manifest(
        latvia_iub_procurement_object_store, context.partition_key
    )
    source_retrieved_at = datetime.fromisoformat(str(manifest["retrieved_at"]))
    parsed_days = []
    for key in manifest["object_keys"]:
        day = datetime.strptime(Path(str(key)).stem, "%d-%m-%Y").date()
        parsed_days.append(
            parse_daily_payload(
                latvia_iub_procurement_object_store.read_bytes(
                    str(key), bucket=tables.S3_BUCKET
                ),
                publication_date=day,
                source_run_id=str(manifest["source_run_id"]),
                source_object_key=str(key),
                source_retrieved_at=source_retrieved_at,
                resolved_at=datetime.now(UTC),
            )
        )
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with latvia_iub_procurement_duckdb.get_connection() as connection:
        counts = write_parsed_month(connection, parsed_days)
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="latvia_iub_procurement_winners_usd",
    deps=[dg.AssetKey("latvia_iub_procurement_normalized_duckdb")],
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb"},
    description=(
        "Converts IUB tender-level EUR values to USD using publication date. "
        "Consortium values remain marked non-attributable to each party."
    ),
)
def latvia_iub_procurement_winners_usd(
    context: dg.AssetExecutionContext,
    latvia_iub_procurement_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with latvia_iub_procurement_duckdb.get_connection() as connection:
        counts = apply_eur_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            qualified_table=(
                f"{tables.DUCKDB_SCHEMA}.{tables.WINNER_CANDIDATES_TABLE}"
            ),
            rate_date_columns=("publication_date",),
            amount_columns=(("tender_value_amount_eur", "tender_value_amount_usd"),),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="latvia_iub_procurement_clickhouse",
    deps=[
        dg.AssetKey("latvia_iub_procurement_winners_usd"),
        dg.AssetKey("latvia_ur_clickhouse_companies"),
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
            f"{tables.CLICKHOUSE_DATABASE}.{tables.EXECUTIONS_TABLE}",
        ]
    },
    description=(
        "Idempotently replaces one IUB publication month in ClickHouse, "
        "resolving domestic winner registration codes against lv_companies. "
        "Execution notices stay separate and never create a second award."
    ),
)
def latvia_iub_procurement_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    latvia_iub_procurement_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(
            tables.NOTICES_TABLE,
            tables.LOTS_TABLE,
            tables.WINNERS_TABLE,
            tables.EXECUTIONS_TABLE,
            "lv_companies",
        ),
    )
    with (
        latvia_iub_procurement_duckdb.get_connection() as connection,
        clickhouse.get_connection() as client,
    ):
        counts = export_iub_partition(
            duckdb_connection=connection,
            clickhouse_client=client,
            partition_key=context.partition_key,
        )
    return dg.MaterializeResult(metadata=counts)


latvia_iub_procurement_backfill_job = dg.define_asset_job(
    "latvia_iub_procurement_backfill_job",
    selection=dg.AssetSelection.assets("latvia_iub_procurement_clickhouse").upstream(),
)

defs = dg.Definitions(
    assets=[
        latvia_iub_procurement_daily_json_s3,
        latvia_iub_procurement_normalized_duckdb,
        latvia_iub_procurement_winners_usd,
        latvia_iub_procurement_clickhouse,
    ],
    jobs=[latvia_iub_procurement_backfill_job],
    resources={
        "latvia_iub_procurement_duckdb": duckdb_resource(DUCKDB_PATH),
        "latvia_iub_procurement_object_store": ObjectStoreResource(
            bucket=tables.S3_BUCKET
        ),
    },
)
