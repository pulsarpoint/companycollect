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
from dagster_v3.defs.slovakia_uvo_procurement import tables
from dagster_v3.defs.slovakia_uvo_procurement.clickhouse import (
    export_uvo_partition,
)
from dagster_v3.defs.slovakia_uvo_procurement.duckdb import replace_candidates
from dagster_v3.defs.slovakia_uvo_procurement.parser import parse_result_notice
from dagster_v3.defs.slovakia_uvo_procurement.resources import (
    latest_month_manifest,
    sync_uvo_month,
)

# No shared DuckDB path: every partitioned asset opens its own file, so an
# export cannot read another month's rows. See
# defs/common/partition_duckdb.py.
SOURCE_NAME = "slovakia_uvo_procurement"
MONTHLY_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date=tables.PARTITION_START_DATE,
    end_offset=1,
)
BACKFILL_POLICY = dg.BackfillPolicy.multi_run(max_partitions_per_run=1)


@dg.asset(
    name="slovakia_uvo_procurement_html_s3",
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    group_name=tables.GROUP_NAME,
    kinds={"python", "html", "s3"},
    description=(
        "Enumerates one month of official UVO bulletin issues and snapshots "
        "result-notice HTML to S3. Network access is blocked until the current "
        "machine-reuse licence or written permission is explicitly confirmed."
    ),
)
def slovakia_uvo_procurement_html_s3(
    context: dg.AssetExecutionContext,
    slovakia_uvo_procurement_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    snapshot = sync_uvo_month(
        object_store=slovakia_uvo_procurement_object_store,
        partition_key=context.partition_key,
        run_id=context.run_id,
        retrieved_at=dagster_now(),
    )
    return dg.MaterializeResult(
        metadata={
            "issue_files": snapshot.issue_files,
            "result_notices": len(snapshot.detail_keys),
            "details_downloaded": snapshot.details_downloaded,
            "details_reused": snapshot.details_reused,
            "manifest_key": snapshot.manifest_key,
        }
    )


@dg.asset(
    name="slovakia_uvo_procurement_notices_duckdb",
    deps=[dg.AssetKey("slovakia_uvo_procurement_html_s3")],
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "html", "s3", "duckdb"},
    description=(
        "Parses UVO result pages to one row per (notice, lot, winning tenderer), "
        "preserving IČO, buyer, CPV, dates, tender ranges, and BT-720 values."
    ),
)
def slovakia_uvo_procurement_notices_duckdb(
    context: dg.AssetExecutionContext,
    slovakia_uvo_procurement_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    manifest = latest_month_manifest(
        slovakia_uvo_procurement_object_store, context.partition_key
    )
    retrieved_at = datetime.fromisoformat(str(manifest["retrieved_at"]))
    resolved_at = dagster_now()
    rows = []
    for metadata in manifest["detail_metadata"]:
        key = str(metadata["object_key"])
        rows.extend(
            parse_result_notice(
                slovakia_uvo_procurement_object_store.read_bytes(
                    key, bucket=tables.S3_BUCKET
                ),
                uvo_notice_id=str(metadata["uvo_notice_id"]),
                bulletin_number=str(metadata["bulletin_number"]),
                bulletin_code=str(metadata["bulletin_code"]),
                publication_date=datetime.fromisoformat(
                    str(metadata["publication_date"])
                ).date(),
                source_run_id=str(manifest["source_run_id"]),
                source_object_key=key,
                source_retrieved_at=retrieved_at,
                resolved_at=resolved_at,
            )
        )
    with open_partition_duckdb(
        source=SOURCE_NAME, partition=context.partition_key
    ) as connection:
        written = replace_candidates(connection, rows)
    return dg.MaterializeResult(metadata={"notice_winner_rows": written})


@dg.asset(
    name="slovakia_uvo_procurement_notices_usd",
    deps=[dg.AssetKey("slovakia_uvo_procurement_notices_duckdb")],
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb"},
    description=(
        "Converts winner-attributable UVO BT-720 EUR values to USD using the "
        "notice publication date."
    ),
)
def slovakia_uvo_procurement_notices_usd(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with require_partition_duckdb(
        source=SOURCE_NAME, partition=context.partition_key
    ) as connection:
        counts = apply_eur_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            qualified_table=(f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"),
            rate_date_columns=("publication_date",),
            amount_columns=(("awarded_amount_eur", "awarded_amount_usd"),),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="slovakia_uvo_procurement_notices_clickhouse",
    deps=[
        dg.AssetKey("slovakia_uvo_procurement_notices_usd"),
        dg.AssetKey("slovakia_rpo_clickhouse_companies"),
    ],
    partitions_def=MONTHLY_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    metadata={"table": tables.QUALIFIED_NOTICES_TABLE},
    description=(
        "Idempotently replaces one UVO publication month and resolves exact "
        "eight-digit winner IČO values against sk_companies."
    ),
)
def slovakia_uvo_procurement_notices_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.NOTICES_TABLE, "sk_companies"),
    )
    with (
        require_partition_duckdb(
            source=SOURCE_NAME, partition=context.partition_key
        ) as connection,
        clickhouse.get_connection() as client,
    ):
        counts = export_uvo_partition(
            duckdb_connection=connection,
            clickhouse_client=client,
            partition_key=context.partition_key,
        )
    return dg.MaterializeResult(metadata=counts)


def dagster_now() -> datetime:
    return datetime.now(UTC)


slovakia_uvo_procurement_backfill_job = dg.define_asset_job(
    "slovakia_uvo_procurement_backfill_job",
    selection=dg.AssetSelection.assets(
        "slovakia_uvo_procurement_notices_clickhouse"
    ).upstream(),
)

defs = dg.Definitions(
    assets=[
        slovakia_uvo_procurement_html_s3,
        slovakia_uvo_procurement_notices_duckdb,
        slovakia_uvo_procurement_notices_usd,
        slovakia_uvo_procurement_notices_clickhouse,
    ],
    jobs=[slovakia_uvo_procurement_backfill_job],
    resources={
        "slovakia_uvo_procurement_object_store": ObjectStoreResource(
            bucket=tables.S3_BUCKET
        ),
    },
)
