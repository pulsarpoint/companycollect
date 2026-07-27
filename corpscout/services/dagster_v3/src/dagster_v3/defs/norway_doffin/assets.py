import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.norway_doffin import tables
from dagster_v3.defs.norway_doffin.clickhouse import export_notices_clickhouse
from dagster_v3.defs.norway_doffin.normalize import build_candidate_rows, write_candidates
from dagster_v3.defs.norway_doffin.parser import parse_notice_amounts
from dagster_v3.defs.norway_doffin.resources import (
    fetch_notice_xml,
    iter_search_hits,
    month_bounds,
)
from dagster_v3.defs.norway_doffin.usd_conversion import (
    apply_norway_doffin_usd_conversion,
)

DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME

# Monthly, keyed on ISSUE date -- publication date is not filterable, so it is
# the only partition key the API can actually serve. Nothing exists before 2018,
# and the busiest month measured is 378 notices against a 1,000-result ceiling.
NOTICE_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date=tables.PARTITION_START_DATE, end_offset=1
)

BACKFILL_POLICY = dg.BackfillPolicy.multi_run(max_partitions_per_run=1)


def _session():
    """dlt's session retries 429 with blind backoff.

    Doffin's 429 carries no Retry-After and no rate-limit headers, and clears
    within seconds -- so blind backoff is not a compromise, it is the only
    handling the server makes possible.
    """
    return dlt_requests.Client(raise_for_status=False).session


@dg.asset(
    name="norway_doffin_notices_s3",
    partitions_def=NOTICE_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "s3"},
    retry_policy=dg.RetryPolicy(
        max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL
    ),
    description=(
        "One month of Doffin award notices: the search listing plus each "
        "notice's eForms XML, snapshotted to S3. Both halves are needed -- the "
        "search names the winners and the XML carries the realized money -- and "
        "the snapshot means a parser change never re-pays for ~300 requests a "
        "month."
    ),
)
def norway_doffin_notices_s3(
    context: dg.AssetExecutionContext,
    norway_doffin_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    partition = context.partition_key
    issue_from, issue_to = month_bounds(partition)
    norway_doffin_object_store.ensure_bucket(tables.S3_BUCKET)
    search_key = f"{tables.S3_SEARCH_PREFIX}/{partition}/hits.jsonl"
    xml_prefix = f"{tables.S3_NOTICE_PREFIX}/{partition}"

    session = _session()
    hits = list(
        iter_search_hits(
            session,
            issue_date_from=issue_from,
            issue_date_to=issue_to,
            on_page=lambda page, total, seen: context.log.info(
                "%s search page %s: %s/%s notices", partition, page, seen, total
            ),
        )
    )
    norway_doffin_object_store.write_bytes(
        search_key,
        "\n".join(json.dumps(hit, ensure_ascii=False) for hit in hits).encode(),
        bucket=tables.S3_BUCKET,
    )

    downloaded = reused = 0
    for index, hit in enumerate(hits, 1):
        doffin_id = str(hit.get("id") or "")
        if not doffin_id:
            continue
        key = f"{xml_prefix}/{doffin_id}.xml"
        # Reuse what the snapshot already holds, as ted_monthly_snapshot does.
        # A re-materialization after a parser change must not re-fetch ~300
        # notices it already has.
        if norway_doffin_object_store.exists(key, bucket=tables.S3_BUCKET):
            reused += 1
            continue
        norway_doffin_object_store.write_bytes(
            key,
            fetch_notice_xml(session, doffin_id=doffin_id),
            bucket=tables.S3_BUCKET,
        )
        downloaded += 1
        if index % 50 == 0:
            context.log.info(
                "%s: %s/%s notices (%s downloaded, %s reused)",
                partition, index, len(hits), downloaded, reused,
            )

    return dg.MaterializeResult(
        metadata={
            "issue_date_from": issue_from,
            "issue_date_to": issue_to,
            "notices": len(hits),
            "xml_downloaded": downloaded,
            "xml_reused": reused,
            "s3_prefix": xml_prefix,
        }
    )


@dg.asset(
    name="norway_doffin_notices_duckdb",
    deps=[dg.AssetKey("norway_doffin_notices_s3")],
    partitions_def=NOTICE_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "duckdb"},
    description=(
        "Joins each search hit to its notice XML into (notice, lot, winner) "
        "rows. Reads from S3, not local scratch, so re-normalising never "
        "re-fetches and works on a machine that never did the download."
    ),
)
def norway_doffin_notices_duckdb(
    context: dg.AssetExecutionContext,
    norway_doffin_duckdb: DuckDBResource,
    norway_doffin_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    partition = context.partition_key
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    search_key = f"{tables.S3_SEARCH_PREFIX}/{partition}/hits.jsonl"
    if not norway_doffin_object_store.exists(search_key, bucket=tables.S3_BUCKET):
        raise ValueError(
            f"No snapshotted search listing at {search_key}; materialize "
            f"norway_doffin_notices_s3 for this partition first"
        )
    hits = [
        json.loads(line)
        for line in norway_doffin_object_store.read_bytes(
            search_key, bucket=tables.S3_BUCKET
        )
        .decode()
        .splitlines()
        if line.strip()
    ]

    source_run_id = uuid.uuid4().hex
    now = datetime.now(UTC)
    rows: list[tuple] = []
    missing_xml = 0
    for hit in hits:
        doffin_id = str(hit.get("id") or "")
        key = f"{tables.S3_NOTICE_PREFIX}/{partition}/{doffin_id}.xml"
        if not norway_doffin_object_store.exists(key, bucket=tables.S3_BUCKET):
            # The notice still becomes rows -- its winners are in the search
            # hit. Only the realized money is lost, so this is counted rather
            # than raised: dropping the notice would lose more.
            missing_xml += 1
            amounts = parse_notice_amounts(b"<empty/>")
        else:
            amounts = parse_notice_amounts(
                norway_doffin_object_store.read_bytes(key, bucket=tables.S3_BUCKET)
            )
        rows += build_candidate_rows(
            hit=hit,
            amounts=amounts,
            source_run_id=source_run_id,
            partition_key=partition,
            retrieved_at=now,
            resolved_at=now,
        )

    with norway_doffin_duckdb.get_connection() as connection:
        written = write_candidates(connection, rows)

    if missing_xml:
        context.log.warning(
            "%s: %s of %s notices had no snapshotted XML, so carry no realized "
            "value", partition, missing_xml, len(hits),
        )
    return dg.MaterializeResult(
        metadata={
            "notices": len(hits),
            "candidate_rows": written,
            "notices_without_xml": missing_xml,
        }
    )


@dg.asset(
    name="norway_doffin_notices_usd",
    deps=[dg.AssetKey("norway_doffin_notices_duckdb")],
    partitions_def=NOTICE_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb"},
    description=(
        "Converts all three figures to USD, keyed on issue date. A separate "
        "step from extraction, so a rate correction never means re-parsing."
    ),
)
def norway_doffin_notices_usd(
    context: dg.AssetExecutionContext,
    norway_doffin_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with norway_doffin_duckdb.get_connection() as connection:
        counts = apply_norway_doffin_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="norway_doffin_notices_clickhouse",
    deps=[dg.AssetKey("norway_doffin_notices_usd")],
    partitions_def=NOTICE_PARTITIONS,
    backfill_policy=BACKFILL_POLICY,
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql"},
    metadata={"tables": [tables.QUALIFIED_NOTICES_TABLE]},
    description=(
        "Replaces one month's partition of no_doffin_notices, resolving each "
        "winner's org number against no_companies. Foreign winners are kept "
        "and labelled rather than dropped."
    ),
)
def norway_doffin_notices_clickhouse(
    context: dg.AssetExecutionContext,
    norway_doffin_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.NOTICES_TABLE, "no_companies"),
    )
    with norway_doffin_duckdb.get_connection() as connection, (
        clickhouse.get_connection()
    ) as client:
        result = export_notices_clickhouse(
            duckdb_connection=connection,
            clickhouse_client=client,
            partition=context.partition_key,
        )
    total = result["notice_winner_rows"]
    context.log.info(
        "%s: %s rows, %s matched, %s foreign, %s with a realized value",
        context.partition_key, total, result["matched_companies"],
        result["foreign_winners"], result["rows_with_realized_value"],
    )
    return dg.MaterializeResult(metadata=result)


norway_doffin_backfill_job = dg.define_asset_job(
    "norway_doffin_backfill_job",
    selection=dg.AssetSelection.assets(
        "norway_doffin_notices_s3",
        "norway_doffin_notices_duckdb",
        "norway_doffin_notices_usd",
        "norway_doffin_notices_clickhouse",
    ),
)

# The current month's partition is re-read daily: end_offset=1 keeps it valid,
# and the XML reuse check means only genuinely new notices are fetched, so a
# daily run costs a handful of requests rather than ~300.
#
# The schedule reuses the backfill job rather than declaring a second one --
# they select the same assets, and the only difference is which partition runs.
norway_doffin_daily_schedule = dg.build_schedule_from_partitioned_job(
    norway_doffin_backfill_job,
    name="norway_doffin_daily_schedule",
    hour_of_day=4,
    minute_of_hour=40,
)

defs = dg.Definitions(
    assets=[
        norway_doffin_notices_s3,
        norway_doffin_notices_duckdb,
        norway_doffin_notices_usd,
        norway_doffin_notices_clickhouse,
    ],
    jobs=[norway_doffin_backfill_job],
    schedules=[norway_doffin_daily_schedule],
    resources={
        "norway_doffin_duckdb": duckdb_resource(DUCKDB_PATH),
        "norway_doffin_object_store": ObjectStoreResource(bucket=tables.S3_BUCKET),
    },
)
