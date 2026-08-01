import re
import shutil
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.brazil_pncp import tables
from dagster_v3.defs.brazil_pncp.clickhouse import (
    export_contracts_clickhouse,
    insert_contracts_clickhouse,
)
from dagster_v3.defs.brazil_pncp.normalize import (
    build_contract_candidates,
    load_raw_pages,
)
from dagster_v3.defs.brazil_pncp.resources import (
    download_partition,
    month_bounds,
    trailing_window,
)
from dagster_v3.defs.brazil_pncp.translation import (
    brazil_pncp_translation_load,
    brazil_pncp_translator_queue_health_check,
)
from dagster_v3.defs.brazil_pncp.usd_conversion import apply_brazil_pncp_usd_conversion
from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.common.resources import ObjectStoreResource

DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME
# Local scratch only. The durable record is S3: re-fetching a month costs
# hundreds of rate-limited requests, and the full history ~6,400, so the raw
# pages must outlive the local disk.
PAGE_SCRATCH = Path("data") / "brazil_pncp_pages"

# PNCP's earliest contracts. 2022-01 holds 203 of them -- the register was
# barely adopted before Lei 14.133 took full effect -- so the early partitions
# cost almost nothing to backfill.
CONTRACTS_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2022-01-01", end_offset=1
)


def _page_number(key: str) -> int | None:
    """The page number in a ``page-00042.jsonl`` name or S3 key, if it is one."""
    match = re.search(r"page-(\d+)\.jsonl$", key)
    return int(match.group(1)) if match else None


def _session():
    """dlt's session retries 429 with backoff.

    PNCP returns no Retry-After and no rate-limit headers of any kind, so a
    client cannot be told when to retry and must back off blindly. That is
    exactly what this gives, and it is why the guidelines mandate it over plain
    requests.
    """
    return dlt_requests.Client(raise_for_status=False).session


@dg.asset(
    name="brazil_pncp_raw_pages_s3",
    partitions_def=CONTRACTS_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "s3"},
    # A transient failure already occurred inside the first 80 pages. Over the
    # ~6,400 requests a full backfill costs they are routine, and without this
    # an overnight run is found stalled rather than done. Retrying is cheap
    # because the per-page cache makes a retry resume rather than re-fetch.
    retry_policy=dg.RetryPolicy(
        max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL
    ),
    description=(
        "One month of PNCP contracts, page by page, snapshotted to S3. The "
        "snapshot is the point: re-fetching a month is hundreds of "
        "rate-limited requests and the full history about 6,400, so changing "
        "how contracts are normalised must never mean paying for the fetch "
        "again. Local pages are scratch that makes a dying run resumable."
    ),
)
def brazil_pncp_raw_pages_s3(
    context: dg.AssetExecutionContext,
    brazil_pncp_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    partition = context.partition_key
    month = partition[:7]
    start, end = month_bounds(partition)
    scratch = PAGE_SCRATCH / month
    scratch.mkdir(parents=True, exist_ok=True)

    brazil_pncp_object_store.ensure_bucket(tables.S3_BUCKET)
    prefix = f"{tables.S3_RAW_PREFIX}/{month}"

    # Resume from the SNAPSHOT, not from local scratch. download_partition
    # resumes at (pages on disk + 1), and on a machine that never fetched this
    # month -- a fresh deploy, another host, or any month whose scratch
    # brazil_pncp_page_cache_cleanup already cleared by design -- that disk is
    # empty, so a re-materialization silently re-paid for every page. With 37
    # months already snapshotted that was ~3,000 rate-limited requests to
    # rebuild data we already had.
    #
    # Only the page COUNT is needed to resume, so the pages themselves are left
    # in S3 rather than copied down and pushed straight back up: for the stored
    # history that round trip is ~2.6 GB each way for nothing. A complete month
    # now costs one list call and one request -- the page past the end, which
    # PNCP rejects explicitly rather than returning empty.
    snapshotted = sorted(
        _page_number(key)
        for key in brazil_pncp_object_store.list_keys(prefix, bucket=tables.S3_BUCKET)
        if _page_number(key)
    )
    # Resuming after N pages is only sound if those pages ARE 1..N. A gap means
    # some earlier page is missing, and continuing past it would leave a hole
    # that nothing downstream could detect -- the month would simply be short.
    if snapshotted and snapshotted != list(range(1, len(snapshotted) + 1)):
        missing = sorted(set(range(1, max(snapshotted) + 1)) - set(snapshotted))
        raise ValueError(
            f"PNCP snapshot for {month} is missing page(s) {missing[:10]} of "
            f"{max(snapshotted)}; refusing to resume past a gap. Delete the "
            f"partition's objects under {prefix} to re-fetch it cleanly."
        )
    if snapshotted:
        context.log.info(
            "%s: %s pages already snapshotted, resuming at %s",
            month,
            len(snapshotted),
            len(snapshotted) + 1,
        )

    def _log_progress(progress: dict) -> None:
        # A 340-page month is otherwise silent until it finishes, which makes a
        # stall indistinguishable from slow going. pages_per_minute is the
        # signal worth watching: PNCP sends no rate-limit headers, so backoff
        # shows up only as that number falling.
        context.log.info(
            "%s page %s/%s: %s records fetched, %.1f pages/min, %.0fs elapsed",
            month,
            progress["page"],
            progress["total_pages"],
            progress["records_fetched"],
            progress["pages_per_minute"],
            progress["elapsed_seconds"],
        )

    downloaded = download_partition(
        _session(),
        destination=scratch,
        path=tables.CONTRACTS_BY_PUBLICATION_PATH,
        start=start,
        end=end,
        on_progress=_log_progress,
        already_snapshotted=len(snapshotted),
    )

    # Only pages this run actually fetched. Re-uploading a page that is already
    # in S3 rewrites an identical object -- the same waste as re-downloading it,
    # and the whole history is 2.6 GB of them.
    uploaded = reused = 0
    for page_file in sorted(scratch.glob("page-*.jsonl")):
        if _page_number(page_file.name) in set(snapshotted):
            reused += 1
            continue
        brazil_pncp_object_store.upload_file(
            f"{prefix}/{page_file.name}", page_file, bucket=tables.S3_BUCKET
        )
        uploaded += 1

    context.log.info(
        "PNCP %s..%s: %s pages total (%s already snapshotted, resumed from page "
        "%s), %s newly uploaded, %s reused",
        start,
        end,
        downloaded["pages"],
        len(snapshotted),
        downloaded["resumed_from_page"],
        uploaded,
        reused,
    )
    return dg.MaterializeResult(
        metadata={
            **downloaded,
            "pages_already_snapshotted": len(snapshotted),
            "pages_uploaded": uploaded,
            "pages_reused": reused,
            "s3_prefix": prefix,
        }
    )


@dg.asset(
    name="brazil_pncp_contracts_duckdb",
    deps=[dg.AssetKey("brazil_pncp_raw_pages_s3")],
    partitions_def=CONTRACTS_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "duckdb"},
    description=(
        "Normalises a month's snapshotted pages into a durable partition of "
        "typed DuckDB candidates. Reads from S3, not from local scratch, so "
        "re-normalising never re-fetches and works on a machine that never "
        "did the download."
    ),
)
def brazil_pncp_contracts_duckdb(
    context: dg.AssetExecutionContext,
    brazil_pncp_duckdb: DuckDBResource,
    brazil_pncp_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    month = context.partition_key[:7]
    partition = month.replace("-", "")
    prefix = f"{tables.S3_RAW_PREFIX}/{month}"
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    keys = brazil_pncp_object_store.list_keys(prefix, bucket=tables.S3_BUCKET)
    if not keys:
        raise ValueError(
            f"No snapshotted pages under {prefix}; refusing to normalise a "
            f"month whose raw data was never stored"
        )

    with tempfile.TemporaryDirectory(prefix="brazil_pncp_") as temp_dir:
        page_dir = Path(temp_dir)
        for key in keys:
            brazil_pncp_object_store.download_file(
                key, page_dir / Path(key).name, bucket=tables.S3_BUCKET
            )

        source_run_id = uuid.uuid4().hex
        now = datetime.now(UTC)
        with brazil_pncp_duckdb.get_connection() as connection:
            raw_rows = load_raw_pages(
                connection=connection,
                page_dir=page_dir,
                source_run_id=source_run_id,
                source_retrieved_at=now,
            )
            counts = build_contract_candidates(
                connection=connection,
                source_run_id=source_run_id,
                resolved_at=now,
                partition=partition,
            )

    return dg.MaterializeResult(
        metadata={
            "partition": partition,
            "table": f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}",
            "raw_rows": raw_rows,
            "pages_read": len(keys),
            **counts,
        }
    )


@dg.asset(
    name="brazil_pncp_contracts_usd",
    deps=[dg.AssetKey("brazil_pncp_contracts_duckdb")],
    partitions_def=CONTRACTS_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb"},
    description=(
        "Converts each contract's valor_global to USD via the shared "
        "ExchangeRateClient, keyed on the signature date and falling back to "
        "the publication date. A separate step from extraction so a rate "
        "correction never means re-parsing contracts. Until it runs, every "
        "Brazilian value is BRL with a NULL USD and cannot be ranked against "
        "another country."
    ),
)
def brazil_pncp_contracts_usd(
    context: dg.AssetExecutionContext,
    brazil_pncp_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    partition = context.partition_key[:7].replace("-", "")
    with brazil_pncp_duckdb.get_connection() as connection:
        counts = apply_brazil_pncp_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
            partition=partition,
        )
    return dg.MaterializeResult(metadata={"partition": partition, **counts})


@dg.asset(
    name="brazil_pncp_contracts_clickhouse",
    deps=[dg.AssetKey("brazil_pncp_contracts_usd")],
    partitions_def=CONTRACTS_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql"},
    metadata={"tables": [tables.QUALIFIED_CONTRACTS_TABLE]},
    description=(
        "Replaces one month's partition of br_pncp_contracts, resolving each "
        "supplier's 14-digit CNPJ to its 8-digit company base against "
        "br_companies. Re-running a month replaces it rather than appending a "
        "second copy."
    ),
)
def brazil_pncp_contracts_clickhouse(
    context: dg.AssetExecutionContext,
    brazil_pncp_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.CONTRACTS_TABLE, "br_companies"),
    )
    partition = context.partition_key[:7].replace("-", "")

    with (
        brazil_pncp_duckdb.get_connection() as connection,
        clickhouse.get_connection() as client,
    ):
        result = export_contracts_clickhouse(
            duckdb_connection=connection,
            clickhouse_client=client,
            partition=partition,
        )

    matched = result["matched_companies"]
    total = result["contract_rows"]
    context.log.info(
        "%s: %s contracts, %s matched to a company (%.1f%%)",
        context.partition_key,
        total,
        matched,
        (matched / total * 100) if total else 0.0,
    )
    return dg.MaterializeResult(metadata=result)


@dg.asset(
    name="brazil_pncp_page_cache_cleanup",
    deps=[dg.AssetKey("brazil_pncp_contracts_clickhouse")],
    partitions_def=CONTRACTS_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    group_name=tables.GROUP_NAME,
    kinds={"python"},
    description=(
        "Clears a partition's LOCAL scratch pages once its rows are in "
        "ClickHouse. The S3 snapshot is deliberately kept: it is the only copy "
        "that survives this machine, and re-fetching it costs hundreds of "
        "rate-limited requests."
    ),
)
def brazil_pncp_page_cache_cleanup(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    scratch = PAGE_SCRATCH / context.partition_key[:7]
    freed = (
        sum(f.stat().st_size for f in scratch.glob("*.jsonl"))
        if scratch.exists()
        else 0
    )
    shutil.rmtree(scratch, ignore_errors=True)
    return dg.MaterializeResult(metadata={"freed_bytes": freed})


@dg.asset(
    name="brazil_pncp_daily_pages_s3",
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "s3"},
    retry_policy=dg.RetryPolicy(
        max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL
    ),
    description=(
        "A trailing 7-day window from BOTH PNCP endpoints, snapshotted to S3. "
        "/contratos finds newly published contracts; /contratos/atualizacao "
        "finds amendments to older ones, which the publication feed "
        "structurally cannot see. ~213 pages a run."
    ),
)
def brazil_pncp_daily_pages_s3(
    context: dg.AssetExecutionContext,
    brazil_pncp_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    today = datetime.now(UTC).date()
    start, end = trailing_window(today, days=tables.DAILY_WINDOW_DAYS)
    brazil_pncp_object_store.ensure_bucket(tables.S3_BUCKET)

    session = _session()
    metadata: dict[str, object] = {"window_start": start, "window_end": end}
    total_pages = 0
    for label, path in (
        ("publication", tables.CONTRACTS_BY_PUBLICATION_PATH),
        ("update", tables.CONTRACTS_BY_UPDATE_PATH),
    ):
        scratch = PAGE_SCRATCH / "daily" / f"{today.isoformat()}-{label}"
        # A retry re-enters here, so the window must be re-read from scratch
        # that this run wrote, never from a previous day's leftovers.
        shutil.rmtree(scratch, ignore_errors=True)
        prefix = f"{tables.S3_DAILY_PREFIX}/{today.isoformat()}/{label}"

        def _log(progress: dict, label: str = label) -> None:
            context.log.info(
                "%s %s..%s page %s/%s: %s records, %.1f pages/min",
                label,
                start,
                end,
                progress["page"],
                progress["total_pages"],
                progress["records_fetched"],
                progress["pages_per_minute"],
            )

        downloaded = download_partition(
            session,
            destination=scratch,
            path=path,
            start=start,
            end=end,
            on_progress=_log,
        )
        for page_file in sorted(scratch.glob("page-*.jsonl")):
            brazil_pncp_object_store.upload_file(
                f"{prefix}/{page_file.name}", page_file, bucket=tables.S3_BUCKET
            )
        total_pages += downloaded["pages"]
        metadata[f"{label}_pages"] = downloaded["pages"]
        metadata[f"{label}_records"] = downloaded["records_fetched"]
        metadata[f"{label}_prefix"] = prefix

    # Both endpoints returning nothing across a whole week is a broken fetch,
    # not a quiet week in Brazilian public procurement.
    if total_pages == 0:
        raise ValueError(
            f"PNCP returned no contracts at all for {start}..{end} from either "
            f"endpoint; refusing to treat that as a quiet week"
        )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="brazil_pncp_daily_duckdb",
    deps=[dg.AssetKey("brazil_pncp_daily_pages_s3")],
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "duckdb"},
    description=(
        "Normalises the day's window into typed candidates, reading both "
        "endpoints' pages together so a contract seen by both is normalised "
        "once."
    ),
)
def brazil_pncp_daily_duckdb(
    context: dg.AssetExecutionContext,
    brazil_pncp_duckdb: DuckDBResource,
    brazil_pncp_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    today = datetime.now(UTC).date()
    prefix = f"{tables.S3_DAILY_PREFIX}/{today.isoformat()}"
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    keys = brazil_pncp_object_store.list_keys(prefix, bucket=tables.S3_BUCKET)
    if not keys:
        raise ValueError(
            f"No snapshotted pages under {prefix}; refusing to normalise a "
            f"window whose raw data was never stored"
        )

    with tempfile.TemporaryDirectory(prefix="brazil_pncp_daily_") as temp_dir:
        page_dir = Path(temp_dir)
        for key in keys:
            # Both endpoints number their pages from 1, so the filenames
            # collide. Qualify by endpoint or half the window is overwritten.
            endpoint = Path(key).parent.name
            brazil_pncp_object_store.download_file(
                key, page_dir / f"{endpoint}-{Path(key).name}", bucket=tables.S3_BUCKET
            )

        source_run_id = uuid.uuid4().hex
        now = datetime.now(UTC)
        with brazil_pncp_duckdb.get_connection() as connection:
            raw_rows = load_raw_pages(
                connection=connection,
                page_dir=page_dir,
                source_run_id=source_run_id,
                source_retrieved_at=now,
                raw_table=tables.DAILY_RAW_TABLE,
            )
            counts = build_contract_candidates(
                connection=connection,
                source_run_id=source_run_id,
                resolved_at=now,
                raw_table=tables.DAILY_RAW_TABLE,
                candidates_table=tables.DAILY_CANDIDATES_TABLE,
            )

    return dg.MaterializeResult(
        metadata={"raw_rows": raw_rows, "pages_read": len(keys), **counts}
    )


@dg.asset(
    name="brazil_pncp_daily_usd",
    deps=[dg.AssetKey("brazil_pncp_daily_duckdb")],
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "duckdb"},
    description="Converts the day's window to USD, as for the backfill.",
)
def brazil_pncp_daily_usd(
    context: dg.AssetExecutionContext,
    brazil_pncp_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with brazil_pncp_duckdb.get_connection() as connection:
        counts = apply_brazil_pncp_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
            candidates_table=tables.DAILY_CANDIDATES_TABLE,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="brazil_pncp_daily_clickhouse",
    deps=[dg.AssetKey("brazil_pncp_daily_usd")],
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"clickhouse", "sql"},
    metadata={"tables": [tables.QUALIFIED_CONTRACTS_TABLE]},
    description=(
        "Appends the day's window to br_pncp_contracts. Appends rather than "
        "replacing a partition: the window is a slice of a month, and "
        "replacing 202607 with seven days of it would delete the other three "
        "weeks. ReplacingMergeTree collapses what overlaps."
    ),
)
def brazil_pncp_daily_clickhouse(
    context: dg.AssetExecutionContext,
    brazil_pncp_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.CONTRACTS_TABLE, "br_companies"),
    )
    with (
        brazil_pncp_duckdb.get_connection() as connection,
        clickhouse.get_connection() as client,
    ):
        result = insert_contracts_clickhouse(
            duckdb_connection=connection, clickhouse_client=client
        )
    matched = result["matched_companies"]
    total = result["contract_rows"]
    context.log.info(
        "daily window: %s contracts appended, %s matched to a company (%.1f%%)",
        total,
        matched,
        (matched / total * 100) if total else 0.0,
    )
    return dg.MaterializeResult(metadata=result)


brazil_pncp_backfill_job = dg.define_asset_job(
    "brazil_pncp_backfill_job",
    selection=dg.AssetSelection.assets(
        "brazil_pncp_raw_pages_s3",
        "brazil_pncp_contracts_duckdb",
        "brazil_pncp_contracts_usd",
        "brazil_pncp_contracts_clickhouse",
        "brazil_pncp_page_cache_cleanup",
    ),
)

# Deliberately no schedule on the backfill. It loads 2022-01 to the present once
# and afterwards exists only to repair a specific month -- re-fetching a
# 340-page month to pick up one day's contracts would be absurd when a day is
# 12 pages. Launch it from the UI, not the CLI: the raw `dagster` CLI tags runs
# with a code-location name that does not match `dg dev`'s, orphaning them.

brazil_pncp_daily_job = dg.define_asset_job(
    "brazil_pncp_daily_job",
    selection=dg.AssetSelection.assets(
        "brazil_pncp_daily_pages_s3",
        "brazil_pncp_daily_duckdb",
        "brazil_pncp_daily_usd",
        "brazil_pncp_daily_clickhouse",
    ),
)

# 04:20 UTC = 01:20 in Brasilia, so the window closes on a finished Brazilian
# day rather than mid-afternoon. The odd minute is the staggering the
# guidelines ask for: at 100+ sources, every schedule on :00 is a thundering
# herd against one Postgres.
brazil_pncp_daily_schedule = dg.ScheduleDefinition(
    name="brazil_pncp_daily_schedule",
    job=brazil_pncp_daily_job,
    cron_schedule="20 4 * * *",
    execution_timezone="UTC",
)

defs = dg.Definitions(
    assets=[
        brazil_pncp_raw_pages_s3,
        brazil_pncp_contracts_duckdb,
        brazil_pncp_contracts_usd,
        brazil_pncp_contracts_clickhouse,
        brazil_pncp_page_cache_cleanup,
        brazil_pncp_daily_pages_s3,
        brazil_pncp_daily_duckdb,
        brazil_pncp_daily_usd,
        brazil_pncp_daily_clickhouse,
        brazil_pncp_translation_load,
    ],
    asset_checks=[brazil_pncp_translator_queue_health_check],
    jobs=[brazil_pncp_backfill_job, brazil_pncp_daily_job],
    schedules=[brazil_pncp_daily_schedule],
    resources={
        "brazil_pncp_duckdb": duckdb_resource(DUCKDB_PATH),
        "brazil_pncp_object_store": ObjectStoreResource(bucket=tables.S3_BUCKET),
    },
)
