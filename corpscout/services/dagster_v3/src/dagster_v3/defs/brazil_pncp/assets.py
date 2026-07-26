import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.brazil_pncp import tables
from dagster_v3.defs.brazil_pncp.clickhouse import export_contracts_clickhouse
from dagster_v3.defs.brazil_pncp.normalize import (
    build_contract_candidates,
    load_raw_pages,
)
from dagster_v3.defs.brazil_pncp.resources import download_partition, month_bounds
from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.duckdb_resources import duckdb_resource

DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME
PAGE_CACHE = Path("data") / "brazil_pncp_pages"

# PNCP's earliest contracts. 2022-01 holds 203 of them -- the register was
# barely adopted before Lei 14.133 took full effect -- so the early partitions
# cost almost nothing to backfill.
CONTRACTS_PARTITIONS = dg.MonthlyPartitionsDefinition(
    start_date="2022-01-01", end_offset=1
)


def _session():
    """dlt's session retries 429 with backoff.

    PNCP returns no Retry-After and no rate-limit headers of any kind, so a
    client cannot be told when to retry and must back off blindly. That is
    exactly what this gives, and it is why the guidelines mandate it over plain
    requests.
    """
    return dlt_requests.Client(raise_for_status=False).session


@dg.asset(
    name="brazil_pncp_contracts_duckdb",
    partitions_def=CONTRACTS_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=tables.DUCKDB_POOL,
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "duckdb"},
    description=(
        "One month of PNCP contracts, fetched page by page and normalised into "
        "typed candidates. Pages are cached per partition, so a run that dies "
        "part-way resumes rather than re-paying for what it already fetched."
    ),
)
def brazil_pncp_contracts_duckdb(
    context: dg.AssetExecutionContext,
    brazil_pncp_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    partition = context.partition_key
    start, end = month_bounds(partition)
    page_dir = PAGE_CACHE / partition[:7]
    page_dir.mkdir(parents=True, exist_ok=True)
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

    downloaded = download_partition(
        _session(),
        destination=page_dir,
        path=tables.CONTRACTS_BY_PUBLICATION_PATH,
        start=start,
        end=end,
    )
    context.log.info(
        "PNCP %s..%s: %s records over %s pages (resumed from page %s)",
        start,
        end,
        downloaded["records"],
        downloaded["pages"],
        downloaded["resumed_from_page"],
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
            connection=connection, source_run_id=source_run_id, resolved_at=now
        )

    return dg.MaterializeResult(
        metadata={"raw_rows": raw_rows, **downloaded, **counts}
    )


@dg.asset(
    name="brazil_pncp_contracts_clickhouse",
    deps=[dg.AssetKey("brazil_pncp_contracts_duckdb")],
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

    with brazil_pncp_duckdb.get_connection() as connection, (
        clickhouse.get_connection()
    ) as client:
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
        "Drops a partition's cached pages once its rows are in ClickHouse. The "
        "cache exists to make a failed run resumable, not to be kept: a full "
        "backfill is ~3.2M records of JSON."
    ),
)
def brazil_pncp_page_cache_cleanup(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    page_dir = PAGE_CACHE / context.partition_key[:7]
    freed = sum(f.stat().st_size for f in page_dir.glob("*.jsonl")) if page_dir.exists() else 0
    shutil.rmtree(page_dir, ignore_errors=True)
    return dg.MaterializeResult(metadata={"freed_bytes": freed})


brazil_pncp_backfill_job = dg.define_asset_job(
    "brazil_pncp_backfill_job",
    selection=dg.AssetSelection.assets(
        "brazil_pncp_contracts_duckdb",
        "brazil_pncp_contracts_clickhouse",
        "brazil_pncp_page_cache_cleanup",
    ),
)

# Deliberately no schedule on the backfill. It loads 2022-01 to the present once
# and afterwards exists only to repair a specific month -- re-fetching a
# 340-page month to pick up one day's contracts would be absurd when a day is
# 12 pages. Launch it from the UI, not the CLI: the raw `dagster` CLI tags runs
# with a code-location name that does not match `dg dev`'s, orphaning them.

defs = dg.Definitions(
    assets=[
        brazil_pncp_contracts_duckdb,
        brazil_pncp_contracts_clickhouse,
        brazil_pncp_page_cache_cleanup,
    ],
    jobs=[brazil_pncp_backfill_job],
    resources={"brazil_pncp_duckdb": duckdb_resource(DUCKDB_PATH)},
)
