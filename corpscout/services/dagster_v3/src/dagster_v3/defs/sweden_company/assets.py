from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company.clickhouse import (
    export_sweden_company_clickhouse_addresses,
    export_sweden_company_clickhouse_companies,
    export_sweden_company_clickhouse_industries,
)
from dagster_v3.defs.sweden_company.normalized_duckdb import (
    replace_sweden_company_normalized_tables,
)
from dagster_v3.defs.sweden_company.raw_duckdb import load_sweden_company_raw_manifest
from dagster_v3.defs.common.tags import HEAVY_BULK_RUN_TAGS
from dagster_v3.defs.sweden_company.resources import (
    SwedenCompanyBulkResource,
    manifest_for_run,
)

GROUP_NAME = "sweden_company"
SWEDEN_COMPANY_DUCKDB_POOL = "sweden_company_duckdb"


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "zip", "bolagsverket"},
    description=(
        "Downloads Sweden company bulk ZIP files from Bolagsverket high-value datasets "
        "into object storage. This asset does not parse or load the ZIP contents."
    ),
)
def sweden_company_raw_snapshot_s3(
    context: dg.AssetExecutionContext,
    sweden_company_bulk: SwedenCompanyBulkResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return sweden_company_bulk.download_snapshot(
        object_store=object_store,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
        log_info=context.log.info,
    )


@dg.asset(
    deps=["sweden_company_raw_snapshot_s3"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "s3", "zip", "bolagsverket"},
    pool=SWEDEN_COMPANY_DUCKDB_POOL,
    description=(
        "Extracts Sweden company raw ZIPs from object storage and rebuilds raw "
        "DuckDB staging tables. This asset does not normalize source records."
    ),
)
def sweden_company_raw_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    sweden_company_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    manifest = manifest_for_run(object_store, context.run_id)
    source_run_id = str(manifest["run_id"])
    with sweden_company_duckdb.get_connection() as connection:
        counts = load_sweden_company_raw_manifest(
            connection=connection,
            object_store=object_store,
            manifest=manifest,
            source_run_id=source_run_id,
        )
    return dg.MaterializeResult(
        metadata={
            "duckdb_path": str(tables.SWEDEN_COMPANY_DUCKDB_PATH),
            "source_run_id": source_run_id,
            "retrieved_date": str(manifest["retrieved_date"]),
            "raw_file_count": counts["raw_files"],
            "bolagsverket_row_count": counts.get("bolagsverket_raw", 0),
            "bolagsverket_rejected_line_count": counts.get(
                "bolagsverket_raw_rejected_lines", 0
            ),
            "scb_row_count": counts.get("scb_raw", 0),
        }
    )


@dg.asset(
    deps=["sweden_company_raw_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "bolagsverket", "scb"},
    pool=SWEDEN_COMPANY_DUCKDB_POOL,
    description=(
        "Rebuilds normalized Sweden company DuckDB tables from raw Bolagsverket "
        "and SCB staging tables."
    ),
)
def sweden_company_normalized_duckdb(
    sweden_company_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    loaded_at = datetime.now(UTC)
    with sweden_company_duckdb.get_connection() as connection:
        counts = replace_sweden_company_normalized_tables(
            connection=connection,
            loaded_at=loaded_at,
        )
    return dg.MaterializeResult(
        metadata={
            "duckdb_path": str(tables.SWEDEN_COMPANY_DUCKDB_PATH),
            "company_count": counts["companies"],
            "address_count": counts["company_addresses"],
            "industry_code_count": counts["company_industry_codes"],
            "bolagsverket_company_count": counts["bolagsverket_company_count"],
            "scb_company_count": counts["scb_company_count"],
            "companies_with_sni_count": counts["companies_with_sni_count"],
            "unknown_sni_count": counts["unknown_sni_count"],
            "loaded_at": loaded_at.isoformat(),
        }
    )


@dg.asset(
    deps=["sweden_company_normalized_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "bolagsverket", "scb"},
    pool=SWEDEN_COMPANY_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_COMPANIES_TABLE},
    description="Exports normalized Sweden companies to ClickHouse corpscout.se_companies.",
)
def sweden_company_companies_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_company_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(sweden_company_duckdb) as connection:
        rows = export_sweden_company_clickhouse_companies(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_COMPANIES_TABLE}
    )


@dg.asset(
    deps=["sweden_company_normalized_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "bolagsverket", "scb"},
    pool=SWEDEN_COMPANY_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_COMPANY_ADDRESSES_TABLE},
    description=(
        "Exports normalized Sweden company addresses to ClickHouse "
        "corpscout.se_company_addresses."
    ),
)
def sweden_company_addresses_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_company_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(sweden_company_duckdb) as connection:
        rows = export_sweden_company_clickhouse_addresses(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_COMPANY_ADDRESSES_TABLE}
    )


@dg.asset(
    deps=["sweden_company_normalized_duckdb"],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "bolagsverket", "scb"},
    pool=SWEDEN_COMPANY_DUCKDB_POOL,
    metadata={"table": tables.QUALIFIED_INDUSTRIES_TABLE},
    description="Exports normalized Sweden industries to ClickHouse corpscout.se_industries.",
)
def sweden_company_industries_clickhouse(
    context: dg.AssetExecutionContext,
    sweden_company_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(sweden_company_duckdb) as connection:
        rows = export_sweden_company_clickhouse_industries(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"rows": rows, "table": tables.QUALIFIED_INDUSTRIES_TABLE}
    )


@dg.asset_check(
    asset="sweden_company_companies_clickhouse",
    name="identities_normalized",
)
def identities_normalized(clickhouse: ClickhouseResource) -> dg.AssetCheckResult:
    """Guard against re-publishing 16-prefixed duplicate identities.

    A dagster deploy running pre-2026-07-18 code would re-export SCB rows
    keyed as 12-digit '16'+orgnr, silently re-splitting ~745k companies and
    breaking every downstream join. Fails loudly instead.
    """
    with clickhouse.get_connection() as client:
        [(prefixed, total, distinct)] = client.execute(
            "SELECT countIf(length(company_id) = 12 AND startsWith(company_id, '16')), "
            "count(), uniqExact(company_id) "
            f"FROM {tables.QUALIFIED_COMPANIES_TABLE}"
        )
    return dg.AssetCheckResult(
        passed=prefixed == 0 and total == distinct,
        metadata={
            "prefixed_16_ids": int(prefixed),
            "rows": int(total),
            "distinct_company_ids": int(distinct),
        },
    )


sweden_company_refresh_job = dg.define_asset_job(
    "sweden_company_refresh_job",
    tags=HEAVY_BULK_RUN_TAGS,
    selection=dg.AssetSelection.assets(
        "sweden_company_companies_clickhouse",
        "sweden_company_addresses_clickhouse",
        "sweden_company_industries_clickhouse",
    ).upstream(),
)

sweden_company_refresh_weekly = dg.ScheduleDefinition(
    name="sweden_company_refresh_weekly",
    job=sweden_company_refresh_job,
    cron_schedule="15 6 * * 1",
    execution_timezone="Europe/Belgrade",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)


defs = dg.Definitions(
    assets=[
        sweden_company_raw_snapshot_s3,
        sweden_company_raw_duckdb,
        sweden_company_normalized_duckdb,
        sweden_company_companies_clickhouse,
        sweden_company_addresses_clickhouse,
        sweden_company_industries_clickhouse,
    ],
    asset_checks=[identities_normalized],
    jobs=[sweden_company_refresh_job],
    schedules=[sweden_company_refresh_weekly],
    resources={
        "sweden_company_bulk": SwedenCompanyBulkResource(),
        "sweden_company_duckdb": duckdb_resource(tables.SWEDEN_COMPANY_DUCKDB_PATH),
    },
)
