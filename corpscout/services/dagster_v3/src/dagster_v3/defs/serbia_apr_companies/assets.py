from collections.abc import Iterator
from datetime import UTC, datetime

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    safe_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.serbia_apr_companies import tables
from dagster_v3.defs.serbia_apr_companies.duckdb import (
    replace_serbia_apr_companies_duckdb,
)
from dagster_v3.defs.serbia_apr_companies.resources import (
    latest_snapshot_manifest,
    sync_apr_companies_snapshot,
)


@dg.asset(
    name="serbia_apr_companies_raw_snapshot_s3",
    group_name=tables.GROUP_NAME,
    kinds={"python", "json", "s3", "apr"},
    tags=tables.ASSET_TAGS,
    metadata={
        "s3_bucket": tables.S3_BUCKET,
        "source_url": tables.SOURCE_URL,
    },
    description=(
        "Streams the complete Serbian APR companies open-data JSON response, "
        "validates its envelope and population, and stores an immutable "
        "content-addressed snapshot in S3-compatible object storage."
    ),
)
def serbia_apr_companies_raw_snapshot_s3(
    context: dg.AssetExecutionContext,
    serbia_apr_companies_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    snapshot = sync_apr_companies_snapshot(
        object_store=serbia_apr_companies_object_store,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
        log_info=context.log.info,
    )
    return dg.MaterializeResult(
        metadata={
            "content_type": snapshot.content_type,
            "downloaded": snapshot.downloaded,
            "manifest_key": snapshot.manifest_key,
            "object_key": snapshot.object_key,
            "record_count": snapshot.record_count,
            "retrieved_at": snapshot.retrieved_at,
            "s3_bucket": tables.S3_BUCKET,
            "sha256": snapshot.sha256,
            "size_bytes": snapshot.size_bytes,
            "snapshot_date": snapshot.snapshot_date,
            "source_url": tables.SOURCE_URL,
        }
    )


@dg.multi_asset(
    name="serbia_apr_companies_duckdb_load",
    specs=[
        dg.AssetSpec(
            tables.SNAPSHOT_RUNS_ASSET,
            deps=[serbia_apr_companies_raw_snapshot_s3],
            group_name=tables.GROUP_NAME,
            kinds={"python", "duckdb", "s3", "json", "apr"},
            tags=tables.DUCKDB_ASSET_TAGS,
            metadata={
                "database": str(tables.DUCKDB_PATH),
                "table": (f"{tables.DUCKDB_SCHEMA}.{tables.SNAPSHOT_RUNS_TABLE}"),
            },
            description=(
                "Catalog of validated APR company snapshots accepted into DuckDB, "
                "including source-object integrity and schema provenance."
            ),
        ),
        dg.AssetSpec(
            tables.COMPANY_OBSERVATIONS_ASSET,
            deps=[serbia_apr_companies_raw_snapshot_s3],
            group_name=tables.GROUP_NAME,
            kinds={"python", "duckdb", "s3", "json", "apr"},
            tags=tables.DUCKDB_ASSET_TAGS,
            metadata={
                "database": str(tables.DUCKDB_PATH),
                "table": (
                    f"{tables.DUCKDB_SCHEMA}.{tables.COMPANY_OBSERVATIONS_TABLE}"
                ),
            },
            description=(
                "Typed, append-over-time APR company observations, with one "
                "idempotently replaceable company population per snapshot date."
            ),
        ),
        dg.AssetSpec(
            tables.COMPANIES_CURRENT_ASSET,
            deps=[serbia_apr_companies_raw_snapshot_s3],
            group_name=tables.GROUP_NAME,
            kinds={"python", "duckdb", "s3", "json", "apr"},
            tags=tables.DUCKDB_ASSET_TAGS,
            metadata={
                "database": str(tables.DUCKDB_PATH),
                "table": f"{tables.DUCKDB_SCHEMA}.{tables.COMPANIES_CURRENT_TABLE}",
            },
            description=(
                "Current complete APR open-data company population, atomically "
                "replaced from the newest accepted raw snapshot."
            ),
        ),
    ],
    pool=tables.DUCKDB_POOL,
    can_subset=False,
)
def serbia_apr_companies_duckdb_load(
    context: dg.AssetExecutionContext,
    serbia_apr_companies_object_store: ObjectStoreResource,
    serbia_apr_companies_duckdb: DuckDBResource,
) -> Iterator[dg.MaterializeResult]:
    manifest = latest_snapshot_manifest(serbia_apr_companies_object_store)
    with safe_duckdb_connection(serbia_apr_companies_duckdb) as connection:
        counts = replace_serbia_apr_companies_duckdb(
            connection=connection,
            object_store=serbia_apr_companies_object_store,
            manifest=manifest,
            loaded_at=datetime.now(UTC),
        )

    for asset_name, table_name in (
        (tables.SNAPSHOT_RUNS_ASSET, tables.SNAPSHOT_RUNS_TABLE),
        (tables.COMPANY_OBSERVATIONS_ASSET, tables.COMPANY_OBSERVATIONS_TABLE),
        (tables.COMPANIES_CURRENT_ASSET, tables.COMPANIES_CURRENT_TABLE),
    ):
        yield dg.MaterializeResult(
            asset_key=dg.AssetKey(asset_name),
            metadata={
                "row_count": counts[table_name],
                "loaded_snapshot_record_count": int(manifest["record_count"]),
                "s3_bucket": tables.S3_BUCKET,
                "source_object_key": str(manifest["object_key"]),
                "source_run_id": str(manifest["source_run_id"]),
                "source_sha256": str(manifest["sha256"]),
                "snapshot_date": str(manifest["snapshot_date"]),
                "table": f"{tables.DUCKDB_SCHEMA}.{table_name}",
            },
        )


defs = dg.Definitions(
    assets=[
        serbia_apr_companies_raw_snapshot_s3,
        serbia_apr_companies_duckdb_load,
    ],
    resources={
        "serbia_apr_companies_object_store": ObjectStoreResource(
            bucket=tables.S3_BUCKET
        ),
        "serbia_apr_companies_duckdb": duckdb_resource(tables.DUCKDB_PATH),
    },
)
