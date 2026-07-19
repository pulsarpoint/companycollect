from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.finland_hilma import tables
from dagster_v3.defs.finland_hilma.clickhouse import export_finland_hilma_clickhouse
from dagster_v3.defs.finland_hilma.parsing import (
    apply_finland_hilma_usd_conversion,
    build_finland_hilma_notices,
    load_export_bytes_into_raw_table,
)

GROUP_NAME = "finland_hilma"
FINLAND_HILMA_DUCKDB_POOL = "finland_hilma_duckdb"
FINLAND_HILMA_DUCKDB_PATH = Path("data") / tables.DUCKDB_FILE_NAME
DLT_DATASET_NAME = tables.DLT_DATASET_NAME

EXPORT_S3_ASSET_KEY = dg.AssetKey("finland_hilma_export_s3")

# External asset: Dagster cannot materialize this — the Hilma portal has no
# keyless machine interface, so an authenticated user exports the search
# results CSV manually and uploads it with scripts/upload_hilma_export.py.
finland_hilma_export_s3 = dg.AssetSpec(
    EXPORT_S3_ASSET_KEY,
    group_name=GROUP_NAME,
    kinds={"s3", "csv"},
    description=(
        "MANUAL UPLOAD — not materializable from Dagster. Log in to "
        "hankintailmoitukset.fi, export the search results CSV with the FULL "
        "column set, then run scripts/upload_hilma_export.py <file.csv> to "
        f"place it under s3://{tables.S3_BUCKET}/{tables.S3_EXPORTS_PREFIX}. "
        "Downstream assets read every uploaded export and dedup by notice+lot."
    ),
    metadata={"bucket": tables.S3_BUCKET, "prefix": tables.S3_EXPORTS_PREFIX},
)


@dg.asset(
    name="finland_hilma_notices_duckdb",
    deps=[EXPORT_S3_ASSET_KEY],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb"},
    pool=FINLAND_HILMA_DUCKDB_POOL,
    description=(
        "Reads every manually uploaded Hilma export CSV from S3, validates the "
        "58-column shape, and builds the deduped typed notices + winners "
        "DuckDB tables."
    ),
)
def finland_hilma_notices_duckdb(
    context: dg.AssetExecutionContext,
    finland_hilma_duckdb: DuckDBResource,
    finland_hilma_object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    keys = sorted(
        key
        for key in finland_hilma_object_store.list_keys(tables.S3_EXPORTS_PREFIX)
        if key.endswith(".csv")
    )
    if not keys:
        raise ValueError(
            f"No Hilma export CSVs under s3://{tables.S3_BUCKET}/"
            f"{tables.S3_EXPORTS_PREFIX} — upload one with "
            "scripts/upload_hilma_export.py first"
        )
    FINLAND_HILMA_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with finland_hilma_duckdb.get_connection() as connection:
        total_raw = 0
        for index, key in enumerate(keys):
            context.log.info("Loading Hilma export %s/%s: %s", index + 1, len(keys), key)
            rows = load_export_bytes_into_raw_table(
                duckdb_connection=connection,
                csv_bytes=finland_hilma_object_store.read_bytes(key),
                source_key=key,
                replace=index == 0,
            )
            total_raw += rows
        counts = build_finland_hilma_notices(
            duckdb_connection=connection,
            source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={"export_files": len(keys), "raw_rows": total_raw, **counts}
    )


@dg.asset(
    name="finland_hilma_notices_usd_duckdb",
    deps=[dg.AssetKey("finland_hilma_notices_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb"},
    pool=FINLAND_HILMA_DUCKDB_POOL,
    description="Adds USD and FX metadata columns to Hilma notice amounts in DuckDB.",
)
def finland_hilma_notices_usd_duckdb(
    context: dg.AssetExecutionContext,
    finland_hilma_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    from exchange_rates import ExchangeRateClient

    with finland_hilma_duckdb.get_connection() as connection:
        counts = apply_finland_hilma_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=ExchangeRateClient.from_env(),
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


@dg.asset(
    name="finland_hilma_clickhouse",
    deps=[dg.AssetKey("finland_hilma_notices_usd_duckdb")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse"},
    pool=FINLAND_HILMA_DUCKDB_POOL,
    metadata={
        "tables": [
            tables.QUALIFIED_FI_HILMA_NOTICES_TABLE,
            tables.QUALIFIED_FI_HILMA_NOTICE_WINNERS_TABLE,
        ]
    },
    description=(
        "Hilma notices and normalized winners exported to ClickHouse "
        "corpscout.fi_hilma_notices / corpscout.fi_hilma_notice_winners."
    ),
)
def finland_hilma_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
    finland_hilma_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with read_only_duckdb_connection(finland_hilma_duckdb) as connection:
        counts = export_finland_hilma_clickhouse(
            duckdb_connection=connection,
            clickhouse=clickhouse,
            log=context.log.info,
        )
    return dg.MaterializeResult(metadata=counts)


# Manual job — no schedule: runs are launched after each manual S3 upload.
finland_hilma_job = dg.define_asset_job(
    "finland_hilma_job",
    selection=dg.AssetSelection.assets("finland_hilma_clickhouse").upstream()
    - dg.AssetSelection.assets(EXPORT_S3_ASSET_KEY),
)


defs = dg.Definitions(
    assets=[
        finland_hilma_export_s3,
        finland_hilma_notices_duckdb,
        finland_hilma_notices_usd_duckdb,
        finland_hilma_clickhouse,
    ],
    jobs=[finland_hilma_job],
    resources={
        "finland_hilma_duckdb": duckdb_resource(FINLAND_HILMA_DUCKDB_PATH),
        "finland_hilma_object_store": ObjectStoreResource(bucket=tables.S3_BUCKET),
    },
)
