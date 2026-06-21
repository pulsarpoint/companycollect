from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
import shutil
import tempfile
from typing import Any, Literal, cast

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    replace_duckdb_tables_in_clickhouse,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.gleif import tables
from dagster_v3.defs.gleif.dlt_csv import (
    GLEIF_RAW_LEI_RECORDS_TABLE,
    GLEIF_RAW_RELATIONSHIPS_TABLE,
    GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE,
    ExtractedGleifCsv,
    FileKind,
    RAW_TABLE_BY_FILE_KIND,
    extract_single_csv_member,
    gleif_csv_dlt_pipeline,
    gleif_csv_dlt_source,
    gleif_definition_time_csv_files,
)
from dagster_v3.defs.gleif.source import (
    GLEIF_RAW_BUCKET,
    GleifRawDownloadConfig,
    download_golden_copy_files,
    manifest_for_run,
    select_gleif_raw_keys_for_deletion,
)

GROUP_NAME = "gleif"
GLEIF_DUCKDB_PATH = Path("data/gleif_reference.duckdb")
GLEIF_DUCKDB_SCHEMA = f"{GLEIF_DUCKDB_PATH.stem}.gleif"
GLEIF_DUCKDB_POOL = "gleif_duckdb"
GLEIF_DUCKDB_RAW_ASSET_KEYS = (
    dg.AssetKey("gleif_raw_lei_records_duckdb"),
    dg.AssetKey("gleif_raw_relationships_duckdb"),
    dg.AssetKey("gleif_raw_reporting_exceptions_duckdb"),
)

GLEIF_DLT_ASSET_BY_TABLE = {
    GLEIF_RAW_LEI_RECORDS_TABLE: "gleif_raw_lei_records_duckdb",
    GLEIF_RAW_RELATIONSHIPS_TABLE: "gleif_raw_relationships_duckdb",
    GLEIF_RAW_REPORTING_EXCEPTIONS_TABLE: "gleif_raw_reporting_exceptions_duckdb",
}


class GleifDltTranslator(DagsterDltTranslator):
    def get_asset_spec(self, data: DltResourceTranslatorData) -> dg.AssetSpec:
        spec = super().get_asset_spec(data)
        asset_key = GLEIF_DLT_ASSET_BY_TABLE.get(data.resource.table_name)
        if asset_key is None:
            return spec
        return spec.replace_attributes(
            key=asset_key,
            deps=[
                dg.AssetKey("gleif_full_raw_reference_files"),
                dg.AssetKey("gleif_delta_raw_reference_files"),
            ],
            group_name=GROUP_NAME,
            description=f"Raw GLEIF CSV table `{data.resource.table_name}` loaded into DuckDB with dlt.",
            kinds={"python", "dlt", "duckdb", "gleif"},
        )


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "gleif"},
    description="Downloads full GLEIF Golden Copy ZIP files into object storage for bootstrap/recovery.",
)
def gleif_full_raw_reference_files(
    context: dg.AssetExecutionContext,
    config: GleifRawDownloadConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return download_golden_copy_files(
        context=context,
        object_store=object_store,
        config=config,
        load_mode="full",
        delta=None,
        run_id=context.run_id,
        pulled_at=datetime.now(UTC),
    )


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "gleif"},
    description="Downloads LastDay GLEIF Golden Copy delta ZIP files into object storage.",
)
def gleif_delta_raw_reference_files(
    context: dg.AssetExecutionContext,
    config: GleifRawDownloadConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return download_golden_copy_files(
        context=context,
        object_store=object_store,
        config=config,
        load_mode="delta",
        delta="LastDay",
        run_id=context.run_id,
        pulled_at=datetime.now(UTC),
    )


@dlt_assets(
    dlt_source=gleif_csv_dlt_source(gleif_definition_time_csv_files()),
    dlt_pipeline=gleif_csv_dlt_pipeline(GLEIF_DUCKDB_PATH),
    name="gleif_raw_duckdb_dlt",
    dagster_dlt_translator=GleifDltTranslator(),
    pool=GLEIF_DUCKDB_POOL,
)
def gleif_raw_duckdb_dlt_assets(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
    object_store: ObjectStoreResource,
) -> Iterator[Any]:
    manifest = manifest_for_run(object_store, context.run_id)
    with tempfile.TemporaryDirectory(prefix="gleif-dlt-csv-") as temp_path:
        extracted_files = _extract_manifest_csv_files(
            context=context,
            object_store=object_store,
            manifest=manifest,
            temp_dir=Path(temp_path),
        )
        yield from dlt.run(
            context=context,
            dlt_source=gleif_csv_dlt_source(extracted_files),
            dlt_pipeline=gleif_csv_dlt_pipeline(GLEIF_DUCKDB_PATH),
        )


@dg.asset(
    deps=GLEIF_DUCKDB_RAW_ASSET_KEYS,
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "gleif"},
    pool=GLEIF_DUCKDB_POOL,
    description="Maintains current GLEIF normalized state in DuckDB from dlt-loaded raw CSV tables.",
)
def gleif_reference_duckdb_state(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    from dagster_v3.defs.gleif.duckdb_state import refresh_gleif_duckdb_state

    return refresh_gleif_duckdb_state(
        context=context,
        object_store=object_store,
        database_path=GLEIF_DUCKDB_PATH,
    )


@dg.asset(
    deps=[dg.AssetKey("gleif_reference_duckdb_state")],
    group_name=GROUP_NAME,
    kinds={"python", "duckdb", "clickhouse", "gleif"},
    pool=GLEIF_DUCKDB_POOL,
    description="Exports current GLEIF DuckDB state to ClickHouse corpscout.gleif_* tables.",
)
def gleif_reference_clickhouse(clickhouse: ClickhouseResource) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.GLEIF_TABLES,
    )
    with clickhouse.get_connection() as client:
        row_counts = replace_duckdb_tables_in_clickhouse(
            duckdb_path=GLEIF_DUCKDB_PATH,
            clickhouse_client=client,
            duckdb_schema=GLEIF_DUCKDB_SCHEMA,
            clickhouse_database=RESOLVED_DATABASE,
            tables=tuple(
                (table_name, tables.GLEIF_TABLE_COLUMNS[table_name])
                for table_name in tables.GLEIF_TABLES
            ),
        )
    return dg.MaterializeResult(
        metadata={f"{table_name}_row_count": count for table_name, count in row_counts.items()}
    )


@dg.asset(
    deps=[dg.AssetKey("gleif_reference_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "gleif"},
    description="Deletes old GLEIF raw blobs while preserving manifests and the newest raw snapshot.",
)
def gleif_raw_retention(object_store: ObjectStoreResource) -> dg.MaterializeResult:
    keys = object_store.list_keys("gleif/raw/", bucket=GLEIF_RAW_BUCKET)
    keys_to_delete = select_gleif_raw_keys_for_deletion(keys)
    deleted_count = object_store.delete_keys(tuple(keys_to_delete), bucket=GLEIF_RAW_BUCKET)
    return dg.MaterializeResult(metadata={"deleted_key_count": deleted_count})


gleif_reference_bootstrap_job = dg.define_asset_job(
    name="gleif_reference_bootstrap_job",
    selection=[
        "gleif_full_raw_reference_files",
        "gleif_raw_lei_records_duckdb",
        "gleif_raw_relationships_duckdb",
        "gleif_raw_reporting_exceptions_duckdb",
        "gleif_reference_duckdb_state",
        "gleif_reference_clickhouse",
        "gleif_raw_retention",
    ],
)

gleif_reference_delta_job = dg.define_asset_job(
    name="gleif_reference_delta_job",
    selection=[
        "gleif_delta_raw_reference_files",
        "gleif_raw_lei_records_duckdb",
        "gleif_raw_relationships_duckdb",
        "gleif_raw_reporting_exceptions_duckdb",
        "gleif_reference_duckdb_state",
        "gleif_reference_clickhouse",
        "gleif_raw_retention",
    ],
)

gleif_reference_delta_daily = dg.ScheduleDefinition(
    name="gleif_reference_delta_daily",
    job=gleif_reference_delta_job,
    cron_schedule="30 20 * * *",
    execution_timezone="UTC",
)

defs = dg.Definitions(
    assets=[
        gleif_full_raw_reference_files,
        gleif_delta_raw_reference_files,
        gleif_raw_duckdb_dlt_assets,
        gleif_reference_duckdb_state,
        gleif_reference_clickhouse,
        gleif_raw_retention,
    ],
    jobs=[gleif_reference_bootstrap_job, gleif_reference_delta_job],
    schedules=[gleif_reference_delta_daily],
)


def _extract_manifest_csv_files(
    *,
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
    temp_dir: Path,
) -> list[ExtractedGleifCsv]:
    extracted_files: list[ExtractedGleifCsv] = []
    for file_entry in manifest["files"]:
        if file_entry.get("file_format") != "csv":
            raise ValueError(
                "GLEIF dlt CSV loader only accepts manifest files with file_format=csv"
            )
        file_kind_value = str(file_entry["file_kind"])
        if file_kind_value not in RAW_TABLE_BY_FILE_KIND:
            raise ValueError(f"Unsupported GLEIF file_kind for dlt CSV load: {file_kind_value}")
        file_kind = cast(FileKind, file_kind_value)
        zip_path = temp_dir / file_kind / "source.csv.zip"
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        context.log.info(
            "copying_gleif_raw_file_from_s3",
            extra={"file_kind": file_kind, "s3_key": str(file_entry["s3_key"])},
        )
        _copy_s3_object_to_path(
            object_store=object_store,
            s3_key=str(file_entry["s3_key"]),
            output_path=zip_path,
        )
        csv_path = extract_single_csv_member(
            zip_path=zip_path,
            output_dir=temp_dir / file_kind / "csv",
            file_kind=file_kind,
        )
        load_mode_value = str(manifest["load_mode"])
        if load_mode_value not in {"full", "delta"}:
            raise ValueError(f"Unsupported GLEIF load mode for dlt CSV load: {load_mode_value}")
        load_mode = cast(Literal["full", "delta"], load_mode_value)
        context.log.info(
            "extracted_gleif_csv",
            extra={
                "file_kind": file_kind,
                "csv_path": str(csv_path),
                "size_bytes": csv_path.stat().st_size,
            },
        )
        extracted_files.append(
            ExtractedGleifCsv(
                file_kind=file_kind,
                csv_path=csv_path,
                source_url=str(file_entry["source_url"]),
                s3_key=str(file_entry["s3_key"]),
                source_sha256=str(file_entry["sha256"]),
                publish_date=str(manifest["publish_date"]),
                load_mode=load_mode,
                run_id=str(manifest["run_id"]),
            )
        )
    return extracted_files


def _copy_s3_object_to_path(
    *,
    object_store: ObjectStoreResource,
    s3_key: str,
    output_path: Path,
) -> None:
    response = object_store.client().get_object(Bucket=GLEIF_RAW_BUCKET, Key=s3_key)
    with output_path.open("wb") as target:
        shutil.copyfileobj(response["Body"], target)
