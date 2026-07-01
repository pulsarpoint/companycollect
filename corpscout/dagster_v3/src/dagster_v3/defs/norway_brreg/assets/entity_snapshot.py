from __future__ import annotations

import os
from collections.abc import Callable
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import dagster as dg
import dlt
import fsspec
import ijson
from dagster_aws.s3 import S3Resource
from dlt.common.configuration.specs import AwsCredentials
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources.filesystem import filesystem

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.norway_brreg.constants import (
    GROUP_NAME,
    NORWAY_BRREG_ENTITY_BUCKET,
)
from dagster_v3.defs.norway_brreg import entity_records
from dagster_v3.defs.norway_brreg.entity_parquet import entity_record_parquet_row
from dagster_v3.defs.norway_brreg.resources import NorwayBrregApiResource

ENTRIES_SNAPSHOT_RAW_OBJECT_KEY = "norway_brreg/entities/raw/snapshot/entities.json.gz"
ENTITY_SNAPSHOT_OBJECT_KEY = "norway_brreg/entities/snapshot/entities.parquet"
DLT_PIPELINE_NAME = "norway_brreg_entities_snapshot"
DLT_DATASET_NAME = "snapshot"
DLT_ENTITIES_TABLE = "entities"
EMPTY_SNAPSHOT_ERROR_MESSAGE = "Norway Brreg entity snapshot produced no rows"
DLT_SOURCE_BUCKET_URL = (
    f"s3://{NORWAY_BRREG_ENTITY_BUCKET}/norway_brreg/entities/raw/snapshot"
)
DLT_SOURCE_FILE_GLOB = "entities.json.gz"
DLT_DESTINATION_BUCKET_URL = f"s3://{NORWAY_BRREG_ENTITY_BUCKET}/norway_brreg/entities"
DLT_PIPELINES_DIR = Path("data/.dlt/norway_brreg_entities_snapshot")
DLT_ENTITY_COLUMNS = {
    "org_number": {"data_type": "text", "nullable": False},
    "change_type": {"data_type": "text", "nullable": False},
    "source_change_type": {"data_type": "text", "nullable": False},
    "updated_at": {"data_type": "text", "nullable": True},
    "update_id": {"data_type": "bigint", "nullable": True},
    "entity_url": {"data_type": "text", "nullable": True},
    "entity_json": {"data_type": "text", "nullable": True},
    "raw_update_json": {"data_type": "text", "nullable": True},
}


@dg.asset(
    name="norway_brreg_entries_snapshot_raw_s3",
    group_name=GROUP_NAME,
    kinds={"python", "s3", "gzip", "brreg"},
    description="Backs up the raw Norway Brreg full entity snapshot gzip archive to S3.",
)
def norway_brreg_entries_snapshot_raw_s3(
    context,
    norway_brreg_api: NorwayBrregApiResource,
    s3: S3Resource,
) -> dg.MaterializeResult:
    metadata = norway_brreg_api.entries_snapshot(
        s3=s3,
        bucket=NORWAY_BRREG_ENTITY_BUCKET,
        key=entries_snapshot_raw_object_key(),
        log=context.log.info,
    )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="norway_brreg_entities_snapshot_s3",
    deps=[dg.AssetKey("norway_brreg_entries_snapshot_raw_s3")],
    group_name=GROUP_NAME,
    kinds={"python", "dlt", "s3", "parquet", "brreg"},
    description="Converts the raw Norway Brreg entity snapshot archive to uniform entity parquet on S3 with dlt.",
)
def norway_brreg_entities_snapshot_s3(
    context,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    s3_key = entity_snapshot_object_key()
    if object_store.exists(s3_key, bucket=NORWAY_BRREG_ENTITY_BUCKET):
        context.log.info(
            "Reusing existing Norway Brreg full entity snapshot parquet: bucket=%s key=%s",
            NORWAY_BRREG_ENTITY_BUCKET,
            s3_key,
        )
        return dg.MaterializeResult(
            metadata={
                "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
                "s3_key": s3_key,
                "converted": False,
                "reused_existing_snapshot": True,
            }
        )

    context.log.info(
        "Converting Norway Brreg raw entity snapshot with dlt: bucket=%s raw_key=%s output_key=%s",
        NORWAY_BRREG_ENTITY_BUCKET,
        entries_snapshot_raw_object_key(),
        s3_key,
    )
    dlt_result = run_norway_brreg_entities_snapshot_dlt_pipeline(
        source_bucket_url=DLT_SOURCE_BUCKET_URL,
        source_file_glob=DLT_SOURCE_FILE_GLOB,
        destination_bucket_url=DLT_DESTINATION_BUCKET_URL,
        credentials=None,
        pipelines_dir=DLT_PIPELINES_DIR,
        output_s3_key=s3_key,
        log=context.log.info,
    )

    context.log.info(
        "Completed Norway Brreg dlt entity snapshot conversion: bucket=%s key=%s rows=%d bytes=%d",
        NORWAY_BRREG_ENTITY_BUCKET,
        s3_key,
        dlt_result["row_count"],
        dlt_result["parquet_size_bytes"],
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": dlt_result["row_count"],
            "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
            "s3_key": s3_key,
            "parquet_size_bytes": dlt_result["parquet_size_bytes"],
            "parquet_uri": dlt_result["parquet_uri"],
            "converted": True,
            "reused_existing_snapshot": False,
            "dlt_pipeline_name": DLT_PIPELINE_NAME,
            "dlt_table_name": DLT_ENTITIES_TABLE,
        }
    )


def entries_snapshot_raw_object_key() -> str:
    return ENTRIES_SNAPSHOT_RAW_OBJECT_KEY


def entity_snapshot_object_key() -> str:
    return ENTITY_SNAPSHOT_OBJECT_KEY


def run_norway_brreg_entities_snapshot_dlt_pipeline(
    *,
    source_bucket_url: str,
    source_file_glob: str,
    destination_bucket_url: str,
    credentials: AwsCredentials | None,
    pipelines_dir: Path,
    output_s3_key: str,
    log: Callable[..., None] | None = None,
) -> dict[str, Any]:
    pipelines_dir.mkdir(parents=True, exist_ok=True)
    resolved_credentials = credentials or _dlt_s3_credentials(source_bucket_url)
    row_counter = {"row_count": 0}
    pipeline = dlt.pipeline(
        pipeline_name=DLT_PIPELINE_NAME,
        destination=dlt.destinations.filesystem(
            bucket_url=destination_bucket_url,
            credentials=resolved_credentials,
            layout="{table_name}.{ext}",
        ),
        dataset_name=DLT_DATASET_NAME,
        dev_mode=False,
        pipelines_dir=str(pipelines_dir),
    )
    try:
        pipeline.run(
            _entity_snapshot_source(
                source_bucket_url=source_bucket_url,
                source_file_glob=source_file_glob,
                credentials=resolved_credentials,
                row_counter=row_counter,
                log=log,
            ),
            loader_file_format="parquet",
        )
    except PipelineStepFailed as exc:
        if _has_empty_snapshot_error(exc):
            raise ValueError(EMPTY_SNAPSHOT_ERROR_MESSAGE) from exc
        raise
    if row_counter["row_count"] == 0:
        raise ValueError(EMPTY_SNAPSHOT_ERROR_MESSAGE)
    parquet_uri = _dlt_parquet_uri(destination_bucket_url)
    if parquet_uri.startswith("s3://") and not parquet_uri.endswith(output_s3_key):
        raise ValueError(
            "Norway Brreg dlt destination no longer matches expected S3 key: "
            f"parquet_uri={parquet_uri} output_s3_key={output_s3_key}"
        )
    parquet_size_bytes = _object_size(parquet_uri, credentials=resolved_credentials)
    return {
        "row_count": row_counter["row_count"],
        "parquet_size_bytes": parquet_size_bytes,
        "parquet_uri": parquet_uri,
    }


def _entity_snapshot_source(
    *,
    source_bucket_url: str,
    source_file_glob: str,
    credentials: AwsCredentials | None,
    row_counter: dict[str, int],
    log: Callable[..., None] | None,
) -> Any:
    kwargs: dict[str, Any] = {
        "bucket_url": source_bucket_url,
        "file_glob": source_file_glob,
        "files_per_page": 1,
    }
    if credentials is not None:
        kwargs["credentials"] = credentials
    return filesystem(**kwargs) | _entity_snapshot_resource(
        row_counter=row_counter,
        log=log,
    )


@dlt.transformer(
    name=DLT_ENTITIES_TABLE,
    write_disposition="replace",
    columns=DLT_ENTITY_COLUMNS,
)
def _entity_snapshot_resource(
    file_items: Any,
    *,
    row_counter: dict[str, int],
    log: Callable[..., None] | None,
) -> Iterator[dict[str, Any]]:
    for file_item in _file_items(file_items):
        with file_item.open() as source_file:
            for entity in ijson.items(source_file, "item"):
                if not isinstance(entity, dict):
                    continue
                row_counter["row_count"] += 1
                if log is not None and row_counter["row_count"] % 1000 == 0:
                    log(
                        "Parsed Norway Brreg entity snapshot rows: rows=%s",
                        row_counter["row_count"],
                    )
                yield entity_record_parquet_row(entity_records.snapshot_entity_record(entity))
    if row_counter["row_count"] == 0:
        raise ValueError(EMPTY_SNAPSHOT_ERROR_MESSAGE)


def _file_items(file_items: Any) -> list[Any]:
    if isinstance(file_items, list):
        return file_items
    return [file_items]


def _dlt_s3_credentials(source_bucket_url: str) -> AwsCredentials | None:
    if not source_bucket_url.startswith("s3://"):
        return None
    return AwsCredentials(
        aws_access_key_id=_required_env("CORPSCOUT_S3_ACCESS_KEY"),
        aws_secret_access_key=_required_env("CORPSCOUT_S3_SECRET_KEY"),
        endpoint_url=_required_env("CORPSCOUT_S3_ENDPOINT"),
        region_name="us-east-1",
        s3_url_style="path",
    )


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _dlt_parquet_uri(destination_bucket_url: str) -> str:
    return (
        f"{destination_bucket_url.rstrip('/')}/{DLT_DATASET_NAME}/"
        f"{DLT_ENTITIES_TABLE}.parquet"
    )


def _object_size(uri: str, *, credentials: AwsCredentials | None) -> int:
    filesystem_client, path = fsspec.core.url_to_fs(
        uri,
        **_fsspec_storage_options(credentials),
    )
    return int(filesystem_client.info(path)["size"])


def _fsspec_storage_options(credentials: AwsCredentials | None) -> dict[str, Any]:
    if credentials is None:
        return {}
    return {
        "key": credentials.aws_access_key_id,
        "secret": credentials.aws_secret_access_key,
        "token": credentials.aws_session_token,
        "client_kwargs": {
            "endpoint_url": credentials.endpoint_url,
            "region_name": credentials.region_name,
        },
        "config_kwargs": {
            "s3": {"addressing_style": credentials.s3_url_style},
        },
    }


def _has_empty_snapshot_error(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, ValueError) and str(current) == EMPTY_SNAPSHOT_ERROR_MESSAGE:
            return True
        current = current.__cause__ or current.__context__
    return False
