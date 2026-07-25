import json
from typing import Any

import pyarrow as pa

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_financial.parsing import SWEDEN_FINANCIAL_DATASET_NAME
from dagster_v3.defs.sweden_financial.resources import (
    SWEDEN_FINANCIAL_RAW_BUCKET,
    SwedenFinancialArchive,
    SwedenFinancialArchiveSyncResult,
    SwedenFinancialStoredArchive,
)

ARCHIVE_SYNC_MANIFEST_PREFIX = "sweden_financial/raw_archive_sync_manifests/"
_ARCHIVE_SYNC_COLUMNS = (
    "source_run_id",
    "sync_kind",
    "load_partition_key",
    "archive_year",
    "upstream_key",
    "source_last_modified",
    "etag",
    "source_size_bytes",
    "source_url",
    "s3_key",
    "downloaded",
    "stored_size_bytes",
    "sha256",
    "content_type",
)
_ARCHIVE_SYNC_ARROW_SCHEMA = pa.schema(
    [
        pa.field("source_run_id", pa.string(), nullable=False),
        pa.field("sync_kind", pa.string(), nullable=False),
        pa.field("load_partition_key", pa.string(), nullable=False),
        pa.field("archive_year", pa.string(), nullable=False),
        pa.field("upstream_key", pa.string(), nullable=False),
        pa.field("source_last_modified", pa.string(), nullable=False),
        pa.field("etag", pa.string(), nullable=False),
        pa.field("source_size_bytes", pa.int64(), nullable=False),
        pa.field("source_url", pa.string(), nullable=False),
        pa.field("s3_key", pa.string(), nullable=False),
        pa.field("downloaded", pa.bool_(), nullable=False),
        pa.field("stored_size_bytes", pa.int64()),
        pa.field("sha256", pa.string()),
        pa.field("content_type", pa.string(), nullable=False),
    ]
)
_ARCHIVE_SYNC_BATCH_RELATION = "_sweden_financial_archive_sync_batch"


def archive_sync_manifest_key(*, sync_kind: str, load_partition_key: str) -> str:
    return (
        f"{ARCHIVE_SYNC_MANIFEST_PREFIX}"
        f"sync_kind={sync_kind}/"
        f"load_partition_key={load_partition_key}/"
        "manifest.json"
    )


def write_sweden_financial_archive_sync_manifest(
    *,
    object_store: ObjectStoreResource,
    sync_result: SwedenFinancialArchiveSyncResult,
    sync_kind: str,
    source_run_id: str,
    load_partition_key: str,
) -> str:
    manifest_key = archive_sync_manifest_key(
        sync_kind=sync_kind,
        load_partition_key=load_partition_key,
    )
    object_store.write_bytes(
        manifest_key,
        json.dumps(
            {
                "source_run_id": source_run_id,
                "sync_kind": sync_kind,
                "load_partition_key": load_partition_key,
                "metadata": sync_result.metadata,
                "stored_archives": [
                    {
                        "archive": {
                            "upstream_key": stored.archive.upstream_key,
                            "source_last_modified": stored.archive.source_last_modified,
                            "etag": stored.archive.etag,
                            "source_size_bytes": stored.archive.source_size_bytes,
                        },
                        "source_url": stored.source_url,
                        "s3_key": stored.s3_key,
                        "downloaded": stored.downloaded,
                        "stored_size_bytes": stored.stored_size_bytes,
                        "sha256": stored.sha256,
                        "content_type": stored.content_type,
                    }
                    for stored in sync_result.stored_archives
                ],
            },
            sort_keys=True,
        ).encode("utf-8"),
        bucket=SWEDEN_FINANCIAL_RAW_BUCKET,
    )
    return manifest_key


def read_sweden_financial_archive_sync_manifest(
    *,
    object_store: ObjectStoreResource,
    sync_kind: str,
    load_partition_key: str,
) -> SwedenFinancialArchiveSyncResult:
    manifest_key = archive_sync_manifest_key(
        sync_kind=sync_kind,
        load_partition_key=load_partition_key,
    )
    payload = json.loads(
        object_store.read_bytes(
            manifest_key,
            bucket=SWEDEN_FINANCIAL_RAW_BUCKET,
        ).decode("utf-8")
    )
    return SwedenFinancialArchiveSyncResult(
        stored_archives=[
            SwedenFinancialStoredArchive(
                archive=SwedenFinancialArchive(
                    upstream_key=item["archive"]["upstream_key"],
                    source_last_modified=item["archive"]["source_last_modified"],
                    etag=item["archive"]["etag"],
                    source_size_bytes=item["archive"]["source_size_bytes"],
                ),
                source_url=item["source_url"],
                s3_key=item["s3_key"],
                downloaded=item["downloaded"],
                stored_size_bytes=item["stored_size_bytes"],
                sha256=item["sha256"],
                content_type=item["content_type"],
            )
            for item in payload["stored_archives"]
        ],
        metadata=payload["metadata"],
    )


def record_sweden_financial_archive_sync(
    *,
    connection: Any,
    sync_result: SwedenFinancialArchiveSyncResult,
    sync_kind: str,
    source_run_id: str,
    load_partition_key: str,
) -> None:
    _ensure_archive_sync_catalog(connection)
    rows = [
        {
            "source_run_id": source_run_id,
            "sync_kind": sync_kind,
            "load_partition_key": load_partition_key,
            "archive_year": stored.archive.year,
            "upstream_key": stored.archive.upstream_key,
            "source_last_modified": stored.archive.source_last_modified,
            "etag": stored.archive.etag,
            "source_size_bytes": stored.archive.source_size_bytes,
            "source_url": stored.source_url,
            "s3_key": stored.s3_key,
            "downloaded": stored.downloaded,
            "stored_size_bytes": stored.stored_size_bytes,
            "sha256": stored.sha256,
            "content_type": stored.content_type,
        }
        for stored in sync_result.stored_archives
    ]
    arrow_table = (
        pa.Table.from_pylist(rows, schema=_ARCHIVE_SYNC_ARROW_SCHEMA) if rows else None
    )
    registered = False
    connection.execute("begin transaction")
    try:
        connection.execute(
            f"""
            delete from {SWEDEN_FINANCIAL_DATASET_NAME}.archive_sync_catalog
            where source_run_id = ? and sync_kind = ? and load_partition_key = ?
            """,
            [source_run_id, sync_kind, load_partition_key],
        )
        if arrow_table is not None:
            column_list = ", ".join(_ARCHIVE_SYNC_COLUMNS)
            connection.register(_ARCHIVE_SYNC_BATCH_RELATION, arrow_table)
            registered = True
            connection.execute(
                f"""
                insert into {SWEDEN_FINANCIAL_DATASET_NAME}.archive_sync_catalog
                ({column_list})
                select {column_list} from {_ARCHIVE_SYNC_BATCH_RELATION}
                """
            )
    except Exception:
        connection.execute("rollback")
        raise
    else:
        connection.execute("commit")
    finally:
        if registered:
            connection.unregister(_ARCHIVE_SYNC_BATCH_RELATION)


def changed_sweden_financial_archive_keys_for_run(
    *,
    connection: Any,
    source_run_id: str,
) -> list[str]:
    _ensure_archive_sync_catalog(connection)
    return [
        row[0]
        for row in connection.execute(
            f"""
            select s3_key
            from {SWEDEN_FINANCIAL_DATASET_NAME}.archive_sync_catalog
            where source_run_id = ? and downloaded
            order by s3_key
            """,
            [source_run_id],
        ).fetchall()
    ]


def _ensure_archive_sync_catalog(connection: Any) -> None:
    connection.execute(f"create schema if not exists {SWEDEN_FINANCIAL_DATASET_NAME}")
    connection.execute(
        f"""
        create table if not exists {SWEDEN_FINANCIAL_DATASET_NAME}.archive_sync_catalog (
            source_run_id varchar,
            sync_kind varchar,
            load_partition_key varchar,
            archive_year varchar,
            upstream_key varchar,
            source_last_modified varchar,
            etag varchar,
            source_size_bytes bigint,
            source_url varchar,
            s3_key varchar,
            downloaded boolean,
            stored_size_bytes bigint,
            sha256 varchar,
            content_type varchar
        )
        """
    )
