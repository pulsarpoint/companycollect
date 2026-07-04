from typing import Any

from dagster_v3.defs.sweden_financial.parsing import SWEDEN_FINANCIAL_DATASET_NAME
from dagster_v3.defs.sweden_financial.resources import SwedenFinancialArchiveSyncResult


def record_sweden_financial_archive_sync(
    *,
    connection: Any,
    sync_result: SwedenFinancialArchiveSyncResult,
    sync_kind: str,
    source_run_id: str,
    load_partition_key: str,
) -> None:
    _ensure_archive_sync_catalog(connection)
    connection.execute(
        f"""
        delete from {SWEDEN_FINANCIAL_DATASET_NAME}.archive_sync_catalog
        where source_run_id = ? and sync_kind = ? and load_partition_key = ?
        """,
        [source_run_id, sync_kind, load_partition_key],
    )
    rows = [
        (
            source_run_id,
            sync_kind,
            load_partition_key,
            stored.archive.year,
            stored.archive.upstream_key,
            stored.archive.source_last_modified,
            stored.archive.etag,
            stored.archive.source_size_bytes,
            stored.source_url,
            stored.s3_key,
            stored.downloaded,
            stored.stored_size_bytes,
            stored.sha256,
            stored.content_type,
        )
        for stored in sync_result.stored_archives
    ]
    if rows:
        connection.executemany(
            f"""
            insert into {SWEDEN_FINANCIAL_DATASET_NAME}.archive_sync_catalog
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


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
