"""Archive complete Brave batches before publishing the latest successful answers."""

import hashlib
import json
import re

from psycopg2.extras import Json

from dagster_v3.defs.common.processing import ExportBatch, ProcessingStore

EXPORT_DESTINATION = "country_brave_domains_v1"
EXPORT_COLUMNS = (
    "result_id",
    "task_id",
    "input_id",
    "work_key",
    "attempt",
    "status",
    "export_batch_id",
    "country_code",
    "company_id",
    "company_name",
    "query",
    "query_type",
    "processor_version",
    "answer_text",
    "route",
    "source_url",
    "error_type",
    "source_run_id",
    "completed_at",
)

COLUMNS_SQL = ", ".join(EXPORT_COLUMNS)
SOURCE_COLUMNS_SQL = COLUMNS_SQL.replace(
    "completed_at", "toDateTime64(completed_at, 6, 'UTC') AS completed_at"
)
SOURCE_SQL = (
    "postgresql(processing_postgres, table='brave_export', schema='processing')"
)
S3_SQL = (
    "s3(brave_history, filename=%(path)s, format='Parquet', structure=%(structure)s)"
)
PUBLISH_SQL = f"""INSERT INTO corpscout.{{table}} ({COLUMNS_SQL}, archive_path)
SELECT {SOURCE_COLUMNS_SQL}, %(path)s FROM {SOURCE_SQL}
WHERE export_batch_id = %(batch_id)s AND country_code = %(country)s AND status = 'success'
"""
POSTGRES_SETTINGS = {
    "max_execution_time": 60,
    "use_query_condition_cache": 0,
    "postgresql_connection_pool_size": 2,
    "postgresql_connection_attempt_timeout": 5,
}


def acknowledge_archive(
    store: ProcessingStore, batch: ExportBatch, manifest: list[dict]
) -> None:
    # Keep result identities and attribution for progress, cache hits and history lookup.
    # Publication, archival receipt and removal of the large answer commit together.
    with store.transaction() as cursor:
        cursor.execute(
            """UPDATE processing.export_batches SET published_at=coalesce(published_at,now()),
            archived_at=coalesce(archived_at,now()), archive_manifest=%s WHERE batch_id=%s""",
            (Json(manifest), batch.batch_id),
        )
        cursor.execute(
            "UPDATE processing.results SET payload=payload-'answer_text' WHERE export_batch_id=%s",
            (batch.batch_id,),
        )


def publish_batch(store: ProcessingStore, client, batch: ExportBatch) -> None:
    rows = client.execute(
        f"SELECT {SOURCE_COLUMNS_SQL} FROM {SOURCE_SQL} WHERE export_batch_id=%(batch_id)s ORDER BY result_id",
        {"batch_id": batch.batch_id},
        settings=POSTGRES_SETTINGS,
    )
    if len(rows) != batch.result_count:
        raise ValueError("PostgreSQL response count does not match the closed batch")
    records = [dict(zip(EXPORT_COLUMNS, row, strict=True)) for row in rows]
    manifest = []
    for country in sorted({row["country_code"] for row in records}):
        if re.fullmatch(r"[A-Z]{2}", country) is None:
            raise ValueError(
                "Brave output requires a two-letter uppercase country code"
            )
        table = f"{country.lower()}_company_brave_search_results_latest_success"
        archive = f"{country.lower()}_company_brave_search_results_s3_archive"
        if client.execute(f"EXISTS TABLE corpscout.{table}") != [(1,)]:
            raise ValueError(
                f"Provision corpscout.{table} before publishing this country's results"
            )
        # Schema belongs to migrations. Reuse it for empty/missing S3 file discovery.
        structure = ", ".join(
            f"{column[0]} {column[1]}"
            for column in client.execute(f"DESCRIBE TABLE corpscout.{archive}")
            if column[0] in EXPORT_COLUMNS
        )
        path = f"v1/country={country}/batch_id={batch.batch_id}/results.parquet"
        params = {
            "batch_id": batch.batch_id,
            "country": country,
            "path": path,
            "structure": structure,
        }
        expected = [
            tuple(record[column] for column in EXPORT_COLUMNS)
            for record in records
            if record["country_code"] == country
        ]
        # A scoped glob returns zero rows only when the immutable batch file is absent.
        archived = client.execute(
            f"SELECT {COLUMNS_SQL} FROM {S3_SQL} ORDER BY result_id",
            {**params, "path": path.replace("results.parquet", "*.parquet")},
            settings={
                "s3_throw_on_zero_files_match": 0,
                "use_query_condition_cache": 0,
            },
        )
        if not archived:
            client.execute(
                "INSERT INTO FUNCTION s3(brave_history, filename=%(path)s, format='Parquet') "
                f"SELECT {SOURCE_COLUMNS_SQL} FROM {SOURCE_SQL} "
                "WHERE export_batch_id=%(batch_id)s AND country_code=%(country)s ORDER BY result_id",
                params,
                settings={
                    **POSTGRES_SETTINGS,
                    "s3_truncate_on_insert": 0,
                    "s3_create_new_file_on_insert": 0,
                },
            )
            archived = client.execute(
                f"SELECT {COLUMNS_SQL} FROM {S3_SQL} ORDER BY result_id",
                params,
                settings={"use_query_condition_cache": 0},
            )
        if archived != expected:
            raise ValueError(
                "S3 archive contents do not match the complete PostgreSQL responses"
            )
        manifest.append(
            {
                "country_code": country,
                "path": path,
                "result_count": len(archived),
                "sha256": hashlib.sha256(
                    json.dumps(archived, default=str, ensure_ascii=False).encode()
                ).hexdigest(),
            }
        )
        client.execute(
            PUBLISH_SQL.format(table=table), params, settings=POSTGRES_SETTINGS
        )
        successful = [
            record
            for record in records
            if record["country_code"] == country and record["status"] == "success"
        ]
        if successful:
            current = client.execute(
                f"SELECT company_id,query_type,completed_at,result_id,answer_text FROM corpscout.{table} FINAL "
                "WHERE (company_id,query_type) IN %(keys)s",
                {
                    "keys": list(
                        {
                            (record["company_id"], record["query_type"])
                            for record in successful
                        }
                    )
                },
                settings={"use_query_condition_cache": 0},
            )
            current_by_key = {(row[0], row[1]): row for row in current}
            for record in successful:
                actual = current_by_key.get(
                    (record["company_id"], record["query_type"])
                )
                if actual is None or actual[2] < record["completed_at"]:
                    raise ValueError("Latest Brave answer was not published")
                if actual[2] == record["completed_at"] and actual[3:] != (
                    record["result_id"],
                    record["answer_text"],
                ):
                    raise ValueError(
                        "Conflicting Brave results share the same completion timestamp"
                    )
    acknowledge_archive(store, batch, manifest)


def publish_results(
    store: ProcessingStore, client, task_id: str, *, batch_size: int
) -> int:
    published = 0
    while True:
        batch = store.export_batch(
            task_id, limit=batch_size, destination=EXPORT_DESTINATION
        )
        if batch is None:
            # Backfill previously published pilot batches while their full PG answers exist.
            with store.transaction() as cursor:
                cursor.execute(
                    """SELECT batch_id::text,task_id::text,destination,result_count FROM processing.export_batches
                    WHERE task_id=%s AND archived_at IS NULL ORDER BY created_at LIMIT 1""",
                    (task_id,),
                )
                row = cursor.fetchone()
            if row is None:
                return published
            batch = ExportBatch(**row)
        publish_batch(store, client, batch)
        published += batch.result_count
