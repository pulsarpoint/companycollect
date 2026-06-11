"""Finland PRH YTJ assets."""

from datetime import datetime, timezone

import dagster as dg

from dagster_corpscout.lib.manifest import Artifact, build_manifest
from dagster_corpscout.lib.streaming import StreamStats
from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland_prhytj import spec
from dagster_corpscout.sources.finland_prhytj.client import (
    fetch_code_list,
    iter_companies,
    ndjson_chunks,
)


@dg.asset(
    key_prefix=[spec.SOURCE_NAME],
    name="raw_snapshot",
    group_name=spec.SOURCE_NAME,
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL),
    op_tags={"dagster/concurrency_key": spec.SOURCE_NAME},
)
def raw_snapshot(context: dg.AssetExecutionContext, rustfs: RustFSResource) -> dg.MaterializeResult:
    """Pull the company snapshot and code lists from PRH YTJ into RustFS."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dagster_run_id = context.run.run_id
    run_id = f"{timestamp}-{dagster_run_id[:8]}"
    artifacts: list[Artifact] = []

    snapshot_key = spec.snapshot_object_key(run_id)
    record_stats = StreamStats()
    chunks = ndjson_chunks(iter_companies(spec.BASE_URL), record_stats)
    upload_stats = rustfs.upload_stream(spec.BUCKET, snapshot_key, chunks)
    context.log.info(
        "snapshot uploaded: %d records, %d bytes",
        record_stats.records,
        upload_stats.bytes_read,
    )
    artifacts.append(
        Artifact(
            key="source",
            object_key=snapshot_key,
            content_sha256=upload_stats.sha256_hex,
            content_length_bytes=upload_stats.bytes_read,
            records_written=record_stats.records,
        )
    )

    for code, lang in spec.CODE_LISTS:
        body = fetch_code_list(spec.BASE_URL, code, lang)
        key = spec.code_list_object_key(run_id, code, lang)
        sha256_hex = rustfs.put_bytes(spec.BUCKET, key, body)
        artifacts.append(
            Artifact(
                key=f"codelist_{code}_{lang}",
                object_key=key,
                content_sha256=sha256_hex,
                content_length_bytes=len(body),
                records_written=0,
            )
        )

    manifest = build_manifest(
        run_id=run_id,
        source=spec.SOURCE_NAME,
        workflow_id=f"dagster-run-{dagster_run_id}",
        artifacts=artifacts,
    )
    rustfs.put_json(spec.BUCKET, spec.manifest_object_key(run_id), manifest)

    return dg.MaterializeResult(
        metadata={
            "run_id": run_id,
            "bucket": spec.BUCKET,
            "snapshot_object_key": snapshot_key,
            "records": record_stats.records,
            "snapshot_bytes": upload_stats.bytes_read,
            "snapshot_sha256": upload_stats.sha256_hex,
            "artifact_count": len(artifacts),
        }
    )
