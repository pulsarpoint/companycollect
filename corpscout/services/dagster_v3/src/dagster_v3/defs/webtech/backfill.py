"""Replay the durable result index into per-technology observations, without scanning."""

import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

from clickhouse_driver import Client

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.webtech.models import (
    StoredDomainResultDocument,
    StoredResultReference,
)
from dagster_v3.defs.webtech.technologies import (
    TechnologyCatalog,
    insert_technology_rows,
    load_technology_catalog,
    technology_rows,
)

INDEX_COLUMNS = (
    "root_domain",
    "crawl_id",
    "detector_version",
    "scan_id",
    "harmonic_rank",
    "outcome",
    "timeout_stage",
    "technology_count",
    "duration_ms",
    "result_bucket",
    "result_object_key",
    "report_sha256",
    "report_size_bytes",
    "run_id",
)


def read_indexed_technologies(
    index: dict[str, Any],
    object_store: ObjectStoreResource,
    catalog: TechnologyCatalog,
) -> list[tuple[object, ...]]:
    reference = StoredResultReference(
        root_domain=index["root_domain"],
        harmonic_rank=index["harmonic_rank"],
        outcome=index["outcome"],
        timeout_stage=index["timeout_stage"] or None,
        technology_count=index["technology_count"],
        duration_ms=index["duration_ms"],
        object_key=index["result_object_key"],
        sha256=index["report_sha256"],
        size_bytes=index["report_size_bytes"],
    )
    body = object_store.read_bytes(reference.object_key, bucket=index["result_bucket"])
    if (
        len(body) != reference.size_bytes
        or hashlib.sha256(body).hexdigest() != reference.sha256
    ):
        raise ValueError(
            f"Stored Webtech report checksum/size mismatch: {reference.object_key}"
        )
    document = StoredDomainResultDocument.model_validate_json(body)
    if (
        document.candidate.root_domain != index["root_domain"]
        or document.candidate.harmonic_rank != index["harmonic_rank"]
        or document.scan_id != index["scan_id"]
        or document.crawl_id != index["crawl_id"]
        or document.detector_version != index["detector_version"]
        or document.outcome != index["outcome"]
    ):
        raise ValueError(
            f"Stored Webtech report identity mismatch: {reference.object_key}"
        )
    rows = technology_rows(
        document,
        reference,
        catalog,
        bucket=index["result_bucket"],
        run_id=index["run_id"],
        recorded_at=datetime.now(UTC),
    )
    if len(rows) != reference.technology_count:
        raise ValueError(
            f"Stored Webtech report detection count mismatch: {reference.object_key}"
        )
    return rows


def backfill_technology_results(
    client: Client,
    object_store: ObjectStoreResource,
    *,
    batch_size: int = 250,
    workers: int = 8,
    limit: int = 0,
) -> dict[str, int]:
    if batch_size < 1 or workers < 1 or limit < 0:
        raise ValueError(
            "batch_size/workers must be positive and limit must be nonnegative"
        )
    catalog = load_technology_catalog(client)
    object_store.client()  # Initialize once before sharing the boto client with workers.
    cursor = ("", "", "", "")
    processed = skipped = detections = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        while limit == 0 or processed + skipped < limit:
            size = (
                batch_size
                if limit == 0
                else min(batch_size, limit - processed - skipped)
            )
            batch = client.execute(
                f"SELECT {', '.join(INDEX_COLUMNS)} "
                "FROM corpscout.webtech_domain_scan_results FINAL "
                "WHERE technology_count > 0 "
                "AND (root_domain, crawl_id, detector_version, scan_id) > %(cursor)s "
                "ORDER BY root_domain, crawl_id, detector_version, scan_id LIMIT %(size)s",
                {"cursor": cursor, "size": size},
            )
            if len(batch) == 0:
                break
            indexes = [dict(zip(INDEX_COLUMNS, row, strict=True)) for row in batch]
            existing = {
                tuple(row[:5]): int(row[5])
                for row in client.execute(
                    "SELECT root_domain, crawl_id, detector_version, scan_id, report_sha256, count() "
                    "FROM corpscout.webtech_domain_technologies FINAL "
                    "WHERE root_domain IN %(domains)s "
                    "GROUP BY root_domain, crawl_id, detector_version, scan_id, report_sha256",
                    {"domains": tuple(index["root_domain"] for index in indexes)},
                )
            }
            pending = []
            for index in indexes:
                key = tuple(index[name] for name in INDEX_COLUMNS[:4]) + (
                    index["report_sha256"],
                )
                if existing.get(key) == index["technology_count"]:
                    skipped += 1
                else:
                    pending.append(index)
            # Validate the whole bounded batch before publishing any of its detections.
            batches = list(
                executor.map(
                    lambda index: read_indexed_technologies(
                        index, object_store, catalog
                    ),
                    pending,
                )
            )
            rows = [row for result in batches for row in result]
            insert_technology_rows(client, rows)
            processed += len(pending)
            detections += len(rows)
            cursor = tuple(indexes[-1][name] for name in INDEX_COLUMNS[:4])
    return {
        "reports_indexed": processed,
        "reports_skipped": skipped,
        "technology_rows": detections,
    }
