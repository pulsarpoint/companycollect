"""Migrate legacy run-scoped Wikidata objects into weekly exchange partitions.

The command is a dry run unless ``--execute`` is supplied. Source deletion is
separately gated by ``--delete-source`` and only happens after the target
bucket passes exact object, byte, ETag, JSON, manifest-reference, and source
immutability checks.

Run from ``services/dagster_v3``:

    uv run python -m dagster_v3.defs.wikidata.migrate_raw_bucket
    uv run python -m dagster_v3.defs.wikidata.migrate_raw_bucket --execute
    uv run python -m dagster_v3.defs.wikidata.migrate_raw_bucket \
        --execute --delete-source
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import boto3
from botocore.config import Config
from dotenv import load_dotenv

from dagster_v3.defs.wikidata.source import (
    WIKIDATA_RAW_BUCKET,
    active_exchanges_object_key,
    seed_units_object_key,
    snapshot_manifest_object_key,
)

LEGACY_WIKIDATA_RAW_BUCKET = "source-wikidata-company-seed"
LEGACY_OBJECT_KEY_PATTERN = re.compile(
    r"^raw/run_id=(?P<run_id>[^/]+)/"
    r"retrieved_date=(?P<retrieved_date>\d{4}-\d{2}-\d{2})/"
    r"(?P<suffix>.+)$"
)


@dataclass(frozen=True)
class LegacyObject:
    key: str
    target_key: str
    size: int
    etag: str
    last_modified: datetime


@dataclass(frozen=True)
class PlannedJsonWrite:
    key: str
    body: bytes


@dataclass(frozen=True)
class WeeklyPartitionSummary:
    partition_date: str
    exchange_count: int
    registry_property_count: int
    manifest_count: int
    source_row_count: int
    source_page_count: int


@dataclass(frozen=True)
class WikidataRawMigrationPlan:
    source_objects: tuple[LegacyObject, ...]
    copies: tuple[LegacyObject, ...]
    json_writes: tuple[PlannedJsonWrite, ...]
    partitions: tuple[WeeklyPartitionSummary, ...]

    @property
    def expected_target_keys(self) -> set[str]:
        return {
            *(item.target_key for item in self.copies),
            *(item.key for item in self.json_writes),
        }

    @property
    def expected_target_bytes(self) -> int:
        return sum(item.size for item in self.copies) + sum(
            len(item.body) for item in self.json_writes
        )


def weekly_partition_date(retrieved_date: str) -> str:
    parsed_date = date.fromisoformat(retrieved_date)
    return (parsed_date - timedelta(days=parsed_date.weekday())).isoformat()


def legacy_object_target_key(
    legacy_key: str,
    *,
    partition_date_override: str | None = None,
) -> str:
    match = LEGACY_OBJECT_KEY_PATTERN.fullmatch(legacy_key)
    if match is None:
        raise ValueError(f"Unexpected legacy Wikidata object key: {legacy_key}")
    partition_date = partition_date_override or weekly_partition_date(
        match.group("retrieved_date")
    )
    date.fromisoformat(partition_date)
    return f"partition_date={partition_date}/{match.group('suffix')}"


def rewrite_legacy_manifest(
    manifest: dict[str, Any],
    *,
    target_key: str,
) -> dict[str, Any]:
    partition_date, exchange_id = _partition_and_exchange_from_manifest_key(target_key)
    rewritten = {
        key: value
        for key, value in manifest.items()
        if key not in {"run_id", "retrieved_date"}
    }
    rewritten.update(
        {
            "status": "complete",
            "partition_date": partition_date,
            "source_run_id": partition_date,
            "exchange_id": exchange_id,
        }
    )
    for field_name in ("objects", "augmentation_objects"):
        raw_keys = rewritten.get(field_name, [])
        if not isinstance(raw_keys, list):
            raise ValueError(f"Legacy manifest field {field_name} is not a list")
        rewritten[field_name] = [
            legacy_object_target_key(
                str(object_key),
                partition_date_override=partition_date,
            )
            for object_key in raw_keys
        ]
    return rewritten


def build_wikidata_raw_migration_plan(
    client: Any,
    *,
    source_bucket: str,
    partition_date_override: str | None = None,
) -> WikidataRawMigrationPlan:
    """Read all source metadata and manifests without changing either bucket."""
    source_objects = tuple(
        _list_legacy_objects(
            client,
            source_bucket=source_bucket,
            partition_date_override=partition_date_override,
        )
    )
    if not source_objects:
        raise ValueError(f"No legacy Wikidata objects found in {source_bucket}")

    selected_objects_by_target: dict[str, LegacyObject] = {}
    for source_object in source_objects:
        selected_objects_by_target[source_object.target_key] = max(
            selected_objects_by_target.get(source_object.target_key, source_object),
            source_object,
            key=lambda item: (item.last_modified, item.key),
        )

    selected_manifests_by_partition: dict[
        str, dict[str, tuple[str, dict[str, Any]]]
    ] = defaultdict(dict)
    copied_objects: list[LegacyObject] = []
    generated_writes_by_key: dict[str, PlannedJsonWrite] = {}

    for target_key, source_object in sorted(selected_objects_by_target.items()):
        if target_key.endswith("/manifest.json") and not target_key.endswith(
            "/snapshot_manifest.json"
        ):
            manifest = _read_json(client, source_bucket, source_object.key)
            rewritten = rewrite_legacy_manifest(manifest, target_key=target_key)
            partition_date, exchange_id = _partition_and_exchange_from_manifest_key(
                target_key
            )
            selected_manifests_by_partition[partition_date][exchange_id] = (
                target_key,
                rewritten,
            )
            generated_writes_by_key[target_key] = PlannedJsonWrite(
                key=target_key,
                body=_json_bytes(rewritten),
            )
            continue
        if target_key.endswith("/snapshot_manifest.json"):
            continue
        copied_objects.append(source_object)

    partition_summaries: list[WeeklyPartitionSummary] = []
    for partition_date, manifests_by_exchange in sorted(
        selected_manifests_by_partition.items()
    ):
        partition_writes, summary = _build_partition_metadata_writes(
            client,
            source_bucket=source_bucket,
            partition_date=partition_date,
            manifests_by_exchange=manifests_by_exchange,
            selected_objects_by_target=selected_objects_by_target,
        )
        for write in partition_writes:
            generated_writes_by_key[write.key] = write
        partition_summaries.append(summary)

    plan = WikidataRawMigrationPlan(
        source_objects=source_objects,
        copies=tuple(copied_objects),
        json_writes=tuple(
            generated_writes_by_key[key] for key in sorted(generated_writes_by_key)
        ),
        partitions=tuple(partition_summaries),
    )
    _validate_plan_references(plan)
    return plan


def execute_wikidata_raw_migration(
    client: Any,
    *,
    plan: WikidataRawMigrationPlan,
    source_bucket: str,
    target_bucket: str,
) -> None:
    _ensure_bucket(client, target_bucket)
    unexpected_existing_keys = (
        set(_list_bucket_objects(client, target_bucket)) - plan.expected_target_keys
    )
    if unexpected_existing_keys:
        raise ValueError(
            f"Target bucket contains unexpected keys: "
            f"{sorted(unexpected_existing_keys)[:10]}"
        )

    for number, source_object in enumerate(plan.copies, start=1):
        client.copy_object(
            Bucket=target_bucket,
            Key=source_object.target_key,
            CopySource={"Bucket": source_bucket, "Key": source_object.key},
        )
        if number % 500 == 0 or number == len(plan.copies):
            print(
                f"copied {number:,}/{len(plan.copies):,} canonical objects", flush=True
            )

    for write in plan.json_writes:
        client.put_object(
            Bucket=target_bucket,
            Key=write.key,
            Body=write.body,
            ContentType="application/json",
        )
    print(f"wrote {len(plan.json_writes):,} canonical metadata objects", flush=True)


def verify_wikidata_raw_migration(
    client: Any,
    *,
    plan: WikidataRawMigrationPlan,
    target_bucket: str,
) -> None:
    target_objects = _list_bucket_objects(client, target_bucket)
    actual_keys = set(target_objects)
    expected_keys = plan.expected_target_keys
    if actual_keys != expected_keys:
        raise ValueError(
            "Target key verification failed: "
            f"missing={sorted(expected_keys - actual_keys)[:10]} "
            f"unexpected={sorted(actual_keys - expected_keys)[:10]}"
        )
    actual_bytes = sum(int(item["Size"]) for item in target_objects.values())
    if actual_bytes != plan.expected_target_bytes:
        raise ValueError(
            "Target byte verification failed: "
            f"expected={plan.expected_target_bytes} actual={actual_bytes}"
        )

    for copy in plan.copies:
        target = target_objects[copy.target_key]
        if int(target["Size"]) != copy.size or str(target["ETag"]) != copy.etag:
            raise ValueError(f"Copied object verification failed: {copy.target_key}")

    expected_json_bodies = {write.key: write.body for write in plan.json_writes}
    for number, target_key in enumerate(sorted(expected_keys), start=1):
        body = client.get_object(Bucket=target_bucket, Key=target_key)["Body"].read()
        try:
            json.loads(body)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Target object is not valid JSON: {target_key}") from exc
        expected_body = expected_json_bodies.get(target_key)
        if expected_body is not None and body != expected_body:
            raise ValueError(f"Generated JSON verification failed: {target_key}")
        if number % 500 == 0 or number == len(expected_keys):
            print(
                f"validated JSON {number:,}/{len(expected_keys):,} target objects",
                flush=True,
            )

    _validate_target_manifest_references(
        client,
        target_bucket=target_bucket,
        expected_keys=expected_keys,
    )
    print(
        f"verified {len(expected_keys):,} objects and "
        f"{actual_bytes:,} bytes in {target_bucket}",
        flush=True,
    )


def assert_source_inventory_unchanged(
    client: Any,
    *,
    plan: WikidataRawMigrationPlan,
    source_bucket: str,
) -> None:
    current_objects = _list_bucket_objects(client, source_bucket, prefix="raw/")
    expected = {item.key: (item.size, item.etag) for item in plan.source_objects}
    current = {
        key: (int(item["Size"]), str(item["ETag"]))
        for key, item in current_objects.items()
    }
    if current != expected:
        raise ValueError("Source bucket changed during migration; refusing deletion")


def delete_verified_source_bucket(
    client: Any,
    *,
    plan: WikidataRawMigrationPlan,
    source_bucket: str,
) -> None:
    assert_source_inventory_unchanged(
        client,
        plan=plan,
        source_bucket=source_bucket,
    )
    source_keys = [item.key for item in plan.source_objects]
    for offset in range(0, len(source_keys), 1000):
        batch = source_keys[offset : offset + 1000]
        response = client.delete_objects(
            Bucket=source_bucket,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )
        errors = response.get("Errors", [])
        if errors:
            raise ValueError(f"Source object deletion failed: {errors[:5]}")
    remaining_objects = _list_bucket_objects(client, source_bucket)
    if remaining_objects:
        raise ValueError(
            f"Source bucket still contains objects: {sorted(remaining_objects)[:10]}"
        )
    client.delete_bucket(Bucket=source_bucket)
    try:
        client.head_bucket(Bucket=source_bucket)
    except Exception as exc:
        if _s3_error_code(exc) in {"404", "NoSuchBucket", "NotFound"}:
            print(f"deleted verified source bucket {source_bucket}", flush=True)
            return
        raise
    raise ValueError(f"Source bucket still exists after deletion: {source_bucket}")


def print_migration_plan(
    plan: WikidataRawMigrationPlan,
    *,
    source_bucket: str,
    target_bucket: str,
) -> None:
    print(f"source_bucket={source_bucket}")
    print(f"target_bucket={target_bucket}")
    print(f"source_objects={len(plan.source_objects):,}")
    print(f"canonical_copies={len(plan.copies):,}")
    print(f"canonical_json_writes={len(plan.json_writes):,}")
    print(f"target_objects={len(plan.expected_target_keys):,}")
    print(f"target_bytes={plan.expected_target_bytes:,}")
    print(
        f"deduplicated_legacy_objects="
        f"{len(plan.source_objects) - len(plan.expected_target_keys):,}"
    )
    for summary in plan.partitions:
        print(
            "partition="
            f"{summary.partition_date} "
            f"exchanges={summary.exchange_count} "
            f"registry_properties={summary.registry_property_count} "
            f"manifests={summary.manifest_count} "
            f"pages={summary.source_page_count} "
            f"rows={summary.source_row_count}"
        )


def _list_legacy_objects(
    client: Any,
    *,
    source_bucket: str,
    partition_date_override: str | None,
) -> list[LegacyObject]:
    listed_objects = _list_bucket_objects(client, source_bucket, prefix="raw/")
    return [
        LegacyObject(
            key=key,
            target_key=legacy_object_target_key(
                key,
                partition_date_override=partition_date_override,
            ),
            size=int(item["Size"]),
            etag=str(item["ETag"]),
            last_modified=item["LastModified"],
        )
        for key, item in sorted(listed_objects.items())
    ]


def _build_partition_metadata_writes(
    client: Any,
    *,
    source_bucket: str,
    partition_date: str,
    manifests_by_exchange: dict[str, tuple[str, dict[str, Any]]],
    selected_objects_by_target: dict[str, LegacyObject],
) -> tuple[list[PlannedJsonWrite], WeeklyPartitionSummary]:
    active_key = active_exchanges_object_key(partition_date=partition_date)
    active_source = selected_objects_by_target.get(active_key)
    if active_source is None:
        raise ValueError(
            f"No active exchange catalog exists for partition {partition_date}"
        )
    active_payload = _read_json(client, source_bucket, active_source.key)
    active_exchange_ids = _active_exchange_ids(active_payload)
    exchange_manifest_ids = {
        exchange_id
        for exchange_id, (_, manifest) in manifests_by_exchange.items()
        if manifest.get("query_mode") == "exchange"
    }
    if exchange_manifest_ids != active_exchange_ids:
        raise ValueError(
            f"Incomplete exchange manifests for partition {partition_date}: "
            f"missing={sorted(active_exchange_ids - exchange_manifest_ids)[:10]} "
            f"unexpected={sorted(exchange_manifest_ids - active_exchange_ids)[:10]}"
        )

    ordered_manifests = [
        manifests_by_exchange[exchange_id][1]
        for exchange_id in sorted(manifests_by_exchange)
    ]
    manifest_keys = [
        manifests_by_exchange[exchange_id][0]
        for exchange_id in sorted(manifests_by_exchange)
    ]
    seed_units = [_seed_unit_from_manifest(manifest) for manifest in ordered_manifests]
    completed_at = max(
        (
            str(manifest.get("completed_at") or manifest.get("started_at") or "")
            for manifest in ordered_manifests
        ),
        default=f"{partition_date}T00:00:00+00:00",
    )
    seed_catalog = {
        "source": "wikidata",
        "status": "complete",
        "partition_date": partition_date,
        "source_run_id": partition_date,
        "retrieved_at": completed_at,
        "active_exchanges_key": active_key,
        "seed_units": seed_units,
    }
    exchange_count = len(exchange_manifest_ids)
    row_count = sum(
        int(manifest.get("row_count") or 0) for manifest in ordered_manifests
    )
    page_count = sum(
        int(manifest.get("page_count") or 0) for manifest in ordered_manifests
    )
    augmentation_row_count = sum(
        int(manifest.get("augmentation_row_count") or 0)
        for manifest in ordered_manifests
    )
    augmentation_object_count = sum(
        len(manifest.get("augmentation_objects", [])) for manifest in ordered_manifests
    )
    snapshot_manifest = {
        "source": "wikidata",
        "status": "complete",
        "partition_date": partition_date,
        "source_run_id": partition_date,
        "completed_at": completed_at,
        "exchange_count": exchange_count,
        "registry_property_count": len(ordered_manifests) - exchange_count,
        "page_count": page_count,
        "row_count": row_count,
        "augmentation_object_count": augmentation_object_count,
        "augmentation_row_count": augmentation_row_count,
        "manifest_keys": manifest_keys,
    }
    writes = [
        PlannedJsonWrite(
            key=seed_units_object_key(partition_date=partition_date),
            body=_json_bytes(seed_catalog),
        ),
        PlannedJsonWrite(
            key=snapshot_manifest_object_key(partition_date=partition_date),
            body=_json_bytes(snapshot_manifest),
        ),
    ]
    return writes, WeeklyPartitionSummary(
        partition_date=partition_date,
        exchange_count=exchange_count,
        registry_property_count=len(ordered_manifests) - exchange_count,
        manifest_count=len(ordered_manifests),
        source_row_count=row_count,
        source_page_count=page_count,
    )


def _seed_unit_from_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "exchange_wikidata_id": str(manifest["exchange_id"]),
        "exchange_name": str(manifest.get("exchange_name") or ""),
        "listed_company_count_on_exchange": int(
            manifest.get("listed_company_count_on_exchange") or 0
        ),
        "mics": list(manifest.get("mics") or []),
        "country_wikidata_id": str(manifest.get("country_wikidata_id") or ""),
        "country_name": str(manifest.get("country_name") or ""),
        "country_iso2": str(manifest.get("country_iso2") or ""),
        "query_mode": str(manifest.get("query_mode") or "exchange"),
        "registry_property_id": manifest.get("registry_property_id"),
    }


def _active_exchange_ids(payload: dict[str, Any]) -> set[str]:
    bindings = payload.get("results", {}).get("bindings", [])
    if not isinstance(bindings, list):
        raise ValueError("Active exchange payload has no bindings list")
    exchange_ids = {
        str(binding.get("exchange", {}).get("value", "")).rsplit("/", 1)[-1]
        for binding in bindings
        if isinstance(binding, dict)
        and str(binding.get("exchange", {}).get("value", ""))
    }
    if not exchange_ids:
        raise ValueError("Active exchange payload contains no exchange ids")
    return exchange_ids


def _validate_plan_references(plan: WikidataRawMigrationPlan) -> None:
    expected_keys = plan.expected_target_keys
    for write in plan.json_writes:
        if not write.key.endswith("/manifest.json") or write.key.endswith(
            "/snapshot_manifest.json"
        ):
            continue
        manifest = json.loads(write.body)
        referenced_keys = {
            *(str(key) for key in manifest.get("objects", [])),
            *(str(key) for key in manifest.get("augmentation_objects", [])),
        }
        missing_keys = referenced_keys - expected_keys
        if missing_keys:
            raise ValueError(
                f"Rewritten manifest {write.key} references missing objects: "
                f"{sorted(missing_keys)[:10]}"
            )


def _validate_target_manifest_references(
    client: Any,
    *,
    target_bucket: str,
    expected_keys: set[str],
) -> None:
    snapshot_keys = sorted(
        key for key in expected_keys if key.endswith("/snapshot_manifest.json")
    )
    if not snapshot_keys:
        raise ValueError("Target bucket contains no snapshot manifest")
    for snapshot_key in snapshot_keys:
        snapshot = _read_json(client, target_bucket, snapshot_key)
        if snapshot.get("status") != "complete":
            raise ValueError(f"Snapshot is not complete: {snapshot_key}")
        for manifest_key in snapshot.get("manifest_keys", []):
            manifest_key = str(manifest_key)
            if manifest_key not in expected_keys:
                raise ValueError(
                    f"Snapshot references missing manifest: {manifest_key}"
                )
            manifest = _read_json(client, target_bucket, manifest_key)
            if manifest.get("status") != "complete":
                raise ValueError(f"Unit manifest is not complete: {manifest_key}")
            references = {
                *(str(key) for key in manifest.get("objects", [])),
                *(str(key) for key in manifest.get("augmentation_objects", [])),
            }
            missing_keys = references - expected_keys
            if missing_keys:
                raise ValueError(
                    f"Manifest {manifest_key} references missing objects: "
                    f"{sorted(missing_keys)[:10]}"
                )


def _partition_and_exchange_from_manifest_key(target_key: str) -> tuple[str, str]:
    parts = target_key.split("/")
    if (
        len(parts) != 3
        or not parts[0].startswith("partition_date=")
        or not parts[1].startswith("exchange_id=")
        or parts[2] != "manifest.json"
    ):
        raise ValueError(f"Unexpected target manifest key: {target_key}")
    return parts[0].split("=", 1)[1], parts[1].split("=", 1)[1]


def _read_json(client: Any, bucket: str, key: str) -> dict[str, Any]:
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    payload = json.loads(body)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in s3://{bucket}/{key}")
    return payload


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _list_bucket_objects(
    client: Any,
    bucket: str,
    *,
    prefix: str = "",
) -> dict[str, dict[str, Any]]:
    paginator = client.get_paginator("list_objects_v2")
    return {
        str(item["Key"]): item
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for item in page.get("Contents", [])
    }


def _ensure_bucket(client: Any, bucket: str) -> None:
    try:
        client.create_bucket(Bucket=bucket)
    except Exception as exc:
        if _s3_error_code(exc) not in {
            "BucketAlreadyExists",
            "BucketAlreadyOwnedByYou",
        }:
            raise


def _s3_error_code(exc: Exception) -> str:
    response = getattr(exc, "response", {})
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    return str(error.get("Code", ""))


def _s3_client() -> Any:
    load_dotenv()
    return boto3.client(
        "s3",
        endpoint_url=os.environ["CORPSCOUT_S3_ENDPOINT"],
        aws_access_key_id=os.environ["CORPSCOUT_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["CORPSCOUT_S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-bucket",
        default=LEGACY_WIKIDATA_RAW_BUCKET,
    )
    parser.add_argument(
        "--target-bucket",
        default=WIKIDATA_RAW_BUCKET,
    )
    parser.add_argument(
        "--partition-date",
        help="Force every legacy object into this weekly partition date",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Copy and rewrite objects into the target bucket",
    )
    parser.add_argument(
        "--delete-source",
        action="store_true",
        help="Delete the verified legacy source bucket after migration",
    )
    args = parser.parse_args()
    if args.delete_source and not args.execute:
        parser.error("--delete-source requires --execute")
    if args.source_bucket == args.target_bucket:
        parser.error("source and target buckets must be different")

    client = _s3_client()
    plan = build_wikidata_raw_migration_plan(
        client,
        source_bucket=args.source_bucket,
        partition_date_override=args.partition_date,
    )
    print_migration_plan(
        plan,
        source_bucket=args.source_bucket,
        target_bucket=args.target_bucket,
    )
    if not args.execute:
        print("dry run only; no buckets or objects changed")
        return 0

    execute_wikidata_raw_migration(
        client,
        plan=plan,
        source_bucket=args.source_bucket,
        target_bucket=args.target_bucket,
    )
    verify_wikidata_raw_migration(
        client,
        plan=plan,
        target_bucket=args.target_bucket,
    )
    assert_source_inventory_unchanged(
        client,
        plan=plan,
        source_bucket=args.source_bucket,
    )
    if args.delete_source:
        delete_verified_source_bucket(
            client,
            plan=plan,
            source_bucket=args.source_bucket,
        )
    else:
        print("source bucket retained; pass --delete-source after verification")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
