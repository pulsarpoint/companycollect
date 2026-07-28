import calendar
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Protocol

import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.latvia_iub_procurement import tables

USER_AGENT = "Corpscout/1.0 (Latvia IUB CC0 open-data ingestion)"


class _Response(Protocol):
    status_code: int
    content: bytes
    text: str

    def raise_for_status(self) -> None: ...


class _Session(Protocol):
    def get(self, url: str, timeout: int) -> _Response: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class IubMonthSnapshot:
    manifest_key: str
    object_keys: tuple[str, ...]
    downloaded: int
    reused: int
    absent: int
    absent_dates: tuple[str, ...]
    bytes_written: int


def daily_notice_url(day: date) -> str:
    return f"{tables.SOURCE_BASE_URL}/{day:%Y/%m/%d-%m-%Y}.json"


def month_dates(partition_key: str) -> tuple[date, ...]:
    start = date.fromisoformat(partition_key).replace(day=1)
    return tuple(
        start.replace(day=day)
        for day in range(1, calendar.monthrange(start.year, start.month)[1] + 1)
    )


def iub_http_session() -> requests.Session:
    client = dlt_requests.Client(
        request_timeout=120,
        request_max_attempts=4,
        request_backoff_factor=5.0,
        respect_retry_after_header=True,
    )
    client.session.headers.update({"User-Agent": USER_AGENT})
    return client.session


def sync_iub_month(
    *,
    object_store: ObjectStoreResource,
    partition_key: str,
    run_id: str,
    retrieved_at: datetime,
    session: _Session | None = None,
) -> IubMonthSnapshot:
    object_store.ensure_bucket(tables.S3_BUCKET)
    owns_session = session is None
    http_session = session or iub_http_session()
    object_keys: list[str] = []
    absent_dates: list[str] = []
    downloaded = reused = absent = bytes_written = 0
    try:
        for day in month_dates(partition_key):
            key = f"{tables.S3_NOTICE_PREFIX}/{day:%Y/%m/%d-%m-%Y}.json"
            if object_store.exists(key, bucket=tables.S3_BUCKET):
                object_keys.append(key)
                reused += 1
                continue
            response = http_session.get(daily_notice_url(day), timeout=120)
            if response.status_code == 404:
                absent += 1
                absent_dates.append(day.isoformat())
                continue
            response.raise_for_status()
            payload = json.loads(response.content)
            if not isinstance(payload, list):
                raise ValueError(f"IUB {day} payload is not a JSON array")
            object_store.write_bytes(key, response.content, bucket=tables.S3_BUCKET)
            object_keys.append(key)
            downloaded += 1
            bytes_written += len(response.content)
    finally:
        if owns_session:
            http_session.close()

    timestamp = retrieved_at.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%S.%fZ")
    manifest_key = (
        f"{tables.S3_MANIFEST_PREFIX}/{partition_key}/retrieved_at={timestamp}/"
        f"run_id={run_id}.json"
    )
    manifest = {
        "source_slug": tables.SOURCE_SLUG,
        "source_run_id": run_id,
        "partition_key": partition_key,
        "retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
        "licence": tables.SOURCE_LICENCE,
        "object_keys": object_keys,
        "downloaded": downloaded,
        "reused": reused,
        "absent": absent,
        "absent_dates": absent_dates,
        "bytes_written": bytes_written,
    }
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        bucket=tables.S3_BUCKET,
    )
    return IubMonthSnapshot(
        manifest_key=manifest_key,
        object_keys=tuple(object_keys),
        downloaded=downloaded,
        reused=reused,
        absent=absent,
        absent_dates=tuple(absent_dates),
        bytes_written=bytes_written,
    )


def latest_month_manifest(
    object_store: ObjectStoreResource, partition_key: str
) -> dict[str, Any]:
    prefix = f"{tables.S3_MANIFEST_PREFIX}/{partition_key}/"
    keys = sorted(
        key
        for key in object_store.list_keys(prefix, bucket=tables.S3_BUCKET)
        if key.endswith(".json")
    )
    if not keys:
        raise ValueError(
            f"No IUB manifest under s3://{tables.S3_BUCKET}/{prefix}; "
            "materialize the raw snapshot first"
        )
    manifests = [
        json.loads(object_store.read_bytes(key, bucket=tables.S3_BUCKET))
        for key in keys
    ]
    return max(manifests, key=lambda manifest: str(manifest["retrieved_at"]))
