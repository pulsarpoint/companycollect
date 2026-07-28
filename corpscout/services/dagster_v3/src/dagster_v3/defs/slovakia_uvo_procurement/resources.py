import calendar
import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Protocol

import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.slovakia_uvo_procurement import tables
from dagster_v3.defs.slovakia_uvo_procurement.parser import parse_bulletin_issue

USER_AGENT = "Corpscout/1.0 (Slovakia UVO public-bulletin ingestion)"


class _Response(Protocol):
    status_code: int
    content: bytes
    text: str

    def raise_for_status(self) -> None: ...


class _Session(Protocol):
    def get(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        timeout: int,
    ) -> _Response: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class UvoMonthSnapshot:
    manifest_key: str
    detail_keys: tuple[str, ...]
    detail_metadata: tuple[dict[str, str], ...]
    issue_files: int
    details_downloaded: int
    details_reused: int


def assert_machine_reuse_confirmed() -> None:
    confirmed = os.getenv(tables.LICENCE_CONFIRMATION_ENV, "").strip().lower()
    if confirmed not in {"1", "true", "yes", "on"}:
        raise RuntimeError(
            "Slovakia UVO machine-reuse licence has not been confirmed. "
            f"Set {tables.LICENCE_CONFIRMATION_ENV}=true only after an official "
            "licence or written reuse permission is recorded."
        )


def month_dates(partition_key: str) -> tuple[date, ...]:
    start = date.fromisoformat(partition_key).replace(day=1)
    return tuple(
        start.replace(day=day)
        for day in range(1, calendar.monthrange(start.year, start.month)[1] + 1)
    )


def uvo_http_session() -> requests.Session:
    client = dlt_requests.Client(
        request_timeout=120,
        request_max_attempts=4,
        request_backoff_factor=5.0,
        respect_retry_after_header=True,
    )
    client.session.headers.update({"User-Agent": USER_AGENT})
    return client.session


def sync_uvo_month(
    *,
    object_store: ObjectStoreResource,
    partition_key: str,
    run_id: str,
    retrieved_at: datetime,
    session: _Session | None = None,
) -> UvoMonthSnapshot:
    assert_machine_reuse_confirmed()
    object_store.ensure_bucket(tables.S3_BUCKET)
    owns_session = session is None
    http_session = session or uvo_http_session()
    issue_files = details_downloaded = details_reused = 0
    detail_keys: list[str] = []
    detail_metadata: list[dict[str, str]] = []
    seen_notice_ids: set[str] = set()
    partition_month = date.fromisoformat(partition_key).replace(day=1)
    try:
        for day in month_dates(partition_key):
            issue_key = f"{tables.S3_ISSUE_PREFIX}/{day:%Y/%m/%d-%m-%Y}.html"
            if object_store.exists(issue_key, bucket=tables.S3_BUCKET):
                issue_html = object_store.read_bytes(issue_key, bucket=tables.S3_BUCKET)
            else:
                response = http_session.get(
                    tables.BULLETIN_URL,
                    params={"date": day.strftime("%d.%m.%Y")},
                    timeout=120,
                )
                response.raise_for_status()
                issue_html = response.content
                object_store.write_bytes(issue_key, issue_html, bucket=tables.S3_BUCKET)
            issue_files += 1
            for notice in parse_bulletin_issue(issue_html, publication_date=day):
                notice_month = notice.publication_date.replace(day=1)
                if (
                    notice.uvo_notice_id in seen_notice_ids
                    or notice_month != partition_month
                ):
                    continue
                seen_notice_ids.add(notice.uvo_notice_id)
                detail_key = (
                    f"{tables.S3_DETAIL_PREFIX}/"
                    f"{notice.publication_date:%Y/%m}/"
                    f"{notice.uvo_notice_id}.html"
                )
                if object_store.exists(detail_key, bucket=tables.S3_BUCKET):
                    details_reused += 1
                else:
                    response = http_session.get(
                        notice.detail_url, params=None, timeout=120
                    )
                    response.raise_for_status()
                    object_store.write_bytes(
                        detail_key, response.content, bucket=tables.S3_BUCKET
                    )
                    details_downloaded += 1
                detail_keys.append(detail_key)
                detail_metadata.append(
                    {
                        "object_key": detail_key,
                        "uvo_notice_id": notice.uvo_notice_id,
                        "bulletin_number": notice.bulletin_number,
                        "bulletin_code": notice.bulletin_code,
                        "publication_date": notice.publication_date.isoformat(),
                    }
                )
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
        "licence_status": tables.SOURCE_LICENCE,
        "issue_files": issue_files,
        "detail_keys": detail_keys,
        "detail_metadata": detail_metadata,
        "details_downloaded": details_downloaded,
        "details_reused": details_reused,
    }
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        bucket=tables.S3_BUCKET,
    )
    return UvoMonthSnapshot(
        manifest_key=manifest_key,
        detail_keys=tuple(detail_keys),
        detail_metadata=tuple(detail_metadata),
        issue_files=issue_files,
        details_downloaded=details_downloaded,
        details_reused=details_reused,
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
            f"No UVO manifest under s3://{tables.S3_BUCKET}/{prefix}; "
            "materialize the raw snapshot first"
        )
    manifests = [
        json.loads(object_store.read_bytes(key, bucket=tables.S3_BUCKET))
        for key in keys
    ]
    return max(manifests, key=lambda manifest: str(manifest["retrieved_at"]))
