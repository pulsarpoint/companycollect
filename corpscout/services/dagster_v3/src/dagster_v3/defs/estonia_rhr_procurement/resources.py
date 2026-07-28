from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from typing import Any, Protocol

import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.estonia_rhr_procurement import tables
from dagster_v3.defs.ted_procurement.client import iter_search_notices

USER_AGENT = "Corpscout/1.0 (Estonia RHR open-data ingestion)"


class _Response(Protocol):
    content: bytes

    def raise_for_status(self) -> None: ...


class _Session(Protocol):
    def get(self, url: str, timeout: int) -> _Response: ...

    def post(self, url: str, *, json: Any, timeout: int) -> Any: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class RhrMonthSnapshot:
    manifest_key: str
    xml_object_key: str
    ted_index_object_key: str
    xml_bytes: int
    ted_notices: int
    reused_objects: int


def award_notice_url(partition_key: str) -> str:
    month = date.fromisoformat(partition_key)
    return (
        f"{tables.SOURCE_API_ROOT}/notice_award/"
        f"{month.year}/month/{month.month}/xml"
    )


def rhr_http_session() -> requests.Session:
    client = dlt_requests.Client(
        request_timeout=180,
        request_max_attempts=5,
        request_backoff_factor=5.0,
        respect_retry_after_header=True,
    )
    client.session.headers.update({"User-Agent": USER_AGENT})
    return client.session


def sync_rhr_month(
    *,
    object_store: ObjectStoreResource,
    partition_key: str,
    run_id: str,
    retrieved_at: datetime,
    session: _Session | None = None,
) -> RhrMonthSnapshot:
    object_store.ensure_bucket(tables.S3_BUCKET)
    owns_session = session is None
    http_session = session or rhr_http_session()
    try:
        response = http_session.get(award_notice_url(partition_key), timeout=180)
        response.raise_for_status()
        xml_bytes = response.content
        if b"<OPEN-DATA" not in xml_bytes[:500]:
            raise ValueError("RHR award response is not an OPEN-DATA XML bundle")
        ted_index = fetch_ted_index(partition_key=partition_key, session=http_session)
    finally:
        if owns_session:
            http_session.close()

    month = date.fromisoformat(partition_key)
    xml_digest = sha256(xml_bytes).hexdigest()
    xml_object_key = (
        f"{tables.S3_AWARDS_PREFIX}/year={month.year}/month={month.month:02d}/"
        f"sha256={xml_digest}/HLST_{month.year}_{month.month}.xml"
    )
    ted_bytes = json.dumps(
        ted_index, ensure_ascii=False, sort_keys=True
    ).encode()
    ted_digest = sha256(ted_bytes).hexdigest()
    ted_index_object_key = (
        f"{tables.S3_TED_INDEX_PREFIX}/year={month.year}/month={month.month:02d}/"
        f"sha256={ted_digest}/index.json"
    )
    reused = 0
    for key, payload in (
        (xml_object_key, xml_bytes),
        (ted_index_object_key, ted_bytes),
    ):
        if object_store.exists(key, bucket=tables.S3_BUCKET):
            reused += 1
        else:
            object_store.write_bytes(key, payload, bucket=tables.S3_BUCKET)

    timestamp = retrieved_at.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%S.%fZ")
    manifest_key = (
        f"{tables.S3_MANIFEST_PREFIX}/{partition_key}/retrieved_at={timestamp}/"
        f"run_id={run_id}.json"
    )
    object_store.write_json(
        manifest_key,
        json.dumps(
            {
                "source_slug": tables.SOURCE_SLUG,
                "source_run_id": run_id,
                "partition_key": partition_key,
                "retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
                "licence": tables.SOURCE_LICENCE,
                "source_url": award_notice_url(partition_key),
                "xml_object_key": xml_object_key,
                "ted_index_object_key": ted_index_object_key,
                "xml_bytes": len(xml_bytes),
                "ted_notices": len(ted_index),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        bucket=tables.S3_BUCKET,
    )
    return RhrMonthSnapshot(
        manifest_key=manifest_key,
        xml_object_key=xml_object_key,
        ted_index_object_key=ted_index_object_key,
        xml_bytes=len(xml_bytes),
        ted_notices=len(ted_index),
        reused_objects=reused,
    )


def fetch_ted_index(
    *, partition_key: str, session: _Session | None = None
) -> dict[str, dict[str, str]]:
    month_start = date.fromisoformat(partition_key).replace(day=1)
    if month_start.month == 12:
        next_month = month_start.replace(
            year=month_start.year + 1, month=1, day=1
        )
    else:
        next_month = month_start.replace(month=month_start.month + 1, day=1)
    query_start = month_start - timedelta(days=14)
    query_end = next_month + timedelta(days=14)
    query = (
        "buyer-country IN (EST) AND "
        "notice-type IN (can-standard, can-social, can-desg, can-modif) "
        f"AND publication-date>={query_start:%Y%m%d} "
        f"AND publication-date<{query_end:%Y%m%d}"
    )
    result: dict[str, dict[str, str]] = {}
    for notice in iter_search_notices(
        query=query,
        fields=(
            "notice-identifier",
            "publication-number",
            "publication-date",
        ),
        session=session,
        page_sleep_seconds=0,
    ):
        notice_id = str(notice.get("notice-identifier", ""))
        if notice_id == "":
            continue
        result[notice_id] = {
            "publication_number": str(notice.get("publication-number", "")),
            "publication_date": str(notice.get("publication-date", ""))[:10],
        }
    return result


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
            f"No RHR manifest under s3://{tables.S3_BUCKET}/{prefix}; "
            "materialize the raw XML snapshot first"
        )
    manifests = [
        json.loads(object_store.read_bytes(key, bucket=tables.S3_BUCKET))
        for key in keys
    ]
    return max(manifests, key=lambda manifest: str(manifest["retrieved_at"]))
