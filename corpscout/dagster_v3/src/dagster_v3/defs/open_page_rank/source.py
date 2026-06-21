from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import dagster as dg
import requests
from pydantic import field_validator

from dagster_v3.defs.common.resources import ObjectStoreResource

OPEN_PAGE_RANK_RAW_BUCKET = "source-open-page-rank-domains"
OPEN_PAGE_RANK_SOURCE_URL = "https://www.domcop.com/files/top/top10milliondomains.csv.zip"

_RAW_KEY_RE = re.compile(
    r"^raw/run_id=(?P<run_id>[^/]+)/retrieved_date=(?P<retrieved_date>\d{4}-\d{2}-\d{2})/"
    r"source\.csv\.zip$"
)


class OpenPageRankDownloadConfig(dg.Config):
    source_url: str = OPEN_PAGE_RANK_SOURCE_URL
    request_timeout_seconds: int = 600

    @field_validator("request_timeout_seconds")
    @classmethod
    def validate_request_timeout_seconds(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("request_timeout_seconds must be greater than zero")
        return value


@dataclass(frozen=True)
class OpenPageRankRawFile:
    source_url: str
    s3_key: str
    size_bytes: int
    sha256: str


def raw_file_object_key(*, run_id: str, retrieved_date: str) -> str:
    return f"raw/run_id={run_id}/retrieved_date={retrieved_date}/source.csv.zip"


def manifest_object_key(*, run_id: str, retrieved_date: str) -> str:
    return f"raw/run_id={run_id}/retrieved_date={retrieved_date}/manifest.json"


def build_manifest(
    *,
    run_id: str,
    retrieved_at: datetime,
    file: OpenPageRankRawFile,
) -> dict[str, Any]:
    return {
        "source": "open_page_rank",
        "source_list_name": "domcop_top_10m_domains",
        "run_id": run_id,
        "retrieved_at": retrieved_at.isoformat(),
        "retrieved_date": retrieved_at.date().isoformat(),
        "file": {
            "source_url": file.source_url,
            "s3_key": file.s3_key,
            "size_bytes": file.size_bytes,
            "sha256": file.sha256,
        },
    }


def download_raw_file(
    *,
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    config: OpenPageRankDownloadConfig,
    run_id: str,
    retrieved_at: datetime,
    session: requests.Session | None = None,
) -> dg.MaterializeResult:
    object_store.ensure_bucket(OPEN_PAGE_RANK_RAW_BUCKET)
    retrieved_date = retrieved_at.date().isoformat()
    s3_key = raw_file_object_key(run_id=run_id, retrieved_date=retrieved_date)
    manifest_key = manifest_object_key(run_id=run_id, retrieved_date=retrieved_date)
    temp_path = Path("data/tmp/open_page_rank") / f"{context.run_id}.csv.zip"
    temp_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        size_bytes, digest = _stream_download_to_path(
            source_url=config.source_url,
            target_path=temp_path,
            timeout_seconds=config.request_timeout_seconds,
            session=session or requests.Session(),
        )
        object_store.upload_file(s3_key, temp_path, bucket=OPEN_PAGE_RANK_RAW_BUCKET)
    finally:
        temp_path.unlink(missing_ok=True)

    raw_file = OpenPageRankRawFile(
        source_url=config.source_url,
        s3_key=s3_key,
        size_bytes=size_bytes,
        sha256=digest,
    )
    manifest = build_manifest(run_id=run_id, retrieved_at=retrieved_at, file=raw_file)
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, sort_keys=True),
        bucket=OPEN_PAGE_RANK_RAW_BUCKET,
    )
    return dg.MaterializeResult(
        metadata={
            "s3_bucket": OPEN_PAGE_RANK_RAW_BUCKET,
            "s3_key": s3_key,
            "manifest_key": manifest_key,
            "size_bytes": size_bytes,
            "sha256": digest,
            "source_url": config.source_url,
        }
    )


def manifest_for_run(object_store: ObjectStoreResource, run_id: str) -> dict[str, Any]:
    manifest_keys = [
        key
        for key in object_store.list_keys("raw/", bucket=OPEN_PAGE_RANK_RAW_BUCKET)
        if key.endswith("/manifest.json") and f"/run_id={run_id}/" in key
    ]
    if not manifest_keys:
        raise ValueError(f"No Open PageRank manifest found for Dagster run_id={run_id}")
    manifests = [
        json.loads(object_store.read_bytes(key, bucket=OPEN_PAGE_RANK_RAW_BUCKET))
        for key in manifest_keys
    ]
    return max(manifests, key=lambda item: str(item["retrieved_at"]))


def select_open_page_rank_raw_keys_for_deletion(
    keys: list[str] | tuple[str, ...],
) -> list[str]:
    parsed_keys = [
        (key, match.group("retrieved_date"))
        for key in keys
        if (match := _RAW_KEY_RE.match(key)) is not None
    ]
    if not parsed_keys:
        return []

    newest_retrieved_date = max(retrieved_date for _, retrieved_date in parsed_keys)
    return [
        key
        for key, retrieved_date in parsed_keys
        if retrieved_date < newest_retrieved_date
    ]


def _stream_download_to_path(
    *,
    source_url: str,
    target_path: Path,
    timeout_seconds: int,
    session: requests.Session,
) -> tuple[int, str]:
    response = session.get(source_url, stream=True, timeout=timeout_seconds)
    response.raise_for_status()

    digest = sha256()
    size_bytes = 0
    with target_path.open("wb") as target:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            if not chunk:
                continue
            digest.update(chunk)
            size_bytes += len(chunk)
            target.write(chunk)
    return size_bytes, digest.hexdigest()
