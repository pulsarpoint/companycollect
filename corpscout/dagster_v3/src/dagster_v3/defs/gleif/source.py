from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import Any, Literal
from urllib.parse import urlencode

import dagster as dg
import requests
from pydantic import field_validator

from dagster_v3.defs.common.resources import ObjectStoreResource

GLEIF_RAW_BUCKET = "source-gleif-reference-data"
GLEIF_STATE_OBJECT_KEY = "gleif/state/current.json"
GLEIF_GOLDEN_COPY_BASE_URL = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes"
GLEIF_GOLDEN_COPY_FILE_KINDS = ("lei_records", "relationships", "reporting_exceptions")
GLEIF_FILE_KIND_TO_GOLDEN_COPY_KIND = {
    "lei_records": "lei2",
    "relationships": "rr",
    "reporting_exceptions": "repex",
}


class GleifRawDownloadConfig(dg.Config):
    file_format: str = "csv"
    request_timeout_seconds: int = 300

    @field_validator("file_format")
    @classmethod
    def validate_file_format(cls, value: str) -> str:
        if value not in {"json", "csv", "xml"}:
            raise ValueError("file_format must be json, csv, or xml")
        return value

    @field_validator("request_timeout_seconds")
    @classmethod
    def validate_request_timeout_seconds(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("request_timeout_seconds must be greater than zero")
        return value


@dataclass(frozen=True)
class DownloadedFile:
    file_kind: str
    file_format: str
    source_url: str
    s3_key: str
    size_bytes: int
    sha256: str
    etag: str | None
    last_modified: str | None


def golden_copy_url(*, file_kind: str, file_format: str, delta: str | None) -> str:
    path = f"{GLEIF_GOLDEN_COPY_BASE_URL}/{file_kind}/latest.{file_format}"
    if delta is None:
        return path
    return f"{path}?{urlencode({'delta': delta})}"


def normalize_publish_date_for_key(publish_date: str) -> str:
    normalized = publish_date.replace("+00:00", "Z")
    return normalized.replace(":", "-")


def raw_file_object_key(
    *,
    load_mode: Literal["full", "delta", "mapping_refresh"],
    publish_date: str,
    run_id: str,
    file_kind: str,
    delta: str | None,
    extension: str,
) -> str:
    parts = _snapshot_key_parts(
        load_mode=load_mode,
        publish_date=publish_date,
        run_id=run_id,
        delta=delta,
    )
    parts.extend((f"file_kind={file_kind}", f"source.{extension}"))
    return "/".join(parts)


def manifest_object_key(
    *,
    load_mode: Literal["full", "delta", "mapping_refresh"],
    publish_date: str,
    run_id: str,
    delta: str | None,
) -> str:
    parts = _snapshot_key_parts(
        load_mode=load_mode,
        publish_date=publish_date,
        run_id=run_id,
        delta=delta,
    )
    parts.append("manifest.json")
    return "/".join(parts)


def build_manifest(
    *,
    load_mode: str,
    delta: str | None,
    publish_date: str,
    run_id: str,
    pulled_at: datetime,
    files: list[DownloadedFile],
) -> dict[str, Any]:
    return {
        "source": "gleif",
        "load_mode": load_mode,
        "delta": delta,
        "publish_date": publish_date,
        "run_id": run_id,
        "pulled_at": pulled_at.isoformat(),
        "files": [
            {
                "file_kind": item.file_kind,
                "file_format": item.file_format,
                "source_url": item.source_url,
                "s3_key": item.s3_key,
                "size_bytes": item.size_bytes,
                "sha256": item.sha256,
                "etag": item.etag,
                "last_modified": item.last_modified,
            }
            for item in files
        ],
    }


def read_gleif_state(object_store: ObjectStoreResource) -> dict[str, Any] | None:
    if not object_store.exists(GLEIF_STATE_OBJECT_KEY, bucket=GLEIF_RAW_BUCKET):
        return None
    return json.loads(object_store.read_bytes(GLEIF_STATE_OBJECT_KEY, bucket=GLEIF_RAW_BUCKET))


def write_gleif_state(object_store: ObjectStoreResource, state: dict[str, Any]) -> None:
    object_store.write_json(
        GLEIF_STATE_OBJECT_KEY,
        json.dumps(state, sort_keys=True),
        bucket=GLEIF_RAW_BUCKET,
    )


def latest_manifest(object_store: ObjectStoreResource) -> dict[str, Any]:
    manifest_keys = _manifest_keys(object_store)
    if not manifest_keys:
        raise ValueError("No GLEIF raw manifest found in object storage")
    return max(
        (_read_manifest(object_store, key) for key in manifest_keys),
        key=_manifest_order_key,
    )


def manifest_for_run(object_store: ObjectStoreResource, run_id: str) -> dict[str, Any]:
    manifest_keys = _manifest_keys(object_store)
    run_manifest_keys = [
        key
        for key in manifest_keys
        if f"/run_id={run_id}/" in key
    ]
    if not run_manifest_keys:
        return latest_manifest(object_store)
    return max(
        (_read_manifest(object_store, key) for key in run_manifest_keys),
        key=_manifest_order_key,
    )


def ensure_bootstrap_state_for_delta(state: dict[str, Any] | None) -> None:
    if not state or not state.get("last_full_publish_date"):
        raise ValueError("GLEIF delta cannot run before a successful full bootstrap")


def select_gleif_raw_keys_for_deletion(keys: list[str] | tuple[str, ...]) -> list[str]:
    newest_publish_date_by_mode: dict[str, str] = {}
    parsed_keys: list[tuple[str, str, str]] = []
    for key in keys:
        parsed = _parse_raw_key(key)
        if parsed is None:
            continue
        load_mode, publish_date = parsed
        parsed_keys.append((key, load_mode, publish_date))
        newest_publish_date_by_mode[load_mode] = max(
            publish_date,
            newest_publish_date_by_mode.get(load_mode, ""),
        )

    return [
        key
        for key, load_mode, publish_date in parsed_keys
        if not key.endswith("/manifest.json")
        and publish_date < newest_publish_date_by_mode[load_mode]
    ]


def download_golden_copy_files(
    *,
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    config: GleifRawDownloadConfig,
    load_mode: Literal["full", "delta"],
    delta: str | None,
    run_id: str,
    pulled_at: datetime,
    session: requests.Session | None = None,
) -> dg.MaterializeResult:
    object_store.ensure_bucket(GLEIF_RAW_BUCKET)
    http = session or requests.Session()
    files: list[DownloadedFile] = []
    publish_date: str | None = None

    for file_kind in GLEIF_GOLDEN_COPY_FILE_KINDS:
        golden_copy_kind = GLEIF_FILE_KIND_TO_GOLDEN_COPY_KIND[file_kind]
        source_url = golden_copy_url(
            file_kind=golden_copy_kind,
            file_format=config.file_format,
            delta=delta,
        )
        downloaded_file, file_publish_date = _download_one_file(
            object_store=object_store,
            session=http,
            source_url=source_url,
            file_kind=file_kind,
            file_format=config.file_format,
            load_mode=load_mode,
            delta=delta,
            run_id=run_id,
            timeout_seconds=config.request_timeout_seconds,
        )
        if publish_date is None:
            publish_date = file_publish_date
        elif publish_date != file_publish_date:
            raise ValueError(
                "GLEIF publish date changed within one raw download run: "
                f"{publish_date} != {file_publish_date}"
            )
        files.append(downloaded_file)

    if publish_date is None:
        raise ValueError("No GLEIF files were downloaded")

    manifest = build_manifest(
        load_mode=load_mode,
        delta=delta,
        publish_date=publish_date,
        run_id=run_id,
        pulled_at=pulled_at,
        files=files,
    )
    manifest_key = manifest_object_key(
        load_mode=load_mode,
        publish_date=publish_date,
        run_id=run_id,
        delta=delta,
    )
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, sort_keys=True),
        bucket=GLEIF_RAW_BUCKET,
    )
    context.log.info(
        "downloaded_gleif_golden_copy_files",
        extra={
            "load_mode": load_mode,
            "delta": delta,
            "publish_date": publish_date,
            "file_count": len(files),
            "manifest_key": manifest_key,
        },
    )
    return dg.MaterializeResult(
        metadata={
            "load_mode": load_mode,
            "delta": delta or "",
            "publish_date": publish_date,
            "manifest_key": manifest_key,
            "file_count": len(files),
            "total_size_bytes": sum(item.size_bytes for item in files),
        }
    )


def _snapshot_key_parts(
    *,
    load_mode: str,
    publish_date: str,
    run_id: str,
    delta: str | None,
) -> list[str]:
    parts = ["gleif/raw", f"load_mode={load_mode}"]
    if delta is not None:
        parts.append(f"delta={delta}")
    parts.extend(
        [
            f"publish_date={normalize_publish_date_for_key(publish_date)}",
            f"run_id={run_id}",
        ]
    )
    return parts


def _parse_raw_key(key: str) -> tuple[str, str] | None:
    if not key.startswith("gleif/raw/"):
        return None
    parts = key.split("/")
    load_mode = _partition_value(parts, "load_mode")
    publish_date = _partition_value(parts, "publish_date")
    if load_mode is None or publish_date is None:
        return None
    return load_mode, publish_date


def _partition_value(parts: list[str], name: str) -> str | None:
    prefix = f"{name}="
    for part in parts:
        if part.startswith(prefix):
            return part[len(prefix) :]
    return None


def _download_one_file(
    *,
    object_store: ObjectStoreResource,
    session: requests.Session,
    source_url: str,
    file_kind: str,
    file_format: str,
    load_mode: Literal["full", "delta"],
    delta: str | None,
    run_id: str,
    timeout_seconds: int,
) -> tuple[DownloadedFile, str]:
    response = session.get(source_url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    publish_date = _publish_date_from_response(response)
    if not publish_date:
        raise ValueError(f"GLEIF response missing x-gleif-publish-date for {source_url}")

    digest = sha256()
    size_bytes = 0
    with tempfile.NamedTemporaryFile() as temp_file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            digest.update(chunk)
            size_bytes += len(chunk)
            temp_file.write(chunk)
        temp_file.flush()

        s3_key = raw_file_object_key(
            load_mode=load_mode,
            publish_date=publish_date,
            run_id=run_id,
            file_kind=file_kind,
            delta=delta,
            extension=f"{file_format}.zip",
        )
        temp_file.seek(0)
        object_store.client().put_object(
            Bucket=GLEIF_RAW_BUCKET,
            Key=s3_key,
            Body=temp_file,
        )

    downloaded_file = DownloadedFile(
        file_kind=file_kind,
        file_format=file_format,
        source_url=source_url,
        s3_key=s3_key,
        size_bytes=size_bytes,
        sha256=digest.hexdigest(),
        etag=response.headers.get("etag"),
        last_modified=response.headers.get("last-modified"),
    )
    return downloaded_file, publish_date


def _publish_date_from_response(response: requests.Response) -> str | None:
    publish_date = response.headers.get("x-gleif-publish-date")
    if publish_date:
        return publish_date

    for historical_response in reversed(getattr(response, "history", ())):
        publish_date = historical_response.headers.get("x-gleif-publish-date")
        if publish_date:
            return publish_date
    return None


def _manifest_keys(object_store: ObjectStoreResource) -> list[str]:
    return [
        key
        for key in object_store.list_keys("gleif/raw/", bucket=GLEIF_RAW_BUCKET)
        if key.endswith("/manifest.json")
    ]


def _read_manifest(object_store: ObjectStoreResource, key: str) -> dict[str, Any]:
    return json.loads(object_store.read_bytes(key, bucket=GLEIF_RAW_BUCKET))


def _manifest_order_key(manifest: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(manifest.get("publish_date") or ""),
        str(manifest.get("pulled_at") or ""),
        str(manifest.get("run_id") or ""),
    )
