import json
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.common.object_catalog import (
    OBJECT_CATALOG_SCHEMA_VERSION,
    ObjectCatalogCommit,
    ObjectCatalogFile,
    ObjectCatalogLocation,
)
from dagster_v3.defs.common.resources import ObjectStoreResource

SWEDEN_COMPANY_RAW_BUCKET = "source-sweden-company"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 1_800
DEFAULT_DOWNLOAD_MAX_ATTEMPTS = 4
DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS = 5.0
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
DEFAULT_USER_AGENT = "corpscout-dagster-v3-sweden-company/0.1"
SWEDEN_COMPANY_CATALOG_DATASET = "raw_archives"

SCB_BULK_URL = "https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip"
BOLAGSVERKET_BULK_URL = "https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip"

_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)

_CATALOG_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.int32(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("dataset", pa.string(), nullable=False),
        pa.field("partition_json", pa.string(), nullable=False),
        pa.field("source_run_id", pa.string(), nullable=False),
        pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("object_key", pa.string(), nullable=False),
        pa.field("object_format", pa.string(), nullable=False),
        pa.field("size_bytes", pa.int64(), nullable=False),
        pa.field("sha256", pa.string(), nullable=False),
        pa.field("row_count", pa.int64()),
        pa.field("source_slug", pa.string(), nullable=False),
        pa.field("source_name", pa.string(), nullable=False),
        pa.field("source_url", pa.string(), nullable=False),
        pa.field("source_last_modified", pa.string(), nullable=False),
        pa.field("content_type", pa.string(), nullable=False),
        pa.field("last_modified", pa.string(), nullable=False),
    ]
)


@dataclass(frozen=True)
class SwedenCompanySourceFile:
    source_slug: str
    source_name: str
    url: str


@dataclass(frozen=True)
class SwedenCompanyDownloadedFile:
    source_slug: str
    source_name: str
    source_url: str
    source_last_modified: str
    s3_key: str
    downloaded: bool
    size_bytes: int | None
    sha256: str | None
    content_type: str
    last_modified: str


@dataclass(frozen=True)
class SourceFileHttpMetadata:
    source_last_modified: str
    content_length: int | None
    content_type: str
    last_modified: str


@dataclass(frozen=True)
class SwedenCompanyRawSnapshotReference:
    """Exact object-catalog commit produced by the raw snapshot asset."""

    bucket: str
    snapshot_date: str
    commit_key: str
    source_run_id: str


def raw_file_object_key(*, source_slug: str, source_last_modified: str) -> str:
    return (
        "sweden_company/raw/"
        f"source_last_modified={source_last_modified}/"
        f"source={source_slug}/"
        "source.zip"
    )


def manifest_object_key(*, retrieved_date: str, run_id: str) -> str:
    return (
        f"sweden_company/raw/retrieved_date={retrieved_date}/"
        f"run_id={run_id}/manifest.json"
    )


def latest_manifest_object_key(*, retrieved_date: str) -> str:
    return f"sweden_company/raw/retrieved_date={retrieved_date}/manifest.json"


def integrity_object_key(raw_object_key: str) -> str:
    return f"{raw_object_key}.integrity.json"


def catalog_location(*, retrieved_date: str) -> ObjectCatalogLocation:
    return ObjectCatalogLocation(
        source="sweden_company",
        dataset=SWEDEN_COMPANY_CATALOG_DATASET,
        partition={"snapshot_date": retrieved_date},
    )


def build_manifest(
    *,
    run_id: str,
    retrieved_at: datetime,
    files: list[SwedenCompanyDownloadedFile],
) -> dict[str, Any]:
    return {
        "source": "sweden_company",
        "run_id": run_id,
        "retrieved_at": retrieved_at.isoformat(),
        "retrieved_date": retrieved_at.date().isoformat(),
        "bucket": SWEDEN_COMPANY_RAW_BUCKET,
        "files": [
            {
                "source_slug": file.source_slug,
                "source_name": file.source_name,
                "source_url": file.source_url,
                "source_last_modified": file.source_last_modified,
                "s3_key": file.s3_key,
                "downloaded": file.downloaded,
                "size_bytes": file.size_bytes,
                "sha256": file.sha256,
                "content_type": file.content_type,
                "last_modified": file.last_modified,
            }
            for file in files
        ],
    }


class SwedenCompanyBulkResource(dg.ConfigurableResource):
    """Downloads Sweden company bulk ZIP snapshots into object storage."""

    scb_bulk_url: str = SCB_BULK_URL
    bolagsverket_bulk_url: str = BOLAGSVERKET_BULK_URL
    request_timeout_seconds: int = DEFAULT_REQUEST_TIMEOUT_SECONDS
    download_max_attempts: int = DEFAULT_DOWNLOAD_MAX_ATTEMPTS
    download_retry_base_seconds: float = DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS
    user_agent: str = DEFAULT_USER_AGENT

    def download_snapshot(
        self,
        *,
        object_store: ObjectStoreResource,
        run_id: str,
        retrieved_at: datetime,
        session: Any | None = None,
        log_info: Callable[..., object] | None = None,
    ) -> dg.MaterializeResult[SwedenCompanyRawSnapshotReference]:
        retrieved_date = retrieved_at.date().isoformat()
        http_session = session or self._session()
        object_store.ensure_bucket(SWEDEN_COMPANY_RAW_BUCKET)

        files = [
            self._download_or_reuse_file(
                object_store=object_store,
                source_file=source_file,
                retrieved_date=retrieved_date,
                session=http_session,
                log_info=log_info,
            )
            for source_file in self._source_files()
        ]
        files = self._ensure_file_integrity(
            object_store=object_store,
            files=files,
        )
        manifest_key = manifest_object_key(retrieved_date=retrieved_date, run_id=run_id)
        manifest = build_manifest(
            run_id=run_id,
            retrieved_at=retrieved_at,
            files=files,
        )
        manifest_body = json.dumps(manifest, sort_keys=True)
        object_store.write_json(
            manifest_key,
            manifest_body,
            bucket=SWEDEN_COMPANY_RAW_BUCKET,
        )
        object_store.write_json(
            latest_manifest_object_key(retrieved_date=retrieved_date),
            manifest_body,
            bucket=SWEDEN_COMPANY_RAW_BUCKET,
        )
        catalog_commit = _publish_catalog(
            object_store=object_store,
            manifest=manifest,
            created_at=retrieved_at,
        )

        downloaded_file_count = sum(1 for file in files if file.downloaded)
        reused_file_count = len(files) - downloaded_file_count
        total_size_bytes = sum(
            file.size_bytes or 0 for file in files if file.downloaded
        )
        if log_info is not None:
            log_info(
                "Sweden company raw snapshot complete: bucket=%s manifest_key=%s "
                "downloaded=%s reused=%s bytes=%s",
                SWEDEN_COMPANY_RAW_BUCKET,
                manifest_key,
                downloaded_file_count,
                reused_file_count,
                total_size_bytes,
            )
        snapshot = SwedenCompanyRawSnapshotReference(
            bucket=SWEDEN_COMPANY_RAW_BUCKET,
            snapshot_date=retrieved_date,
            commit_key=catalog_commit.location.commit_object_key(),
            source_run_id=catalog_commit.source_run_id,
        )
        return dg.MaterializeResult(
            value=snapshot,
            metadata={
                "s3_bucket": SWEDEN_COMPANY_RAW_BUCKET,
                "manifest_key": manifest_key,
                "retrieved_date": retrieved_date,
                "source_file_count": len(files),
                "downloaded_file_count": downloaded_file_count,
                "reused_file_count": reused_file_count,
                "total_size_bytes": total_size_bytes,
                "s3_keys": [file.s3_key for file in files],
                "object_catalog_schema_version": OBJECT_CATALOG_SCHEMA_VERSION,
                "object_catalog_bucket": SWEDEN_COMPANY_RAW_BUCKET,
                "object_catalog_commit_key": catalog_commit.location.commit_object_key(),
                "object_catalog_key": catalog_commit.catalog.key,
                "object_catalog_sha256": catalog_commit.catalog.sha256,
                "data_object_count": catalog_commit.data_object_count,
                "data_size_bytes": catalog_commit.data_size_bytes,
                "source_run_id": catalog_commit.source_run_id,
            },
        )

    def _ensure_file_integrity(
        self,
        *,
        object_store: ObjectStoreResource,
        files: list[SwedenCompanyDownloadedFile],
    ) -> list[SwedenCompanyDownloadedFile]:
        """Bootstrap missing sidecars once; normal reuse needs exact-key reads only."""
        resolved_by_key: dict[str, tuple[int, str]] = {}
        unresolved_keys: set[str] = set()
        existing_sidecars: set[str] = set()

        for file in files:
            if file.size_bytes is not None and file.sha256 is not None:
                resolved_by_key[file.s3_key] = _validated_integrity(
                    object_key=file.s3_key,
                    size_bytes=file.size_bytes,
                    digest=file.sha256,
                )
                continue

            sidecar_key = integrity_object_key(file.s3_key)
            if object_store.exists(sidecar_key, bucket=SWEDEN_COMPANY_RAW_BUCKET):
                sidecar = json.loads(
                    object_store.read_bytes(
                        sidecar_key,
                        bucket=SWEDEN_COMPANY_RAW_BUCKET,
                    )
                )
                if sidecar.get("object_key") != file.s3_key:
                    raise ValueError(
                        "Sweden company integrity sidecar object key mismatch: "
                        f"expected={file.s3_key} actual={sidecar.get('object_key')}"
                    )
                resolved_by_key[file.s3_key] = _validated_integrity(
                    object_key=file.s3_key,
                    size_bytes=sidecar.get("size_bytes"),
                    digest=sidecar.get("sha256"),
                )
                existing_sidecars.add(file.s3_key)
                continue

            unresolved_keys.add(file.s3_key)

        for object_key in sorted(unresolved_keys):
            resolved_by_key[object_key] = _downloaded_object_integrity(
                object_store=object_store,
                object_key=object_key,
            )

        resolved_files: list[SwedenCompanyDownloadedFile] = []
        for file in files:
            size_bytes, digest = resolved_by_key[file.s3_key]
            stored_size_bytes = object_store.object_size(
                file.s3_key,
                bucket=SWEDEN_COMPANY_RAW_BUCKET,
            )
            if stored_size_bytes != size_bytes:
                raise ValueError(
                    "Sweden company raw object size does not match its integrity record: "
                    f"key={file.s3_key} expected={size_bytes} actual={stored_size_bytes}"
                )
            if file.s3_key not in existing_sidecars:
                object_store.write_json(
                    integrity_object_key(file.s3_key),
                    json.dumps(
                        {
                            "object_key": file.s3_key,
                            "sha256": digest,
                            "size_bytes": size_bytes,
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    bucket=SWEDEN_COMPANY_RAW_BUCKET,
                )
            resolved_files.append(replace(file, size_bytes=size_bytes, sha256=digest))
        return resolved_files

    def _source_files(self) -> tuple[SwedenCompanySourceFile, ...]:
        return (
            SwedenCompanySourceFile(
                source_slug="scb_bulkfil",
                source_name="SCB/FDB company bulk file",
                url=self.scb_bulk_url,
            ),
            SwedenCompanySourceFile(
                source_slug="bolagsverket_bulkfil",
                source_name="Bolagsverket legal-register bulk file",
                url=self.bolagsverket_bulk_url,
            ),
        )

    def _download_or_reuse_file(
        self,
        *,
        object_store: ObjectStoreResource,
        source_file: SwedenCompanySourceFile,
        retrieved_date: str,
        session: Any,
        log_info: Callable[..., object] | None,
    ) -> SwedenCompanyDownloadedFile:
        http_metadata = self._http_metadata(
            url=source_file.url,
            retrieved_date=retrieved_date,
            session=session,
        )
        s3_key = raw_file_object_key(
            source_slug=source_file.source_slug,
            source_last_modified=http_metadata.source_last_modified,
        )
        if object_store.exists(s3_key, bucket=SWEDEN_COMPANY_RAW_BUCKET):
            if log_info is not None:
                log_info(
                    "Reusing existing Sweden company raw ZIP: bucket=%s key=%s",
                    SWEDEN_COMPANY_RAW_BUCKET,
                    s3_key,
                )
            return SwedenCompanyDownloadedFile(
                source_slug=source_file.source_slug,
                source_name=source_file.source_name,
                source_url=source_file.url,
                source_last_modified=http_metadata.source_last_modified,
                s3_key=s3_key,
                downloaded=False,
                size_bytes=http_metadata.content_length,
                sha256=None,
                content_type=http_metadata.content_type,
                last_modified=http_metadata.last_modified,
            )

        if log_info is not None:
            log_info(
                "Downloading Sweden company raw ZIP: source=%s url=%s bucket=%s key=%s",
                source_file.source_slug,
                source_file.url,
                SWEDEN_COMPANY_RAW_BUCKET,
                s3_key,
            )
        with tempfile.TemporaryDirectory(prefix="sweden_company_") as tmpdir:
            temp_path = Path(tmpdir) / f"{source_file.source_slug}.zip"
            size_bytes, digest, content_type, last_modified = self._download_to_path(
                url=source_file.url,
                target_path=temp_path,
                session=session,
                log_info=log_info,
            )
            object_store.upload_file(
                s3_key,
                temp_path,
                bucket=SWEDEN_COMPANY_RAW_BUCKET,
            )
        return SwedenCompanyDownloadedFile(
            source_slug=source_file.source_slug,
            source_name=source_file.source_name,
            source_url=source_file.url,
            source_last_modified=http_metadata.source_last_modified,
            s3_key=s3_key,
            downloaded=True,
            size_bytes=size_bytes,
            sha256=digest,
            content_type=content_type,
            last_modified=last_modified,
        )

    def _http_metadata(
        self,
        *,
        url: str,
        retrieved_date: str,
        session: Any,
    ) -> SourceFileHttpMetadata:
        response = session.head(url, timeout=self.request_timeout_seconds)
        response.raise_for_status()
        last_modified = response.headers.get("Last-Modified", "")
        return SourceFileHttpMetadata(
            source_last_modified=_source_last_modified_key(
                last_modified=last_modified,
                fallback=retrieved_date,
            ),
            content_length=_content_length(response.headers.get("Content-Length")),
            content_type=response.headers.get("Content-Type", ""),
            last_modified=last_modified,
        )

    def _download_to_path(
        self,
        *,
        url: str,
        target_path: Path,
        session: Any,
        log_info: Callable[..., object] | None,
    ) -> tuple[int, str, str, str]:
        last_error: Exception | None = None
        for attempt in range(1, self.download_max_attempts + 1):
            try:
                return self._stream_download_to_path(
                    url=url,
                    target_path=target_path,
                    session=session,
                )
            except _DOWNLOAD_RETRYABLE_ERRORS as exc:
                last_error = exc
                target_path.unlink(missing_ok=True)
                if attempt >= self.download_max_attempts:
                    break
                wait_seconds = self.download_retry_base_seconds * attempt
                if log_info is not None:
                    log_info(
                        "Sweden company ZIP download failed; retrying: attempt=%s/%s "
                        "wait_seconds=%s url=%s error=%s",
                        attempt,
                        self.download_max_attempts,
                        wait_seconds,
                        url,
                        exc,
                    )
                time.sleep(wait_seconds)
        assert last_error is not None
        raise last_error

    def _stream_download_to_path(
        self,
        *,
        url: str,
        target_path: Path,
        session: Any,
    ) -> tuple[int, str, str, str]:
        response = session.get(url, timeout=self.request_timeout_seconds, stream=True)
        response.raise_for_status()

        digest = sha256()
        size_bytes = 0
        with target_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if not chunk:
                    continue
                digest.update(chunk)
                size_bytes += len(chunk)
                handle.write(chunk)

        expected = response.headers.get("Content-Length")
        if expected is not None and expected.isdigit() and size_bytes < int(expected):
            raise requests.exceptions.ChunkedEncodingError(
                f"incomplete download: {size_bytes}/{expected} bytes from {url}"
            )

        return (
            size_bytes,
            digest.hexdigest(),
            response.headers.get("Content-Type", ""),
            response.headers.get("Last-Modified", ""),
        )

    def _session(self) -> Any:
        session = dlt_requests.Session(timeout=self.request_timeout_seconds)
        session.headers.update({"User-Agent": self.user_agent})
        return session


def _source_last_modified_key(*, last_modified: str, fallback: str) -> str:
    if last_modified == "":
        return fallback
    try:
        parsed = parsedate_to_datetime(last_modified)
    except TypeError, ValueError, IndexError, AttributeError:
        return fallback
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    timestamp = parsed.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    return timestamp


def _content_length(value: str | None) -> int | None:
    if value is None or not value.isdigit():
        return None
    return int(value)


def _downloaded_object_integrity(
    *,
    object_store: ObjectStoreResource,
    object_key: str,
) -> tuple[int, str]:
    with tempfile.TemporaryDirectory(prefix="sweden_company_integrity_") as tmpdir:
        target_path = Path(tmpdir) / "source.zip"
        object_store.download_file(
            object_key,
            target_path,
            bucket=SWEDEN_COMPANY_RAW_BUCKET,
        )
        digest = sha256()
        size_bytes = 0
        with target_path.open("rb") as handle:
            while chunk := handle.read(DOWNLOAD_CHUNK_BYTES):
                digest.update(chunk)
                size_bytes += len(chunk)
    return _validated_integrity(
        object_key=object_key,
        size_bytes=size_bytes,
        digest=digest.hexdigest(),
    )


def _validated_integrity(
    *,
    object_key: str,
    size_bytes: Any,
    digest: Any,
) -> tuple[int, str]:
    if (
        not isinstance(size_bytes, int)
        or isinstance(size_bytes, bool)
        or size_bytes < 1
    ):
        raise ValueError(
            "Sweden company raw object has invalid size: "
            f"key={object_key} size_bytes={size_bytes!r}"
        )
    if (
        not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
    ):
        raise ValueError(
            "Sweden company raw object has invalid SHA-256: "
            f"key={object_key} sha256={digest!r}"
        )
    return size_bytes, digest


def _publish_catalog(
    *,
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
    created_at: datetime,
) -> ObjectCatalogCommit:
    """Publish and verify the immutable catalog before replacing the commit."""
    retrieved_date = str(manifest["retrieved_date"])
    source_run_id = str(manifest["run_id"])
    location = catalog_location(retrieved_date=retrieved_date)
    partition_json = json.dumps(
        location.partition,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    rows = []
    for file in sorted(manifest["files"], key=lambda item: str(item["s3_key"])):
        size_bytes, digest = _validated_integrity(
            object_key=str(file["s3_key"]),
            size_bytes=file.get("size_bytes"),
            digest=file.get("sha256"),
        )
        rows.append(
            {
                "schema_version": OBJECT_CATALOG_SCHEMA_VERSION,
                "source": location.source,
                "dataset": location.dataset,
                "partition_json": partition_json,
                "source_run_id": source_run_id,
                "created_at": created_at.astimezone(UTC),
                "object_key": str(file["s3_key"]),
                "object_format": "zip",
                "size_bytes": size_bytes,
                "sha256": digest,
                "row_count": None,
                "source_slug": str(file["source_slug"]),
                "source_name": str(file["source_name"]),
                "source_url": str(file["source_url"]),
                "source_last_modified": str(file["source_last_modified"]),
                "content_type": str(file["content_type"]),
                "last_modified": str(file["last_modified"]),
            }
        )

    table = pa.Table.from_pylist(rows, schema=_CATALOG_SCHEMA)
    sink = BytesIO()
    pq.write_table(table, sink, compression="zstd")
    catalog_body = sink.getvalue()
    catalog_digest = sha256(catalog_body).hexdigest()
    catalog_key = location.catalog_object_key(source_run_id)

    object_store.write_bytes(
        catalog_key,
        catalog_body,
        bucket=SWEDEN_COMPANY_RAW_BUCKET,
    )
    stored_catalog = object_store.read_bytes(
        catalog_key,
        bucket=SWEDEN_COMPANY_RAW_BUCKET,
    )
    if (
        len(stored_catalog) != len(catalog_body)
        or sha256(stored_catalog).hexdigest() != catalog_digest
    ):
        raise ValueError(
            "Sweden company object catalog verification failed after upload: "
            f"bucket={SWEDEN_COMPANY_RAW_BUCKET} key={catalog_key}"
        )

    commit = ObjectCatalogCommit(
        location=location,
        source_run_id=source_run_id,
        created_at=created_at,
        catalog=ObjectCatalogFile(
            key=catalog_key,
            sha256=catalog_digest,
            size_bytes=len(catalog_body),
            row_count=table.num_rows,
        ),
        data_object_count=table.num_rows,
        data_size_bytes=sum(int(row["size_bytes"]) for row in rows),
    )
    object_store.write_bytes(
        location.commit_object_key(),
        commit.to_json_bytes(),
        bucket=SWEDEN_COMPANY_RAW_BUCKET,
    )
    return commit


def load_catalog_manifest(
    *,
    object_store: ObjectStoreResource,
    snapshot: SwedenCompanyRawSnapshotReference,
) -> tuple[ObjectCatalogCommit, dict[str, Any]]:
    """Load and validate one committed catalog without listing object prefixes."""
    expected_location = catalog_location(retrieved_date=snapshot.snapshot_date)
    expected_commit_key = expected_location.commit_object_key()
    if snapshot.bucket != SWEDEN_COMPANY_RAW_BUCKET:
        raise ValueError(
            "Sweden company snapshot bucket mismatch: "
            f"expected={SWEDEN_COMPANY_RAW_BUCKET} actual={snapshot.bucket}"
        )
    if snapshot.commit_key != expected_commit_key:
        raise ValueError(
            "Sweden company snapshot commit key mismatch: "
            f"expected={expected_commit_key} actual={snapshot.commit_key}"
        )
    if not object_store.exists(snapshot.commit_key, bucket=snapshot.bucket):
        raise ValueError(
            "Sweden company object catalog commit does not exist: "
            f"bucket={snapshot.bucket} key={snapshot.commit_key}"
        )

    try:
        commit = ObjectCatalogCommit.from_json_bytes(
            object_store.read_bytes(snapshot.commit_key, bucket=snapshot.bucket)
        )
    except ValueError as exc:
        raise ValueError(
            "Sweden company object catalog commit is invalid: "
            f"bucket={snapshot.bucket} key={snapshot.commit_key}"
        ) from exc
    _validate_catalog_commit(
        commit=commit,
        snapshot=snapshot,
        expected_location=expected_location,
    )

    if not object_store.exists(commit.catalog.key, bucket=snapshot.bucket):
        raise ValueError(
            "Sweden company object catalog does not exist: "
            f"bucket={snapshot.bucket} key={commit.catalog.key}"
        )
    catalog_body = object_store.read_bytes(
        commit.catalog.key,
        bucket=snapshot.bucket,
    )
    if len(catalog_body) != commit.catalog.size_bytes:
        raise ValueError(
            "Sweden company object catalog size mismatch: "
            f"key={commit.catalog.key} expected={commit.catalog.size_bytes} "
            f"actual={len(catalog_body)}"
        )
    catalog_digest = sha256(catalog_body).hexdigest()
    if catalog_digest != commit.catalog.sha256:
        raise ValueError(
            "Sweden company object catalog SHA-256 mismatch: "
            f"key={commit.catalog.key} expected={commit.catalog.sha256} "
            f"actual={catalog_digest}"
        )

    try:
        catalog = pq.read_table(BytesIO(catalog_body))
    except (pa.ArrowInvalid, OSError) as exc:
        raise ValueError(
            "Sweden company object catalog is not readable Parquet: "
            f"key={commit.catalog.key}"
        ) from exc
    if not catalog.schema.equals(_CATALOG_SCHEMA, check_metadata=False):
        raise ValueError(
            "Sweden company object catalog schema mismatch: "
            f"key={commit.catalog.key} expected={_CATALOG_SCHEMA} "
            f"actual={catalog.schema}"
        )

    rows = catalog.to_pylist()
    _validate_catalog_rows(commit=commit, rows=rows)
    return commit, _catalog_rows_to_manifest(commit=commit, rows=rows)


def _validate_catalog_commit(
    *,
    commit: ObjectCatalogCommit,
    snapshot: SwedenCompanyRawSnapshotReference,
    expected_location: ObjectCatalogLocation,
) -> None:
    if commit.location != expected_location:
        raise ValueError(
            "Sweden company object catalog location mismatch: "
            f"expected={expected_location.model_dump()} "
            f"actual={commit.location.model_dump()}"
        )
    if commit.source_run_id != snapshot.source_run_id:
        raise ValueError(
            "Sweden company object catalog source run ID mismatch: "
            f"expected={snapshot.source_run_id} actual={commit.source_run_id}"
        )
    if commit.data_row_count is not None:
        raise ValueError(
            "Sweden company raw archive catalog must not declare a data row count: "
            f"actual={commit.data_row_count}"
        )


def _validate_catalog_rows(
    *,
    commit: ObjectCatalogCommit,
    rows: list[dict[str, Any]],
) -> None:
    if len(rows) != commit.catalog.row_count:
        raise ValueError(
            "Sweden company object catalog row count mismatch: "
            f"expected={commit.catalog.row_count} actual={len(rows)}"
        )

    expected_partition_json = json.dumps(
        commit.location.partition,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    object_keys: set[str] = set()
    ordered_object_keys: list[str] = []
    source_slugs: list[str] = []
    total_size_bytes = 0
    for row in rows:
        _validate_catalog_row_identity(
            row=row,
            commit=commit,
            expected_partition_json=expected_partition_json,
        )
        object_key = str(row["object_key"])
        if object_key in object_keys:
            raise ValueError(
                "Sweden company object catalog contains a duplicate object key: "
                f"key={object_key}"
            )
        object_keys.add(object_key)
        ordered_object_keys.append(object_key)
        source_slug = str(row["source_slug"])
        source_slugs.append(source_slug)
        expected_object_key = raw_file_object_key(
            source_slug=source_slug,
            source_last_modified=str(row["source_last_modified"]),
        )
        if object_key != expected_object_key:
            raise ValueError(
                "Sweden company object catalog raw object key mismatch: "
                f"expected={expected_object_key} actual={object_key}"
            )
        size_bytes, _ = _validated_integrity(
            object_key=object_key,
            size_bytes=row["size_bytes"],
            digest=row["sha256"],
        )
        total_size_bytes += size_bytes

    _validate_source_slugs(source_slugs)
    if ordered_object_keys != sorted(ordered_object_keys):
        raise ValueError(
            "Sweden company object catalog rows must be sorted by object key"
        )
    if total_size_bytes != commit.data_size_bytes:
        raise ValueError(
            "Sweden company object catalog data size mismatch: "
            f"expected={commit.data_size_bytes} actual={total_size_bytes}"
        )


def _validate_catalog_row_identity(
    *,
    row: dict[str, Any],
    commit: ObjectCatalogCommit,
    expected_partition_json: str,
) -> None:
    expected_values = {
        "schema_version": OBJECT_CATALOG_SCHEMA_VERSION,
        "source": commit.location.source,
        "dataset": commit.location.dataset,
        "partition_json": expected_partition_json,
        "source_run_id": commit.source_run_id,
        "created_at": commit.created_at,
        "object_format": "zip",
        "row_count": None,
    }
    for column, expected in expected_values.items():
        if row[column] != expected:
            raise ValueError(
                "Sweden company object catalog row identity mismatch: "
                f"column={column} expected={expected!r} actual={row[column]!r}"
            )


def _validate_source_slugs(source_slugs: list[str]) -> None:
    actual = set(source_slugs)
    expected = {"bolagsverket_bulkfil", "scb_bulkfil"}
    duplicates = sorted(slug for slug in actual if source_slugs.count(slug) > 1)
    if actual == expected and not duplicates:
        return
    raise ValueError(
        "Sweden company object catalog source slugs mismatch: "
        f"expected={sorted(expected)} actual={sorted(actual)} duplicates={duplicates}"
    )


def _catalog_rows_to_manifest(
    *,
    commit: ObjectCatalogCommit,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "source": commit.location.source,
        "run_id": commit.source_run_id,
        "retrieved_at": commit.created_at.isoformat(),
        "retrieved_date": commit.location.partition["snapshot_date"],
        "bucket": SWEDEN_COMPANY_RAW_BUCKET,
        "files": [
            {
                "source_slug": str(row["source_slug"]),
                "source_name": str(row["source_name"]),
                "source_url": str(row["source_url"]),
                "source_last_modified": str(row["source_last_modified"]),
                "s3_key": str(row["object_key"]),
                "downloaded": False,
                "size_bytes": int(row["size_bytes"]),
                "sha256": str(row["sha256"]),
                "content_type": str(row["content_type"]),
                "last_modified": str(row["last_modified"]),
            }
            for row in rows
        ],
    }
