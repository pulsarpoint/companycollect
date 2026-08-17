import json
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from typing import Literal

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from dagster_v3.defs.common.object_catalog import (
    OBJECT_CATALOG_SCHEMA_VERSION,
    ObjectCatalogCommit,
    ObjectCatalogFile,
    ObjectCatalogLocation,
)
from dagster_v3.defs.common.resources import ObjectStoreResource

DENMARK_CVR_BUCKET = "source-denmark-cvr"
DENMARK_CVR_COMPANY_DETAIL_CATALOG_DATASET = "company_details"
DENMARK_CVR_COMPANY_DETAIL_CATALOG_PILOT_PARTITION = "bucket_000"
DENMARK_CVR_COMPANY_DETAIL_BOOTSTRAP_WORKERS = 16

type DenmarkCvrCompanyDetailObjectKind = Literal["original", "english", "failure"]

_OBJECT_KIND_BY_FILENAME: dict[str, DenmarkCvrCompanyDetailObjectKind] = {
    "company.json": "original",
    "company_en.json": "english",
    "company_error.json": "failure",
}
_FILENAME_BY_OBJECT_KIND = {
    object_kind: filename for filename, object_kind in _OBJECT_KIND_BY_FILENAME.items()
}
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
        pa.field("row_count", pa.int64(), nullable=False),
        pa.field("cvr", pa.string(), nullable=False),
        pa.field("object_kind", pa.string(), nullable=False),
    ]
)


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailCatalogEntry:
    cvr: str
    object_kind: DenmarkCvrCompanyDetailObjectKind
    object_key: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailCatalogReference:
    bucket: str
    partition_key: str
    commit_key: str
    source_run_id: str


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailCatalog:
    reference: DenmarkCvrCompanyDetailCatalogReference
    commit: ObjectCatalogCommit
    entries: tuple[DenmarkCvrCompanyDetailCatalogEntry, ...]


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailCatalogBootstrap:
    entries: tuple[DenmarkCvrCompanyDetailCatalogEntry, ...]
    object_read_count: int


def company_detail_catalog_location(partition_key: str) -> ObjectCatalogLocation:
    _validate_partition_key(partition_key)
    return ObjectCatalogLocation(
        source="denmark_cvr",
        dataset=DENMARK_CVR_COMPANY_DETAIL_CATALOG_DATASET,
        partition={"hash_bucket": partition_key},
    )


def bootstrap_company_detail_catalog(
    *,
    object_store: ObjectStoreResource,
    partition_key: str,
    object_keys: Mapping[str, tuple[str, str, str]],
) -> DenmarkCvrCompanyDetailCatalogBootstrap:
    """Build the first catalog from deterministic exact keys, never a prefix scan."""
    _validate_partition_key(partition_key)
    selected = tuple(sorted(object_keys.items()))
    with ThreadPoolExecutor(
        max_workers=DENMARK_CVR_COMPANY_DETAIL_BOOTSTRAP_WORKERS
    ) as executor:
        entries_by_cvr = executor.map(
            lambda item: _bootstrap_company_entries(
                object_store=object_store,
                cvr=item[0],
                original_key=item[1][0],
                english_key=item[1][1],
                failure_key=item[1][2],
            ),
            selected,
        )
        entries = tuple(
            sorted(
                (entry for cvr_entries in entries_by_cvr for entry in cvr_entries),
                key=lambda entry: entry.object_key,
            )
        )
    return DenmarkCvrCompanyDetailCatalogBootstrap(
        entries=entries,
        object_read_count=len(entries),
    )


def catalog_entry_from_body(
    *,
    cvr: str,
    object_kind: DenmarkCvrCompanyDetailObjectKind,
    object_key: str,
    body: bytes,
) -> DenmarkCvrCompanyDetailCatalogEntry:
    _validate_cvr(cvr)
    if not body:
        raise ValueError(
            f"Denmark CVR company-detail object must not be empty: {object_key}"
        )
    return DenmarkCvrCompanyDetailCatalogEntry(
        cvr=cvr,
        object_kind=object_kind,
        object_key=object_key,
        size_bytes=len(body),
        sha256=sha256(body).hexdigest(),
    )


def load_optional_company_detail_catalog(
    *,
    object_store: ObjectStoreResource,
    partition_key: str,
) -> DenmarkCvrCompanyDetailCatalog | None:
    location = company_detail_catalog_location(partition_key)
    commit_key = location.commit_object_key()
    if not object_store.exists(commit_key, bucket=DENMARK_CVR_BUCKET):
        return None
    commit = _load_commit(
        object_store=object_store,
        commit_key=commit_key,
    )
    return _load_catalog(
        object_store=object_store,
        reference=DenmarkCvrCompanyDetailCatalogReference(
            bucket=DENMARK_CVR_BUCKET,
            partition_key=partition_key,
            commit_key=commit_key,
            source_run_id=commit.source_run_id,
        ),
        commit=commit,
    )


def load_company_detail_catalog(
    *,
    object_store: ObjectStoreResource,
    reference: DenmarkCvrCompanyDetailCatalogReference,
) -> DenmarkCvrCompanyDetailCatalog:
    expected_location = company_detail_catalog_location(reference.partition_key)
    expected_commit_key = expected_location.commit_object_key()
    if reference.bucket != DENMARK_CVR_BUCKET:
        raise ValueError(
            "Denmark CVR company-detail catalog bucket mismatch: "
            f"expected={DENMARK_CVR_BUCKET} actual={reference.bucket}"
        )
    if reference.commit_key != expected_commit_key:
        raise ValueError(
            "Denmark CVR company-detail catalog commit key mismatch: "
            f"expected={expected_commit_key} actual={reference.commit_key}"
        )
    if not object_store.exists(reference.commit_key, bucket=reference.bucket):
        raise ValueError(
            "Denmark CVR company-detail catalog commit does not exist: "
            f"bucket={reference.bucket} key={reference.commit_key}"
        )
    commit = _load_commit(
        object_store=object_store,
        commit_key=reference.commit_key,
    )
    return _load_catalog(
        object_store=object_store,
        reference=reference,
        commit=commit,
    )


def publish_company_detail_catalog(
    *,
    object_store: ObjectStoreResource,
    partition_key: str,
    entries: tuple[DenmarkCvrCompanyDetailCatalogEntry, ...],
    source_run_id: str,
    created_at: datetime,
) -> DenmarkCvrCompanyDetailCatalog:
    location = company_detail_catalog_location(partition_key)
    normalized_entries = tuple(sorted(entries, key=lambda entry: entry.object_key))
    _validate_entries(
        entries=normalized_entries,
        partition_key=partition_key,
    )
    partition_json = _partition_json(location)
    normalized_created_at = created_at.astimezone(UTC)
    table = pa.Table.from_pylist(
        [
            {
                "schema_version": OBJECT_CATALOG_SCHEMA_VERSION,
                "source": location.source,
                "dataset": location.dataset,
                "partition_json": partition_json,
                "source_run_id": source_run_id,
                "created_at": normalized_created_at,
                "object_key": entry.object_key,
                "object_format": "json",
                "size_bytes": entry.size_bytes,
                "sha256": entry.sha256,
                "row_count": 1,
                "cvr": entry.cvr,
                "object_kind": entry.object_kind,
            }
            for entry in normalized_entries
        ],
        schema=_CATALOG_SCHEMA,
    )
    catalog_body = _parquet_bytes(table)
    catalog_key = location.catalog_object_key(source_run_id)
    catalog_digest = sha256(catalog_body).hexdigest()
    object_store.write_bytes(
        catalog_key,
        catalog_body,
        bucket=DENMARK_CVR_BUCKET,
    )
    _verify_written_object(
        object_store=object_store,
        object_key=catalog_key,
        expected_body=catalog_body,
    )

    commit = ObjectCatalogCommit(
        location=location,
        source_run_id=source_run_id,
        created_at=normalized_created_at,
        catalog=ObjectCatalogFile(
            key=catalog_key,
            sha256=catalog_digest,
            size_bytes=len(catalog_body),
            row_count=table.num_rows,
        ),
        data_object_count=table.num_rows,
        data_size_bytes=sum(entry.size_bytes for entry in normalized_entries),
        data_row_count=table.num_rows,
    )
    object_store.write_bytes(
        location.commit_object_key(),
        commit.to_json_bytes(),
        bucket=DENMARK_CVR_BUCKET,
    )
    reference = DenmarkCvrCompanyDetailCatalogReference(
        bucket=DENMARK_CVR_BUCKET,
        partition_key=partition_key,
        commit_key=location.commit_object_key(),
        source_run_id=source_run_id,
    )
    return DenmarkCvrCompanyDetailCatalog(
        reference=reference,
        commit=commit,
        entries=normalized_entries,
    )


def read_company_detail_catalog_objects(
    *,
    object_store: ObjectStoreResource,
    catalog: DenmarkCvrCompanyDetailCatalog,
) -> tuple[tuple[DenmarkCvrCompanyDetailCatalogEntry, bytes], ...]:
    with ThreadPoolExecutor(
        max_workers=DENMARK_CVR_COMPANY_DETAIL_BOOTSTRAP_WORKERS
    ) as executor:
        bodies = executor.map(
            lambda entry: object_store.read_bytes(
                entry.object_key,
                bucket=DENMARK_CVR_BUCKET,
            ),
            catalog.entries,
        )
        objects = tuple(zip(catalog.entries, bodies, strict=True))
    for entry, body in objects:
        if len(body) != entry.size_bytes:
            raise ValueError(
                "Denmark CVR company-detail object size mismatch: "
                f"key={entry.object_key} expected={entry.size_bytes} actual={len(body)}"
            )
        actual_digest = sha256(body).hexdigest()
        if actual_digest != entry.sha256:
            raise ValueError(
                "Denmark CVR company-detail object SHA-256 mismatch: "
                f"key={entry.object_key} expected={entry.sha256} "
                f"actual={actual_digest}"
            )
    return objects


def _bootstrap_company_entries(
    *,
    object_store: ObjectStoreResource,
    cvr: str,
    original_key: str,
    english_key: str,
    failure_key: str,
) -> tuple[DenmarkCvrCompanyDetailCatalogEntry, ...]:
    entries: list[DenmarkCvrCompanyDetailCatalogEntry] = []
    original_body = _read_optional_object(
        object_store=object_store,
        object_key=original_key,
    )
    english_body = _read_optional_object(
        object_store=object_store,
        object_key=english_key,
    )
    if original_body is not None:
        entries.append(
            catalog_entry_from_body(
                cvr=cvr,
                object_kind="original",
                object_key=original_key,
                body=original_body,
            )
        )
    if english_body is not None:
        entries.append(
            catalog_entry_from_body(
                cvr=cvr,
                object_kind="english",
                object_key=english_key,
                body=english_body,
            )
        )
    if original_body is None or english_body is None:
        failure_body = _read_optional_object(
            object_store=object_store,
            object_key=failure_key,
        )
        if failure_body is not None:
            entries.append(
                catalog_entry_from_body(
                    cvr=cvr,
                    object_kind="failure",
                    object_key=failure_key,
                    body=failure_body,
                )
            )
    return tuple(entries)


def _read_optional_object(
    *,
    object_store: ObjectStoreResource,
    object_key: str,
) -> bytes | None:
    try:
        return object_store.read_bytes(object_key, bucket=DENMARK_CVR_BUCKET)
    except KeyError:
        return None
    except ClientError as exc:
        error = exc.response.get("Error", {})
        if str(error.get("Code", "")) in {
            "404",
            "NoSuchBucket",
            "NoSuchKey",
            "NotFound",
        }:
            return None
        raise


def _load_commit(
    *,
    object_store: ObjectStoreResource,
    commit_key: str,
) -> ObjectCatalogCommit:
    try:
        return ObjectCatalogCommit.from_json_bytes(
            object_store.read_bytes(commit_key, bucket=DENMARK_CVR_BUCKET)
        )
    except ValueError as exc:
        raise ValueError(
            "Denmark CVR company-detail catalog commit is invalid: "
            f"bucket={DENMARK_CVR_BUCKET} key={commit_key}"
        ) from exc


def _load_catalog(
    *,
    object_store: ObjectStoreResource,
    reference: DenmarkCvrCompanyDetailCatalogReference,
    commit: ObjectCatalogCommit,
) -> DenmarkCvrCompanyDetailCatalog:
    expected_location = company_detail_catalog_location(reference.partition_key)
    if commit.location != expected_location:
        raise ValueError(
            "Denmark CVR company-detail catalog location mismatch: "
            f"expected={expected_location.model_dump()} "
            f"actual={commit.location.model_dump()}"
        )
    if commit.source_run_id != reference.source_run_id:
        raise ValueError(
            "Denmark CVR company-detail catalog source run ID mismatch: "
            f"expected={reference.source_run_id} actual={commit.source_run_id}"
        )
    if not object_store.exists(commit.catalog.key, bucket=reference.bucket):
        raise ValueError(
            "Denmark CVR company-detail catalog does not exist: "
            f"bucket={reference.bucket} key={commit.catalog.key}"
        )
    catalog_body = object_store.read_bytes(
        commit.catalog.key,
        bucket=reference.bucket,
    )
    _validate_catalog_body(commit=commit, catalog_body=catalog_body)
    try:
        table = pq.read_table(BytesIO(catalog_body))
    except (pa.ArrowInvalid, OSError) as exc:
        raise ValueError(
            "Denmark CVR company-detail catalog is not readable Parquet: "
            f"key={commit.catalog.key}"
        ) from exc
    if not table.schema.equals(_CATALOG_SCHEMA, check_metadata=False):
        raise ValueError(
            "Denmark CVR company-detail catalog schema mismatch: "
            f"key={commit.catalog.key} expected={_CATALOG_SCHEMA} "
            f"actual={table.schema}"
        )
    entries = _entries_from_rows(
        rows=table.to_pylist(),
        commit=commit,
        partition_key=reference.partition_key,
    )
    return DenmarkCvrCompanyDetailCatalog(
        reference=reference,
        commit=commit,
        entries=entries,
    )


def _entries_from_rows(
    *,
    rows: list[dict[str, object]],
    commit: ObjectCatalogCommit,
    partition_key: str,
) -> tuple[DenmarkCvrCompanyDetailCatalogEntry, ...]:
    if len(rows) != commit.catalog.row_count:
        raise ValueError(
            "Denmark CVR company-detail catalog row count mismatch: "
            f"expected={commit.catalog.row_count} actual={len(rows)}"
        )
    expected_partition_json = _partition_json(commit.location)
    entries: list[DenmarkCvrCompanyDetailCatalogEntry] = []
    for row in rows:
        expected_identity = {
            "schema_version": OBJECT_CATALOG_SCHEMA_VERSION,
            "source": commit.location.source,
            "dataset": commit.location.dataset,
            "partition_json": expected_partition_json,
            "source_run_id": commit.source_run_id,
            "created_at": commit.created_at,
            "object_format": "json",
            "row_count": 1,
        }
        for field, expected in expected_identity.items():
            if row[field] != expected:
                raise ValueError(
                    "Denmark CVR company-detail catalog row identity mismatch: "
                    f"field={field} expected={expected!r} actual={row[field]!r}"
                )
        object_kind = str(row["object_kind"])
        if object_kind not in _FILENAME_BY_OBJECT_KIND:
            raise ValueError(
                "Denmark CVR company-detail catalog has invalid object kind: "
                f"{object_kind!r}"
            )
        entries.append(
            DenmarkCvrCompanyDetailCatalogEntry(
                cvr=str(row["cvr"]),
                object_kind=object_kind,
                object_key=str(row["object_key"]),
                size_bytes=int(row["size_bytes"]),
                sha256=str(row["sha256"]),
            )
        )
    normalized_entries = tuple(entries)
    _validate_entries(entries=normalized_entries, partition_key=partition_key)
    if sum(entry.size_bytes for entry in normalized_entries) != commit.data_size_bytes:
        raise ValueError(
            "Denmark CVR company-detail catalog data size mismatch: "
            f"expected={commit.data_size_bytes} "
            f"actual={sum(entry.size_bytes for entry in normalized_entries)}"
        )
    if commit.data_row_count != len(normalized_entries):
        raise ValueError(
            "Denmark CVR company-detail catalog data row count mismatch: "
            f"expected={commit.data_row_count} actual={len(normalized_entries)}"
        )
    return normalized_entries


def _validate_entries(
    *,
    entries: tuple[DenmarkCvrCompanyDetailCatalogEntry, ...],
    partition_key: str,
) -> None:
    object_keys = [entry.object_key for entry in entries]
    if object_keys != sorted(object_keys):
        raise ValueError("Denmark CVR company-detail catalog keys must be sorted")
    if len(set(object_keys)) != len(object_keys):
        raise ValueError("Denmark CVR company-detail catalog keys must be unique")
    for entry in entries:
        _validate_cvr(entry.cvr)
        expected_key = (
            f"denmark_cvr/company_details/{partition_key}/cvr={entry.cvr}/"
            f"{_FILENAME_BY_OBJECT_KIND[entry.object_kind]}"
        )
        if entry.object_key != expected_key:
            raise ValueError(
                "Denmark CVR company-detail catalog object key mismatch: "
                f"expected={expected_key} actual={entry.object_key}"
            )
        if entry.size_bytes <= 0:
            raise ValueError(
                "Denmark CVR company-detail catalog object size must be positive: "
                f"key={entry.object_key} size={entry.size_bytes}"
            )
        if len(entry.sha256) != 64 or any(
            character not in "0123456789abcdef" for character in entry.sha256
        ):
            raise ValueError(
                "Denmark CVR company-detail catalog object SHA-256 is invalid: "
                f"key={entry.object_key} sha256={entry.sha256!r}"
            )


def _validate_catalog_body(
    *,
    commit: ObjectCatalogCommit,
    catalog_body: bytes,
) -> None:
    if len(catalog_body) != commit.catalog.size_bytes:
        raise ValueError(
            "Denmark CVR company-detail catalog size mismatch: "
            f"key={commit.catalog.key} expected={commit.catalog.size_bytes} "
            f"actual={len(catalog_body)}"
        )
    actual_digest = sha256(catalog_body).hexdigest()
    if actual_digest != commit.catalog.sha256:
        raise ValueError(
            "Denmark CVR company-detail catalog SHA-256 mismatch: "
            f"key={commit.catalog.key} expected={commit.catalog.sha256} "
            f"actual={actual_digest}"
        )


def _verify_written_object(
    *,
    object_store: ObjectStoreResource,
    object_key: str,
    expected_body: bytes,
) -> None:
    stored_body = object_store.read_bytes(object_key, bucket=DENMARK_CVR_BUCKET)
    if stored_body != expected_body:
        raise ValueError(
            "Denmark CVR company-detail catalog verification failed after upload: "
            f"bucket={DENMARK_CVR_BUCKET} key={object_key}"
        )


def _parquet_bytes(table: pa.Table) -> bytes:
    sink = BytesIO()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue()


def _partition_json(location: ObjectCatalogLocation) -> str:
    return json.dumps(
        location.partition,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _validate_partition_key(partition_key: str) -> None:
    prefix, separator, suffix = partition_key.partition("_")
    if (
        prefix != "bucket"
        or separator == ""
        or not suffix.isdigit()
        or len(suffix) != 3
        or not 0 <= int(suffix) < 128
    ):
        raise ValueError(
            f"Invalid Denmark CVR company-detail partition: {partition_key!r}"
        )


def _validate_cvr(cvr: str) -> None:
    if len(cvr) != 8 or not cvr.isascii() or not cvr.isdigit():
        raise ValueError(
            "Denmark CVR company-detail catalog CVR must contain eight digits"
        )
