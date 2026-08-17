import json
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq

from dagster_v3.defs.common.object_catalog import (
    OBJECT_CATALOG_SCHEMA_VERSION,
    ObjectCatalogCommit,
    ObjectCatalogFile,
    ObjectCatalogLocation,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.denmark_cvr.company_detail_catalog import (
    DENMARK_CVR_BUCKET,
    DENMARK_CVR_COMPANY_DETAIL_CATALOG_CANARY_PARTITIONS,
    DenmarkCvrCompanyDetailCatalogEntry,
    DenmarkCvrCompanyDetailCatalogReference,
    company_detail_catalog_enabled,
    load_company_detail_catalog,
    read_company_detail_catalog_objects,
)
from dagster_v3.defs.denmark_cvr.company_details import (
    DENMARK_CVR_COMPANY_DETAIL_PARTITIONS,
    DENMARK_CVR_COMPANY_DETAIL_POOL,
    DenmarkCvrCompanyDetailPartitionSnapshot,
)

DENMARK_CVR_COMPANY_DETAIL_COMPACTED_DATASET = "company_details_compacted"
DENMARK_CVR_COMPANY_DETAIL_COMPACTED_TARGET_SOURCE_BYTES = 256 * 1024 * 1024
type DenmarkCvrCompanyDetailSourceObject = tuple[
    DenmarkCvrCompanyDetailCatalogEntry, bytes
]

_COMPACTED_DATA_SCHEMA = pa.schema(
    [
        pa.field("cvr", pa.string(), nullable=False),
        pa.field("object_kind", pa.string(), nullable=False),
        pa.field("source_object_key", pa.string(), nullable=False),
        pa.field("source_size_bytes", pa.int64(), nullable=False),
        pa.field("source_sha256", pa.string(), nullable=False),
        pa.field("payload_json", pa.string(), nullable=False),
    ]
)
_COMPACTED_CATALOG_SCHEMA = pa.schema(
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
        pa.field("source_catalog_sha256", pa.string(), nullable=False),
        pa.field("source_object_count", pa.int64(), nullable=False),
        pa.field("source_size_bytes", pa.int64(), nullable=False),
        pa.field("first_cvr", pa.string(), nullable=False),
        pa.field("last_cvr", pa.string(), nullable=False),
    ]
)


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailCompactedCatalogEntry:
    object_key: str
    size_bytes: int
    sha256: str
    row_count: int
    source_catalog_sha256: str
    source_object_count: int
    source_size_bytes: int
    first_cvr: str
    last_cvr: str


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailCompactedCatalogReference:
    bucket: str
    partition_key: str
    commit_key: str
    source_run_id: str


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailCompactedCatalog:
    reference: DenmarkCvrCompanyDetailCompactedCatalogReference
    commit: ObjectCatalogCommit
    entries: tuple[DenmarkCvrCompanyDetailCompactedCatalogEntry, ...]


@dataclass(frozen=True)
class DenmarkCvrCompanyDetailCompactionResult:
    catalog_reference: DenmarkCvrCompanyDetailCompactedCatalogReference
    reused: bool
    source_object_count: int
    source_size_bytes: int
    compacted_object_count: int
    compacted_size_bytes: int


def company_detail_compacted_catalog_location(
    partition_key: str,
) -> ObjectCatalogLocation:
    if not company_detail_catalog_enabled(partition_key):
        raise ValueError(
            "Denmark CVR company-detail compaction is limited to the canary "
            f"partitions: partition={partition_key} "
            f"canary={DENMARK_CVR_COMPANY_DETAIL_CATALOG_CANARY_PARTITIONS}"
        )
    return ObjectCatalogLocation(
        source="denmark_cvr",
        dataset=DENMARK_CVR_COMPANY_DETAIL_COMPACTED_DATASET,
        partition={"hash_bucket": partition_key},
    )


def compact_company_detail_partition(
    *,
    object_store: ObjectStoreResource,
    source_catalog_reference: DenmarkCvrCompanyDetailCatalogReference,
    source_run_id: str,
    created_at: datetime,
) -> DenmarkCvrCompanyDetailCompactionResult:
    source_catalog = load_company_detail_catalog(
        object_store=object_store,
        reference=source_catalog_reference,
    )
    existing = load_optional_company_detail_compacted_catalog(
        object_store=object_store,
        partition_key=source_catalog_reference.partition_key,
    )
    if (
        existing is not None
        and bool(existing.entries)
        and sum(entry.source_object_count for entry in existing.entries)
        == len(source_catalog.entries)
        and sum(entry.source_size_bytes for entry in existing.entries)
        == source_catalog.commit.data_size_bytes
        and all(
            entry.source_catalog_sha256 == source_catalog.commit.catalog.sha256
            for entry in existing.entries
        )
    ):
        return DenmarkCvrCompanyDetailCompactionResult(
            catalog_reference=existing.reference,
            reused=True,
            source_object_count=len(source_catalog.entries),
            source_size_bytes=source_catalog.commit.data_size_bytes,
            compacted_object_count=len(existing.entries),
            compacted_size_bytes=existing.commit.data_size_bytes,
        )

    source_objects = read_company_detail_catalog_objects(
        object_store=object_store,
        catalog=source_catalog,
    )
    compacted_entries = tuple(
        _write_compacted_shard(
            object_store=object_store,
            location=company_detail_compacted_catalog_location(
                source_catalog_reference.partition_key
            ),
            source_catalog_sha256=source_catalog.commit.catalog.sha256,
            source_objects=shard,
        )
        for shard in _source_object_shards(source_objects)
    )
    compacted_catalog = _publish_compacted_catalog(
        object_store=object_store,
        partition_key=source_catalog_reference.partition_key,
        entries=compacted_entries,
        source_run_id=source_run_id,
        created_at=created_at,
    )
    return DenmarkCvrCompanyDetailCompactionResult(
        catalog_reference=compacted_catalog.reference,
        reused=False,
        source_object_count=len(source_catalog.entries),
        source_size_bytes=source_catalog.commit.data_size_bytes,
        compacted_object_count=len(compacted_entries),
        compacted_size_bytes=compacted_catalog.commit.data_size_bytes,
    )


def load_optional_company_detail_compacted_catalog(
    *,
    object_store: ObjectStoreResource,
    partition_key: str,
) -> DenmarkCvrCompanyDetailCompactedCatalog | None:
    location = company_detail_compacted_catalog_location(partition_key)
    commit_key = location.commit_object_key()
    if not object_store.exists(commit_key, bucket=DENMARK_CVR_BUCKET):
        return None
    commit = _load_compacted_commit(
        object_store=object_store,
        commit_key=commit_key,
    )
    return _load_compacted_catalog(
        object_store=object_store,
        reference=DenmarkCvrCompanyDetailCompactedCatalogReference(
            bucket=DENMARK_CVR_BUCKET,
            partition_key=partition_key,
            commit_key=commit_key,
            source_run_id=commit.source_run_id,
        ),
        commit=commit,
    )


def load_company_detail_compacted_catalog(
    *,
    object_store: ObjectStoreResource,
    reference: DenmarkCvrCompanyDetailCompactedCatalogReference,
) -> DenmarkCvrCompanyDetailCompactedCatalog:
    expected_location = company_detail_compacted_catalog_location(
        reference.partition_key
    )
    if reference.bucket != DENMARK_CVR_BUCKET:
        raise ValueError(
            "Denmark CVR compacted company-detail bucket mismatch: "
            f"expected={DENMARK_CVR_BUCKET} actual={reference.bucket}"
        )
    if reference.commit_key != expected_location.commit_object_key():
        raise ValueError(
            "Denmark CVR compacted company-detail commit key mismatch: "
            f"expected={expected_location.commit_object_key()} "
            f"actual={reference.commit_key}"
        )
    if not object_store.exists(reference.commit_key, bucket=reference.bucket):
        raise ValueError(
            "Denmark CVR compacted company-detail commit does not exist: "
            f"key={reference.commit_key}"
        )
    commit = _load_compacted_commit(
        object_store=object_store,
        commit_key=reference.commit_key,
    )
    return _load_compacted_catalog(
        object_store=object_store,
        reference=reference,
        commit=commit,
    )


def _write_compacted_shard(
    *,
    object_store: ObjectStoreResource,
    location: ObjectCatalogLocation,
    source_catalog_sha256: str,
    source_objects: tuple[DenmarkCvrCompanyDetailSourceObject, ...],
) -> DenmarkCvrCompanyDetailCompactedCatalogEntry:
    rows = [
        {
            "cvr": entry.cvr,
            "object_kind": entry.object_kind,
            "source_object_key": entry.object_key,
            "source_size_bytes": entry.size_bytes,
            "source_sha256": entry.sha256,
            "payload_json": body.decode("utf-8"),
        }
        for entry, body in source_objects
    ]
    table = pa.Table.from_pylist(rows, schema=_COMPACTED_DATA_SCHEMA)
    body = _parquet_bytes(table)
    digest = sha256(body).hexdigest()
    object_key = location.data_object_key(digest, object_format="parquet")
    if not object_store.exists(object_key, bucket=DENMARK_CVR_BUCKET):
        object_store.write_bytes(object_key, body, bucket=DENMARK_CVR_BUCKET)
    stored_body = object_store.read_bytes(object_key, bucket=DENMARK_CVR_BUCKET)
    if stored_body != body:
        raise ValueError(
            "Denmark CVR compacted company-detail object verification failed: "
            f"key={object_key}"
        )
    cvrs = [entry.cvr for entry, _ in source_objects]
    return DenmarkCvrCompanyDetailCompactedCatalogEntry(
        object_key=object_key,
        size_bytes=len(body),
        sha256=digest,
        row_count=table.num_rows,
        source_catalog_sha256=source_catalog_sha256,
        source_object_count=len(source_objects),
        source_size_bytes=sum(entry.size_bytes for entry, _ in source_objects),
        first_cvr=min(cvrs),
        last_cvr=max(cvrs),
    )


def _source_object_shards(
    source_objects: tuple[DenmarkCvrCompanyDetailSourceObject, ...],
) -> tuple[tuple[DenmarkCvrCompanyDetailSourceObject, ...], ...]:
    if not source_objects:
        raise ValueError("Denmark CVR company-detail source catalog must not be empty")
    shards: list[tuple[DenmarkCvrCompanyDetailSourceObject, ...]] = []
    current: list[DenmarkCvrCompanyDetailSourceObject] = []
    current_size = 0
    for source_object in source_objects:
        entry, _ = source_object
        if (
            current
            and current_size + entry.size_bytes
            > DENMARK_CVR_COMPANY_DETAIL_COMPACTED_TARGET_SOURCE_BYTES
        ):
            shards.append(tuple(current))
            current = []
            current_size = 0
        current.append(source_object)
        current_size += entry.size_bytes
    if current:
        shards.append(tuple(current))
    return tuple(shards)


def _publish_compacted_catalog(
    *,
    object_store: ObjectStoreResource,
    partition_key: str,
    entries: tuple[DenmarkCvrCompanyDetailCompactedCatalogEntry, ...],
    source_run_id: str,
    created_at: datetime,
) -> DenmarkCvrCompanyDetailCompactedCatalog:
    location = company_detail_compacted_catalog_location(partition_key)
    normalized_entries = tuple(sorted(entries, key=lambda entry: entry.object_key))
    normalized_created_at = created_at.astimezone(UTC)
    partition_json = _partition_json(location)
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
                "object_format": "parquet",
                "size_bytes": entry.size_bytes,
                "sha256": entry.sha256,
                "row_count": entry.row_count,
                "source_catalog_sha256": entry.source_catalog_sha256,
                "source_object_count": entry.source_object_count,
                "source_size_bytes": entry.source_size_bytes,
                "first_cvr": entry.first_cvr,
                "last_cvr": entry.last_cvr,
            }
            for entry in normalized_entries
        ],
        schema=_COMPACTED_CATALOG_SCHEMA,
    )
    body = _parquet_bytes(table)
    digest = sha256(body).hexdigest()
    catalog_key = location.catalog_object_key(source_run_id)
    object_store.write_bytes(catalog_key, body, bucket=DENMARK_CVR_BUCKET)
    if object_store.read_bytes(catalog_key, bucket=DENMARK_CVR_BUCKET) != body:
        raise ValueError(
            "Denmark CVR compacted company-detail catalog verification failed: "
            f"key={catalog_key}"
        )
    commit = ObjectCatalogCommit(
        location=location,
        source_run_id=source_run_id,
        created_at=normalized_created_at,
        catalog=ObjectCatalogFile(
            key=catalog_key,
            sha256=digest,
            size_bytes=len(body),
            row_count=table.num_rows,
        ),
        data_object_count=table.num_rows,
        data_size_bytes=sum(entry.size_bytes for entry in normalized_entries),
        data_row_count=sum(entry.row_count for entry in normalized_entries),
    )
    object_store.write_bytes(
        location.commit_object_key(),
        commit.to_json_bytes(),
        bucket=DENMARK_CVR_BUCKET,
    )
    reference = DenmarkCvrCompanyDetailCompactedCatalogReference(
        bucket=DENMARK_CVR_BUCKET,
        partition_key=partition_key,
        commit_key=location.commit_object_key(),
        source_run_id=source_run_id,
    )
    return DenmarkCvrCompanyDetailCompactedCatalog(
        reference=reference,
        commit=commit,
        entries=normalized_entries,
    )


def _load_compacted_commit(
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
            f"Denmark CVR compacted company-detail commit is invalid: key={commit_key}"
        ) from exc


def _load_compacted_catalog(
    *,
    object_store: ObjectStoreResource,
    reference: DenmarkCvrCompanyDetailCompactedCatalogReference,
    commit: ObjectCatalogCommit,
) -> DenmarkCvrCompanyDetailCompactedCatalog:
    expected_location = company_detail_compacted_catalog_location(
        reference.partition_key
    )
    if commit.location != expected_location:
        raise ValueError(
            "Denmark CVR compacted company-detail location mismatch: "
            f"expected={expected_location.model_dump()} "
            f"actual={commit.location.model_dump()}"
        )
    if commit.source_run_id != reference.source_run_id:
        raise ValueError(
            "Denmark CVR compacted company-detail source run ID mismatch: "
            f"expected={reference.source_run_id} actual={commit.source_run_id}"
        )
    if not object_store.exists(commit.catalog.key, bucket=reference.bucket):
        raise ValueError(
            "Denmark CVR compacted company-detail catalog does not exist: "
            f"key={commit.catalog.key}"
        )
    body = object_store.read_bytes(commit.catalog.key, bucket=reference.bucket)
    if len(body) != commit.catalog.size_bytes:
        raise ValueError(
            "Denmark CVR compacted company-detail catalog size mismatch: "
            f"expected={commit.catalog.size_bytes} actual={len(body)}"
        )
    actual_digest = sha256(body).hexdigest()
    if actual_digest != commit.catalog.sha256:
        raise ValueError(
            "Denmark CVR compacted company-detail catalog SHA-256 mismatch: "
            f"expected={commit.catalog.sha256} actual={actual_digest}"
        )
    try:
        table = pq.read_table(BytesIO(body))
    except (pa.ArrowInvalid, OSError) as exc:
        raise ValueError(
            "Denmark CVR compacted company-detail catalog is not readable Parquet: "
            f"key={commit.catalog.key}"
        ) from exc
    if not table.schema.equals(_COMPACTED_CATALOG_SCHEMA, check_metadata=False):
        raise ValueError(
            "Denmark CVR compacted company-detail catalog schema mismatch: "
            f"expected={_COMPACTED_CATALOG_SCHEMA} actual={table.schema}"
        )
    entries = _compacted_entries_from_rows(
        rows=table.to_pylist(),
        commit=commit,
    )
    return DenmarkCvrCompanyDetailCompactedCatalog(
        reference=reference,
        commit=commit,
        entries=entries,
    )


def _compacted_entries_from_rows(
    *,
    rows: list[dict[str, object]],
    commit: ObjectCatalogCommit,
) -> tuple[DenmarkCvrCompanyDetailCompactedCatalogEntry, ...]:
    if len(rows) != commit.catalog.row_count:
        raise ValueError(
            "Denmark CVR compacted company-detail catalog row count mismatch: "
            f"expected={commit.catalog.row_count} actual={len(rows)}"
        )
    expected_partition_json = _partition_json(commit.location)
    entries: list[DenmarkCvrCompanyDetailCompactedCatalogEntry] = []
    for row in rows:
        expected_identity = {
            "schema_version": OBJECT_CATALOG_SCHEMA_VERSION,
            "source": commit.location.source,
            "dataset": commit.location.dataset,
            "partition_json": expected_partition_json,
            "source_run_id": commit.source_run_id,
            "created_at": commit.created_at,
            "object_format": "parquet",
        }
        for field, expected in expected_identity.items():
            if row[field] != expected:
                raise ValueError(
                    "Denmark CVR compacted company-detail row identity mismatch: "
                    f"field={field} expected={expected!r} actual={row[field]!r}"
                )
        entries.append(
            DenmarkCvrCompanyDetailCompactedCatalogEntry(
                object_key=str(row["object_key"]),
                size_bytes=int(row["size_bytes"]),
                sha256=str(row["sha256"]),
                row_count=int(row["row_count"]),
                source_catalog_sha256=str(row["source_catalog_sha256"]),
                source_object_count=int(row["source_object_count"]),
                source_size_bytes=int(row["source_size_bytes"]),
                first_cvr=str(row["first_cvr"]),
                last_cvr=str(row["last_cvr"]),
            )
        )
    normalized_entries = tuple(entries)
    object_keys = [entry.object_key for entry in normalized_entries]
    if object_keys != sorted(object_keys):
        raise ValueError("Denmark CVR compacted catalog keys must be sorted")
    if len(set(object_keys)) != len(object_keys):
        raise ValueError("Denmark CVR compacted catalog keys must be unique")
    for entry in normalized_entries:
        expected_object_key = commit.location.data_object_key(
            entry.sha256,
            object_format="parquet",
        )
        if entry.object_key != expected_object_key:
            raise ValueError(
                "Denmark CVR compacted catalog object key mismatch: "
                f"expected={expected_object_key} actual={entry.object_key}"
            )
        if entry.source_object_count != entry.row_count:
            raise ValueError(
                "Denmark CVR compacted catalog source object count mismatch: "
                f"key={entry.object_key} source_objects={entry.source_object_count} "
                f"rows={entry.row_count}"
            )
    if sum(entry.size_bytes for entry in normalized_entries) != commit.data_size_bytes:
        raise ValueError(
            "Denmark CVR compacted catalog data size does not match commit"
        )
    if sum(entry.row_count for entry in normalized_entries) != commit.data_row_count:
        raise ValueError(
            "Denmark CVR compacted catalog row count does not match commit"
        )
    return normalized_entries


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


@dg.asset(
    group_name="denmark_cvr_company_details",
    kinds={"python", "json", "parquet", "s3"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "entity_type": "virksomhed",
        "layer": "compacted_detail",
    },
    partitions_def=DENMARK_CVR_COMPANY_DETAIL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=DENMARK_CVR_COMPANY_DETAIL_POOL,
    description=(
        "Compacts the first eight canary company-detail catalogs into "
        "content-addressed Parquet shards without enumerating legacy JSON "
        "object prefixes."
    ),
)
def denmark_cvr_company_details_compacted_s3(
    context: dg.AssetExecutionContext,
    denmark_cvr_company_details_s3: DenmarkCvrCompanyDetailPartitionSnapshot,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult[DenmarkCvrCompanyDetailCompactedCatalogReference]:
    snapshot = denmark_cvr_company_details_s3
    if snapshot.partition_key != context.partition_key:
        raise ValueError(
            "Denmark CVR company-detail compaction partition mismatch: "
            f"input={snapshot.partition_key} run={context.partition_key}"
        )
    if snapshot.catalog_reference is None:
        raise ValueError(
            "Denmark CVR company-detail compaction requires a v2 canary catalog"
        )
    result = compact_company_detail_partition(
        object_store=object_store,
        source_catalog_reference=snapshot.catalog_reference,
        source_run_id=context.run_id,
        created_at=datetime.now(UTC),
    )
    return dg.MaterializeResult(
        value=result.catalog_reference,
        metadata={
            "partition_key": context.partition_key,
            "source_catalog_commit_key": snapshot.catalog_reference.commit_key,
            "compacted_catalog_commit_key": result.catalog_reference.commit_key,
            "compaction_reused": result.reused,
            "source_object_count": result.source_object_count,
            "source_size_bytes": result.source_size_bytes,
            "compacted_object_count": result.compacted_object_count,
            "compacted_size_bytes": result.compacted_size_bytes,
            "target_source_bytes_per_shard": (
                DENMARK_CVR_COMPANY_DETAIL_COMPACTED_TARGET_SOURCE_BYTES
            ),
        },
    )


defs = dg.Definitions(assets=[denmark_cvr_company_details_compacted_s3])
