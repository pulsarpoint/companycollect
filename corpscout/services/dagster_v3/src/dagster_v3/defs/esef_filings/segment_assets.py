"""Processed-week ESEF source-document archive and deterministic extraction.

Each filings.xbrl.org report package is preserved once by content hash. The
queryable rows retain the upstream ``fxo_id`` so they join directly to
``esef_facts`` and, through the ESEF entity map, to ``(country, company_id)``.

No ``from __future__ import annotations``: Dagster inspects asset annotations.
"""

import json
import re
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from typing import Any, TextIO

import dagster as dg
import ijson
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from pydantic import Field

from dagster_v3.defs.common.duckdb_resources import safe_duckdb_connection
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.artifact_contract import (
    ARTIFACT_SCHEMA_VERSION,
    SUPPORTED_CORE_OUTPUT_ARTIFACT_SCHEMA_VERSIONS,
)
from dagster_v3.defs.esef_filings.assets import (
    ESEF_PROCESSED_WEEK_PARTITIONS,
    processed_week_bounds,
)
from dagster_v3.defs.esef_filings.client import EsefFilingsClient
from dagster_v3.defs.esef_filings.segment_parser import (
    ARTIFACT_PREFIX,
    EsefArtifactSource,
    artifact_object_key,
    parse_esef_report_package,
    write_artifact_json,
)

GROUP_NAME = "esef_filings"
ESEF_DOCUMENT_BUCKET = "source-esef-filings"
ESEF_ARELLE_POOL = "esef_arelle"
ESEF_REPORT_PACKAGE_PREFIX = "esef_filings/report_packages/"
ESEF_DOCUMENT_MANIFEST_PREFIX = "esef_filings/document_extraction_manifests/"
ESEF_DOCUMENT_RESULT_PREFIX = "esef_filings/document_extraction_results/"
_PROGRESS_INTERVAL = 25
_SHA256_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")
_ARTIFACT_PACKAGE_DIGEST_PATTERN = re.compile(
    r"/package_sha256=([0-9a-f]{64})/artifact\.json$"
)
_DOCUMENT_MANIFEST_SCHEMA_VERSION = 3
_SUPPORTED_DOCUMENT_MANIFEST_SCHEMA_VERSIONS = (2, _DOCUMENT_MANIFEST_SCHEMA_VERSION)
_DOCUMENT_RESULT_SCHEMA_VERSION = 3
_DEFAULT_DOCUMENT_PARSE_WORKERS = 4
_MAX_DOCUMENT_PARSE_WORKERS = 8
_DOCUMENT_PARSE_TASKS_PER_CHILD = 1
_DOCUMENT_PUBLICATION_INSERT_BATCH_SIZE = 25_000


class EsefDocumentManifestConfig(dg.Config):
    source_document_ids: list[str] = Field(default_factory=list)
    max_documents: int | None = Field(default=None, ge=1, le=100_000)


class EsefDocumentArtifactConfig(dg.Config):
    refresh_existing: bool = False
    validate_esef: bool = True
    parse_workers: int = Field(
        default=_DEFAULT_DOCUMENT_PARSE_WORKERS,
        ge=1,
        le=_MAX_DOCUMENT_PARSE_WORKERS,
    )


@dataclass(frozen=True)
class _DocumentParseTask:
    package_path: str
    artifact_path: str
    source: EsefArtifactSource
    validate_esef: bool


@dataclass(frozen=True)
class _DocumentParseResult:
    artifact_path: str
    parse_seconds: float
    serialization_seconds: float
    artifact_size_bytes: int


@dataclass(frozen=True)
class _ScheduledDocumentParse:
    digest: str
    documents: tuple[dict[str, object], ...]
    package_object_key: str
    package_size_bytes: int
    parsed_artifact_object_key: str
    archive_status: str
    local_package: Path


class _JsonlRowSpool:
    """Append JSON rows to disk while retaining only row counts in memory."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: TextIO = path.open("w", encoding="utf-8", newline="")

    def append(self, row: Mapping[str, object]) -> None:
        self._handle.write(_json_text(row))
        self._handle.write("\n")

    def close(self) -> None:
        if not self._handle.closed:
            self._handle.close()


def _parse_document_package_worker(task: _DocumentParseTask) -> _DocumentParseResult:
    parse_started = perf_counter()
    parsed = parse_esef_report_package(
        task.package_path,
        source=task.source,
        validate_esef=task.validate_esef,
    )
    parse_seconds = perf_counter() - parse_started

    serialization_started = perf_counter()
    write_artifact_json(parsed, task.artifact_path)
    artifact_size_bytes = Path(task.artifact_path).stat().st_size
    return _DocumentParseResult(
        artifact_path=task.artifact_path,
        parse_seconds=parse_seconds,
        serialization_seconds=perf_counter() - serialization_started,
        artifact_size_bytes=artifact_size_bytes,
    )


def report_package_object_key(package_sha256: str) -> str:
    digest = _validated_sha256(package_sha256)
    return f"{ESEF_REPORT_PACKAGE_PREFIX}package_sha256={digest}/report-package.zip"


def document_manifest_object_key(partition_key: str) -> str:
    processed_week_bounds(partition_key)
    return (
        f"{ESEF_DOCUMENT_MANIFEST_PREFIX}processed_week={partition_key}/manifest.json"
    )


def document_result_object_key(partition_key: str) -> str:
    processed_week_bounds(partition_key)
    return f"{ESEF_DOCUMENT_RESULT_PREFIX}processed_week={partition_key}/result.json"


def _write_document_result_file(
    result_path: Path,
    *,
    partition_key: str,
    source_run_id: str,
    extracted_at: str,
    source_document_ids: Sequence[str],
    document_rows_path: Path,
    candidate_rows_path: Path,
    concept_label_rows_path: Path,
    metadata: Mapping[str, object],
) -> None:
    with result_path.open("w", encoding="utf-8", newline="") as result:
        result.write('{"schema_version":')
        result.write(str(_DOCUMENT_RESULT_SCHEMA_VERSION))
        result.write(',"processed_week":')
        result.write(_json_text(partition_key))
        result.write(',"source_run_id":')
        result.write(_json_text(source_run_id))
        result.write(',"extracted_at":')
        result.write(_json_text(extracted_at))
        result.write(',"source_document_ids":')
        result.write(_json_text(source_document_ids))
        for property_name, spool_path in (
            ("document_rows", document_rows_path),
            ("candidate_rows", candidate_rows_path),
            ("concept_label_rows", concept_label_rows_path),
        ):
            result.write(",")
            result.write(_json_text(property_name))
            result.write(":")
            _write_jsonl_array(result, spool_path)
        result.write(',"metadata":')
        result.write(_json_text(metadata))
        result.write("}")


def _write_jsonl_array(result: TextIO, rows_path: Path) -> None:
    result.write("[")
    separator = ""
    with rows_path.open("r", encoding="utf-8") as rows:
        for serialized_row in rows:
            serialized_row = serialized_row.rstrip("\n")
            if serialized_row == "":
                continue
            result.write(separator)
            result.write(serialized_row)
            separator = ","
    result.write("]")


def load_esef_company_links(
    clickhouse: ClickhouseResource,
) -> dict[str, tuple[str, str]]:
    """Load the current LEI -> (country, registry company id) relation."""
    with clickhouse.get_connection() as client:
        rows = client.execute(
            "SELECT lei, country_iso2, registry_id "
            f"FROM {tables.QUALIFIED_ESEF_ENTITY_REGISTRY_MAP_TABLE} FINAL "
            "WHERE registry_id != ''"
        )
    return {
        str(lei): (str(country_iso2).upper(), str(company_id))
        for lei, country_iso2, company_id in rows
    }


def run_esef_document_manifest_partition(
    *,
    esef_filings_duckdb: DuckDBResource,
    object_store: Any,
    company_links: Mapping[str, tuple[str, str]],
    partition_key: str,
    source_run_id: str,
    source_document_ids: Sequence[str],
    max_documents: int | None,
    log_info: Callable[..., object],
) -> dict[str, object]:
    """Snapshot one processed week's work while holding the DuckDB pool."""
    started = perf_counter()
    processed_from, processed_until = processed_week_bounds(partition_key)
    selected_ids = {value.strip() for value in source_document_ids if value.strip()}
    index_rows = _load_filing_index_rows(
        esef_filings_duckdb,
        processed_from=processed_from,
        processed_until=processed_until,
        source_document_ids=selected_ids,
        max_documents=max_documents,
    )
    object_store.ensure_bucket(ESEF_DOCUMENT_BUCKET)
    documents = [
        {
            **index_row,
            "company_id": _company_id_for_index_row(index_row, company_links),
        }
        for index_row in index_rows
    ]
    manifest = {
        "schema_version": _DOCUMENT_MANIFEST_SCHEMA_VERSION,
        "processed_week": partition_key,
        "processed_window_start": processed_from.isoformat(),
        "processed_window_end": processed_until.isoformat(),
        "source_run_id": source_run_id,
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "documents": documents,
    }
    manifest_bytes = _json_text(manifest).encode("utf-8")
    manifest_key = document_manifest_object_key(partition_key)
    object_store.write_bytes(
        manifest_key,
        manifest_bytes,
        bucket=ESEF_DOCUMENT_BUCKET,
    )
    elapsed = perf_counter() - started
    log_info(
        "ESEF processed week %s: wrote %s-document extraction manifest in %.2fs",
        partition_key,
        len(documents),
        elapsed,
    )
    return {
        "partition_key": partition_key,
        "processed_window_start": processed_from.isoformat(),
        "processed_window_end": processed_until.isoformat(),
        "selected_document_count": len(documents),
        "manifest_object_key": manifest_key,
        "manifest_size_bytes": len(manifest_bytes),
        "manifest_seconds": elapsed,
    }


def run_esef_document_artifacts_partition(
    *,
    object_store: Any,
    client: Any,
    partition_key: str,
    source_run_id: str,
    refresh_existing: bool,
    validate_esef: bool,
    parse_workers: int,
    log_info: Callable[..., object],
) -> dict[str, object]:
    """Archive and parse unique packages in worker processes outside DuckDB."""
    if parse_workers < 1 or parse_workers > _MAX_DOCUMENT_PARSE_WORKERS:
        raise ValueError(
            "ESEF document parse_workers must be between 1 and "
            f"{_MAX_DOCUMENT_PARSE_WORKERS}"
        )
    started = perf_counter()
    manifest = _read_document_stage_object(
        object_store,
        key=document_manifest_object_key(partition_key),
        partition_key=partition_key,
        object_name="manifest",
        supported_schema_versions=_SUPPORTED_DOCUMENT_MANIFEST_SCHEMA_VERSIONS,
    )
    documents = [
        dict(_mapping(value, name="manifest document"))
        for value in _list(manifest.get("documents"), name="manifest documents")
    ]
    extracted_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    downloaded_packages = 0
    reused_packages = 0
    parsed_packages = 0
    reused_artifacts = 0
    reused_artifact_schema_counts: dict[str, int] = {}
    skipped_packages = 0
    processed_documents = 0
    next_progress = 1
    package_io_seconds = 0.0
    artifact_read_seconds = 0.0
    artifact_upload_seconds = 0.0
    parse_seconds = 0.0
    serialization_seconds = 0.0
    artifact_size_bytes = 0
    document_row_count = 0
    candidate_row_count = 0
    concept_label_row_count = 0
    email_candidate_count = 0
    phone_candidate_count = 0
    website_candidate_count = 0

    log_info(
        "ESEF processed week %s: %s source documents selected; %s parse workers",
        partition_key,
        len(documents),
        parse_workers,
    )

    with tempfile.TemporaryDirectory(prefix="esef_document_extraction_") as temp_name:
        temp_root = Path(temp_name)
        document_spool = _JsonlRowSpool(temp_root / "document_rows.jsonl")
        candidate_spool = _JsonlRowSpool(temp_root / "candidate_rows.jsonl")
        concept_label_spool = _JsonlRowSpool(temp_root / "concept_label_rows.jsonl")

        documents_by_digest: dict[str, list[dict[str, object]]] = {}
        for document in documents:
            package_url = str(document["package_url"])
            package_sha256 = str(document["package_sha256"])
            if package_url != "" and package_sha256 != "":
                digest = _validated_sha256(package_sha256)
                documents_by_digest.setdefault(digest, []).append(document)
                continue
            document_spool.append(
                _unavailable_document_row(
                    document,
                    company_id=str(document["company_id"]),
                    fiscal_year=int(document["fiscal_year"]),
                    source_run_id=source_run_id,
                    extracted_at=extracted_at,
                )
            )
            document_row_count += 1
            skipped_packages += 1
            processed_documents += 1

        current_artifact_keys_by_digest: dict[str, str] = {}
        if not refresh_existing:
            for digest in documents_by_digest:
                current_key = artifact_object_key(digest)
                if object_store.exists(current_key, bucket=ESEF_DOCUMENT_BUCKET):
                    current_artifact_keys_by_digest[digest] = current_key
        compatible_artifacts_by_digest = compatible_artifact_keys_by_digest(
            object_store,
            package_sha256s=(
                set(documents_by_digest) - set(current_artifact_keys_by_digest)
                if not refresh_existing
                else set()
            ),
        )

        def append_artifact_rows(
            artifact_documents: Sequence[Mapping[str, object]],
            *,
            artifact: Mapping[str, Any],
            package_object_key: str,
            package_size_bytes: int,
            parsed_artifact_object_key: str,
            archive_status: str,
            extraction_status: str,
        ) -> None:
            nonlocal candidate_row_count
            nonlocal concept_label_row_count
            nonlocal document_row_count
            nonlocal email_candidate_count
            nonlocal phone_candidate_count
            nonlocal website_candidate_count

            for document in artifact_documents:
                company_id = str(document["company_id"])
                document_spool.append(
                    _document_row(
                        document,
                        artifact=artifact,
                        company_id=company_id,
                        fiscal_year=int(document["fiscal_year"]),
                        package_object_key=package_object_key,
                        package_size_bytes=package_size_bytes,
                        parsed_artifact_object_key=parsed_artifact_object_key,
                        archive_status=archive_status,
                        extraction_status=extraction_status,
                        source_run_id=source_run_id,
                        extracted_at=extracted_at,
                    )
                )
                document_row_count += 1
                for candidate_row in _contact_candidate_rows(
                    document,
                    artifact=artifact,
                    company_id=company_id,
                    fiscal_year=int(document["fiscal_year"]),
                    source_run_id=source_run_id,
                    extracted_at=extracted_at,
                ):
                    candidate_spool.append(candidate_row)
                    candidate_row_count += 1
                    candidate_kind = candidate_row["candidate_kind"]
                    email_candidate_count += candidate_kind == "email"
                    phone_candidate_count += candidate_kind == "phone"
                    website_candidate_count += candidate_kind == "website"
                for concept_label_row in _concept_label_rows(
                    document,
                    artifact=artifact,
                    company_id=company_id,
                    fiscal_year=int(document["fiscal_year"]),
                    source_run_id=source_run_id,
                    extracted_at=extracted_at,
                ):
                    concept_label_spool.append(concept_label_row)
                    concept_label_row_count += 1

        pending: dict[Future[_DocumentParseResult], _ScheduledDocumentParse] = {}

        def complete_futures(completed: set[Future[_DocumentParseResult]]) -> None:
            nonlocal artifact_size_bytes
            nonlocal artifact_upload_seconds
            nonlocal parse_seconds
            nonlocal parsed_packages
            nonlocal processed_documents
            nonlocal serialization_seconds
            for future in completed:
                scheduled = pending.pop(future)
                result = future.result()
                artifact_path = Path(result.artifact_path)
                artifact = _validated_artifact_file(
                    artifact_path,
                    expected_package_sha256=scheduled.digest,
                )
                upload_started = perf_counter()
                object_store.upload_file(
                    scheduled.parsed_artifact_object_key,
                    artifact_path,
                    bucket=ESEF_DOCUMENT_BUCKET,
                )
                artifact_upload_seconds += perf_counter() - upload_started
                append_artifact_rows(
                    scheduled.documents,
                    artifact=artifact,
                    package_object_key=scheduled.package_object_key,
                    package_size_bytes=scheduled.package_size_bytes,
                    parsed_artifact_object_key=scheduled.parsed_artifact_object_key,
                    archive_status=scheduled.archive_status,
                    extraction_status="parsed",
                )
                del artifact
                parse_seconds += result.parse_seconds
                serialization_seconds += result.serialization_seconds
                artifact_size_bytes += result.artifact_size_bytes
                parsed_packages += 1
                processed_documents += len(scheduled.documents)
                scheduled.local_package.unlink(missing_ok=True)
                artifact_path.unlink(missing_ok=True)

        with ProcessPoolExecutor(
            max_workers=parse_workers,
            max_tasks_per_child=_DOCUMENT_PARSE_TASKS_PER_CHILD,
        ) as executor:
            for digest, grouped_documents in documents_by_digest.items():
                representative = grouped_documents[0]
                package_key = report_package_object_key(digest)
                parsed_key = artifact_object_key(digest)
                reusable_artifact = (
                    (ARTIFACT_SCHEMA_VERSION, current_artifact_keys_by_digest[digest])
                    if digest in current_artifact_keys_by_digest
                    else compatible_artifacts_by_digest.get(digest)
                )
                local_package = temp_root / f"{digest}.zip"
                io_started = perf_counter()
                package_exists = object_store.exists(
                    package_key,
                    bucket=ESEF_DOCUMENT_BUCKET,
                )

                if refresh_existing or not package_exists:
                    client.download_json_facts(
                        str(representative["package_url"]),
                        local_package,
                    )
                    _verify_package_hash(local_package, expected_sha256=digest)
                    object_store.upload_file(
                        package_key,
                        local_package,
                        bucket=ESEF_DOCUMENT_BUCKET,
                    )
                    package_size_bytes = local_package.stat().st_size
                    archive_status = "downloaded"
                    downloaded_packages += 1
                else:
                    package_size_bytes = 0
                    archive_status = "reused"
                    reused_packages += 1
                package_io_seconds += perf_counter() - io_started

                if not refresh_existing and reusable_artifact is not None:
                    artifact_schema_version, reusable_artifact_key = reusable_artifact
                    artifact_read_started = perf_counter()
                    local_artifact = temp_root / f"{digest}.artifact.json"
                    object_store.download_file(
                        reusable_artifact_key,
                        local_artifact,
                        bucket=ESEF_DOCUMENT_BUCKET,
                    )
                    artifact_read_seconds += perf_counter() - artifact_read_started
                    artifact = _validated_artifact_file(
                        local_artifact,
                        expected_package_sha256=digest,
                        supported_schema_versions=(artifact_schema_version,),
                    )
                    append_artifact_rows(
                        grouped_documents,
                        artifact=artifact,
                        package_object_key=package_key,
                        package_size_bytes=package_size_bytes,
                        parsed_artifact_object_key=reusable_artifact_key,
                        archive_status=archive_status,
                        extraction_status="reused",
                    )
                    del artifact
                    artifact_size_bytes += local_artifact.stat().st_size
                    reused_artifacts += 1
                    schema_key = str(artifact_schema_version)
                    reused_artifact_schema_counts[schema_key] = (
                        reused_artifact_schema_counts.get(schema_key, 0) + 1
                    )
                    processed_documents += len(grouped_documents)
                    local_package.unlink(missing_ok=True)
                    local_artifact.unlink(missing_ok=True)
                else:
                    if not local_package.exists():
                        download_started = perf_counter()
                        object_store.download_file(
                            package_key,
                            local_package,
                            bucket=ESEF_DOCUMENT_BUCKET,
                        )
                        _verify_package_hash(local_package, expected_sha256=digest)
                        package_io_seconds += perf_counter() - download_started
                        if package_size_bytes == 0:
                            package_size_bytes = local_package.stat().st_size
                    artifact_path = temp_root / f"{digest}.artifact.json"
                    future = executor.submit(
                        _parse_document_package_worker,
                        _DocumentParseTask(
                            package_path=str(local_package),
                            artifact_path=str(artifact_path),
                            source=EsefArtifactSource(
                                fxo_id=str(representative["source_document_id"]),
                                country=str(representative["country_iso2"]),
                                source_url=str(representative["package_url"]),
                                object_key=(
                                    f"s3://{ESEF_DOCUMENT_BUCKET}/{package_key}"
                                ),
                                company_id=str(representative["company_id"]),
                                source_run_id=source_run_id,
                                expected_package_sha256=digest,
                            ),
                            validate_esef=validate_esef,
                        ),
                    )
                    pending[future] = _ScheduledDocumentParse(
                        digest=digest,
                        documents=tuple(grouped_documents),
                        package_object_key=package_key,
                        package_size_bytes=package_size_bytes,
                        parsed_artifact_object_key=parsed_key,
                        archive_status=archive_status,
                        local_package=local_package,
                    )

                if len(pending) >= parse_workers * 2:
                    completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                    complete_futures(completed)

                if processed_documents >= next_progress:
                    log_info(
                        "ESEF processed week %s: %s/%s documents processed",
                        partition_key,
                        processed_documents,
                        len(documents),
                    )
                    while next_progress <= processed_documents:
                        next_progress += _PROGRESS_INTERVAL

            while pending:
                completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                complete_futures(completed)
                log_info(
                    "ESEF processed week %s: %s/%s documents processed",
                    partition_key,
                    processed_documents,
                    len(documents),
                )

        document_spool.close()
        candidate_spool.close()
        concept_label_spool.close()

        elapsed = perf_counter() - started
        metadata: dict[str, object] = {
            "partition_key": partition_key,
            "fiscal_years": sorted(
                {int(document["fiscal_year"]) for document in documents}
            ),
            "selected_document_count": len(documents),
            "unique_package_count": len(documents_by_digest),
            "source_document_row_count": document_row_count,
            "contact_candidate_row_count": candidate_row_count,
            "concept_label_row_count": concept_label_row_count,
            "downloaded_package_count": downloaded_packages,
            "reused_package_count": reused_packages,
            "parsed_package_count": parsed_packages,
            "reused_artifact_count": reused_artifacts,
            "reused_artifact_schema_counts": dict(
                sorted(reused_artifact_schema_counts.items())
            ),
            "skipped_package_count": skipped_packages,
            "parse_worker_count": parse_workers,
            "parse_tasks_per_worker": _DOCUMENT_PARSE_TASKS_PER_CHILD,
            "wall_seconds": elapsed,
            "package_io_seconds": package_io_seconds,
            "artifact_read_seconds": artifact_read_seconds,
            "artifact_upload_seconds": artifact_upload_seconds,
            "parse_worker_seconds": parse_seconds,
            "serialization_worker_seconds": serialization_seconds,
            "artifact_size_bytes": artifact_size_bytes,
            "email_candidate_count": email_candidate_count,
            "phone_candidate_count": phone_candidate_count,
            "website_candidate_count": website_candidate_count,
            "contact_table": tables.QUALIFIED_ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE,
            "concept_label_table": tables.QUALIFIED_ESEF_DOCUMENT_CONCEPT_LABELS_TABLE,
        }
        result_path = temp_root / "result.json"
        _write_document_result_file(
            result_path,
            partition_key=partition_key,
            source_run_id=source_run_id,
            extracted_at=extracted_at,
            source_document_ids=[
                str(document["source_document_id"]) for document in documents
            ],
            document_rows_path=document_spool.path,
            candidate_rows_path=candidate_spool.path,
            concept_label_rows_path=concept_label_spool.path,
            metadata=metadata,
        )
        result_key = document_result_object_key(partition_key)
        object_store.upload_file(
            result_key,
            result_path,
            bucket=ESEF_DOCUMENT_BUCKET,
        )
        metadata["result_object_key"] = result_key
        metadata["result_size_bytes"] = result_path.stat().st_size

    log_info(
        "ESEF processed week %s: artifact stage completed in %.2fs "
        "(parsed=%s reused=%s workers=%s)",
        partition_key,
        elapsed,
        parsed_packages,
        reused_artifacts,
        parse_workers,
    )
    return metadata


@contextmanager
def local_esef_document_result(
    object_store: Any,
    *,
    partition_key: str,
) -> Iterator[Path]:
    """Download and validate a result while keeping its rows file-backed."""
    with tempfile.TemporaryDirectory(prefix="esef_document_result_") as temp_name:
        result_path = Path(temp_name) / "result.json"
        object_store.download_file(
            document_result_object_key(partition_key),
            result_path,
            bucket=ESEF_DOCUMENT_BUCKET,
        )
        schema_version = _document_result_value(result_path, "schema_version")
        if schema_version != _DOCUMENT_RESULT_SCHEMA_VERSION:
            raise ValueError(
                "ESEF document result has an unsupported schema version: "
                f"expected={[_DOCUMENT_RESULT_SCHEMA_VERSION]} "
                f"actual={schema_version!r}"
            )
        processed_week = _document_result_value(result_path, "processed_week")
        if processed_week != partition_key:
            raise ValueError(
                "ESEF document result processed week does not match its key"
            )
        yield result_path


def iter_esef_document_result_rows(
    result_path: Path,
    *,
    property_name: str,
) -> Iterator[Mapping[str, Any]]:
    """Yield one normalized result row at a time from a local result file."""
    for value in _iter_document_result_items(result_path, f"{property_name}.item"):
        yield _mapping(value, name=f"result {property_name} row")


def esef_document_result_metadata(result_path: Path) -> Mapping[str, Any]:
    """Read the producer's row-count contract from a local result file."""
    return _mapping(
        _document_result_value(result_path, "metadata"),
        name="result metadata",
    )


def _iter_document_result_items(result_path: Path, prefix: str) -> Iterator[Any]:
    with result_path.open("rb") as result:
        yield from ijson.items(result, prefix, use_float=True)


def _document_result_value(result_path: Path, prefix: str) -> Any:
    value = next(_iter_document_result_items(result_path, prefix), None)
    if value is None:
        raise ValueError(f"ESEF document result is missing {prefix}")
    return value


def load_esef_document_result(
    object_store: Any,
    *,
    partition_key: str,
) -> Mapping[str, Any]:
    """Load and validate the artifact-stage result for one processed week."""
    return _read_document_stage_object(
        object_store,
        key=document_result_object_key(partition_key),
        partition_key=partition_key,
        object_name="result",
        supported_schema_versions=(_DOCUMENT_RESULT_SCHEMA_VERSION,),
    )


def _read_document_stage_object(
    object_store: Any,
    *,
    key: str,
    partition_key: str,
    object_name: str,
    supported_schema_versions: tuple[int, ...],
) -> Mapping[str, Any]:
    serialized = object_store.read_bytes(key, bucket=ESEF_DOCUMENT_BUCKET)
    value = json.loads(serialized)
    document_object = _mapping(value, name=object_name)
    schema_version = document_object.get("schema_version")
    if schema_version not in supported_schema_versions:
        raise ValueError(
            f"ESEF document {object_name} has an unsupported schema version: "
            f"expected={list(supported_schema_versions)} actual={schema_version!r}"
        )
    if document_object.get("processed_week") != partition_key:
        raise ValueError(
            f"ESEF document {object_name} processed week does not match its key"
        )
    return document_object


def _load_filing_index_rows(
    esef_filings_duckdb: DuckDBResource,
    *,
    processed_from: datetime,
    processed_until: datetime,
    source_document_ids: set[str],
    max_documents: int | None,
) -> list[dict[str, object]]:
    with safe_duckdb_connection(esef_filings_duckdb) as connection:
        if not _duckdb_table_exists(
            connection,
            schema=tables.DLT_DATASET_NAME,
            table=tables.FILINGS_INDEX_TABLE,
        ):
            raise ValueError(
                "Missing ESEF filing index; materialize esef_filings_index_duckdb "
                "before processed-week document extraction"
            )
        query = (
            "select fxo_id as source_document_id, lei, entity_name, country as "
            "country_iso2, coalesce(cast(period_end as varchar), '') as period_end, "
            "coalesce(package_url, '') as package_url, "
            "coalesce(report_url, '') as report_url, "
            "coalesce(viewer_url, '') as viewer_url, "
            "coalesce(package_sha256, '') as package_sha256, "
            "coalesce(cast(processed_at as varchar), '') as source_processed_at, "
            "coalesce(year(try_cast(period_end as date)), 0) as fiscal_year "
            f"from {tables.QUALIFIED_FILINGS_INDEX_TABLE} "
            "where try_cast(processed_at as timestamptz) >= ? "
            "and try_cast(processed_at as timestamptz) < ? "
        )
        parameters: list[object] = [processed_from, processed_until]
        if source_document_ids:
            placeholders = ", ".join("?" for _ in source_document_ids)
            query += f"and fxo_id in ({placeholders}) "
            parameters.extend(sorted(source_document_ids))
        query += "order by fxo_id"
        if max_documents is not None:
            query += " limit ?"
            parameters.append(max_documents)
        result = connection.execute(query, parameters)
        columns = [description[0] for description in result.description]
        return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def _company_id_for_index_row(
    index_row: Mapping[str, object],
    company_links: Mapping[str, tuple[str, str]],
) -> str:
    lei = str(index_row["lei"])
    if lei not in company_links:
        return ""
    mapped_country, company_id = company_links[lei]
    return (
        company_id if mapped_country == str(index_row["country_iso2"]).upper() else ""
    )


def _unavailable_document_row(
    index_row: Mapping[str, object],
    *,
    company_id: str,
    fiscal_year: int,
    source_run_id: str,
    extracted_at: str,
) -> dict[str, object]:
    missing_field = (
        "package_url" if str(index_row["package_url"]) == "" else "package_sha256"
    )
    return {
        **_document_identity_fields(
            index_row,
            company_id=company_id,
            fiscal_year=fiscal_year,
        ),
        "package_object_key": "",
        "package_size_bytes": 0,
        "parsed_artifact_object_key": "",
        "artifact_schema_version": 0,
        "parser_name": "",
        "parser_version": "",
        "archive_status": f"missing_{missing_field}",
        "extraction_status": "not_parsed",
        "fact_count": 0,
        "text_fact_count": 0,
        "numeric_fact_count": 0,
        "contact_candidate_count": 0,
        "website_candidate_count": 0,
        "validation_error_count": 0,
        "validation_warning_count": 0,
        "source_run_id": source_run_id,
        "extracted_at": extracted_at,
    }


def _document_row(
    index_row: Mapping[str, object],
    *,
    artifact: Mapping[str, Any],
    company_id: str,
    fiscal_year: int,
    package_object_key: str,
    package_size_bytes: int,
    parsed_artifact_object_key: str,
    archive_status: str,
    extraction_status: str,
    source_run_id: str,
    extracted_at: str,
) -> dict[str, object]:
    quality = _mapping(artifact.get("quality"), name="quality")
    parser = _mapping(artifact.get("parser"), name="parser")
    contact_candidates = _list(
        artifact.get("contact_candidates"), name="contact_candidates"
    )
    website_candidates = _list(
        artifact.get("website_candidates"), name="website_candidates"
    )
    return {
        **_document_identity_fields(
            index_row,
            company_id=company_id,
            fiscal_year=fiscal_year,
        ),
        "package_object_key": package_object_key,
        "package_size_bytes": package_size_bytes,
        "parsed_artifact_object_key": parsed_artifact_object_key,
        "artifact_schema_version": int(artifact["schema_version"]),
        "parser_name": str(parser.get("engine", "")),
        "parser_version": str(parser.get("version", "")),
        "archive_status": archive_status,
        "extraction_status": extraction_status,
        "fact_count": int(quality.get("fact_count", 0)),
        "text_fact_count": int(quality.get("text_fact_count", 0)),
        "numeric_fact_count": int(quality.get("numeric_fact_count", 0)),
        "contact_candidate_count": len(contact_candidates),
        "website_candidate_count": len(website_candidates),
        "validation_error_count": int(quality.get("error_count", 0)),
        "validation_warning_count": int(quality.get("warning_count", 0)),
        "source_run_id": source_run_id,
        "extracted_at": extracted_at,
    }


def _document_identity_fields(
    index_row: Mapping[str, object],
    *,
    company_id: str,
    fiscal_year: int,
) -> dict[str, object]:
    return {
        "source_document_id": str(index_row["source_document_id"]),
        "document_type": "esef_report_package",
        "lei": str(index_row["lei"]),
        "entity_name": str(index_row["entity_name"]),
        "country_iso2": str(index_row["country_iso2"]).upper(),
        "company_id": company_id,
        "period_end": str(index_row["period_end"]),
        "fiscal_year": fiscal_year,
        "package_url": str(index_row["package_url"]),
        "report_url": str(index_row["report_url"]),
        "viewer_url": str(index_row["viewer_url"]),
        "package_sha256": str(index_row["package_sha256"]).lower(),
        "source_processed_at": str(index_row["source_processed_at"]),
    }


def _contact_candidate_rows(
    index_row: Mapping[str, object],
    *,
    artifact: Mapping[str, Any],
    company_id: str,
    fiscal_year: int,
    source_run_id: str,
    extracted_at: str,
) -> Iterator[dict[str, object]]:
    parser = _mapping(artifact.get("parser"), name="parser")
    extractor_versions = _mapping(
        parser.get("candidate_extractor_versions", {}),
        name="candidate_extractor_versions",
    )
    common = {
        "source_document_id": str(index_row["source_document_id"]),
        "package_sha256": str(index_row["package_sha256"]).lower(),
        "lei": str(index_row["lei"]),
        "country_iso2": str(index_row["country_iso2"]).upper(),
        "company_id": company_id,
        "period_end": str(index_row["period_end"]),
        "fiscal_year": fiscal_year,
        "extractor_versions_json": _json_text(extractor_versions),
        "source_run_id": source_run_id,
        "extracted_at": extracted_at,
    }
    for value in _list(artifact.get("contact_candidates"), name="contact_candidates"):
        candidate = _mapping(value, name="contact candidate")
        kind = str(candidate["kind"])
        normalized_value = str(candidate["normalized_value"])
        yield {
            "candidate_id": _candidate_id(
                str(index_row["source_document_id"]), kind, normalized_value
            ),
            **common,
            "candidate_kind": kind,
            "normalized_value": normalized_value,
            "country_code": str(candidate.get("country_code", "")),
            "registrable_domain": "",
            "hosts_json": "[]",
            "normalized_urls_json": "[]",
            "suggested_roles_json": _json_text(candidate.get("suggested_roles", [])),
            "evidence_json": _json_text(candidate.get("evidence", [])),
            "evidence_count": len(
                _list(candidate.get("evidence", []), name="evidence")
            ),
        }
    for value in _list(artifact.get("website_candidates"), name="website_candidates"):
        candidate = _mapping(value, name="website candidate")
        domain = str(candidate["registrable_domain"])
        yield {
            "candidate_id": _candidate_id(
                str(index_row["source_document_id"]), "website", domain
            ),
            **common,
            "candidate_kind": "website",
            "normalized_value": domain,
            "country_code": "",
            "registrable_domain": domain,
            "hosts_json": _json_text(candidate.get("hosts", [])),
            "normalized_urls_json": _json_text(candidate.get("normalized_urls", [])),
            "suggested_roles_json": _json_text(candidate.get("suggested_roles", [])),
            "evidence_json": _json_text(candidate.get("evidence", [])),
            "evidence_count": len(
                _list(candidate.get("evidence", []), name="evidence")
            ),
        }


def _concept_label_rows(
    index_row: Mapping[str, object],
    *,
    artifact: Mapping[str, Any],
    company_id: str,
    fiscal_year: int,
    source_run_id: str,
    extracted_at: str,
) -> Iterator[dict[str, object]]:
    document = _mapping(artifact.get("document"), name="document")
    report_languages = {
        str(language).strip().lower()
        for language in _list(document.get("languages", []), name="document languages")
        if str(language).strip() != ""
    }
    concepts = _mapping(artifact.get("concepts"), name="concepts")
    common = {
        "source_document_id": str(index_row["source_document_id"]),
        "package_sha256": str(index_row["package_sha256"]).lower(),
        "lei": str(index_row["lei"]),
        "country_iso2": str(index_row["country_iso2"]).upper(),
        "company_id": company_id,
        "period_end": str(index_row["period_end"]),
        "fiscal_year": fiscal_year,
        "source_run_id": source_run_id,
        "extracted_at": extracted_at,
    }
    for concept_qname, concept_value in sorted(concepts.items()):
        if str(concept_qname) == "xbrl:note":
            continue
        concept = _mapping(concept_value, name=f"concept {concept_qname}")
        for label_role, field_name in (
            ("standard", "labels"),
            ("documentation", "documentation"),
        ):
            labels = _mapping(
                concept.get(field_name, {}),
                name=f"concept {concept_qname} {field_name}",
            )
            for language_value, label_value in sorted(labels.items()):
                language = str(language_value).strip().lower()
                label = str(label_value).strip()
                if language == "" or label == "":
                    continue
                yield {
                    "label_id": _concept_label_id(
                        str(index_row["source_document_id"]),
                        str(concept_qname),
                        label_role,
                        language,
                    ),
                    **common,
                    "concept_qname": str(concept_qname),
                    "concept_namespace_uri": str(concept.get("namespace", "")),
                    "concept_local_name": str(concept.get("local_name", "")),
                    "is_extension": bool(concept.get("is_extension", False)),
                    "label_role": label_role,
                    "language": language,
                    "label": label,
                    "is_report_language": _is_report_language(
                        language,
                        report_languages,
                    ),
                }


def _is_report_language(language: str, report_languages: set[str]) -> bool:
    if language in report_languages:
        return True
    base_language = language.split("-", 1)[0]
    return any(
        report_language.split("-", 1)[0] == base_language
        for report_language in report_languages
    )


def ensure_document_company_information_table(connection: Any) -> None:
    connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")
    connection.execute(
        f"create table if not exists {tables.DLT_DATASET_NAME}."
        f"{tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE} ("
        "source_document_id varchar, package_sha256 varchar, lei varchar, "
        "country_iso2 varchar, company_id varchar, period_end varchar, "
        "fiscal_year integer, extraction_status varchar, company_description varchar, "
        "description_language varchar, description_confidence double, "
        "description_evidence_ids_json varchar, people_json varchar, "
        "products_and_services_json varchar, customer_markets_json varchar, "
        "operating_geographies_json varchar, business_segments_json varchar, "
        "material_group_relationships_json varchar, "
        "enrichment_artifact_object_key varchar, input_artifact_object_key varchar, "
        "llm_request_object_key varchar, llm_request_sha256 varchar, "
        "llm_response_text varchar, llm_response_sha256 varchar, "
        "model_provider varchar, model_name varchar, prompt_version varchar, "
        "prompt_tokens bigint, completion_tokens bigint, input_character_count bigint, "
        "source_run_id varchar, extracted_at varchar)"
    )
    for column in (
        "llm_request_object_key",
        "llm_request_sha256",
        "llm_response_text",
        "llm_response_sha256",
    ):
        connection.execute(
            f"alter table {tables.DLT_DATASET_NAME}."
            f"{tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE} "
            f"add column if not exists {column} varchar default ''"
        )


def _duckdb_table_exists(connection: Any, *, schema: str, table: str) -> bool:
    return bool(
        connection.execute(
            "select count(*) from information_schema.tables "
            "where table_schema = ? and table_name = ?",
            [schema, table],
        ).fetchone()[0]
    )


def _validated_artifact_file(
    artifact_path: Path,
    *,
    expected_package_sha256: str,
    supported_schema_versions: tuple[int, ...] = (ARTIFACT_SCHEMA_VERSION,),
) -> Mapping[str, Any]:
    with artifact_path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    artifact = _mapping(value, name="artifact")
    if artifact.get("schema_version") not in supported_schema_versions:
        raise ValueError(
            "Stored ESEF artifact has an unsupported schema version: "
            f"expected={list(supported_schema_versions)} "
            f"actual={artifact.get('schema_version')!r}"
        )
    if artifact.get("package_sha256") != expected_package_sha256:
        raise ValueError("Stored ESEF artifact package SHA-256 does not match its key")
    return artifact


def compatible_artifact_keys_by_digest(
    object_store: Any,
    *,
    package_sha256s: set[str],
) -> dict[str, tuple[int, str]]:
    """Find persisted core-output artifacts when the current v5 key is absent."""
    if not package_sha256s:
        return {}

    compatible: dict[str, tuple[int, str]] = {}
    for schema_version in SUPPORTED_CORE_OUTPUT_ARTIFACT_SCHEMA_VERSIONS:
        if schema_version == ARTIFACT_SCHEMA_VERSION:
            continue
        prefix = f"{ARTIFACT_PREFIX}/schema=v{schema_version}/"
        for key in object_store.list_keys(prefix, bucket=ESEF_DOCUMENT_BUCKET):
            digest_match = _ARTIFACT_PACKAGE_DIGEST_PATTERN.search(key)
            if digest_match is None:
                continue
            digest = digest_match.group(1)
            if digest not in package_sha256s:
                continue
            existing = compatible.get(digest)
            if existing is None or key > existing[1]:
                compatible[digest] = (schema_version, key)
    return compatible


def _verify_package_hash(package_path: Path, *, expected_sha256: str) -> None:
    actual_sha256 = sha256(package_path.read_bytes()).hexdigest()
    if actual_sha256 != expected_sha256:
        raise ValueError(
            "ESEF report package SHA-256 mismatch: "
            f"expected={expected_sha256} actual={actual_sha256}"
        )


def _validated_sha256(value: str) -> str:
    clean_value = value.strip().lower()
    if _SHA256_PATTERN.fullmatch(clean_value) is None:
        raise ValueError("ESEF package SHA-256 must contain 64 hexadecimal characters")
    return clean_value


def _candidate_id(source_document_id: str, kind: str, value: str) -> str:
    return sha256(f"{source_document_id}\0{kind}\0{value}".encode()).hexdigest()


def _concept_label_id(
    source_document_id: str,
    concept_qname: str,
    label_role: str,
    language: str,
) -> str:
    identity = "\0".join((source_document_id, concept_qname, label_role, language))
    return sha256(identity.encode()).hexdigest()


def _json_text(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _mapping(value: Any, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"ESEF {name} must be an object")
    return value


def _list(value: Any, *, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"ESEF {name} must be a list")
    return value


_SOURCE_DOCUMENT_DEPENDENCIES = [
    dg.AssetDep(dg.AssetKey("esef_filings_index_duckdb")),
    dg.AssetDep(dg.AssetKey("esef_entity_registry_map_clickhouse")),
]


@dg.asset(
    name="esef_document_extraction_manifest_s3",
    deps=_SOURCE_DOCUMENT_DEPENDENCIES,
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_filings_duckdb",
    description=(
        "Snapshots the compact processed-week ESEF work manifest while holding the "
        "shared DuckDB pool only for the source-index read."
    ),
)
def esef_document_extraction_manifest_s3(
    context: dg.AssetExecutionContext,
    config: EsefDocumentManifestConfig,
    esef_filings_duckdb: DuckDBResource,
    object_store: ObjectStoreResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = run_esef_document_manifest_partition(
        esef_filings_duckdb=esef_filings_duckdb,
        object_store=object_store,
        company_links=load_esef_company_links(clickhouse),
        partition_key=context.partition_key,
        source_run_id=context.run_id,
        source_document_ids=config.source_document_ids,
        max_documents=config.max_documents,
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="esef_document_artifacts_s3",
    deps=[dg.AssetKey("esef_document_extraction_manifest_s3")],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "xbrl", "ixbrl", "arelle"},
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=ESEF_ARELLE_POOL,
    description=(
        "Archives and parses unique ESEF packages in bounded worker processes, "
        "outside the single-writer DuckDB pool."
    ),
)
def esef_document_artifacts_s3(
    context: dg.AssetExecutionContext,
    config: EsefDocumentArtifactConfig,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    metadata = run_esef_document_artifacts_partition(
        object_store=object_store,
        client=EsefFilingsClient(),
        partition_key=context.partition_key,
        source_run_id=context.run_id,
        refresh_existing=config.refresh_existing,
        validate_esef=config.validate_esef,
        parse_workers=config.parse_workers,
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=metadata)


defs = dg.Definitions(
    assets=[
        esef_document_extraction_manifest_s3,
        esef_document_artifacts_s3,
    ]
)
