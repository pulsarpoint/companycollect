from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import polars as pl

from dagster_v3.defs.norway_brreg_financial import financial_fetches
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    NorwayBrregFinancialParquetStorageResource,
    financial_response_checkpoint_object_key,
    financial_response_object_key,
    financial_response_success_object_key,
)

RESPONSE_DOWNLOAD_BATCH_SIZE = 250
RESPONSE_DOWNLOAD_WORKERS = 8
RESPONSE_VERIFY_WORKERS = 32
RESPONSE_MANIFEST_SCHEMA_VERSION = 1


def materialize_response_json_partition(
    *,
    candidates: Sequence[Mapping[str, Any]],
    partition_prefix: str,
    source_run_id: str,
    storage: NorwayBrregFinancialParquetStorageResource,
    log: Callable[..., None] | None = None,
    client_factory: Callable[[], Any] = financial_fetches.build_financial_fetch_http_client,
    downloader: Callable[..., list[dict[str, Any]]] = (
        financial_fetches.download_financial_responses_for_orgs
    ),
    batch_size: int = RESPONSE_DOWNLOAD_BATCH_SIZE,
    max_workers: int = RESPONSE_DOWNLOAD_WORKERS,
) -> dict[str, Any]:
    """Download only unresolved BRREG responses and checkpoint each completed batch."""
    if batch_size < 1:
        raise ValueError("Norway response download batch_size must be greater than zero")
    if max_workers < 1:
        raise ValueError("Norway response max_workers must be greater than zero")

    success_key = financial_response_success_object_key(partition_prefix)
    if storage.response_exists(success_key):
        _frame, completed = verified_response_index_frame(
            partition_prefix=partition_prefix,
            storage=storage,
        )
        metadata = {
            "candidate_count": completed["candidate_count"],
            "reused_count": completed["row_count"],
            "downloaded_count": 0,
            "status_counts": completed["status_counts"],
            "partition_prefix": partition_prefix,
            "success_manifest_key": success_key,
        }
        _log(
            log,
            "Verified completed immutable Norway BRREG response JSON partition: %s",
            metadata,
        )
        return metadata

    requested_candidates = _unique_candidates(candidates)
    existing_records = storage.read_response_records(partition_prefix)
    normalized_candidates = _candidates_with_preserved_outcomes(
        requested_candidates,
        existing_records,
    )
    candidates_by_org = {
        _string(candidate.get("org_number")): candidate
        for candidate in normalized_candidates
    }
    latest_by_org = {
        _string(record.get("org_number")): record
        for record in financial_fetches.latest_response_records(existing_records)
    }

    def classify_candidate(
        candidate: Mapping[str, Any],
    ) -> tuple[str, Mapping[str, Any] | dict[str, Any]]:
        org_number = _string(candidate.get("org_number"))
        response_key = financial_response_object_key(partition_prefix, org_number)
        current = latest_by_org.get(org_number)
        if current is not None and _string(current.get("fetch_status")) == (
            financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS
        ):
            _verify_success_record(storage, current)
            return "reused", candidate
        if current is not None and _is_terminal_record(current):
            return "reused", candidate
        if storage.response_exists(response_key):
            return (
                "recovered",
                _recovered_success_record(
                    candidate=candidate,
                    response_key=response_key,
                    response_body=storage.read_response(response_key),
                    source_run_id=source_run_id,
                ),
            )
        return "pending", candidate

    with ThreadPoolExecutor(max_workers=RESPONSE_VERIFY_WORKERS) as executor:
        classifications = list(executor.map(classify_candidate, normalized_candidates))
    recovered_records = [
        dict(value) for state, value in classifications if state == "recovered"
    ]
    pending = [value for state, value in classifications if state == "pending"]
    reused_count = sum(
        1 for state, _value in classifications if state in {"reused", "recovered"}
    )

    checkpoint_index = _next_checkpoint_index(storage, partition_prefix, source_run_id)
    if recovered_records:
        _write_checkpoint(
            storage=storage,
            partition_prefix=partition_prefix,
            source_run_id=source_run_id,
            batch_index=checkpoint_index,
            records=recovered_records,
            checkpoint_kind="recovered_existing_json",
        )
        checkpoint_index += 1

    _log(
        log,
        "Starting Norway BRREG response JSON partition: candidates=%d reused=%d "
        "pending=%d batch_size=%d workers=%d prefix=%s",
        len(normalized_candidates),
        reused_count,
        len(pending),
        batch_size,
        max_workers,
        partition_prefix,
    )
    client = client_factory() if pending else None
    downloaded_count = 0
    for batch_start in range(0, len(pending), batch_size):
        batch_candidates = pending[batch_start : batch_start + batch_size]
        batch_records = downloader(
            orgs=batch_candidates,
            source_run_id=source_run_id,
            client=client,
            max_workers=max_workers,
            log=log,
        )
        if len(batch_records) != len(batch_candidates):
            raise RuntimeError(
                "Norway BRREG downloader returned an unexpected record count: "
                f"expected={len(batch_candidates)} actual={len(batch_records)}"
            )
        persisted_records = [
            _persist_download_result(
                storage=storage,
                partition_prefix=partition_prefix,
                record=record,
                prior_record=latest_by_org.get(_string(record.get("org_number"))),
            )
            for record in batch_records
        ]
        _write_checkpoint(
            storage=storage,
            partition_prefix=partition_prefix,
            source_run_id=source_run_id,
            batch_index=checkpoint_index,
            records=persisted_records,
            checkpoint_kind="http_download",
        )
        checkpoint_index += 1
        downloaded_count += len(persisted_records)
        _log(
            log,
            "Checkpointed Norway BRREG response JSON batch: completed=%d total=%d "
            "statuses=%s",
            downloaded_count,
            len(pending),
            financial_fetches.status_counts(persisted_records),
        )

    final_records = _records_for_candidates(
        storage.read_response_records(partition_prefix),
        candidates_by_org,
    )
    retryable_statuses = sorted(
        {
            _string(record.get("fetch_status"))
            for record in final_records
            if financial_fetches.financial_fetch_status_requires_failure(
                _string(record.get("fetch_status"))
            )
        }
    )
    final_counts = financial_fetches.status_counts(final_records)
    metadata = {
        "candidate_count": len(normalized_candidates),
        "reused_count": reused_count,
        "downloaded_count": downloaded_count,
        "status_counts": final_counts,
        "partition_prefix": partition_prefix,
    }
    if retryable_statuses:
        raise RuntimeError(
            "Norway BRREG response JSON partition contains retryable outcomes after "
            f"checkpointing: statuses={retryable_statuses} prefix={partition_prefix}"
        )

    storage.write_json_object(
        success_key,
        {
            "schema_version": RESPONSE_MANIFEST_SCHEMA_VERSION,
            "source_run_id": source_run_id,
            "partition_prefix": partition_prefix,
            "candidate_count": len(normalized_candidates),
            "candidate_org_numbers": sorted(candidates_by_org),
            "status_counts": final_counts,
            "completed_at": financial_fetches.utc_now_iso(),
        },
    )
    metadata["success_manifest_key"] = success_key
    _log(log, "Completed Norway BRREG response JSON partition: %s", metadata)
    return metadata


def verified_response_index_frame(
    *,
    partition_prefix: str,
    storage: NorwayBrregFinancialParquetStorageResource,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    success_key = financial_response_success_object_key(partition_prefix)
    if not storage.response_exists(success_key):
        raise RuntimeError(
            "Norway BRREG response JSON partition has no success manifest: "
            f"{success_key}"
        )
    success_manifest = storage.read_json_object(success_key)
    candidate_org_numbers = success_manifest.get("candidate_org_numbers")
    if not isinstance(candidate_org_numbers, list) or not all(
        isinstance(org_number, str) for org_number in candidate_org_numbers
    ):
        raise RuntimeError(
            "Norway BRREG response success manifest has invalid candidates: "
            f"{success_key}"
        )
    expected_orgs = set(candidate_org_numbers)
    latest = financial_fetches.latest_response_records(
        storage.read_response_records(partition_prefix)
    )
    records_by_org = {
        _string(record.get("org_number")): record
        for record in latest
        if _string(record.get("org_number")) in expected_orgs
    }
    missing_orgs = sorted(expected_orgs - records_by_org.keys())
    if missing_orgs:
        raise RuntimeError(
            "Norway BRREG response index is missing candidate outcomes: "
            f"missing_count={len(missing_orgs)} sample={missing_orgs[:10]}"
        )

    records = [records_by_org[org_number] for org_number in sorted(expected_orgs)]
    success_records: list[dict[str, Any]] = []
    for record in records:
        if financial_fetches.financial_fetch_status_requires_failure(
            _string(record.get("fetch_status"))
        ):
            raise RuntimeError(
                "Norway BRREG response index contains a retryable outcome: "
                f"org={record.get('org_number')} status={record.get('fetch_status')}"
            )
        if _string(record.get("fetch_status")) == (
            financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS
        ):
            success_records.append(record)
        elif _string(record.get("source_object_key")) or _string(
            record.get("source_payload_hash")
        ):
            raise RuntimeError(
                "Norway BRREG non-success response outcome must not reference a JSON "
                f"object: org={record.get('org_number')}"
            )
    with ThreadPoolExecutor(max_workers=RESPONSE_VERIFY_WORKERS) as executor:
        list(
            executor.map(
                lambda record: _verify_success_record(storage, record),
                success_records,
            )
        )

    frame = financial_fetches.financial_fetches_frame(records)
    if "raw_response" in frame.columns:
        raise RuntimeError("Norway BRREG response index must not contain raw_response")
    status_counts = financial_fetches.status_counts(records)
    expected_counts = success_manifest.get("status_counts")
    if expected_counts != status_counts:
        raise RuntimeError(
            "Norway BRREG response status counts do not match the success manifest: "
            f"expected={expected_counts} actual={status_counts}"
        )
    return frame, {
        "candidate_count": len(expected_orgs),
        "row_count": frame.height,
        "status_counts": status_counts,
        "success_manifest_key": success_key,
    }


def _unique_candidates(
    candidates: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    candidates_by_org: dict[str, Mapping[str, Any]] = {}
    for candidate in candidates:
        org_number = _string(candidate.get("org_number"))
        if org_number == "":
            raise RuntimeError("Norway BRREG financial candidate has no org_number")
        if org_number in candidates_by_org:
            raise RuntimeError(
                f"Duplicate Norway BRREG financial candidate org_number: {org_number}"
            )
        candidates_by_org[org_number] = candidate
    return [candidates_by_org[org] for org in sorted(candidates_by_org)]


def _candidates_with_preserved_outcomes(
    requested_candidates: Sequence[Mapping[str, Any]],
    existing_records: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    candidates_by_org = {
        _string(candidate.get("org_number")): candidate
        for candidate in requested_candidates
    }
    for record in financial_fetches.latest_response_records(existing_records):
        org_number = _string(record.get("org_number"))
        if org_number and _is_terminal_record(record):
            candidates_by_org.setdefault(org_number, record)
    return [candidates_by_org[org] for org in sorted(candidates_by_org)]


def _records_for_candidates(
    records: Sequence[Mapping[str, Any]],
    candidates_by_org: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    latest = financial_fetches.latest_response_records(records)
    latest_by_org = {
        _string(record.get("org_number")): record
        for record in latest
        if _string(record.get("org_number")) in candidates_by_org
    }
    missing = sorted(set(candidates_by_org) - latest_by_org.keys())
    if missing:
        raise RuntimeError(
            "Norway BRREG response partition has candidates without checkpointed "
            f"outcomes: missing_count={len(missing)} sample={missing[:10]}"
        )
    return [latest_by_org[org] for org in sorted(candidates_by_org)]


def _recovered_success_record(
    *,
    candidate: Mapping[str, Any],
    response_key: str,
    response_body: bytes,
    source_run_id: str,
) -> dict[str, Any]:
    _validate_response_json(response_body, response_key)
    return financial_fetches.response_record(
        org=candidate,
        source_url=(
            f"{financial_fetches.BRREG_REGNSKAP_BASE_URL}/"
            f"{_string(candidate.get('org_number'))}"
        ),
        source_run_id=source_run_id,
        source_line_number=1,
        fetch_status=financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS,
        http_status=200,
        error_type="",
        error_message="",
        attempt_count=1,
        fetched_at=financial_fetches.utc_now_iso(),
        source_object_key=response_key,
        source_payload_hash=financial_fetches.sha256_hex(response_body),
        capture_method="recovered_existing_json",
        original_http_bytes_preserved=False,
    )


def _persist_download_result(
    *,
    storage: NorwayBrregFinancialParquetStorageResource,
    partition_prefix: str,
    record: Mapping[str, Any],
    prior_record: Mapping[str, Any] | None,
) -> dict[str, Any]:
    persisted = dict(record)
    response_body = persisted.pop("_response_body", None)
    prior_attempt_count = int((prior_record or {}).get("attempt_count") or 0)
    persisted["attempt_count"] = prior_attempt_count + int(
        persisted.get("attempt_count") or 1
    )
    if _string(persisted.get("fetch_status")) == (
        financial_fetches.FINANCIAL_FETCH_STATUS_SUCCESS
    ):
        if not isinstance(response_body, bytes):
            raise RuntimeError(
                "Successful Norway BRREG financial download has no response bytes: "
                f"org={persisted.get('org_number')}"
            )
        response_key = financial_response_object_key(
            partition_prefix,
            _string(persisted.get("org_number")),
        )
        _validate_response_json(response_body, response_key)
        storage.write_response(response_key, response_body)
        persisted["source_object_key"] = response_key
        persisted["source_payload_hash"] = financial_fetches.sha256_hex(response_body)
        persisted["capture_method"] = "http_download"
        persisted["original_http_bytes_preserved"] = True
    else:
        persisted["source_object_key"] = None
        persisted["source_payload_hash"] = None
        persisted["original_http_bytes_preserved"] = False
    return persisted


def _write_checkpoint(
    *,
    storage: NorwayBrregFinancialParquetStorageResource,
    partition_prefix: str,
    source_run_id: str,
    batch_index: int,
    records: list[dict[str, Any]],
    checkpoint_kind: str,
) -> str:
    return storage.write_json_object(
        financial_response_checkpoint_object_key(
            partition_prefix,
            source_run_id,
            batch_index,
        ),
        {
            "schema_version": RESPONSE_MANIFEST_SCHEMA_VERSION,
            "source_run_id": source_run_id,
            "checkpoint_kind": checkpoint_kind,
            "batch_index": batch_index,
            "record_count": len(records),
            "status_counts": financial_fetches.status_counts(records),
            "created_at": financial_fetches.utc_now_iso(),
            "records": records,
        },
    )


def _next_checkpoint_index(
    storage: NorwayBrregFinancialParquetStorageResource,
    partition_prefix: str,
    source_run_id: str,
) -> int:
    run_component = f"/run={source_run_id}/"
    run_keys = [
        key
        for key in storage.list_response_checkpoint_keys(partition_prefix)
        if run_component in key
    ]
    return len(run_keys)


def _verify_success_record(
    storage: NorwayBrregFinancialParquetStorageResource,
    record: Mapping[str, Any],
) -> None:
    response_key = _string(record.get("source_object_key"))
    expected_hash = _string(record.get("source_payload_hash"))
    if response_key == "" or expected_hash == "":
        raise RuntimeError(
            "Successful Norway BRREG response record is missing its object key or "
            f"hash: org={record.get('org_number')}"
        )
    try:
        response_body = storage.read_response(response_key)
    except Exception as error:
        raise RuntimeError(
            f"Norway BRREG response JSON object is missing: {response_key}"
        ) from error
    actual_hash = financial_fetches.sha256_hex(response_body)
    if actual_hash != expected_hash:
        raise RuntimeError(
            "Norway BRREG response JSON hash mismatch: "
            f"key={response_key} expected={expected_hash} actual={actual_hash}"
        )
    _validate_response_json(response_body, response_key)


def _validate_response_json(response_body: bytes, response_key: str) -> None:
    try:
        payload = json.loads(response_body)
    except Exception as error:
        raise RuntimeError(
            f"Norway BRREG response object is invalid JSON: {response_key}"
        ) from error
    if not isinstance(payload, list) or not all(
        isinstance(record, dict) for record in payload
    ):
        raise RuntimeError(
            "Norway BRREG response object must contain a list of objects: "
            f"{response_key}"
        )


def _is_terminal_record(record: Mapping[str, Any]) -> bool:
    return not financial_fetches.financial_fetch_status_requires_failure(
        _string(record.get("fetch_status"))
    )


def _log(log: Callable[..., None] | None, message: str, *args: object) -> None:
    if log is not None:
        log(message, *args)


def _string(value: Any) -> str:
    return "" if value is None else str(value)
