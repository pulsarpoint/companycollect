from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

import polars as pl
from dlt.sources.helpers.requests import Client as DltRequestsClient

BRREG_REGNSKAP_BASE_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
FINANCIAL_FETCHED_AT_DTYPE = pl.Datetime(time_unit="ms", time_zone="UTC")

FINANCIAL_FETCH_STATUS_SUCCESS = "success"
FINANCIAL_FETCH_STATUS_NOT_FOUND = "not_found"
FINANCIAL_FETCH_STATUS_SERVER_ERROR = "server_error"
FINANCIAL_FETCH_STATUS_NETWORK_ERROR = "network_error"
FINANCIAL_FETCH_STATUS_INVALID_PAYLOAD = "invalid_payload"
FINANCIAL_FETCH_STATUS_UNSUPPORTED_LAYOUT = "unsupported_layout"
UNSUPPORTED_LAYOUT_RESPONSE_MARKER = "oppstillingsplan"

SOURCE_OUTCOME_FETCH_STATUSES = {
    FINANCIAL_FETCH_STATUS_SUCCESS,
    FINANCIAL_FETCH_STATUS_NOT_FOUND,
    FINANCIAL_FETCH_STATUS_UNSUPPORTED_LAYOUT,
    "gone",
    "empty",
}

FINANCIAL_RESPONSE_INDEX_COLUMNS: dict[str, dict[str, Any]] = {
    "country_iso2": {"data_type": "text"},
    "source_slug": {"data_type": "text"},
    "source_run_id": {"data_type": "text"},
    "source_line_number": {"data_type": "bigint"},
    "source_record_id": {"data_type": "text"},
    "source_payload_hash": {"data_type": "text"},
    "source_object_key": {"data_type": "text"},
    "capture_method": {"data_type": "text"},
    "original_http_bytes_preserved": {"data_type": "boolean"},
    "org_number": {"data_type": "text", "nullable": False},
    "legal_name": {"data_type": "text"},
    "website": {"data_type": "text"},
    "last_submitted_accounts_year": {"data_type": "text"},
    "source_url": {"data_type": "text"},
    "fetch_status": {"data_type": "text"},
    "http_status": {"data_type": "bigint"},
    "error_type": {"data_type": "text"},
    "error_message": {"data_type": "text"},
    "attempt_count": {"data_type": "bigint"},
    "fetched_at": {"data_type": "timestamp"},
}

# Compatibility names for callers that still describe this metadata as fetches.
BRREG_FINANCIAL_FETCHES_COLUMNS = FINANCIAL_RESPONSE_INDEX_COLUMNS


def polars_type_for_financial_fetch_column(
    column_schema: dict[str, Any],
) -> pl.DataType:
    data_type = column_schema["data_type"]
    if data_type == "text":
        return pl.Utf8
    if data_type == "bigint":
        return pl.Int64
    if data_type == "boolean":
        return pl.Boolean
    if data_type == "timestamp":
        return FINANCIAL_FETCHED_AT_DTYPE
    raise ValueError(f"Unsupported Norway Brreg financial fetch column type: {data_type}")


def financial_fetches_parquet_schema() -> dict[str, pl.DataType]:
    return {
        column_name: polars_type_for_financial_fetch_column(column_schema)
        for column_name, column_schema in FINANCIAL_RESPONSE_INDEX_COLUMNS.items()
    }


def financial_fetches_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    schema = financial_fetches_parquet_schema()
    if not rows:
        return pl.DataFrame(schema=schema)
    frame = pl.DataFrame(rows)
    return frame.select(
        [
            _financial_fetch_column_expression(frame, column_name, data_type)
            for column_name, data_type in schema.items()
        ]
    )


def _financial_fetch_column_expression(
    frame: pl.DataFrame,
    column_name: str,
    data_type: pl.DataType,
) -> pl.Expr:
    if column_name not in frame.columns:
        return pl.lit(None, dtype=data_type).alias(column_name)
    if data_type == FINANCIAL_FETCHED_AT_DTYPE and frame.schema[column_name] == pl.Utf8:
        return (
            pl.col(column_name)
            .str.to_datetime(time_unit="ms", time_zone="UTC", strict=False)
            .alias(column_name)
        )
    return pl.col(column_name).cast(data_type, strict=False).alias(column_name)


def build_financial_fetch_http_client(
    *,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    max_connections: int = 8,
) -> DltRequestsClient:
    return DltRequestsClient(
        request_timeout=timeout_seconds,
        max_connections=max_connections,
        raise_for_status=False,
        request_max_attempts=5,
        request_backoff_factor=2.0,
        request_max_retry_delay=120.0,
        respect_retry_after_header=True,
        session_attrs={"headers": {"User-Agent": user_agent}},
    )


def download_financial_responses_for_orgs(
    *,
    orgs: Iterable[Mapping[str, Any]],
    source_run_id: str,
    client: Any,
    max_workers: int,
    base_url: str = BRREG_REGNSKAP_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    fetched_at: str | None = None,
    log: Callable[..., None] | None = None,
) -> list[dict[str, Any]]:
    if max_workers < 1:
        raise ValueError("Norway Brreg financial max_workers must be greater than zero")

    candidates = list(orgs)
    fetch_timestamp = fetched_at or utc_now_iso()
    if log is not None:
        log(
            "Preparing Norway Brreg financial JSON downloads: candidates=%d workers=%d",
            len(candidates),
            max_workers,
        )

    indexed_candidates = list(enumerate(candidates, start=1))

    def download(indexed_candidate: tuple[int, Mapping[str, Any]]) -> dict[str, Any]:
        source_line_number, candidate = indexed_candidate
        org_number = _string(candidate.get("org_number"))
        return _download_brreg_financial_response(
            client=client,
            org=candidate,
            source_url=f"{base_url}/{org_number}",
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            timeout_seconds=timeout_seconds,
            fetched_at=fetch_timestamp,
        )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(download, indexed_candidates))

    if log is not None:
        log(
            "Completed Norway Brreg financial JSON downloads: downloaded=%d statuses=%s",
            len(results),
            status_counts(results),
        )
    return results


def response_record(
    *,
    org: Mapping[str, Any],
    source_url: str,
    source_run_id: str,
    source_line_number: int,
    fetch_status: str,
    http_status: int | None,
    error_type: str,
    error_message: str,
    attempt_count: int,
    fetched_at: str,
    source_object_key: str | None = None,
    source_payload_hash: str | None = None,
    capture_method: str = "http_download",
    original_http_bytes_preserved: bool = True,
) -> dict[str, Any]:
    return {
        "country_iso2": "NO",
        "source_slug": "norway_brregregnskap_fetch",
        "source_run_id": source_run_id,
        "source_line_number": source_line_number,
        "source_record_id": _string(org.get("org_number")),
        "source_payload_hash": source_payload_hash,
        "source_object_key": source_object_key,
        "capture_method": capture_method,
        "original_http_bytes_preserved": original_http_bytes_preserved,
        "org_number": _string(org.get("org_number")),
        "legal_name": _string(org.get("legal_name")),
        "website": _string(org.get("website")),
        "last_submitted_accounts_year": _string(
            org.get("last_submitted_accounts_year")
        ),
        "source_url": source_url,
        "fetch_status": fetch_status,
        "http_status": http_status,
        "error_type": error_type,
        "error_message": error_message,
        "attempt_count": attempt_count,
        "fetched_at": fetched_at,
    }


def financial_fetch_status_requires_failure(fetch_status: str) -> bool:
    return fetch_status not in SOURCE_OUTCOME_FETCH_STATUSES


def status_counts(rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        fetch_status = _string(row.get("fetch_status"))
        counts[fetch_status] = counts.get(fetch_status, 0) + 1
    return counts


def latest_response_records(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    latest_by_org: dict[str, dict[str, Any]] = {}
    for value in rows:
        row = dict(value)
        org_number = _string(row.get("org_number"))
        current = latest_by_org.get(org_number)
        if current is None or _response_record_order(row) >= _response_record_order(
            current
        ):
            latest_by_org[org_number] = row
    return [latest_by_org[org] for org in sorted(latest_by_org)]


def sha256_hex(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _download_brreg_financial_response(
    *,
    client: Any,
    org: Mapping[str, Any],
    source_url: str,
    source_run_id: str,
    source_line_number: int,
    timeout_seconds: int,
    fetched_at: str,
) -> dict[str, Any]:
    try:
        response = client.get(source_url, timeout=timeout_seconds)
    except Exception as error:
        record = response_record(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            fetch_status=FINANCIAL_FETCH_STATUS_NETWORK_ERROR,
            http_status=None,
            error_type=type(error).__name__,
            error_message=str(error),
            attempt_count=1,
            fetched_at=fetched_at,
        )
        record["_response_body"] = None
        return record

    response_body = _response_bytes(response)
    response_text = response_body.decode("utf-8", errors="replace")
    if response.status_code == 404:
        record = response_record(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            fetch_status=FINANCIAL_FETCH_STATUS_NOT_FOUND,
            http_status=404,
            error_type="HTTPStatusError",
            error_message="HTTP 404",
            attempt_count=1,
            fetched_at=fetched_at,
        )
        record["_response_body"] = None
        return record

    if response.status_code >= 400:
        unsupported = UNSUPPORTED_LAYOUT_RESPONSE_MARKER in response_text
        record = response_record(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            fetch_status=(
                FINANCIAL_FETCH_STATUS_UNSUPPORTED_LAYOUT
                if unsupported
                else FINANCIAL_FETCH_STATUS_SERVER_ERROR
            ),
            http_status=response.status_code,
            error_type=(
                "UnsupportedStatementLayout" if unsupported else "HTTPStatusError"
            ),
            error_message=(
                f"HTTP {response.status_code}: BRREG does not serve this statement layout"
                if unsupported
                else f"HTTP {response.status_code}"
            ),
            attempt_count=1,
            fetched_at=fetched_at,
        )
        record["response_excerpt"] = response_text[:4096]
        record["_response_body"] = None
        return record

    try:
        payload = json.loads(response_body)
    except Exception as error:
        record = response_record(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            fetch_status=FINANCIAL_FETCH_STATUS_INVALID_PAYLOAD,
            http_status=response.status_code,
            error_type=type(error).__name__,
            error_message=str(error),
            attempt_count=1,
            fetched_at=fetched_at,
        )
        record["response_excerpt"] = response_text[:4096]
        record["_response_body"] = None
        return record

    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        record = response_record(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            fetch_status=FINANCIAL_FETCH_STATUS_INVALID_PAYLOAD,
            http_status=response.status_code,
            error_type="InvalidPayload",
            error_message="Expected BRREG financial response payload to be a list of objects",
            attempt_count=1,
            fetched_at=fetched_at,
        )
        record["response_excerpt"] = response_text[:4096]
        record["_response_body"] = None
        return record

    record = response_record(
        org=org,
        source_url=source_url,
        source_run_id=source_run_id,
        source_line_number=source_line_number,
        fetch_status=FINANCIAL_FETCH_STATUS_SUCCESS,
        http_status=response.status_code,
        error_type="",
        error_message="",
        attempt_count=1,
        fetched_at=fetched_at,
        source_payload_hash=sha256_hex(response_body),
    )
    record["_response_body"] = response_body
    return record


def _response_bytes(response: Any) -> bytes:
    content = getattr(response, "content", None)
    if isinstance(content, bytes):
        return content
    text = getattr(response, "text", "")
    return _string(text).encode("utf-8")


def _response_record_order(row: Mapping[str, Any]) -> tuple[str, int, str]:
    return (
        _string(row.get("fetched_at")),
        int(row.get("attempt_count") or 0),
        _string(row.get("source_run_id")),
    )


def _string(value: Any) -> str:
    return "" if value is None else str(value)
