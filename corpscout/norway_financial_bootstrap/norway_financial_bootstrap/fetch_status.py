from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

BRREG_REGNSKAP_BASE_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
DEFAULT_TIMEOUT_SECONDS = 120

FINANCIAL_FETCH_STATUS_SUCCESS = "success"
FINANCIAL_FETCH_STATUS_NOT_FOUND = "not_found"
FINANCIAL_FETCH_STATUS_SERVER_ERROR = "server_error"
FINANCIAL_FETCH_STATUS_NETWORK_ERROR = "network_error"
FINANCIAL_FETCH_STATUS_INVALID_PAYLOAD = "invalid_payload"

SOURCE_OUTCOME_FETCH_STATUSES = {
    FINANCIAL_FETCH_STATUS_SUCCESS,
    FINANCIAL_FETCH_STATUS_NOT_FOUND,
    "gone",
    "empty",
}


def financial_fetch_success_row(
    *,
    org: Mapping[str, Any],
    source_url: str,
    payload: list[dict[str, Any]],
    source_run_id: str,
    source_line_number: int,
    status_code: int,
    fetched_at: str,
    attempt_count: int,
) -> dict[str, Any]:
    raw_response = _json_dumps(payload)
    return _base_financial_fetch_row(
        org=org,
        source_url=source_url,
        source_run_id=source_run_id,
        source_line_number=source_line_number,
        source_payload_hash=hashlib.sha256(raw_response.encode("utf-8")).hexdigest(),
        fetch_status=FINANCIAL_FETCH_STATUS_SUCCESS,
        http_status=status_code,
        error_type="",
        error_message="",
        attempt_count=attempt_count,
        fetched_at=fetched_at,
        raw_response=raw_response,
    )


def financial_fetch_failure_row(
    *,
    org: Mapping[str, Any],
    source_url: str,
    source_run_id: str,
    source_line_number: int,
    status_code: int | None,
    fetch_status: str,
    error_type: str,
    error_message: str,
    fetched_at: str,
    attempt_count: int,
    raw_response: str,
) -> dict[str, Any]:
    return _base_financial_fetch_row(
        org=org,
        source_url=source_url,
        source_run_id=source_run_id,
        source_line_number=source_line_number,
        source_payload_hash="0" * 64,
        fetch_status=fetch_status,
        http_status=status_code,
        error_type=error_type,
        error_message=error_message,
        attempt_count=attempt_count,
        fetched_at=fetched_at,
        raw_response=raw_response,
    )


def financial_fetch_status_requires_failure(fetch_status: str) -> bool:
    return fetch_status not in SOURCE_OUTCOME_FETCH_STATUSES


def _base_financial_fetch_row(
    *,
    org: Mapping[str, Any],
    source_url: str,
    source_run_id: str,
    source_line_number: int,
    source_payload_hash: str,
    fetch_status: str,
    http_status: int | None,
    error_type: str,
    error_message: str,
    attempt_count: int,
    fetched_at: str,
    raw_response: str,
) -> dict[str, Any]:
    return {
        "country_iso2": "NO",
        "source_slug": "norway_brregregnskap_fetch",
        "source_run_id": source_run_id,
        "source_line_number": source_line_number,
        "source_record_id": _string(org.get("org_number")),
        "source_payload_hash": source_payload_hash,
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
        "raw_response": raw_response,
    }


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _string(value: Any) -> str:
    if value is None:
        return ""
    return str(value)
