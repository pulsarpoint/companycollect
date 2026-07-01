import hashlib
import time
from collections.abc import Callable
from typing import Any

import requests

from norway_financial_bootstrap.candidates import FinancialCandidate

BRREG_REGNSKAP_BASE_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
COUNTRY_ISO2 = "NO"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
SOURCE_SLUG = "norway_brregregnskap_fetch"
RETRY_DELAYS_SECONDS = (30.0, 60.0, 120.0, 240.0)


class BrregFinancialClient:
    def __init__(
        self,
        *,
        session: Any | None = None,
        sleep: Callable[[float], None] | None = None,
        base_url: str = BRREG_REGNSKAP_BASE_URL,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        self._session = session if session is not None else requests.Session()
        if session is None:
            self._session.headers.update({"User-Agent": user_agent})
        self._sleep = sleep if sleep is not None else time.sleep
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds

    def fetch_candidate(
        self,
        candidate: FinancialCandidate,
        *,
        source_run_id: str,
        source_line_number: int,
        fetched_at: str,
    ) -> dict[str, Any]:
        source_url = f"{self._base_url}/{candidate.org_number}"
        for retry_index in range(len(RETRY_DELAYS_SECONDS) + 1):
            attempt_count = retry_index + 1
            try:
                response = self._session.get(
                    source_url,
                    timeout=self._timeout_seconds,
                )
            except Exception as exc:
                if retry_index < len(RETRY_DELAYS_SECONDS):
                    self._sleep(RETRY_DELAYS_SECONDS[retry_index])
                    continue
                return _fetch_row(
                    candidate=candidate,
                    source_url=source_url,
                    source_run_id=source_run_id,
                    source_line_number=source_line_number,
                    fetch_status="network_error",
                    http_status=None,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    attempt_count=attempt_count,
                    fetched_at=fetched_at,
                    raw_response="",
                )

            status_code = response.status_code
            raw_response = _response_text(response)
            if status_code == 404:
                return _fetch_row(
                    candidate=candidate,
                    source_url=source_url,
                    source_run_id=source_run_id,
                    source_line_number=source_line_number,
                    fetch_status="not_found",
                    http_status=status_code,
                    error_type="HTTPStatusError",
                    error_message="HTTP 404",
                    attempt_count=attempt_count,
                    fetched_at=fetched_at,
                    raw_response=raw_response,
                )

            if _is_retryable_http_status(status_code) and retry_index < len(
                RETRY_DELAYS_SECONDS
            ):
                self._sleep(RETRY_DELAYS_SECONDS[retry_index])
                continue

            if status_code >= 400:
                return _fetch_row(
                    candidate=candidate,
                    source_url=source_url,
                    source_run_id=source_run_id,
                    source_line_number=source_line_number,
                    fetch_status="server_error",
                    http_status=status_code,
                    error_type="HTTPStatusError",
                    error_message=f"HTTP {status_code}",
                    attempt_count=attempt_count,
                    fetched_at=fetched_at,
                    raw_response=raw_response,
                )

            try:
                payload = response.json()
            except Exception as exc:
                return _fetch_row(
                    candidate=candidate,
                    source_url=source_url,
                    source_run_id=source_run_id,
                    source_line_number=source_line_number,
                    fetch_status="invalid_payload",
                    http_status=status_code,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    attempt_count=attempt_count,
                    fetched_at=fetched_at,
                    raw_response=raw_response,
                )

            if not isinstance(payload, list) or not all(
                isinstance(item, dict) for item in payload
            ):
                return _fetch_row(
                    candidate=candidate,
                    source_url=source_url,
                    source_run_id=source_run_id,
                    source_line_number=source_line_number,
                    fetch_status="invalid_payload",
                    http_status=status_code,
                    error_type="InvalidPayload",
                    error_message=(
                        "Expected BRREG financial response payload to be a list of objects"
                    ),
                    attempt_count=attempt_count,
                    fetched_at=fetched_at,
                    raw_response=raw_response,
                )

            return _fetch_row(
                candidate=candidate,
                source_url=source_url,
                source_run_id=source_run_id,
                source_line_number=source_line_number,
                fetch_status="success",
                http_status=status_code,
                error_type="",
                error_message="",
                attempt_count=attempt_count,
                fetched_at=fetched_at,
                raw_response=raw_response,
            )

        return _fetch_row(
            candidate=candidate,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            fetch_status="network_error",
            http_status=None,
            error_type="RetryStateError",
            error_message="BRREG financial fetch retry state exhausted",
            attempt_count=len(RETRY_DELAYS_SECONDS) + 1,
            fetched_at=fetched_at,
            raw_response="",
        )


def _fetch_row(
    *,
    candidate: FinancialCandidate,
    source_url: str,
    source_run_id: str,
    source_line_number: int,
    fetch_status: str,
    http_status: int | None,
    error_type: str,
    error_message: str,
    attempt_count: int,
    fetched_at: str,
    raw_response: str,
) -> dict[str, Any]:
    return {
        "country_iso2": COUNTRY_ISO2,
        "source_slug": SOURCE_SLUG,
        "source_run_id": source_run_id,
        "source_line_number": source_line_number,
        "source_record_id": candidate.org_number,
        "source_payload_hash": _payload_hash(raw_response),
        "org_number": candidate.org_number,
        "legal_name": candidate.legal_name,
        "website": candidate.website,
        "last_submitted_accounts_year": candidate.last_submitted_accounts_year,
        "source_url": source_url,
        "fetch_status": fetch_status,
        "http_status": http_status,
        "error_type": error_type,
        "error_message": error_message,
        "attempt_count": attempt_count,
        "fetched_at": fetched_at,
        "raw_response": raw_response,
    }


def _is_retryable_http_status(status_code: int) -> bool:
    return status_code == 429 or status_code >= 500


def _payload_hash(raw_response: str) -> str:
    return hashlib.sha256(raw_response.encode("utf-8")).hexdigest()


def _response_text(response: Any) -> str:
    value = getattr(response, "text", "")
    return "" if value is None else str(value)
