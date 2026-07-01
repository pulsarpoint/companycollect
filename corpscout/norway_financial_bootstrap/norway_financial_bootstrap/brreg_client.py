import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import requests

from norway_financial_bootstrap import fetch_status as financial_fetches
from norway_financial_bootstrap.candidates import FinancialCandidate

BRREG_REGNSKAP_BASE_URL = financial_fetches.BRREG_REGNSKAP_BASE_URL
DEFAULT_TIMEOUT_SECONDS = financial_fetches.DEFAULT_TIMEOUT_SECONDS
DEFAULT_USER_AGENT = "corpscout-norway-financial-bootstrap/0.1"
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
        fetched_at: str | None = None,
    ) -> dict[str, Any]:
        fetch_timestamp = fetched_at or _utc_now_iso()
        org = candidate.as_org_mapping()
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
                return financial_fetches.financial_fetch_failure_row(
                    org=org,
                    source_url=source_url,
                    source_run_id=source_run_id,
                    source_line_number=source_line_number,
                    status_code=None,
                    fetch_status=financial_fetches.FINANCIAL_FETCH_STATUS_NETWORK_ERROR,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    fetched_at=fetch_timestamp,
                    attempt_count=attempt_count,
                    raw_response="",
                )

            status_code = response.status_code
            raw_response = _response_text(response)
            if status_code == 404:
                return financial_fetches.financial_fetch_failure_row(
                    org=org,
                    source_url=source_url,
                    source_run_id=source_run_id,
                    source_line_number=source_line_number,
                    status_code=status_code,
                    fetch_status=financial_fetches.FINANCIAL_FETCH_STATUS_NOT_FOUND,
                    error_type="HTTPStatusError",
                    error_message="HTTP 404",
                    fetched_at=fetch_timestamp,
                    attempt_count=attempt_count,
                    raw_response=raw_response,
                )

            if _is_retryable_http_status(status_code) and retry_index < len(
                RETRY_DELAYS_SECONDS
            ):
                self._sleep(RETRY_DELAYS_SECONDS[retry_index])
                continue

            if status_code >= 400:
                return financial_fetches.financial_fetch_failure_row(
                    org=org,
                    source_url=source_url,
                    source_run_id=source_run_id,
                    source_line_number=source_line_number,
                    status_code=status_code,
                    fetch_status=financial_fetches.FINANCIAL_FETCH_STATUS_SERVER_ERROR,
                    error_type="HTTPStatusError",
                    error_message=f"HTTP {status_code}",
                    fetched_at=fetch_timestamp,
                    attempt_count=attempt_count,
                    raw_response=raw_response,
                )

            try:
                payload = response.json()
            except Exception as exc:
                return financial_fetches.financial_fetch_failure_row(
                    org=org,
                    source_url=source_url,
                    source_run_id=source_run_id,
                    source_line_number=source_line_number,
                    status_code=status_code,
                    fetch_status=financial_fetches.FINANCIAL_FETCH_STATUS_INVALID_PAYLOAD,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    fetched_at=fetch_timestamp,
                    attempt_count=attempt_count,
                    raw_response=raw_response,
                )

            if not isinstance(payload, list) or not all(
                isinstance(item, dict) for item in payload
            ):
                return financial_fetches.financial_fetch_failure_row(
                    org=org,
                    source_url=source_url,
                    source_run_id=source_run_id,
                    source_line_number=source_line_number,
                    status_code=status_code,
                    fetch_status=financial_fetches.FINANCIAL_FETCH_STATUS_INVALID_PAYLOAD,
                    error_type="InvalidPayload",
                    error_message=(
                        "Expected BRREG financial response payload to be a list of objects"
                    ),
                    fetched_at=fetch_timestamp,
                    attempt_count=attempt_count,
                    raw_response=raw_response,
                )

            return financial_fetches.financial_fetch_success_row(
                org=org,
                source_url=source_url,
                payload=payload,
                source_run_id=source_run_id,
                source_line_number=source_line_number,
                status_code=status_code,
                attempt_count=attempt_count,
                fetched_at=fetch_timestamp,
            )

        return financial_fetches.financial_fetch_failure_row(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=None,
            fetch_status=financial_fetches.FINANCIAL_FETCH_STATUS_NETWORK_ERROR,
            error_type="RetryStateError",
            error_message="BRREG financial fetch retry state exhausted",
            fetched_at=fetch_timestamp,
            attempt_count=len(RETRY_DELAYS_SECONDS) + 1,
            raw_response="",
        )


def _is_retryable_http_status(status_code: int) -> bool:
    return status_code == 429 or status_code >= 500


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _response_text(response: Any) -> str:
    value = getattr(response, "text", "")
    return "" if value is None else str(value)
