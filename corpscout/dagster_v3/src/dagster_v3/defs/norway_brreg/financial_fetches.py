from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import duckdb
import polars as pl
from dlt.sources.helpers.requests import Client as DltRequestsClient

BRREG_REGNSKAP_BASE_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
FINANCIAL_FETCHES_TABLE = "financial_fetches"
FINANCIAL_FETCH_PROGRESS_LOG_EVERY_ROWS = 100

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

BRREG_FINANCIAL_FETCHES_COLUMNS: dict[str, dict[str, Any]] = {
    "country_iso2": {"data_type": "text"},
    "source_slug": {"data_type": "text"},
    "source_run_id": {"data_type": "text"},
    "source_line_number": {"data_type": "bigint"},
    "source_record_id": {"data_type": "text"},
    "source_payload_hash": {"data_type": "text"},
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
    "raw_response": {"data_type": "text"},
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


def financial_fetch_candidates_from_no_companies(frame: pl.DataFrame) -> list[dict[str, Any]]:
    if frame.is_empty():
        return []
    return [
        {
            "org_number": str(row["org_number"]),
            "legal_name": str(row["name"] or ""),
            "website": str(row["primary_website_url"] or ""),
            "last_submitted_accounts_year": str(row["last_submitted_accounts_year"] or ""),
        }
        for row in frame.filter(
            pl.col("is_active") & pl.col("last_submitted_accounts_year").is_not_null()
        )
        .sort("org_number")
        .to_dicts()
    ]


def fetch_financial_rows_for_orgs(
    *,
    orgs: Iterable[Mapping[str, Any]],
    source_run_id: str,
    base_url: str = BRREG_REGNSKAP_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    fetched_at: str | None = None,
    client: Any | None = None,
) -> list[dict[str, Any]]:
    http_client = client or _default_financial_fetch_http_client(
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
    )
    fetch_timestamp = fetched_at or _utc_now_iso()
    rows: list[dict[str, Any]] = []
    for source_line_number, org in enumerate(orgs, start=1):
        org_number = _string(org.get("org_number"))
        rows.append(
            _fetch_brreg_financial_statement(
                client=http_client,
                org=org,
                source_url=f"{base_url}/{org_number}",
                source_run_id=source_run_id,
                source_line_number=source_line_number,
                timeout_seconds=timeout_seconds,
                fetched_at=fetch_timestamp,
            )
        )
    return rows


def run_brreg_financial_statement_fetches(
    *,
    duckdb_connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    base_url: str = BRREG_REGNSKAP_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    fetched_at: str | None = None,
    client: Any | None = None,
    log: Callable[..., None] | None = None,
    progress_every_rows: int = FINANCIAL_FETCH_PROGRESS_LOG_EVERY_ROWS,
    commit_every_rows: int = 25,
) -> dict[str, int]:
    http_client = client or _default_financial_fetch_http_client(
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
    )
    fetch_timestamp = fetched_at or _utc_now_iso()
    candidates = _financial_fetch_candidates(duckdb_connection)
    _ensure_financial_fetches_table(duckdb_connection)
    existing_org_numbers = _existing_financial_fetch_org_numbers(duckdb_connection)
    progress_log = log
    skipped_existing = 0
    fetched = 0
    status_counts: dict[str, int] = {}
    pending_rows: list[dict[str, Any]] = []

    if progress_log is not None:
        progress_log(
            "Prepared Norway Brreg financial fetch candidates: candidates=%s existing=%s",
            len(candidates),
            len(existing_org_numbers),
        )

    for source_line_number, org in enumerate(candidates, start=1):
        org_number = _string(org.get("org_number"))
        if org_number in existing_org_numbers:
            skipped_existing += 1
            continue

        source_url = f"{base_url}/{org_number}"
        row = _fetch_brreg_financial_statement(
            client=http_client,
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            timeout_seconds=timeout_seconds,
            fetched_at=fetch_timestamp,
        )
        pending_rows.append(row)
        fetched += 1
        fetch_status = _string(row.get("fetch_status"))
        status_counts[fetch_status] = status_counts.get(fetch_status, 0) + 1

        if len(pending_rows) >= commit_every_rows:
            _upsert_financial_fetch_rows(duckdb_connection, pending_rows)
            pending_rows = []

        if (
            progress_log is not None
            and progress_every_rows > 0
            and fetched % progress_every_rows == 0
        ):
            progress_log(
                "Fetched Norway Brreg financial statements: fetched=%s skipped_existing=%s "
                "total=%s statuses=%s",
                fetched,
                skipped_existing,
                len(candidates),
                status_counts,
            )

    if pending_rows:
        _upsert_financial_fetch_rows(duckdb_connection, pending_rows)

    final_rows = _financial_fetch_row_count(duckdb_connection)
    if progress_log is not None:
        progress_log(
            "Completed Norway Brreg financial statement fetches: fetched=%s "
            "skipped_existing=%s total=%s final_rows=%s statuses=%s",
            fetched,
            skipped_existing,
            len(candidates),
            final_rows,
            status_counts,
        )

    return {
        "total_candidates": len(candidates),
        "skipped_existing": skipped_existing,
        "fetched": fetched,
        "final_rows": final_rows,
        **{f"status_{status}": count for status, count in status_counts.items()},
    }


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
        "last_submitted_accounts_year": _string(org.get("last_submitted_accounts_year")),
        "source_url": source_url,
        "fetch_status": fetch_status,
        "http_status": http_status,
        "error_type": error_type,
        "error_message": error_message,
        "attempt_count": attempt_count,
        "fetched_at": fetched_at,
        "raw_response": raw_response,
    }


def _fetch_brreg_financial_statement(
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
    except Exception as exc:
        return financial_fetch_failure_row(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=None,
            fetch_status=FINANCIAL_FETCH_STATUS_NETWORK_ERROR,
            error_type=type(exc).__name__,
            error_message=str(exc),
            fetched_at=fetched_at,
            attempt_count=1,
            raw_response="",
        )

    if response.status_code == 404:
        return financial_fetch_failure_row(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=response.status_code,
            fetch_status=FINANCIAL_FETCH_STATUS_NOT_FOUND,
            error_type="HTTPStatusError",
            error_message="HTTP 404",
            fetched_at=fetched_at,
            attempt_count=1,
            raw_response=_response_text(response),
        )

    if response.status_code >= 400:
        return financial_fetch_failure_row(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=response.status_code,
            fetch_status=FINANCIAL_FETCH_STATUS_SERVER_ERROR,
            error_type="HTTPStatusError",
            error_message=f"HTTP {response.status_code}",
            fetched_at=fetched_at,
            attempt_count=1,
            raw_response=_response_text(response),
        )

    try:
        payload = response.json()
    except Exception as exc:
        return financial_fetch_failure_row(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=response.status_code,
            fetch_status=FINANCIAL_FETCH_STATUS_INVALID_PAYLOAD,
            error_type=type(exc).__name__,
            error_message=str(exc),
            fetched_at=fetched_at,
            attempt_count=1,
            raw_response=_response_text(response),
        )

    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        return financial_fetch_failure_row(
            org=org,
            source_url=source_url,
            source_run_id=source_run_id,
            source_line_number=source_line_number,
            status_code=response.status_code,
            fetch_status=FINANCIAL_FETCH_STATUS_INVALID_PAYLOAD,
            error_type="InvalidPayload",
            error_message="Expected BRREG financial response payload to be a list of objects",
            fetched_at=fetched_at,
            attempt_count=1,
            raw_response=_response_text(response),
        )

    return financial_fetch_success_row(
        org=org,
        source_url=source_url,
        payload=payload,
        source_run_id=source_run_id,
        source_line_number=source_line_number,
        status_code=response.status_code,
        fetched_at=fetched_at,
        attempt_count=1,
    )


def _financial_fetch_candidates(
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> list[dict[str, Any]]:
    rows = duckdb_connection.execute(
        """
        select org_number, legal_name, website, last_submitted_accounts_year
        from norway_brreg.entities
        where is_active = true
          and nullif(trim(website), '') is not null
          and nullif(trim(last_submitted_accounts_year), '') is not null
        order by org_number
        """
    ).fetchall()
    return [
        {
            "org_number": _string(org_number),
            "legal_name": _string(legal_name),
            "website": _string(website),
            "last_submitted_accounts_year": _string(last_submitted_accounts_year),
        }
        for org_number, legal_name, website, last_submitted_accounts_year in rows
    ]


def _ensure_financial_fetches_table(
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    column_defs = ", ".join(
        f"{column_name} {_duckdb_type_for_financial_fetch_column(column_schema)}"
        for column_name, column_schema in BRREG_FINANCIAL_FETCHES_COLUMNS.items()
    )
    duckdb_connection.execute("create schema if not exists norway_brreg")
    duckdb_connection.execute(
        f"create table if not exists norway_brreg.{FINANCIAL_FETCHES_TABLE} ({column_defs})"
    )


def _existing_financial_fetch_org_numbers(
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> set[str]:
    table_exists = duckdb_connection.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = 'norway_brreg'
          and table_name = ?
        """,
        [FINANCIAL_FETCHES_TABLE],
    ).fetchone()[0]
    if table_exists == 0:
        return set()
    rows = duckdb_connection.execute(
        f"select org_number from norway_brreg.{FINANCIAL_FETCHES_TABLE}"
    ).fetchall()
    return {_string(row[0]) for row in rows}


def _upsert_financial_fetch_rows(
    duckdb_connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    column_names = tuple(BRREG_FINANCIAL_FETCHES_COLUMNS)
    placeholders = ", ".join("?" for _ in column_names)
    org_numbers = [_string(row.get("org_number")) for row in rows]
    duckdb_connection.execute("begin transaction")
    try:
        duckdb_connection.executemany(
            f"delete from norway_brreg.{FINANCIAL_FETCHES_TABLE} where org_number = ?",
            [(org_number,) for org_number in org_numbers],
        )
        duckdb_connection.executemany(
            f"""
            insert into norway_brreg.{FINANCIAL_FETCHES_TABLE}
                ({", ".join(column_names)})
            values ({placeholders})
            """,
            [tuple(row.get(column_name) for column_name in column_names) for row in rows],
        )
    except BaseException:
        duckdb_connection.execute("rollback")
        raise
    else:
        duckdb_connection.execute("commit")


def _financial_fetch_row_count(duckdb_connection: duckdb.DuckDBPyConnection) -> int:
    return duckdb_connection.execute(
        f"select count(*) from norway_brreg.{FINANCIAL_FETCHES_TABLE}"
    ).fetchone()[0]


def _duckdb_type_for_financial_fetch_column(column_schema: dict[str, Any]) -> str:
    data_type = column_schema["data_type"]
    if data_type == "text":
        return "varchar"
    if data_type == "bigint":
        return "bigint"
    if data_type == "timestamp":
        return "timestamp"
    raise ValueError(f"Unsupported DuckDB column type: {data_type}")


def _default_financial_fetch_http_client(
    *,
    timeout_seconds: int,
    user_agent: str,
) -> DltRequestsClient:
    return DltRequestsClient(
        request_timeout=timeout_seconds,
        max_connections=8,
        raise_for_status=False,
        request_max_attempts=5,
        request_backoff_factor=2.0,
        request_max_retry_delay=120.0,
        respect_retry_after_header=True,
        session_attrs={"headers": {"User-Agent": user_agent}},
    )


def _response_text(response: Any) -> str:
    return _string(getattr(response, "text", ""))


def _json_dumps(payload: Any) -> str:
    return json.dumps(
        payload,
        default=_json_default,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _string(value: Any) -> str:
    return "" if value is None else str(value)
