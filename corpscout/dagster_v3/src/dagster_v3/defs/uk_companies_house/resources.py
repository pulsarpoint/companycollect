import logging
import re
import tempfile
import time
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

import duckdb
import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.uk_companies_house import tables

LOGGER = logging.getLogger(__name__)

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
COMPANIES_RAW_TABLE = tables.COMPANIES_RAW_TABLE
COMPANIES_TABLE = tables.COMPANIES_TABLE
REGISTER_SOURCE_SLUG = "uk_companies_house_register"

DEFAULT_TIMEOUT_SECONDS = 600
DEFAULT_USER_AGENT = "corpscout-dagster-v3/0.1 (goran.raovic@gmail.com)"
DOWNLOAD_CHUNK_BYTES = 1 << 20
DOWNLOAD_MAX_ATTEMPTS = 5
DOWNLOAD_RETRY_BASE_SECONDS = 2.0

_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


class HttpSession(Protocol):
    def get(self, url: str, *, timeout: int, stream: bool = False) -> Any: ...


def resolve_basic_company_data_url(
    *,
    session: HttpSession | None = None,
    index_url: str = tables.DOWNLOAD_INDEX_URL,
    base_url: str = tables.DOWNLOAD_BASE_URL,
    timeout_seconds: int = 60,
) -> str:
    """Resolve the current BasicCompanyDataAsOneFile zip URL from the index page."""
    http_session = session or dlt_requests.Session()
    response = http_session.get(index_url, timeout=timeout_seconds)
    response.raise_for_status()
    matches = re.findall(tables.BASIC_DATA_FILENAME_RE, response.text)
    if not matches:
        raise LookupError(
            f"could not find BasicCompanyDataAsOneFile zip on {index_url}"
        )
    # Filenames are date-sortable; take the latest.
    return base_url + sorted(set(matches))[-1]


def _stream_download(
    *, url: str, dest: Path, timeout_seconds: int, session: HttpSession | None
) -> None:
    http_session = session or dlt_requests.Session()
    response = http_session.get(url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    iter_content = getattr(response, "iter_content", None)
    written = 0
    with dest.open("wb") as out:
        if callable(iter_content):
            for chunk in iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if chunk:
                    out.write(chunk)
                    written += len(chunk)
        else:
            body = response.content
            out.write(body)
            written = len(body)
    headers = getattr(response, "headers", None)
    expected = headers.get("Content-Length") if hasattr(headers, "get") else None
    if expected is not None and str(expected).isdigit() and written < int(expected):
        raise requests.exceptions.ChunkedEncodingError(
            f"incomplete download: {written}/{expected} bytes from {url}"
        )


def _download_to_path(
    *,
    url: str,
    dest: Path,
    timeout_seconds: int,
    session: HttpSession | None,
    log: Callable[..., None] | None = None,
    max_attempts: int = DOWNLOAD_MAX_ATTEMPTS,
    retry_base_seconds: float = DOWNLOAD_RETRY_BASE_SECONDS,
) -> None:
    progress_log = log or LOGGER.info
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            _stream_download(
                url=url, dest=dest, timeout_seconds=timeout_seconds, session=session
            )
            return
        except _DOWNLOAD_RETRYABLE_ERRORS as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            wait_seconds = retry_base_seconds * attempt
            progress_log(
                "UK Companies House download failed (attempt %s/%s), retrying in %ss: "
                "url=%s error=%s",
                attempt,
                max_attempts,
                wait_seconds,
                url,
                exc,
            )
            time.sleep(wait_seconds)
    assert last_error is not None
    raise last_error


def _extract_single_csv(zip_path: Path, dest_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as archive:
        members = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if not members:
            raise ValueError(f"no CSV member found in {zip_path}")
        archive.extract(members[0], dest_dir)
        return dest_dir / members[0]


def load_uk_companies_house_raw(
    *,
    connection: duckdb.DuckDBPyConnection,
    download_url: str,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    log: Callable[..., object] | None = None,
) -> int:
    """Download the basic company data zip and load the CSV into a DuckDB raw table.

    normalize_names=true tidies the leading-space/dotted headers
    (` CompanyNumber`, `RegAddress.PostTown`, `SICCode.SicText_1`).
    """
    with tempfile.TemporaryDirectory(prefix="uk_companies_house_") as tmpdir:
        tmp = Path(tmpdir)
        zip_path = tmp / "basic_company_data.zip"
        _download_to_path(
            url=download_url,
            dest=zip_path,
            timeout_seconds=timeout_seconds,
            session=session,
            log=log if callable(log) else None,
        )
        csv_path = _extract_single_csv(zip_path, tmp)
        connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
        connection.execute(
            f"create or replace table {DLT_DATASET_NAME}.{COMPANIES_RAW_TABLE} as "
            "select * from read_csv(?, header=true, all_varchar=true, "
            "normalize_names=true, quote='\"', escape='\"')",
            [str(csv_path)],
        )
        count = int(
            connection.execute(
                f"select count(*) from {DLT_DATASET_NAME}.{COMPANIES_RAW_TABLE}"
            ).fetchone()[0]
        )
    if count == 0:
        raise ValueError(
            "UK Companies House basic data produced no rows; refusing to replace the table"
        )
    if log is not None:
        log("Loaded UK Companies House raw: rows=%s", count)
    return count


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_uk_companies_house_companies(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    source_url: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Normalize the raw basic-company-data table into uk_companies_house.companies."""
    raw = f"{DLT_DATASET_NAME}.{COMPANIES_RAW_TABLE}"
    qualified = f"{DLT_DATASET_NAME}.{COMPANIES_TABLE}"
    sql = f"""
        create or replace table {qualified} as
        select
            'GB' as country_iso2,
            '{REGISTER_SOURCE_SLUG}' as source_slug,
            {_sql_literal(source_run_id)} as source_run_id,
            companynumber as source_record_id,
            '' as source_payload_hash,
            companynumber as company_number,
            coalesce(trim(companyname), '') as name,
            coalesce(trim(companycategory), '') as company_category,
            coalesce(trim(companystatus), '') as company_status,
            coalesce(trim(companystatus) = 'Active', false) as is_active,
            try_strptime(incorporationdate, '%d/%m/%Y')::date as incorporation_date,
            try_strptime(dissolutiondate, '%d/%m/%Y')::date as dissolution_date,
            coalesce(trim(regaddressaddressline1), '') as address,
            coalesce(trim(regaddressaddressline2), '') as address_line_2,
            coalesce(trim(regaddresspostcode), '') as postal_code,
            coalesce(trim(regaddressposttown), '') as city,
            coalesce(trim(regaddresscounty), '') as county,
            coalesce(trim(regaddresscountry), '') as country,
            coalesce(trim(countryoforigin), '') as country_of_origin,
            {_sql_literal(source_url)} as source_url,
            '' as raw_entity
        from {raw}
        where companynumber is not null and trim(companynumber) <> ''
    """
    connection.execute(sql)
    rows = int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])
    active = int(
        connection.execute(
            f"select count(*) from {qualified} where is_active"
        ).fetchone()[0]
    )
    if rows == 0:
        raise ValueError(
            "UK Companies House produced no companies; refusing to replace the table"
        )
    if log is not None:
        log("Built UK Companies House companies: rows=%s active=%s", rows, active)
    return {"companies": rows, "active": active}
