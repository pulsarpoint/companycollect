import logging
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

import duckdb
import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.czech_ares import tables

LOGGER = logging.getLogger(__name__)

DUCKDB_SCHEMA = tables.DUCKDB_SCHEMA
RES_RAW_TABLE = tables.RES_RAW_TABLE
COMPANIES_TABLE = tables.COMPANIES_TABLE
REGISTER_SOURCE_SLUG = "czech_ares_register"

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


# CZ legal-form code (FORMA) -> English. Common codes; unknown -> "" (extensible).
CZ_LEGAL_FORM_EN_BY_CODE = {
    "101": "Sole trader (trade licence)",
    "102": "Sole trader (other regulations)",
    "105": "Sole trader (farmer)",
    "107": "Sole trader (other)",
    "111": "General partnership",
    "112": "Limited liability company (s.r.o.)",
    "113": "Limited partnership",
    "115": "Limited liability company (s.r.o.)",
    "117": "European company",
    "121": "Joint-stock company (a.s.)",
    "141": "Mutual insurance company",
    "145": "Mutual fund",
    "151": "Other limited company",
    "205": "Cooperative",
    "301": "State enterprise",
    "325": "State organizational unit",
    "331": "State-funded organization (founded by region/municipality)",
    "332": "Contributory organization",
    "352": "Public research institution",
    "421": "Foreign legal person's branch",
    "521": "Foreign person's branch",
    "525": "Foreign legal person",
    "601": "University",
    "641": "Local government association",
    "701": "Association",
    "705": "Association",
    "706": "Trade union organization",
    "711": "Political party or movement",
    "721": "Church or religious society",
    "731": "Organizational unit of an association",
    "741": "Interest association of legal entities",
    "745": "Professional chamber",
    "751": "Self-governing professional organization",
    "761": "Hunting association",
    "771": "Owners' association of units",
    "801": "Municipality",
    "804": "Region",
    "805": "Voluntary municipality association",
    "921": "International organization",
}


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
                "Czech RES download failed (attempt %s/%s), retrying in %ss: url=%s error=%s",
                attempt, max_attempts, wait_seconds, url, exc,
            )
            time.sleep(wait_seconds)
    assert last_error is not None
    raise last_error


def load_czech_ares_res(
    *,
    connection: duckdb.DuckDBPyConnection,
    download_url: str = tables.RES_DATA_URL,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    log: Callable[..., object] | None = None,
) -> int:
    """Download res_data.csv and load it into a DuckDB raw table (multithreaded read_csv)."""
    with tempfile.TemporaryDirectory(prefix="czech_ares_") as tmpdir:
        csv_path = Path(tmpdir) / "res_data.csv"
        _download_to_path(
            url=download_url, dest=csv_path, timeout_seconds=timeout_seconds,
            session=session, log=log if callable(log) else None,
        )
        connection.execute(f"create schema if not exists {DUCKDB_SCHEMA}")
        connection.execute(
            f"create or replace table {DUCKDB_SCHEMA}.{RES_RAW_TABLE} as "
            "select * from read_csv(?, header=true, all_varchar=true, "
            "quote='\"', escape='\"')",
            [str(csv_path)],
        )
        count = int(
            connection.execute(
                f"select count(*) from {DUCKDB_SCHEMA}.{RES_RAW_TABLE}"
            ).fetchone()[0]
        )
    if count == 0:
        raise ValueError("Czech RES produced no rows; refusing to replace the table")
    if log is not None:
        log("Loaded Czech RES raw: rows=%s", count)
    return count


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_czech_ares_companies(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    source_url: str = tables.RES_DATA_URL,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Normalize the raw RES table into czech_ares.companies (with address)."""
    raw = f"{DUCKDB_SCHEMA}.{RES_RAW_TABLE}"
    qualified = f"{DUCKDB_SCHEMA}.{COMPANIES_TABLE}"
    legal_form_en = "case FORMA " + " ".join(
        f"when {_sql_literal(code)} then {_sql_literal(en)}"
        for code, en in CZ_LEGAL_FORM_EN_BY_CODE.items()
    ) + " else '' end"
    sql = f"""
        create or replace table {qualified} as
        select
            'CZ' as country_iso2,
            '{REGISTER_SOURCE_SLUG}' as source_slug,
            {_sql_literal(source_run_id)} as source_run_id,
            ICO as source_record_id,
            '' as source_payload_hash,
            ICO as ico,
            coalesce(trim(FIRMA), '') as name,
            coalesce(FORMA, '') as legal_form_code,
            {legal_form_en} as legal_form_en,
            (nullif(trim(DDATZAN), '') is null) as is_active,
            try_strptime(DDATVZN, '%Y-%m-%d')::date as established_date,
            try_strptime(DDATZAN, '%Y-%m-%d')::date as terminated_date,
            coalesce(nullif(trim(concat_ws(' ',
                nullif(trim(ULICE_TEXT), ''),
                nullif(concat_ws('/', nullif(trim(CDOM), ''), nullif(trim(COR), '')), ''))), ''), '') as address,
            coalesce(trim(PSC), '') as postal_code,
            coalesce(trim(OBEC_TEXT), '') as city,
            coalesce(trim(COBCE_TEXT), '') as city_part,
            coalesce(trim(ULICE_TEXT), '') as street,
            coalesce(trim(OKRESLAU), '') as district_code,
            coalesce(trim(KATPO), '') as size_category,
            coalesce(trim(CISS2010), '') as institutional_sector,
            {_sql_literal(source_url)} as source_url,
            '' as raw_entity
        from {raw}
        where ICO is not null and trim(ICO) <> ''
    """
    connection.execute(sql)
    rows = int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])
    active = int(
        connection.execute(f"select count(*) from {qualified} where is_active").fetchone()[0]
    )
    if rows == 0:
        raise ValueError("Czech RES produced no companies; refusing to replace the table")
    if log is not None:
        log("Built Czech ARES companies: rows=%s active=%s", rows, active)
    return {"companies": rows, "active": active}
