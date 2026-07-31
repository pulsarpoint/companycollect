import logging
import tempfile
import time
from collections.abc import Callable
from pathlib import Path

import duckdb
import requests

from dagster_v3.defs.czech_ares import tables

LOGGER = logging.getLogger(__name__)

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
# CZ legal-form code (FORMA) -> English. Common codes; unknown -> "" (extensible).
# The curated English lives in czech_legal_forms.english, keyed against ARES's
# own Czech label. The map that used to sit here was displaced against the
# codes -- 706 "Spolek" (an association) read "Trade union organization" on
# 111,581 companies, and 145 (unit owners' associations) read "Mutual fund" on
# 80,414 -- so it is imported rather than kept in a second copy that can drift
# out of step with the nomenclature again.
from dagster_v3.defs.czech_legal_forms.english import (  # noqa: E402
    CZ_LEGAL_FORM_EN_BY_CODE,
)


def _stream_download(
    *, url: str, dest: Path, timeout_seconds: int, session: requests.Session | None
) -> None:
    http_session = session or requests.Session()
    if session is None:
        http_session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
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
    session: requests.Session | None,
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
    session: requests.Session | None = None,
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
        connection.execute("create schema if not exists czech_ares")
        connection.execute(
            "create or replace table czech_ares.res_raw as "
            "select * from read_csv(?, header=true, all_varchar=true, "
            "quote='\"', escape='\"')",
            [str(csv_path)],
        )
        count = int(
            connection.execute("select count(*) from czech_ares.res_raw").fetchone()[0]
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
    raw = "czech_ares.res_raw"
    qualified = "czech_ares.companies"
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
