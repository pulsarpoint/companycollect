import gzip
import json
import logging
import re
import tempfile
import time
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any, Protocol

import duckdb
import pyarrow as pa
import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.slovakia_rpo import tables

LOGGER = logging.getLogger(__name__)

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
RPO_RAW_TABLE = tables.RPO_RAW_TABLE
COMPANIES_TABLE = tables.COMPANIES_TABLE
RPO_RAW_COLUMNS = tables.RPO_RAW_COLUMNS
REGISTER_SOURCE_SLUG = "slovakia_rpo_register"

DEFAULT_TIMEOUT_SECONDS = 900
DOWNLOAD_CHUNK_BYTES = 1 << 20
DOWNLOAD_MAX_ATTEMPTS = 5
DOWNLOAD_RETRY_BASE_SECONDS = 2.0
DEFAULT_INSERT_BATCH_ROWS = 50_000

_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


class HttpSession(Protocol):
    def get(self, url: str, *, timeout: int, stream: bool = False) -> Any: ...


# SK legal-form code (codelist CL000056) -> English. EN labels verified against the
# Slovak `legalForms.value.value` text in the dump; the Slovak name is always kept
# verbatim in legal_form_original, so an unmapped code -> "" is harmless.
SK_LEGAL_FORM_EN_BY_CODE = {
    "001": "Self-employed natural person",
    "101": "Sole trader (not registered in commercial register)",
    "102": "Sole trader (registered in commercial register)",
    "103": "Self-farming farmer (not registered in commercial register)",
    "105": "Liberal profession (natural person under non-trade-licence law)",
    "111": "General partnership (v.o.s.)",
    "112": "Limited liability company (s.r.o.)",
    "113": "Limited partnership (k.s.)",
    "117": "Foundation",
    "119": "Non-profit organization",
    "120": "Non-profit organization providing public benefit services",
    "121": "Joint-stock company (a.s.)",
    "141": "Hunting organization",
    "205": "Cooperative (družstvo)",
    "271": "Owners' association of flats and non-residential premises",
    "272": "Land association",
    "273": "Association of land consolidation participants",
    "301": "State enterprise",
    "321": "Budgetary organization",
    "331": "Contributory organization",
    "701": "Association (union, society, club, etc.)",
    "721": "Church organization",
    "751": "Interest association of legal entities",
    "801": "Municipality / town",
    "995": "Unspecified legal form",
}

# Postgres COPY (text format) un-escaping. The dump's `data` column is jsonb output
# (single line); COPY then escapes backslashes/tabs/newlines. Reverse left-to-right.
_COPY_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f", "v": "\v"}
_COPY_ESCAPE_RE = re.compile(r"\\(.)")
_COPY_HEADER = "COPY rpo2.organizations "


def _unescape_copy(value: str) -> str:
    return _COPY_ESCAPE_RE.sub(lambda m: _COPY_ESCAPES.get(m.group(1), m.group(1)), value)


def _current_entry(items: Any) -> dict[str, Any] | None:
    """Pick the active time-versioned entry (no validTo); else the latest validFrom."""
    if not isinstance(items, list) or not items:
        return None
    entries = [item for item in items if isinstance(item, dict)]
    if not entries:
        return None
    active = [item for item in entries if not item.get("validTo")]
    pool = active or entries
    return max(pool, key=lambda item: item.get("validFrom") or "")


def extract_company(record: dict[str, Any]) -> dict[str, str] | None:
    """Flatten one RPO organization record to current-version scalar fields.

    Returns None when the record carries no IČO (cannot key a company).
    """
    ico_entry = _current_entry(record.get("identifiers"))
    ico = (ico_entry or {}).get("value")
    if not ico:
        return None

    name_entry = _current_entry(record.get("fullNames"))
    name = (name_entry or {}).get("value") or ""

    legal_form = (_current_entry(record.get("legalForms")) or {}).get("value") or {}

    address = _current_entry(record.get("addresses")) or {}
    municipality = address.get("municipality") or {}
    country = address.get("country") or {}
    postal_codes = address.get("postalCodes") or []
    postal = postal_codes[0] if postal_codes else ""

    main_activity = (record.get("statisticalCodes") or {}).get("mainActivity") or {}
    source_register = (record.get("sourceRegister") or {}).get("value") or {}

    return {
        "rpo_id": str(record.get("id") or ""),
        "ico": str(ico),
        "name": name,
        "legal_form_code": str(legal_form.get("code") or ""),
        "legal_form_original": legal_form.get("value") or "",
        "established": record.get("establishment") or "",
        "termination": record.get("termination") or "",
        "main_activity_code": str(main_activity.get("code") or ""),
        "main_activity_original": main_activity.get("value") or "",
        "address_street": address.get("street") or "",
        "address_building_number": str(address.get("buildingNumber") or ""),
        "address_reg_number": str(address.get("regNumber") or ""),
        "postal_code": str(postal or "").replace(" ", ""),
        "municipality_code": municipality.get("code") or "",
        "city": municipality.get("value") or "",
        "country_code": str(country.get("code") or ""),
        "source_register": source_register.get("value") or "",
    }


def iter_dump_records(lines: Iterable[Any]) -> Iterator[dict[str, Any]]:
    """Yield organization JSON objects from the decompressed RPO SQL dump.

    Streams the `COPY rpo2.organizations ... FROM stdin;` block, un-escapes the
    tab-separated `data` column, and parses it. Stops at the block terminator.
    """
    in_copy = False
    for raw in lines:
        line = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
        if not in_copy:
            if line.startswith(_COPY_HEADER):
                in_copy = True
            continue
        if line.startswith("\\."):
            break
        columns = line.rstrip("\n").split("\t")
        if len(columns) < 2:
            continue
        try:
            yield json.loads(_unescape_copy(columns[1]))
        except json.JSONDecodeError:
            continue


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
                "Slovak RPO download failed (attempt %s/%s), retrying in %ss: url=%s error=%s",
                attempt, max_attempts, wait_seconds, url, exc,
            )
            time.sleep(wait_seconds)
    assert last_error is not None
    raise last_error


def _create_raw_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
    column_ddl = ", ".join(f'"{column}" varchar' for column in RPO_RAW_COLUMNS)
    connection.execute(
        f"create or replace table {DLT_DATASET_NAME}.{RPO_RAW_TABLE} ({column_ddl})"
    )


def load_records_into_duckdb(
    *,
    connection: duckdb.DuckDBPyConnection,
    records: Iterable[dict[str, Any]],
    batch_rows: int = DEFAULT_INSERT_BATCH_ROWS,
) -> int:
    """Extract and bulk-insert organization records into the DuckDB raw table."""
    if batch_rows < 1:
        raise ValueError("Slovak RPO insert batch size must be greater than zero")
    _create_raw_table(connection)
    batch: list[tuple[str, ...]] = []
    count = 0
    for record in records:
        row = extract_company(record)
        if row is None:
            continue
        batch.append(tuple(row[column] for column in RPO_RAW_COLUMNS))
        if len(batch) >= batch_rows:
            _insert_raw_batch(connection, batch)
            count += len(batch)
            batch.clear()
    if batch:
        _insert_raw_batch(connection, batch)
        count += len(batch)
    return count


def _insert_raw_batch(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[str, ...]],
) -> None:
    values_by_column = zip(*rows, strict=True)
    arrow_table = pa.Table.from_arrays(
        [pa.array(values, type=pa.string()) for values in values_by_column],
        names=RPO_RAW_COLUMNS,
    )
    registered_name = "_slovakia_rpo_raw_batch"
    column_list = ", ".join(f'"{column}"' for column in RPO_RAW_COLUMNS)
    connection.register(registered_name, arrow_table)
    try:
        connection.execute(
            f"insert into {DLT_DATASET_NAME}.{RPO_RAW_TABLE} ({column_list}) "
            f"select {column_list} from {registered_name}"
        )
    finally:
        connection.unregister(registered_name)


def load_slovakia_rpo_dump(
    *,
    connection: duckdb.DuckDBPyConnection,
    download_url: str = tables.RPO_DUMP_URL,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    batch_rows: int = DEFAULT_INSERT_BATCH_ROWS,
    log: Callable[..., object] | None = None,
) -> int:
    """Download rpo2.sql.gz, parse the organizations dump, load into DuckDB raw."""
    with tempfile.TemporaryDirectory(prefix="slovakia_rpo_") as tmpdir:
        gz_path = Path(tmpdir) / "rpo2.sql.gz"
        _download_to_path(
            url=download_url, dest=gz_path, timeout_seconds=timeout_seconds,
            session=session, log=log if callable(log) else None,
        )
        with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as handle:
            count = load_records_into_duckdb(
                connection=connection,
                records=iter_dump_records(handle),
                batch_rows=batch_rows,
            )
    if count == 0:
        raise ValueError("Slovak RPO dump produced no rows; refusing to replace the table")
    if log is not None:
        log("Loaded Slovak RPO raw: rows=%s", count)
    return count


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def build_slovakia_rpo_companies(
    *,
    connection: duckdb.DuckDBPyConnection,
    source_run_id: str,
    source_url: str = tables.RPO_DUMP_URL,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Normalize the raw RPO table into slovakia_rpo.companies (with address)."""
    raw = f"{DLT_DATASET_NAME}.{RPO_RAW_TABLE}"
    qualified = f"{DLT_DATASET_NAME}.{COMPANIES_TABLE}"
    legal_form_en = "case legal_form_code " + " ".join(
        f"when {_sql_literal(code)} then {_sql_literal(en)}"
        for code, en in SK_LEGAL_FORM_EN_BY_CODE.items()
    ) + " else '' end"
    sql = f"""
        create or replace table {qualified} as
        select
            'SK' as country_iso2,
            '{REGISTER_SOURCE_SLUG}' as source_slug,
            {_sql_literal(source_run_id)} as source_run_id,
            ico as source_record_id,
            '' as source_payload_hash,
            ico,
            coalesce(trim(name), '') as name,
            coalesce(legal_form_code, '') as legal_form_code,
            coalesce(trim(legal_form_original), '') as legal_form_original,
            {legal_form_en} as legal_form_en,
            (nullif(trim(termination), '') is null) as is_active,
            try_strptime(nullif(trim(established), ''), '%Y-%m-%d')::date as established_date,
            try_strptime(nullif(trim(termination), ''), '%Y-%m-%d')::date as terminated_date,
            coalesce(nullif(trim(concat_ws(' ',
                nullif(trim(address_street), ''),
                nullif(concat_ws('/',
                    nullif(trim(address_reg_number), ''),
                    nullif(trim(address_building_number), '')), ''))), ''), '') as address,
            coalesce(trim(postal_code), '') as postal_code,
            coalesce(trim(city), '') as city,
            coalesce(trim(address_street), '') as street,
            coalesce(trim(municipality_code), '') as municipality_code,
            coalesce(trim(country_code), '') as country_code,
            coalesce(trim(source_register), '') as source_register,
            {_sql_literal(source_url)} as source_url,
            '' as raw_entity
        from {raw}
        where ico is not null and trim(ico) <> ''
    """
    connection.execute(sql)
    rows = int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])
    active = int(
        connection.execute(f"select count(*) from {qualified} where is_active").fetchone()[0]
    )
    if rows == 0:
        raise ValueError("Slovak RPO produced no companies; refusing to replace the table")
    if log is not None:
        log("Built Slovak RPO companies: rows=%s active=%s", rows, active)
    return {"companies": rows, "active": active}
