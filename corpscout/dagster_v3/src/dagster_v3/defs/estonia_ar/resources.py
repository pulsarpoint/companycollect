from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import tempfile
import time
import zipfile
from collections.abc import Callable, Iterator
from datetime import date, datetime
from pathlib import Path
from typing import Any, Protocol

import dlt
import requests
from dlt.extract.resource import DltResource
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.estonia_ar import tables

COUNTRY = "EE"
SOURCE_SLUG = "estonia_ar_register"
DLT_DATASET_NAME = tables.DLT_DATASET_NAME
ENTITIES_TABLE = tables.ENTITIES_TABLE
# Stable URL: refreshed daily, overwrites the full company snapshot (zipped CSV).
REGISTER_DOWNLOAD_URL = (
    "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/"
    "ettevotja_rekvisiidid__lihtandmed.csv.zip"
)
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
DOWNLOAD_MAX_ATTEMPTS = 4
DOWNLOAD_RETRY_BASE_SECONDS = 5
PROGRESS_LOG_EVERY_ROWS = 50000
CSV_DELIMITER = ";"
LOGGER = logging.getLogger(__name__)

# Estonian legal-form labels (`ettevotja_oiguslik_vorm`, the value IS the label) ->
# English. Verified against the live register vocabulary. Unknown -> "".
EE_LEGAL_FORM_EN_BY_NAME = {
    "Osaühing": "Private limited company",
    "Aktsiaselts": "Public limited company",
    "Täisühing": "General partnership",
    "Usaldusühing": "Limited partnership",
    "Tulundusühistu": "Commercial association (cooperative)",
    "Füüsilisest isikust ettevõtja": "Self-employed person (sole proprietor)",
    "Sihtasutus": "Foundation",
    "Mittetulundusühing": "Non-profit association",
    "Korteriühistu": "Apartment association",
    "Kohaliku omavalitsuse asutus": "Local government institution",
    "Välismaa äriühingu filiaal": "Branch of a foreign company",
    "Ametiühing": "Trade union",
    "Täidesaatva riigivõimu asutus või riigi muu institutsioon": "State executive authority or institution",
    "Maaparandusühistu": "Land improvement association",
    "Avalik-õiguslik juriidiline isik, põhiseaduslik institutsioon või nende asutus": "Public-law legal person",
    "Euroopa majandushuviühing": "European Economic Interest Grouping",
    "Euroopa äriühing (Societas Europaea)": "European company (SE)",
    "Erakond": "Political party",
    "Euroopa ühistu": "European cooperative society (SCE)",
}

# Legal-form subtype labels (`ettevotja_oigusliku_vormi_alaliik`) -> English.
# The field is rarely populated; map known values, default "".
EE_LEGAL_FORM_SUBTYPE_EN_BY_NAME: dict[str, str] = {}

# Status codes (`ettevotja_staatus`) -> English. Text label is in
# `ettevotja_staatus_tekstina`. Unknown -> "".
EE_STATUS_EN_BY_CODE = {
    "R": "Registered",
    "L": "In liquidation",
    "N": "Bankrupt",
    "K": "Deleted",
}


class HttpSession(Protocol):
    def get(self, url: str, *, timeout: int, stream: bool = False) -> Any: ...


@dlt.source(name="estonia_ar_register")
def estonia_ar_source(
    *,
    download_url: str = REGISTER_DOWNLOAD_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    run_id: str = "",
    session: HttpSession | None = None,
) -> DltResource:
    return _entities_resource(
        download_url=download_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        run_id=run_id,
        session=session,
    )


@dlt.resource(
    name=ENTITIES_TABLE,
    write_disposition="replace",
    primary_key="reg_code",
    columns=tables.copy_dlt_columns(tables.ESTONIA_AR_ENTITIES_COLUMNS),
)
def _entities_resource(
    *,
    download_url: str,
    timeout_seconds: int,
    user_agent: str,
    run_id: str,
    session: HttpSession | None,
) -> Iterator[dict[str, Any]]:
    with tempfile.TemporaryDirectory(prefix="estonia_ar_") as tmpdir:
        tmp = Path(tmpdir)
        zip_path = tmp / "register.csv.zip"
        _download_to_path(
            url=download_url,
            dest=zip_path,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            session=session,
        )
        csv_path = _extract_single_csv(zip_path, tmp)
        seen = False
        # utf-8-sig strips the register CSV's leading BOM so the first header is "nimi".
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in iter_estonia_ar_entity_rows(handle, run_id=run_id):
                seen = True
                yield row
        if not seen:
            raise ValueError(
                "Estonia AR register produced no entity rows; refusing to replace the table"
            )


def _extract_single_csv(zip_path: Path, dest_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as archive:
        members = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if not members:
            raise ValueError(f"no CSV member found in {zip_path}")
        member = members[0]
        archive.extract(member, dest_dir)
        return dest_dir / member


def iter_estonia_ar_entity_rows_from_text(
    csv_text: str,
    *,
    run_id: str = "",
) -> Iterator[dict[str, Any]]:
    yield from iter_estonia_ar_entity_rows(io.StringIO(csv_text), run_id=run_id)


def iter_estonia_ar_entity_rows(
    handle: Any,
    *,
    run_id: str = "",
    log: Callable[..., None] | None = None,
) -> Iterator[dict[str, Any]]:
    progress_log = log or LOGGER.info
    reader = csv.DictReader(
        handle, delimiter=CSV_DELIMITER, restkey="_extra", restval=""
    )
    emitted = 0
    for source_row in reader:
        reg_code = _clean(source_row.get("ariregistri_kood"))
        if reg_code == "":
            continue
        emitted += 1
        if PROGRESS_LOG_EVERY_ROWS > 0 and emitted % PROGRESS_LOG_EVERY_ROWS == 0:
            progress_log("Processed Estonia AR register rows: rows=%s", emitted)
        yield _entity_row(source_row, line_number=emitted, run_id=run_id)


def _entity_row(
    source_row: dict[str, Any],
    *,
    line_number: int,
    run_id: str,
) -> dict[str, Any]:
    reg_code = _clean(source_row.get("ariregistri_kood"))
    legal_form = _clean(source_row.get("ettevotja_oiguslik_vorm"))
    legal_form_subtype = _clean(source_row.get("ettevotja_oigusliku_vormi_alaliik"))
    status_code = _clean(source_row.get("ettevotja_staatus"))
    address = _clean(source_row.get("ads_normaliseeritud_taisaadress")) or _clean(
        source_row.get("ettevotja_aadress")
    )
    return {
        "country_iso2": COUNTRY,
        "source_slug": SOURCE_SLUG,
        "source_run_id": run_id,
        "source_line_number": line_number,
        "source_record_id": reg_code,
        "source_payload_hash": _payload_hash(source_row),
        "reg_code": reg_code,
        "name": _clean(source_row.get("nimi")),
        "vat_id": _clean(source_row.get("kmkr_nr")),
        "legal_form_original": legal_form,
        "legal_form_en": EE_LEGAL_FORM_EN_BY_NAME.get(legal_form, ""),
        "legal_form_subtype_original": legal_form_subtype,
        "legal_form_subtype_en": EE_LEGAL_FORM_SUBTYPE_EN_BY_NAME.get(
            legal_form_subtype, ""
        ),
        "status_code": status_code,
        "status_original": _clean(source_row.get("ettevotja_staatus_tekstina")),
        "status_en": EE_STATUS_EN_BY_CODE.get(status_code, ""),
        "is_active": status_code == "R",
        "first_entry_date": _parse_ee_date(source_row.get("ettevotja_esmakande_kpv")),
        "address": address,
        "ehak_code": _clean(source_row.get("asukoha_ehak_kood")),
        "location": _clean(source_row.get("asukoha_ehak_tekstina")),
        "postal_code": _clean(source_row.get("indeks_ettevotja_aadressis")),
        "address_id": _clean(source_row.get("ads_adr_id")),
        "company_url": _clean(source_row.get("teabesysteemi_link")),
        "source_url": REGISTER_DOWNLOAD_URL,
        "raw_entity": _json_dumps(source_row),
    }


def _parse_ee_date(value: Any) -> date | None:
    text = _clean(value)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%d.%m.%Y").date()
    except ValueError:
        return None


# Transient network failures these large downloads must survive (the remote drops
# the stream mid-transfer -> ChunkedEncodingError / IncompleteRead).
_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


def _download_to_path(
    *,
    url: str,
    dest: Path,
    timeout_seconds: int,
    user_agent: str,
    session: HttpSession | None,
    log: Callable[..., None] | None = None,
    max_attempts: int = DOWNLOAD_MAX_ATTEMPTS,
    retry_base_seconds: float = DOWNLOAD_RETRY_BASE_SECONDS,
) -> None:
    """Stream a URL to a file, retrying transient mid-stream failures.

    Each attempt re-truncates the destination, so a broken download never leaves
    a partial file behind. A short read vs the server's Content-Length is treated
    as a retryable failure.
    """
    progress_log = log or LOGGER.info
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            _stream_download(
                url=url,
                dest=dest,
                timeout_seconds=timeout_seconds,
                session=session,
            )
            return
        except _DOWNLOAD_RETRYABLE_ERRORS as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            wait_seconds = retry_base_seconds * attempt
            progress_log(
                "Estonia AR download failed (attempt %s/%s), retrying in %ss: "
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


def _stream_download(
    *,
    url: str,
    dest: Path,
    timeout_seconds: int,
    session: HttpSession | None,
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


def _payload_hash(source_row: dict[str, Any]) -> str:
    body = _json_dumps(source_row, sort_keys=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _json_dumps(payload: dict[str, Any], *, sort_keys: bool = False) -> str:
    return json.dumps(
        {key: _clean(value) for key, value in payload.items()},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=sort_keys,
    )


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
