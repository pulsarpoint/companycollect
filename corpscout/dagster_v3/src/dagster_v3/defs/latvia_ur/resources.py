from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Protocol

import dlt
import requests
from dlt.extract.resource import DltResource

from dagster_v3.defs.latvia_ur import tables

COUNTRY = "LV"
SOURCE_SLUG = "latvia_ur_register"
DLT_DATASET_NAME = tables.DLT_DATASET_NAME
ENTITIES_TABLE = tables.ENTITIES_TABLE
REGISTER_DOWNLOAD_URL = (
    "https://data.gov.lv/dati/dataset/4de9697f-850b-45ec-8bba-61fa09ce932f/"
    "resource/25e80bf3-f107-4ab4-89ef-251b5b9374e9/download/register.csv"
)
DEFAULT_TIMEOUT_SECONDS = 300
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
PROGRESS_LOG_EVERY_ROWS = 50000
CSV_DELIMITER = ";"
LOGGER = logging.getLogger(__name__)

# Common Latvian legal forms (code -> English description). Unknown -> "".
LATVIA_LEGAL_FORM_DESCRIPTION_EN_BY_CODE = {
    "SIA": "Private limited company",
    "AS": "Public limited company",
    "IK": "Individual merchant (sole trader)",
    "IU": "Individual undertaking",
    "ZS": "Farm holding",
    "ZemnSaimn": "Farm holding",
    "KS": "Limited partnership",
    "PS": "General partnership",
    "BO": "Association or foundation",
    "NO": "Association or foundation",
    "VAS": "State joint-stock company",
    "PAS": "Municipal joint-stock company",
}


class HttpSession(Protocol):
    def get(self, url: str, *, timeout: int, stream: bool = False) -> Any: ...


@dlt.source(name="latvia_ur_register")
def latvia_ur_source(
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
    primary_key="regcode",
    columns=tables.copy_dlt_columns(tables.LATVIA_UR_ENTITIES_COLUMNS),
)
def _entities_resource(
    *,
    download_url: str,
    timeout_seconds: int,
    user_agent: str,
    run_id: str,
    session: HttpSession | None,
) -> Iterator[dict[str, Any]]:
    with tempfile.TemporaryDirectory(prefix="latvia_ur_") as tmpdir:
        csv_path = Path(tmpdir) / "register.csv"
        _download_to_path(
            url=download_url,
            dest=csv_path,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            session=session,
        )
        seen = False
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            for row in iter_latvia_ur_entity_rows(handle, run_id=run_id):
                seen = True
                yield row
        if not seen:
            raise ValueError(
                "Latvia UR register.csv produced no entity rows; refusing to replace the table"
            )


def iter_latvia_ur_entity_rows_from_text(
    csv_text: str,
    *,
    run_id: str = "",
) -> Iterator[dict[str, Any]]:
    yield from iter_latvia_ur_entity_rows(io.StringIO(csv_text), run_id=run_id)


def iter_latvia_ur_entity_rows(
    handle: Any,
    *,
    run_id: str = "",
    log: Callable[..., None] | None = None,
) -> Iterator[dict[str, Any]]:
    progress_log = log or LOGGER.info
    # restkey/restval keep over-/under-long malformed rows parseable instead of
    # crashing the load: extra fields land under the string key "_extra" (so JSON
    # key-sorting stays valid) and missing fields default to "".
    reader = csv.DictReader(
        handle, delimiter=CSV_DELIMITER, restkey="_extra", restval=""
    )
    emitted = 0
    for source_row in reader:
        regcode = _clean(source_row.get("regcode"))
        if regcode == "":
            continue
        emitted += 1
        if PROGRESS_LOG_EVERY_ROWS > 0 and emitted % PROGRESS_LOG_EVERY_ROWS == 0:
            progress_log("Processed Latvia UR register rows: rows=%s", emitted)
        yield _entity_row(source_row, line_number=emitted, run_id=run_id)


def _entity_row(
    source_row: dict[str, Any],
    *,
    line_number: int,
    run_id: str,
) -> dict[str, Any]:
    regcode = _clean(source_row.get("regcode"))
    terminated = _clean(source_row.get("terminated"))
    closed = _clean(source_row.get("closed"))
    legal_form_code = _clean(source_row.get("type"))
    status = _status(terminated=terminated, closed=closed)
    return {
        "country_iso2": COUNTRY,
        "source_slug": SOURCE_SLUG,
        "source_run_id": run_id,
        "source_line_number": line_number,
        "source_record_id": regcode,
        "source_payload_hash": _payload_hash(source_row),
        "regcode": regcode,
        "vat_id": f"LV{regcode}" if regcode else "",
        "sepa": _clean(source_row.get("sepa")),
        "legal_name": _clean(source_row.get("name")),
        "name_in_quotes": _clean(source_row.get("name_in_quotes")),
        "legal_form_code": legal_form_code,
        "legal_form_text": _clean(source_row.get("type_text")),
        "legal_form_description_en": LATVIA_LEGAL_FORM_DESCRIPTION_EN_BY_CODE.get(
            legal_form_code, ""
        ),
        "regtype_code": _clean(source_row.get("regtype")),
        "regtype_text": _clean(source_row.get("regtype_text")),
        "registered_date": _clean(source_row.get("registered")),
        "terminated_date": terminated,
        "closed_flag": closed,
        "status": status,
        "is_active": status == "active",
        "address": _clean(source_row.get("address")),
        "postal_code": _clean(source_row.get("index")),
        "address_id": _clean(source_row.get("addressid")),
        "region_code": _clean(source_row.get("region")),
        "city_code": _clean(source_row.get("city")),
        "atvk_code": _clean(source_row.get("atvk")),
        "reregistration_term": _clean(source_row.get("reregistration_term")),
        "source_url": REGISTER_DOWNLOAD_URL,
        "raw_entity": _json_dumps(source_row),
    }


def _status(*, terminated: str, closed: str) -> str:
    if terminated:
        return "terminated"
    if closed:
        return "closed"
    return "active"


def _download_to_path(
    *,
    url: str,
    dest: Path,
    timeout_seconds: int,
    user_agent: str,
    session: HttpSession | None,
    log: Callable[..., None] | None = None,
) -> None:
    http_session = session or requests.Session()
    response = http_session.get(url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    iter_content = getattr(response, "iter_content", None)
    with dest.open("wb") as out:
        if callable(iter_content):
            for chunk in iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if chunk:
                    out.write(chunk)
        else:
            out.write(response.content)


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
