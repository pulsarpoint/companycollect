from __future__ import annotations

import gzip
import hashlib
import json
import logging
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from decimal import Decimal
from io import BytesIO
from typing import Any, Protocol

import dagster as dg
import dlt
import ijson
import requests
from dlt.extract.source import DltSource
from pydantic import PrivateAttr

from dagster_v3.defs.norway_brreg import entity_records
from dagster_v3.defs.norway_brreg import tables

COUNTRY = "NO"
DLT_DATASET_NAME = "norway_brreg"
ENTITIES_TABLE = "entities"
ENTITY_SOURCE_SLUG = "norway_brregenhet"
BRREG_BASE_URL = "https://data.brreg.no/enhetsregisteret/api"
BRREG_REGNSKAP_BASE_URL = "https://data.brreg.no/regnskapsregisteret/regnskap"
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_USER_AGENT = "corpscout-dagster-v3-dev/0.1"
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
DOWNLOAD_PROGRESS_LOG_EVERY_BYTES = 100 * 1024 * 1024
ENTITY_PROGRESS_LOG_EVERY_ROWS = 1000
LOGGER = logging.getLogger(__name__)

BRREG_ENTITIES_COLUMNS = tables.BRREG_ENTITIES_COLUMNS
BRREG_FINANCIAL_STATEMENTS_COLUMNS = tables.BRREG_FINANCIAL_STATEMENTS_COLUMNS

BRREG_LEGAL_FORM_DESCRIPTION_EN_BY_CODE = {
    "ANS": "General partnership",
    "AS": "Private limited company",
    "ASA": "Public limited company",
    "DA": "Partnership with shared liability",
    "ENK": "Sole proprietorship",
    "FKF": "Municipal enterprise",
    "FORE": "Association",
    "KOMM": "Municipality",
    "NUF": "Norwegian branch of foreign company",
    "SA": "Cooperative",
    "STI": "Foundation",
}


class HttpSession(Protocol):
    headers: dict[str, str]

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        timeout: int,
        stream: bool = False,
    ) -> Any: ...


class NorwayBrregApiResource(dg.ConfigurableResource):
    base_url: str = BRREG_BASE_URL
    financial_base_url: str = BRREG_REGNSKAP_BASE_URL
    user_agent: str = DEFAULT_USER_AGENT
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    update_page_size: int = 10_000

    _session: HttpSession | None = PrivateAttr(default=None)

    def __init__(self, session: HttpSession | None = None, **data: Any) -> None:
        super().__init__(**data)
        self._session = session

    def session(self) -> HttpSession:
        if self._session is None:
            session = requests.Session()
            session.headers["User-Agent"] = self.user_agent
            self._session = session
        return self._session

    def download_entities_snapshot(
        self,
        *,
        log: Callable[..., None] | None = None,
        download_progress_every_bytes: int = DOWNLOAD_PROGRESS_LOG_EVERY_BYTES,
    ) -> bytes:
        return _download_bytes(
            url=f"{self.base_url}/enheter/lastned",
            timeout_seconds=self.timeout_seconds,
            user_agent=self.user_agent,
            session=self.session(),
            log=log,
            progress_every_bytes=download_progress_every_bytes,
        )

    def iter_all_entities(
        self,
        *,
        log: Callable[..., None] | None = None,
        progress_every_rows: int = ENTITY_PROGRESS_LOG_EVERY_ROWS,
        download_progress_every_bytes: int = DOWNLOAD_PROGRESS_LOG_EVERY_BYTES,
    ) -> Iterator[dict[str, Any]]:
        progress_log = log or LOGGER.info
        progress_log("Starting Norway Brreg entity snapshot download")
        response_body = self.download_entities_snapshot(
            log=progress_log,
            download_progress_every_bytes=download_progress_every_bytes,
        )
        progress_log(
            "Completed Norway Brreg entity snapshot download: bytes=%s",
            len(response_body),
        )
        row_count = 0
        for row_count, entity in enumerate(_stream_gzip_json_array(response_body), start=1):
            if progress_every_rows > 0 and row_count % progress_every_rows == 0:
                progress_log("Parsed Norway Brreg entity snapshot rows: rows=%s", row_count)
            yield entity_records.snapshot_entity_record(entity)
        progress_log("Completed Norway Brreg entity snapshot parse: rows=%s", row_count)

    def iter_updated_entities(
        self,
        *,
        start: str,
        end: str,
        include_changes: bool = False,
        log: Callable[..., None] | None = None,
        progress_every_rows: int = ENTITY_PROGRESS_LOG_EVERY_ROWS,
    ) -> Iterator[dict[str, Any]]:
        progress_log = log or LOGGER.info
        page_number = 0
        row_count = 0
        hydrated_row_count = 0
        progress_log(
            "Loading Norway Brreg entity updates: updated_at=%s..%s include_changes=%s",
            start,
            end,
            include_changes,
        )
        while True:
            params: dict[str, Any] = {
                "dato": start,
                "updatedBefore": end,
                "size": self.update_page_size,
                "page": page_number,
                "sort": "id,ASC",
            }
            if include_changes:
                params["includeChanges"] = "true"

            progress_log(
                "Requesting Norway Brreg entity updates page: page=%s size=%s",
                page_number,
                self.update_page_size,
            )
            payload = self._get_json(
                f"{self.base_url}/oppdateringer/enheter",
                params=params,
            )
            updates = entity_records.entity_updates_from_payload(payload)
            progress_log(
                "Norway Brreg entity updates page loaded: page=%s updates=%s",
                page_number,
                len(updates),
            )
            for update in updates:
                if entity_records.entity_update_requires_hydration(update):
                    entity = self.get_entity(_string(update.get("organisasjonsnummer")))
                    hydrated_row_count += 1
                    if (
                        progress_every_rows > 0
                        and hydrated_row_count % progress_every_rows == 0
                    ):
                        progress_log(
                            "Hydrated Norway Brreg entity update rows: rows=%s",
                            hydrated_row_count,
                        )
                else:
                    entity = None
                row_count += 1
                if progress_every_rows > 0 and row_count % progress_every_rows == 0:
                    progress_log("Processed Norway Brreg entity update rows: rows=%s", row_count)
                yield entity_records.updated_entity_record(update, entity=entity)

            if not entity_records.update_payload_has_next_page(
                payload, current_page=page_number
            ):
                break
            page_number += 1
        progress_log(
            "Completed Norway Brreg entity updates load: pages=%s rows=%s hydrated_rows=%s",
            page_number + 1,
            row_count,
            hydrated_row_count,
        )

    def get_entity(self, org_number: str) -> dict[str, Any]:
        payload = self._get_json(f"{self.base_url}/enheter/{org_number}")
        if not isinstance(payload, dict):
            raise ValueError(f"Expected Brreg entity payload to be an object: {org_number}")
        return payload

    def get_financial_accounts(
        self,
        org_number: str,
        *,
        year: str | int | None = None,
        account_type: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if year is not None:
            params["år"] = str(year)
        if account_type is not None:
            params["regnskapstype"] = account_type
        payload = self._get_json(
            f"{self.financial_base_url}/{org_number}",
            params=params or None,
        )
        if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
            raise ValueError(
                f"Expected Brreg financial accounts payload to be a list: {org_number}"
            )
        return payload

    def _get_json(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        response = self.session().get(
            url,
            params=params,
            timeout=self.timeout_seconds,
        )
        _raise_for_status(response)
        return response.json()


@dlt.source(name="norway_brreg_entities")
def norway_brreg_entities_source(
    *,
    base_url: str = BRREG_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    session: HttpSession | None = None,
    log: Callable[..., None] | None = None,
    progress_every_rows: int = ENTITY_PROGRESS_LOG_EVERY_ROWS,
    download_progress_every_bytes: int = DOWNLOAD_PROGRESS_LOG_EVERY_BYTES,
) -> DltSource:
    return _entities_resource(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        session=session,
        log=log,
        progress_every_rows=progress_every_rows,
        download_progress_every_bytes=download_progress_every_bytes,
    )


@dlt.resource(
    name=ENTITIES_TABLE,
    write_disposition="replace",
    primary_key="org_number",
    columns=tables.copy_dlt_columns(BRREG_ENTITIES_COLUMNS),
)
def _entities_resource(
    *,
    base_url: str = BRREG_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    session: HttpSession | None = None,
    log: Callable[..., None] | None = None,
    progress_every_rows: int = ENTITY_PROGRESS_LOG_EVERY_ROWS,
    download_progress_every_bytes: int = DOWNLOAD_PROGRESS_LOG_EVERY_BYTES,
) -> Iterator[dict[str, Any]]:
    yield from iter_brreg_entity_rows(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        session=session,
        log=log,
        progress_every_rows=progress_every_rows,
        download_progress_every_bytes=download_progress_every_bytes,
    )


def iter_brreg_entity_rows(
    *,
    base_url: str = BRREG_BASE_URL,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    user_agent: str = DEFAULT_USER_AGENT,
    run_id: str = "",
    session: HttpSession | None = None,
    log: Callable[..., None] | None = None,
    progress_every_rows: int = ENTITY_PROGRESS_LOG_EVERY_ROWS,
    download_progress_every_bytes: int = DOWNLOAD_PROGRESS_LOG_EVERY_BYTES,
) -> Iterator[dict[str, Any]]:
    progress_log = log or LOGGER.info
    response_body = _download_bytes(
        url=f"{base_url}/enheter/lastned",
        timeout_seconds=timeout_seconds,
        user_agent=user_agent,
        session=session,
        log=progress_log,
        progress_every_bytes=download_progress_every_bytes,
    )
    for line_number, entity in enumerate(_stream_gzip_json_array(response_body), start=1):
        if progress_every_rows > 0 and line_number % progress_every_rows == 0:
            progress_log("Processed Norway Brreg entity rows: rows=%s", line_number)
        yield _entity_row(entity, line_number=line_number, run_id=run_id)


def build_entity_rows(entities: list[dict[str, Any]], *, run_id: str) -> list[dict[str, Any]]:
    return [
        _entity_row(entity, line_number=index, run_id=run_id)
        for index, entity in enumerate(entities, start=1)
    ]


def source_payload_hash(payload: dict[str, Any]) -> str:
    body = _json_dumps(payload, sort_keys=True)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def norway_legal_form_description_en(code: str) -> str:
    return BRREG_LEGAL_FORM_DESCRIPTION_EN_BY_CODE.get(code.upper(), "")


def _entity_row(entity: dict[str, Any], *, line_number: int, run_id: str) -> dict[str, Any]:
    org_number = _string(entity.get("organisasjonsnummer"))
    vat_registered = _bool(entity.get("registrertIMvaregisteret"))
    business_address = _dict(entity.get("forretningsadresse"))
    legal_form = _dict(entity.get("organisasjonsform"))
    nace1 = _dict(entity.get("naeringskode1"))
    nace2 = _dict(entity.get("naeringskode2"))
    nace3 = _dict(entity.get("naeringskode3"))
    status = _entity_status(entity)
    legal_form_description_original = _string(legal_form.get("beskrivelse"))
    legal_form_code = _string(legal_form.get("kode"))
    nace1_description_original = _string(nace1.get("beskrivelse"))
    nace2_description_original = _string(nace2.get("beskrivelse"))
    nace3_description_original = _string(nace3.get("beskrivelse"))
    articles_purpose_original = _joined_text_lines(entity.get("vedtektsfestetFormaal"))
    activity_text_original = _joined_text_lines(entity.get("aktivitet"))
    return {
        "country_iso2": COUNTRY,
        "source_slug": ENTITY_SOURCE_SLUG,
        "source_run_id": run_id,
        "source_line_number": line_number,
        "source_record_id": org_number,
        "source_payload_hash": source_payload_hash(entity),
        "org_number": org_number,
        "vat_id": f"NO{org_number}MVA" if vat_registered and org_number else "",
        "legal_name": _string(entity.get("navn")),
        "legal_form_code": legal_form_code,
        "legal_form_description_original": legal_form_description_original,
        "legal_form_description_en": norway_legal_form_description_en(legal_form_code),
        "registration_date": _string(entity.get("registreringsdatoEnhetsregisteret")),
        "incorporation_date": _string(entity.get("stiftelsesdato")),
        "website": _string(entity.get("hjemmeside")),
        "phone": _string(entity.get("telefon")),
        "nace1_code": _string(nace1.get("kode")),
        "nace1_description_original": nace1_description_original,
        "nace1_description_en": "",
        "nace2_code": _string(nace2.get("kode")),
        "nace2_description_original": nace2_description_original,
        "nace2_description_en": "",
        "nace3_code": _string(nace3.get("kode")),
        "nace3_description_original": nace3_description_original,
        "nace3_description_en": "",
        "articles_purpose_original": articles_purpose_original,
        "articles_purpose_en": "",
        "activity_text_original": activity_text_original,
        "activity_text_en": "",
        "employee_count": _int_or_none(entity.get("antallAnsatte")),
        "has_registered_employee_count": _bool(entity.get("harRegistrertAntallAnsatte")),
        "business_address_lines": _address_lines(business_address),
        "business_postal_code": _string(business_address.get("postnummer")),
        "business_city": _string(business_address.get("poststed")),
        "business_municipality": _string(business_address.get("kommune")),
        "business_municipality_code": _string(business_address.get("kommunenummer")),
        "business_country_code": _string(business_address.get("landkode")),
        "is_vat_registered": vat_registered,
        "is_enterprise_register_registered": _bool(entity.get("registrertIForetaksregisteret")),
        "is_group_member": _bool(entity.get("erIKonsern")),
        "parent_org_number": _string(entity.get("overordnetEnhet")),
        "last_submitted_accounts_year": _string(entity.get("sisteInnsendteAarsregnskap")),
        "status": status,
        "is_active": status == "active",
        "source_url": _source_url(entity),
        "raw_entity": _json_dumps(entity),
    }


def _download_bytes(
    *,
    url: str,
    timeout_seconds: int,
    user_agent: str,
    session: HttpSession | None,
    log: Callable[..., None] | None = None,
    progress_every_bytes: int = DOWNLOAD_PROGRESS_LOG_EVERY_BYTES,
) -> bytes:
    http_session = session or requests.Session()
    http_session.headers["User-Agent"] = user_agent
    response = http_session.get(url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    progress_log = log or LOGGER.info
    iter_content = getattr(response, "iter_content", None)
    if not callable(iter_content):
        return response.content

    chunks: list[bytes] = []
    downloaded_bytes = 0
    next_progress_bytes = progress_every_bytes
    for chunk in iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
        if not chunk:
            continue
        chunks.append(chunk)
        downloaded_bytes += len(chunk)
        while progress_every_bytes > 0 and downloaded_bytes >= next_progress_bytes:
            progress_log(
                "Downloaded Norway Brreg entity archive: downloaded_bytes=%s downloaded_mb=%.1f",
                next_progress_bytes,
                next_progress_bytes / 1024 / 1024,
            )
            next_progress_bytes += progress_every_bytes
    return b"".join(chunks)


def _raise_for_status(response: Any) -> None:
    raise_for_status = getattr(response, "raise_for_status", None)
    if callable(raise_for_status):
        raise_for_status()
        return
    status_code = getattr(response, "status_code", 200)
    if status_code >= 400:
        raise RuntimeError(f"HTTP {status_code}")


def _stream_gzip_json_array(body: bytes) -> Iterator[dict[str, Any]]:
    with gzip.GzipFile(fileobj=BytesIO(body)) as gzip_file:
        for record in ijson.items(gzip_file, "item"):
            if isinstance(record, dict):
                yield record


def _entity_status(entity: dict[str, Any]) -> str:
    if _bool(entity.get("konkurs")):
        return "bankrupt"
    if _bool(entity.get("underTvangsavviklingEllerTvangsopplosning")):
        return "compulsory_liquidation"
    if _bool(entity.get("underAvvikling")):
        return "liquidation"
    return "active"


def _source_url(entity: dict[str, Any]) -> str:
    links = _dict(entity.get("_links"))
    self_link = _dict(links.get("self"))
    return _string(self_link.get("href"))


def _json_dumps(payload: dict[str, Any], *, sort_keys: bool = False) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=sort_keys,
        default=_json_default,
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        if not value.is_finite():
            return str(value)
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _address_lines(address: dict[str, Any]) -> str:
    return "\n".join(_string(line) for line in _list(address.get("adresse")) if _string(line))


def _joined_text_lines(value: Any) -> str:
    return "\n".join(_string(line) for line in _list(value) if _string(line))


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _bool(value: Any) -> bool:
    return bool(value)


def _int_or_none(value: Any) -> int | None:
    return value if isinstance(value, int) else None


def _string(value: Any) -> str:
    return "" if value is None else str(value)
