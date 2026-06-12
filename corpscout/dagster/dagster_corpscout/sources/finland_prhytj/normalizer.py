"""Normalize PRH YTJ records into ClickHouse source table rows."""

import hashlib
import urllib.parse
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from dagster_corpscout.sources.finland_prhytj.parser import ParsedRecord
from dagster_corpscout.sources.finland_prhytj.tables import (
    ADDRESSES_TABLE,
    ADDRESS_POST_OFFICES_TABLE,
    BUSINESS_LINES_TABLE,
    BUSINESS_LINE_DESCRIPTIONS_TABLE,
    COMPANY_FORMS_TABLE,
    COMPANY_FORM_DESCRIPTIONS_TABLE,
    COMPANY_SITUATIONS_TABLE,
    COMPANY_SITUATION_DESCRIPTIONS_TABLE,
    IDENTIFIERS_TABLE,
    NAMES_TABLE,
    NORMALIZED_TABLES,
    REGISTERED_ENTRIES_TABLE,
    REGISTERED_ENTRY_DESCRIPTIONS_TABLE,
    STATUSES_TABLE,
    WEBSITES_TABLE,
)


@dataclass(frozen=True)
class ImportRun:
    run_id: str
    source_export_id: uuid.UUID
    ingested_at: datetime


def normalize_record(run: ImportRun, parsed: ParsedRecord) -> dict[str, list[dict[str, Any]]]:
    payload = parsed.payload
    business_id_record = _dict(payload.get("businessId"))
    business_id = _string(business_id_record.get("value"))
    base = _lineage(run, parsed, business_id)
    rows: dict[str, list[dict[str, Any]]] = {table: [] for table in NORMALIZED_TABLES}

    if business_id:
        rows[IDENTIFIERS_TABLE].append(
            _identifier_row(base, 1, "business_id", business_id_record, True)
        )

    euid = _dict(payload.get("euId"))
    if _string(euid.get("value")):
        rows[IDENTIFIERS_TABLE].append(
            _identifier_row(base, len(rows[IDENTIFIERS_TABLE]) + 1, "euid", euid, False)
        )

    for index, identifier in enumerate(_list(payload.get("identifiers"))):
        rows[IDENTIFIERS_TABLE].append(
            _identifier_row(
                base,
                len(rows[IDENTIFIERS_TABLE]) + 1,
                "identifier",
                _dict(identifier),
                False,
                str(index),
            )
        )

    lifecycle = _lifecycle_status(payload)
    rows[STATUSES_TABLE].append(
        {
            **base,
            "trade_register_status": _string(payload.get("tradeRegisterStatus")),
            "status": _string(payload.get("status")),
            "registration_date": _string(payload.get("registrationDate")),
            "end_date": _string(payload.get("endDate")),
            "last_modified": _string(payload.get("lastModified")),
            "lifecycle_status": lifecycle,
            "is_active": lifecycle == "active",
        }
    )

    for index, name_value in enumerate(_list(payload.get("names"))):
        name = _dict(name_value)
        position = index + 1
        rows[NAMES_TABLE].append(
            {
                **_item_lineage(
                    base,
                    "name",
                    position,
                    _string(name.get("name")),
                    _string(name.get("type")),
                    str(_int(name.get("version"))),
                    _string(name.get("registrationDate")),
                    _string(name.get("endDate")),
                ),
                "name": _string(name.get("name")),
                "name_type_code": _string(name.get("type")),
                "version": _int(name.get("version")),
                "registered_on": _string(name.get("registrationDate")),
                "ended_on": _string(name.get("endDate")),
                "is_current": is_current(_string(name.get("endDate"))),
                "is_primary": _string(name.get("type")) == "1",
            }
        )

    main_business_line = _dict(payload.get("mainBusinessLine"))
    descriptions = _list(main_business_line.get("descriptions"))
    if _string(main_business_line.get("type")) or descriptions:
        line_hash = source_item_hash(
            run.run_id,
            business_id,
            "mainBusinessLine",
            _string(main_business_line.get("type")),
            _string(main_business_line.get("typeCodeSet")),
            _string(main_business_line.get("registrationDate")),
        )
        rows[BUSINESS_LINES_TABLE].append(
            {
                **base,
                "source_item_hash": line_hash,
                "business_line_type": _string(main_business_line.get("type")),
                "business_line_code_set": _string(main_business_line.get("typeCodeSet")),
                "registered_on": _string(main_business_line.get("registrationDate")),
                "source": _string(main_business_line.get("source")),
                "is_primary": True,
            }
        )
        for index, description_value in enumerate(descriptions):
            description = _dict(description_value)
            rows[BUSINESS_LINE_DESCRIPTIONS_TABLE].append(
                {
                    **_item_lineage(
                        base,
                        "mainBusinessLineDescription",
                        index + 1,
                        line_hash,
                        _string(description.get("languageCode")),
                        _string(description.get("description")),
                    ),
                    "business_line_item_hash": line_hash,
                    "language_code": _string(description.get("languageCode")),
                    "description": _string(description.get("description")),
                }
            )

    website = _dict(payload.get("website"))
    website_url = _string(website.get("url")).strip()
    if website_url:
        normalized_url, host, path = normalized_url_parts(website_url)
        rows[WEBSITES_TABLE].append(
            {
                **base,
                "source_item_hash": source_item_hash(
                    run.run_id,
                    business_id,
                    "website",
                    normalized_url,
                    _string(website.get("registrationDate")),
                    _string(website.get("endDate")),
                ),
                "url": _string(website.get("url")),
                "normalized_url": normalized_url,
                "host": host,
                "path": path,
                "registered_on": _string(website.get("registrationDate")),
                "ended_on": _string(website.get("endDate")),
                "is_current": is_current(_string(website.get("endDate"))),
                "is_primary": True,
            }
        )

    _normalize_company_forms(rows, run, base, business_id, payload)
    _normalize_company_situations(rows, run, base, business_id, payload)
    _normalize_registered_entries(rows, run, base, business_id, payload)
    _normalize_addresses(rows, run, base, business_id, payload)

    return rows


def _normalize_company_forms(
    rows: dict[str, list[dict[str, Any]]],
    run: ImportRun,
    base: dict[str, Any],
    business_id: str,
    payload: dict[str, Any],
) -> None:
    for index, form_value in enumerate(_list(payload.get("companyForms"))):
        form = _dict(form_value)
        position = index + 1
        form_hash = source_item_hash(
            run.run_id,
            business_id,
            "companyForm",
            str(index),
            _string(form.get("type")),
            str(_int(form.get("version"))),
            _string(form.get("registrationDate")),
            _string(form.get("endDate")),
        )
        rows[COMPANY_FORMS_TABLE].append(
            {
                **_item_lineage_with_hash(base, form_hash, position),
                "form_type_code": _string(form.get("type")),
                "version": _int(form.get("version")),
                "registered_on": _string(form.get("registrationDate")),
                "ended_on": _string(form.get("endDate")),
                "source": _string(form.get("source")),
                "is_current": is_current(_string(form.get("endDate"))),
            }
        )
        for description_index, description_value in enumerate(_list(form.get("descriptions"))):
            description = _dict(description_value)
            rows[COMPANY_FORM_DESCRIPTIONS_TABLE].append(
                {
                    **_item_lineage(
                        base,
                        "companyFormDescription",
                        description_index + 1,
                        form_hash,
                        _string(description.get("languageCode")),
                        _string(description.get("description")),
                    ),
                    "company_form_item_hash": form_hash,
                    "language_code": _string(description.get("languageCode")),
                    "description": _string(description.get("description")),
                }
            )


def _normalize_company_situations(
    rows: dict[str, list[dict[str, Any]]],
    run: ImportRun,
    base: dict[str, Any],
    business_id: str,
    payload: dict[str, Any],
) -> None:
    for index, situation_value in enumerate(_list(payload.get("companySituations"))):
        situation = _dict(situation_value)
        position = index + 1
        situation_hash = source_item_hash(
            run.run_id,
            business_id,
            "companySituation",
            str(index),
            _string(situation.get("type")),
            _string(situation.get("registrationDate")),
            _string(situation.get("endDate")),
        )
        rows[COMPANY_SITUATIONS_TABLE].append(
            {
                **_item_lineage_with_hash(base, situation_hash, position),
                "situation_type_code": _string(situation.get("type")),
                "registered_on": _string(situation.get("registrationDate")),
                "ended_on": _string(situation.get("endDate")),
                "is_current": is_current(_string(situation.get("endDate"))),
            }
        )
        for description_index, description_value in enumerate(_list(situation.get("descriptions"))):
            description = _dict(description_value)
            rows[COMPANY_SITUATION_DESCRIPTIONS_TABLE].append(
                {
                    **_item_lineage(
                        base,
                        "companySituationDescription",
                        description_index + 1,
                        situation_hash,
                        _string(description.get("languageCode")),
                        _string(description.get("description")),
                    ),
                    "company_situation_item_hash": situation_hash,
                    "language_code": _string(description.get("languageCode")),
                    "description": _string(description.get("description")),
                }
            )


def _normalize_registered_entries(
    rows: dict[str, list[dict[str, Any]]],
    run: ImportRun,
    base: dict[str, Any],
    business_id: str,
    payload: dict[str, Any],
) -> None:
    for index, entry_value in enumerate(_list(payload.get("registeredEntries"))):
        entry = _dict(entry_value)
        position = index + 1
        entry_hash = source_item_hash(
            run.run_id,
            business_id,
            "registeredEntry",
            str(index),
            _string(entry.get("type")),
            _string(entry.get("register")),
            _string(entry.get("registrationDate")),
            _string(entry.get("endDate")),
        )
        rows[REGISTERED_ENTRIES_TABLE].append(
            {
                **_item_lineage_with_hash(base, entry_hash, position),
                "entry_type_code": _string(entry.get("type")),
                "register_code": _string(entry.get("register")),
                "authority": _string(entry.get("authority")),
                "registered_on": _string(entry.get("registrationDate")),
                "ended_on": _string(entry.get("endDate")),
                "is_current": is_current(_string(entry.get("endDate"))),
            }
        )
        for description_index, description_value in enumerate(_list(entry.get("descriptions"))):
            description = _dict(description_value)
            rows[REGISTERED_ENTRY_DESCRIPTIONS_TABLE].append(
                {
                    **_item_lineage(
                        base,
                        "registeredEntryDescription",
                        description_index + 1,
                        entry_hash,
                        _string(description.get("languageCode")),
                        _string(description.get("description")),
                    ),
                    "registered_entry_item_hash": entry_hash,
                    "language_code": _string(description.get("languageCode")),
                    "description": _string(description.get("description")),
                }
            )


def _normalize_addresses(
    rows: dict[str, list[dict[str, Any]]],
    run: ImportRun,
    base: dict[str, Any],
    business_id: str,
    payload: dict[str, Any],
) -> None:
    for index, address_value in enumerate(_list(payload.get("addresses"))):
        address = _dict(address_value)
        position = index + 1
        address_type = _int(address.get("type"))
        address_hash = source_item_hash(
            run.run_id,
            business_id,
            "address",
            str(index),
            str(address_type),
            _string(address.get("street")),
            _string(address.get("postCode")),
            _string(address.get("registrationDate")),
        )
        rows[ADDRESSES_TABLE].append(
            {
                **_item_lineage_with_hash(base, address_hash, position),
                "address_type_code": address_type,
                "street": _string(address.get("street")),
                "post_code": _string(address.get("postCode")),
                "building_number": _string(address.get("buildingNumber")),
                "entrance": _string(address.get("entrance")),
                "apartment_number": _string(address.get("apartmentNumber")),
                "post_office_box": _string(address.get("postOfficeBox")),
                "co": _string(address.get("co")),
                "country": _string(address.get("country")),
                "registered_on": _string(address.get("registrationDate")),
                "source": _string(address.get("source")),
            }
        )
        for post_office_index, post_office_value in enumerate(_list(address.get("postOffices"))):
            post_office = _dict(post_office_value)
            rows[ADDRESS_POST_OFFICES_TABLE].append(
                {
                    **_item_lineage(
                        base,
                        "addressPostOffice",
                        post_office_index + 1,
                        address_hash,
                        _string(post_office.get("languageCode")),
                        _string(post_office.get("city")),
                        _string(post_office.get("municipalityCode")),
                    ),
                    "address_item_hash": address_hash,
                    "language_code": _string(post_office.get("languageCode")),
                    "city": _string(post_office.get("city")),
                    "municipality_code": _string(post_office.get("municipalityCode")),
                }
            )


def source_item_hash(*parts: object) -> str:
    body = "\x00".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def is_current(end_date: str | None) -> bool:
    return not (end_date or "").strip()


def normalize_website(raw: str | None) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme:
        return value
    return f"https://{value}"


def normalized_url_parts(raw: str) -> tuple[str, str, str]:
    normalized = normalize_website(raw)
    parsed = urllib.parse.urlparse(normalized)
    return normalized, parsed.hostname or "", parsed.path


def _lineage(run: ImportRun, parsed: ParsedRecord, business_id: str) -> dict[str, Any]:
    return {
        "country_iso2": "FI",
        "source_slug": "prhytj",
        "source_run_id": run.run_id,
        "source_record_id": business_id,
        "business_id": business_id,
        "source_line_number": parsed.line_number,
        "source_payload_hash": parsed.payload_hash,
        "ingested_at": run.ingested_at,
        "source_export_id": run.source_export_id,
    }


def _identifier_row(
    base: dict[str, Any],
    source_position: int,
    scope: str,
    identifier: dict[str, Any],
    primary: bool,
    *extra_hash_parts: str,
) -> dict[str, Any]:
    hash_parts = [
        "identifier",
        scope,
        str(source_position),
        _string(identifier.get("type")),
        _string(identifier.get("value")),
        _string(identifier.get("registrationDate")),
        _string(identifier.get("endDate")),
        *extra_hash_parts,
    ]
    return {
        **_item_lineage(base, "identifier", source_position, *hash_parts),
        "identifier_scope": scope,
        "identifier_type": _string(identifier.get("type")),
        "identifier_value": _string(identifier.get("value")),
        "registered_on": _string(identifier.get("registrationDate")),
        "ended_on": _string(identifier.get("endDate")),
        "source": _string(identifier.get("source")),
        "is_primary_business_id": primary,
    }


def _item_lineage(
    base: dict[str, Any],
    section: str,
    source_position: int,
    *hash_parts: object,
) -> dict[str, Any]:
    item_hash = source_item_hash(
        base["source_run_id"],
        base["business_id"],
        section,
        str(source_position),
        *hash_parts,
    )
    return _item_lineage_with_hash(base, item_hash, source_position)


def _item_lineage_with_hash(
    base: dict[str, Any],
    item_hash: str,
    source_position: int,
) -> dict[str, Any]:
    return {
        **base,
        "source_item_hash": item_hash,
        "source_position": source_position,
    }


def _lifecycle_status(payload: dict[str, Any]) -> str:
    if _string(payload.get("endDate")) or _string(payload.get("tradeRegisterStatus")) == "3":
        return "ceased"
    return "active"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _string(value: Any) -> str:
    return "" if value is None else str(value)


def _int(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(value)
