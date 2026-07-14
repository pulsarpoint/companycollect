from __future__ import annotations

import json
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import field
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any

import ijson


@dataclass
class NormalizedGleifRows:
    lei_records: list[dict[str, Any]] = field(default_factory=list)
    lei_names: list[dict[str, Any]] = field(default_factory=list)
    lei_addresses: list[dict[str, Any]] = field(default_factory=list)
    lei_identifiers: list[dict[str, Any]] = field(default_factory=list)
    relationships: list[dict[str, Any]] = field(default_factory=list)
    relationship_periods: list[dict[str, Any]] = field(default_factory=list)
    reporting_exceptions: list[dict[str, Any]] = field(default_factory=list)
    lei_issuers: list[dict[str, Any]] = field(default_factory=list)
    code_list_entries: list[dict[str, Any]] = field(default_factory=list)


def parse_lei_records_zip(
    zip_bytes: bytes,
    *,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
    golden_copy_publish_date: str | None,
) -> NormalizedGleifRows:
    rows = NormalizedGleifRows()
    for record in _records_from_payload(_first_json_member(zip_bytes)):
        _extend_rows(
            rows,
            _normalize_lei_record(
                record,
                source_run_id=source_run_id,
                retrieved_at=retrieved_at,
                resolved_at=resolved_at,
                golden_copy_publish_date=golden_copy_publish_date,
            ),
        )
    return rows


def iter_lei_record_rows_from_zip_path(
    zip_path: str | Path,
    *,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
    golden_copy_publish_date: str | None,
) -> Iterator[NormalizedGleifRows]:
    for record in _iter_records_from_zip_path(
        Path(zip_path),
        (
            "records.item",
            "Records.item",
            "LEIData.LEIRecords.LEIRecord.item",
            "lei:LEIData.lei:LEIRecords.lei:LEIRecord.item",
            "data.item",
        ),
    ):
        yield _normalize_lei_record(
            record,
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            resolved_at=resolved_at,
            golden_copy_publish_date=golden_copy_publish_date,
        )


def _normalize_lei_record(
    record: dict[str, Any],
    *,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
    golden_copy_publish_date: str | None,
) -> NormalizedGleifRows:
    rows = NormalizedGleifRows()
    lei = _first_text(record, "LEI", "lei", "id")
    if not lei:
        return rows
    attributes = _dict(record.get("attributes"))
    entity = _dict(record.get("Entity") or attributes.get("entity"))
    registration = _dict(record.get("Registration") or attributes.get("registration"))
    legal_name = _dict(entity.get("LegalName") or entity.get("legalName"))
    legal_address = _dict(entity.get("LegalAddress") or entity.get("legalAddress"))
    headquarters_address = _dict(
        entity.get("HeadquartersAddress") or entity.get("headquartersAddress")
    )
    legal_form = _dict(entity.get("LegalForm") or entity.get("legalForm"))
    registered_at = _dict(entity.get("RegisteredAt") or entity.get("registeredAt"))
    validated_at = _dict(registration.get("ValidatedAt") or registration.get("validatedAt"))

    rows.lei_records.append(
        {
            "lei": lei,
            "legal_name": _text(legal_name) or "",
            "legal_name_language": _language(legal_name),
            "entity_status": _first_text(entity, "EntityStatus", "status") or "",
            "registration_status": _first_text(
                registration,
                "RegistrationStatus",
                "status",
            )
            or "",
            "jurisdiction": _first_text(entity, "LegalJurisdiction", "jurisdiction"),
            "category": _first_text(entity, "EntityCategory", "category"),
            "subcategory": _first_text(entity, "EntitySubCategory", "subCategory"),
            "legal_form_id": _first_text(legal_form, "EntityLegalFormCode", "id"),
            "legal_form_other": _first_text(legal_form, "OtherLegalForm", "other"),
            "registered_at_id": _first_text(registered_at, "RegistrationAuthorityID", "id"),
            "registered_at_other": _first_text(registered_at, "OtherRegistrationAuthorityID", "other"),
            "registered_as": _first_text(entity, "RegisteredAs", "registeredAs"),
            "associated_entity_lei": _nested_text(entity, "AssociatedEntity", "LEI"),
            "associated_entity_name": _nested_text(entity, "AssociatedEntity", "EntityName"),
            "successor_entity_lei": _nested_text(entity, "SuccessorEntity", "LEI"),
            "successor_entity_name": _nested_text(entity, "SuccessorEntity", "EntityName"),
            "creation_date": _first_text(entity, "EntityCreationDate", "creationDate"),
            "expiration_date": _nested_text(entity, "EntityExpiration", "ExpirationDate"),
            "expiration_reason": _nested_text(entity, "EntityExpiration", "ExpirationReason"),
            "initial_registration_date": _first_text(
                registration,
                "InitialRegistrationDate",
                "initialRegistrationDate",
            ),
            "last_update_date": _first_text(
                registration,
                "LastUpdateDate",
                "lastUpdateDate",
            ),
            "next_renewal_date": _first_text(
                registration,
                "NextRenewalDate",
                "nextRenewalDate",
            ),
            "managing_lou": _first_text(registration, "ManagingLOU", "managingLou"),
            "corroboration_level": _first_text(
                registration,
                "ValidationSources",
                "corroborationLevel",
            ),
            "validated_at_id": _first_text(validated_at, "ValidationAuthorityID", "id"),
            "validated_at_other": _first_text(validated_at, "OtherValidationAuthorityID", "other"),
            "validated_as": _first_text(registration, "ValidatedAs", "validatedAs"),
            "conformity_flag": _first_text(attributes, "conformityFlag"),
            "legal_address_country": _first_text(legal_address, "Country", "country"),
            "headquarters_address_country": _first_text(
                headquarters_address,
                "Country",
                "country",
            ),
            "primary_country_iso2": _first_text(legal_address, "Country", "country")
            or _first_text(headquarters_address, "Country", "country"),
            "golden_copy_publish_date": golden_copy_publish_date,
            "source_system": "gleif",
            "source_run_id": source_run_id,
            "retrieved_at": retrieved_at,
            "resolved_at": resolved_at,
        }
    )
    rows.lei_addresses.extend(
        _address_rows(
            lei=lei,
            legal_address=legal_address,
            headquarters_address=headquarters_address,
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            resolved_at=resolved_at,
        )
    )
    rows.lei_names.extend(
        _name_rows(
            lei=lei,
            entity=entity,
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            resolved_at=resolved_at,
        )
    )
    rows.lei_identifiers.extend(
        _identifier_rows(
            lei=lei,
            record=record,
            attributes=attributes,
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            resolved_at=resolved_at,
        )
    )
    return rows

def parse_relationships_zip(
    zip_bytes: bytes,
    *,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> NormalizedGleifRows:
    rows = NormalizedGleifRows()
    for record in _records_from_payload(_first_json_member(zip_bytes)):
        _extend_rows(
            rows,
            _normalize_relationship_record(
                record,
                source_run_id=source_run_id,
                retrieved_at=retrieved_at,
                resolved_at=resolved_at,
            ),
        )
    return rows


def iter_relationship_rows_from_zip_path(
    zip_path: str | Path,
    *,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> Iterator[NormalizedGleifRows]:
    for record in _iter_records_from_zip_path(
        Path(zip_path),
        (
            "records.item",
            "Records.item",
            "RelationshipData.RelationshipRecords.RelationshipRecord.item",
            "rr:RelationshipData.rr:RelationshipRecords.rr:RelationshipRecord.item",
            "data.item",
        ),
    ):
        yield _normalize_relationship_record(
            record,
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            resolved_at=resolved_at,
        )


def _normalize_relationship_record(
    record: dict[str, Any],
    *,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> NormalizedGleifRows:
    rows = NormalizedGleifRows()
    container = _dict(record.get("RelationshipRecord") or record)
    relationship = _dict(container.get("Relationship") or container.get("relationship"))
    registration = _dict(container.get("Registration") or container.get("registration"))
    start_node = _dict(relationship.get("StartNode") or relationship.get("startNode"))
    end_node = _dict(relationship.get("EndNode") or relationship.get("endNode"))
    start_lei = _first_text(start_node, "NodeID", "id") or ""
    end_lei = _first_text(end_node, "NodeID", "id") or ""
    relationship_type = _first_text(relationship, "RelationshipType", "type") or ""
    relationship_record_id = (
        _first_text(container, "RelationshipRecordID", "id")
        or f"{start_lei}:{relationship_type}:{end_lei}"
    )
    rows.relationships.append(
        {
            "relationship_record_id": relationship_record_id,
            "start_node_lei": start_lei,
            "start_node_type": _first_text(start_node, "NodeIDType", "type"),
            "end_node_lei": end_lei,
            "end_node_type": _first_text(end_node, "NodeIDType", "type"),
            "relationship_type": relationship_type,
            "relationship_status": _first_text(relationship, "RelationshipStatus", "status")
            or "",
            "valid_from": _first_text(relationship, "RelationshipStartDate", "validFrom"),
            "valid_to": _first_text(relationship, "RelationshipEndDate", "validTo"),
            "initial_registration_date": _first_text(
                registration,
                "InitialRegistrationDate",
                "initialRegistrationDate",
            ),
            "last_update_date": _first_text(
                registration,
                "LastUpdateDate",
                "lastUpdateDate",
            ),
            "registration_status": _first_text(registration, "RegistrationStatus", "status"),
            "next_renewal_date": _first_text(
                registration,
                "NextRenewalDate",
                "nextRenewalDate",
            ),
            "managing_lou": _first_text(registration, "ManagingLOU", "managingLou"),
            "corroboration_level": _first_text(
                registration,
                "ValidationSources",
                "corroborationLevel",
            ),
            "corroboration_documents": _first_text(
                registration,
                "ValidationDocuments",
                "corroborationDocuments",
            ),
            "corroboration_reference": _first_text(
                registration,
                "ValidationReference",
                "corroborationReference",
            ),
            "deleted_at": _nested_text(container, "Extension", "deletedAt"),
            "source_system": "gleif",
            "source_run_id": source_run_id,
            "retrieved_at": retrieved_at,
            "resolved_at": resolved_at,
        }
    )
    for period in _relationship_periods(relationship):
        rows.relationship_periods.append(
            {
                "relationship_record_id": relationship_record_id,
                "period_type": _first_text(period, "PeriodType", "type") or "",
                "start_date": _date_part(_first_text(period, "StartDate", "startDate")),
                "end_date": _date_part(_first_text(period, "EndDate", "endDate")),
                "source_system": "gleif",
                "source_run_id": source_run_id,
                "retrieved_at": retrieved_at,
                "resolved_at": resolved_at,
            }
        )
    return rows


def parse_reporting_exceptions_zip(
    zip_bytes: bytes,
    *,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> NormalizedGleifRows:
    rows = NormalizedGleifRows()
    for record in _records_from_payload(_first_json_member(zip_bytes)):
        _extend_rows(
            rows,
            _normalize_reporting_exception_record(
                record,
                source_run_id=source_run_id,
                retrieved_at=retrieved_at,
                resolved_at=resolved_at,
            ),
        )
    return rows


def iter_reporting_exception_rows_from_zip_path(
    zip_path: str | Path,
    *,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> Iterator[NormalizedGleifRows]:
    for record in _iter_records_from_zip_path(
        Path(zip_path),
        (
            "records.item",
            "Records.item",
            "ExceptionData.ExceptionRecords.ExceptionRecord.item",
            "repex:ExceptionData.repex:ExceptionRecords.repex:ExceptionRecord.item",
            "data.item",
        ),
    ):
        yield _normalize_reporting_exception_record(
            record,
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            resolved_at=resolved_at,
        )


def _normalize_reporting_exception_record(
    record: dict[str, Any],
    *,
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> NormalizedGleifRows:
    rows = NormalizedGleifRows()
    exception = _dict(record.get("Exception") or record.get("exception") or record)
    registration = _dict(record.get("Registration") or record.get("registration"))
    lei = _first_text(exception, "LEI", "lei") or ""
    parent_relationship_type = _first_text(
        exception,
        "ParentRelationshipType",
        "parentRelationshipType",
    ) or ""
    exception_category = _first_text(exception, "ExceptionCategory", "category") or ""
    exception_record_id = (
        _first_text(record, "ExceptionRecordID", "id")
        or _stable_id(lei, parent_relationship_type, exception_category)
    )
    rows.reporting_exceptions.append(
        {
            "exception_record_id": exception_record_id,
            "lei": lei,
            "parent_relationship_type": parent_relationship_type,
            "exception_category": exception_category,
            "exception_reason": _first_text(exception, "ExceptionReason", "reason"),
            "exception_reference": _first_text(exception, "ExceptionReference", "reference"),
            "initial_registration_date": _first_text(
                registration,
                "InitialRegistrationDate",
                "initialRegistrationDate",
            ),
            "last_update_date": _first_text(
                registration,
                "LastUpdateDate",
                "lastUpdateDate",
            ),
            "registration_status": _first_text(registration, "RegistrationStatus", "status"),
            "next_renewal_date": _first_text(
                registration,
                "NextRenewalDate",
                "nextRenewalDate",
            ),
            "managing_lou": _first_text(registration, "ManagingLOU", "managingLou"),
            "source_system": "gleif",
            "source_run_id": source_run_id,
            "retrieved_at": retrieved_at,
            "resolved_at": resolved_at,
        }
    )
    return rows


def _first_json_member(zip_bytes: bytes) -> dict[str, Any]:
    with zipfile.ZipFile(BytesIO(zip_bytes)) as archive:
        json_names = [name for name in archive.namelist() if name.endswith(".json")]
        if len(json_names) != 1:
            raise ValueError(f"expected exactly one JSON member, found {len(json_names)}")
        with archive.open(json_names[0]) as handle:
            payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("GLEIF JSON member must contain an object")
    return payload


def _iter_records_from_zip_path(
    zip_path: Path,
    record_prefixes: tuple[str, ...],
) -> Iterator[dict[str, Any]]:
    with zipfile.ZipFile(zip_path) as archive:
        json_name = _single_json_member_name(archive)

    for prefix in record_prefixes:
        found_record = False
        with zipfile.ZipFile(zip_path) as archive:
            with archive.open(json_name) as handle:
                for item in ijson.items(handle, prefix):
                    found_record = True
                    yield _dict(item)
        if found_record:
            return


def _single_json_member_name(archive: zipfile.ZipFile) -> str:
    json_names = [name for name in archive.namelist() if name.endswith(".json")]
    if len(json_names) != 1:
        raise ValueError(f"expected exactly one JSON member, found {len(json_names)}")
    return json_names[0]


def _records_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = (
        payload.get("records"),
        payload.get("Records"),
        _nested(payload, "LEIData", "LEIRecords", "LEIRecord"),
        _nested(payload, "lei:LEIData", "lei:LEIRecords", "lei:LEIRecord"),
        _nested(payload, "RelationshipData", "RelationshipRecords", "RelationshipRecord"),
        _nested(payload, "ExceptionData", "ExceptionRecords", "ExceptionRecord"),
        payload.get("data"),
    )
    for candidate in candidates:
        items = _list(candidate)
        if items:
            return [_dict(item) for item in items]
    return []


def _extend_rows(target: NormalizedGleifRows, source: NormalizedGleifRows) -> None:
    target.lei_records.extend(source.lei_records)
    target.lei_names.extend(source.lei_names)
    target.lei_addresses.extend(source.lei_addresses)
    target.lei_identifiers.extend(source.lei_identifiers)
    target.relationships.extend(source.relationships)
    target.relationship_periods.extend(source.relationship_periods)
    target.reporting_exceptions.extend(source.reporting_exceptions)
    target.lei_issuers.extend(source.lei_issuers)
    target.code_list_entries.extend(source.code_list_entries)


def _address_rows(
    *,
    lei: str,
    legal_address: dict[str, Any],
    headquarters_address: dict[str, Any],
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> list[dict[str, Any]]:
    rows = []
    for address_role, address in (
        ("legal", legal_address),
        ("headquarters", headquarters_address),
    ):
        if not address:
            continue
        rows.append(
            {
                "lei": lei,
                "address_role": address_role,
                "language": _language(address),
                "address_lines": _address_lines(address),
                "address_number": _first_text(address, "AddressNumber", "addressNumber"),
                "address_number_within_building": _first_text(
                    address,
                    "AddressNumberWithinBuilding",
                    "addressNumberWithinBuilding",
                ),
                "mail_routing": _first_text(address, "MailRouting", "mailRouting"),
                "city": _first_text(address, "City", "city"),
                "region": _first_text(address, "Region", "region"),
                "country": _first_text(address, "Country", "country"),
                "postal_code": _first_text(address, "PostalCode", "postalCode"),
                "normalized_address": None,
                "latitude": None,
                "longitude": None,
                "source_system": "gleif",
                "source_run_id": source_run_id,
                "retrieved_at": retrieved_at,
                "resolved_at": resolved_at,
            }
        )
    return rows


def _name_rows(
    *,
    lei: str,
    entity: dict[str, Any],
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> list[dict[str, Any]]:
    rows = []
    sequence = 1
    for container_key, child_key, name_type in (
        ("OtherEntityNames", "OtherEntityName", "other_name"),
        ("otherNames", "", "other_name"),
        ("TransliteratedOtherEntityNames", "TransliteratedOtherEntityName", "transliterated_other_name"),
        ("transliteratedOtherNames", "", "transliterated_other_name"),
    ):
        values = _nested(entity, container_key, child_key) if child_key else entity.get(container_key)
        for item in _list(values):
            item_dict = _dict(item)
            name = _text(item_dict)
            if not name:
                continue
            rows.append(
                {
                    "lei": lei,
                    "name_type": name_type,
                    "name": name,
                    "name_normalized": name.strip().lower(),
                    "language": _language(item_dict),
                    "cdf_type": _first_text(item_dict, "@type", "type"),
                    "sequence": sequence,
                    "source_system": "gleif",
                    "source_run_id": source_run_id,
                    "retrieved_at": retrieved_at,
                    "resolved_at": resolved_at,
                }
            )
            sequence += 1
    return rows


def _identifier_rows(
    *,
    lei: str,
    record: dict[str, Any],
    attributes: dict[str, Any],
    source_run_id: str,
    retrieved_at: str,
    resolved_at: str,
) -> list[dict[str, Any]]:
    rows = []
    for identifier_type, keys in {
        "BIC": ("BIC", "bic"),
        "MIC": ("MIC", "mic"),
        "OCID": ("OCID", "ocid"),
        "QCC": ("QCC", "qcc"),
        "SPGLOBAL": ("SPGLOBAL", "spglobal"),
    }.items():
        values = []
        for key in keys:
            values.extend(_list(record.get(key)))
            values.extend(_list(attributes.get(key)))
        for value in values:
            identifier_value = _text(value)
            if not identifier_value:
                continue
            rows.append(
                {
                    "lei": lei,
                    "identifier_type": identifier_type,
                    "identifier_value": identifier_value,
                    "identifier_scope": None,
                    "mapping_source": "gleif",
                    "is_primary": 1,
                    "source_system": "gleif",
                    "source_run_id": source_run_id,
                    "retrieved_at": retrieved_at,
                    "resolved_at": resolved_at,
                }
            )
    return rows


def _relationship_periods(relationship: dict[str, Any]) -> list[dict[str, Any]]:
    periods = relationship.get("RelationshipPeriods") or relationship.get("periods")
    if isinstance(periods, dict):
        return [_dict(item) for item in _list(periods.get("RelationshipPeriod"))]
    return [_dict(item) for item in _list(periods)]


def _address_lines(address: dict[str, Any]) -> list[str]:
    lines = []
    for key in (
        "FirstAddressLine",
        "AdditionalAddressLine1",
        "AdditionalAddressLine2",
        "AdditionalAddressLine3",
    ):
        value = _first_text(address, key)
        if value:
            lines.append(value)
    for value in _list(address.get("addressLines")):
        text = _text(value)
        if text:
            lines.append(text)
    return lines


def _dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: object) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _nested(value: object, *keys: str) -> object:
    current = value
    for key in keys:
        if key == "":
            continue
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _nested_text(value: object, *keys: str) -> str | None:
    return _text(_nested(value, *keys))


def _first_text(value: object, *keys: str) -> str | None:
    source = _dict(value)
    for key in keys:
        text = _text(source.get(key))
        if text:
            return text
    return None


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, dict):
        for key in ("$", "name", "value"):
            item = value.get(key)
            if isinstance(item, str):
                return item
    return None


def _language(value: dict[str, Any]) -> str | None:
    return _first_text(value, "@lang", "language", "lang")


def _date_part(value: str | None) -> str | None:
    if value is None:
        return None
    return value.split("T", 1)[0]


def _stable_id(*parts: str) -> str:
    joined = ":".join(parts)
    return sha256(joined.encode("utf-8")).hexdigest()
