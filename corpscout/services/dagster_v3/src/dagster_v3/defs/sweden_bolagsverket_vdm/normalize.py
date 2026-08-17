import json
from datetime import date, datetime
from hashlib import sha256
from typing import Any

import pyarrow as pa

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_bolagsverket_vdm import source, tables


def load_observations_from_object_store(
    *,
    connection: Any,
    object_store: ObjectStoreResource,
    run_id: str,
    manifest_key: str | None = None,
) -> dict[str, int]:
    """Normalize the current run's persisted responses into two DuckDB tables."""
    resolved_manifest_key = manifest_key or source.manifest_object_key(run_id)
    manifest = _read_json_object(
        object_store.read_bytes(resolved_manifest_key, bucket=source.RAW_BUCKET)
    )
    if manifest.get("run_id") != run_id:
        raise ValueError("raw manifest run_id does not match the Dagster run")
    observed_at = _parse_datetime(manifest.get("observed_at"), "observed_at")
    responses = manifest.get("responses")
    if not isinstance(responses, list):
        raise ValueError("raw manifest responses must be a list")

    response_index: dict[tuple[str, str], dict[str, Any]] = {}
    for response in responses:
        if not isinstance(response, dict):
            raise ValueError("raw manifest response entries must be objects")
        company_id = _required_string(response, "company_id")
        endpoint = _required_string(response, "endpoint")
        response_index[(company_id, endpoint)] = response

    company_rows: list[dict[str, Any]] = []
    document_rows: list[dict[str, Any]] = []
    company_ids = manifest.get("company_ids")
    if not isinstance(company_ids, list):
        raise ValueError("raw manifest company_ids must be a list")
    for raw_company_id in company_ids:
        company_id = str(raw_company_id)
        organisation_ref = _required_response(
            response_index, company_id, "organisationer"
        )
        documents_ref = _required_response(response_index, company_id, "dokumentlista")
        organisation_payload = _read_stored_response(object_store, organisation_ref)
        documents_payload = _read_stored_response(object_store, documents_ref)

        documents = documents_payload.get("dokument")
        if not isinstance(documents, list):
            raise ValueError("dokumentlista response must contain a dokument list")
        organisations = organisation_payload.get("organisationer")
        if not isinstance(organisations, list):
            raise ValueError(
                "organisationer response must contain an organisationer list"
            )

        provenance = _provenance(
            run_id=run_id,
            organisation_ref=organisation_ref,
            documents_ref=documents_ref,
            observed_at=observed_at,
        )
        if not organisations:
            company_rows.append(
                {
                    "company_id": company_id,
                    "name_protection_sequence": None,
                    "identity_type_code": None,
                    "identity_type_label_original": None,
                    "active_status_code": None,
                    "is_active": None,
                    "active_status_producer": None,
                    "active_status_observed_at": observed_at,
                    "organisation_registered_on": None,
                    "introduced_at_scb": None,
                    "organisation_date_producer": None,
                    "digital_report_document_count": len(documents),
                    "organisation_found": 0,
                    **provenance,
                }
            )
        for organisation in organisations:
            if not isinstance(organisation, dict):
                raise ValueError("organisation entries must be objects")
            identity = _optional_object(organisation.get("organisationsidentitet"))
            identity_type = _optional_object(identity.get("typ"))
            active_status = _optional_object(organisation.get("verksamOrganisation"))
            organisation_dates = _optional_object(
                organisation.get("organisationsdatum")
            )
            active_code = _optional_string(active_status.get("kod"))
            response_company_id = _optional_string(identity.get("identitetsbeteckning"))
            company_rows.append(
                {
                    "company_id": response_company_id or company_id,
                    "name_protection_sequence": _optional_uint(
                        organisation.get("namnskyddslopnummer")
                    ),
                    "identity_type_code": _optional_string(identity_type.get("kod")),
                    "identity_type_label_original": _optional_string(
                        identity_type.get("klartext")
                    ),
                    "active_status_code": active_code,
                    "is_active": _active_value(active_code),
                    "active_status_producer": _optional_string(
                        active_status.get("dataproducent")
                    ),
                    "active_status_observed_at": observed_at,
                    "organisation_registered_on": _optional_date(
                        organisation_dates.get("registreringsdatum")
                    ),
                    "introduced_at_scb": _optional_date(
                        organisation_dates.get("infortHosScb")
                    ),
                    "organisation_date_producer": _optional_string(
                        organisation_dates.get("dataproducent")
                    ),
                    "digital_report_document_count": len(documents),
                    "organisation_found": 1,
                    **provenance,
                }
            )

        for document in documents:
            if not isinstance(document, dict):
                raise ValueError("dokument entries must be objects")
            document_rows.append(
                {
                    "company_id": company_id,
                    "bolagsverket_document_id": _required_string(
                        document, "dokumentId"
                    ),
                    "reporting_period_end": _optional_date(
                        document.get("rapporteringsperiodTom")
                    ),
                    "filing_registered_on": _optional_date(
                        document.get("registreringstidpunkt")
                    ),
                    "source_file_format": _optional_string(document.get("filformat")),
                    "source_run_id": run_id,
                    "document_list_object_key": documents_ref["object_key"],
                    "document_list_sha256": documents_ref["sha256"],
                    "document_list_request_id": documents_ref["request_id"],
                    "observed_at": observed_at,
                }
            )

    _replace_table(
        connection=connection,
        schema_name=tables.DUCKDB_SCHEMA,
        table_name=tables.COMPANY_OBSERVATIONS_TABLE,
        rows=company_rows,
        arrow_schema=_company_arrow_schema(),
    )
    _replace_table(
        connection=connection,
        schema_name=tables.DUCKDB_SCHEMA,
        table_name=tables.DOCUMENT_OBSERVATIONS_TABLE,
        rows=document_rows,
        arrow_schema=_document_arrow_schema(),
    )
    return {
        "company_observations": len(company_rows),
        "document_observations": len(document_rows),
    }


def _read_stored_response(
    object_store: ObjectStoreResource,
    response_ref: dict[str, Any],
) -> dict[str, Any]:
    body = object_store.read_bytes(response_ref["object_key"], bucket=source.RAW_BUCKET)
    if sha256(body).hexdigest() != response_ref["sha256"]:
        raise ValueError(
            "stored Bolagsverket response does not match its manifest hash"
        )
    return _read_json_object(body)


def _read_json_object(body: bytes) -> dict[str, Any]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("stored Bolagsverket response is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("stored Bolagsverket response must be a JSON object")
    return value


def _required_response(
    response_index: dict[tuple[str, str], dict[str, Any]],
    company_id: str,
    endpoint: str,
) -> dict[str, Any]:
    response = response_index.get((company_id, endpoint))
    if response is None:
        raise ValueError(f"raw manifest is missing endpoint={endpoint}")
    for field in ("object_key", "sha256", "request_id"):
        _required_string(response, field)
    return response


def _provenance(
    *,
    run_id: str,
    organisation_ref: dict[str, Any],
    documents_ref: dict[str, Any],
    observed_at: datetime,
) -> dict[str, Any]:
    return {
        "source_run_id": run_id,
        "organisation_object_key": organisation_ref["object_key"],
        "organisation_sha256": organisation_ref["sha256"],
        "organisation_request_id": organisation_ref["request_id"],
        "document_list_object_key": documents_ref["object_key"],
        "document_list_sha256": documents_ref["sha256"],
        "document_list_request_id": documents_ref["request_id"],
        "observed_at": observed_at,
    }


def _replace_table(
    *,
    connection: Any,
    schema_name: str,
    table_name: str,
    rows: list[dict[str, Any]],
    arrow_schema: pa.Schema,
) -> None:
    relation_name = f"_{table_name}_arrow"
    arrow_table = pa.Table.from_pylist(rows, schema=arrow_schema)
    connection.register(relation_name, arrow_table)
    try:
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        connection.execute(
            f"CREATE OR REPLACE TABLE {schema_name}.{table_name} AS "
            f"SELECT * FROM {relation_name}"
        )
    finally:
        connection.unregister(relation_name)


def _company_arrow_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("company_id", pa.string(), nullable=False),
            pa.field("name_protection_sequence", pa.uint32()),
            pa.field("identity_type_code", pa.string()),
            pa.field("identity_type_label_original", pa.string()),
            pa.field("active_status_code", pa.string()),
            pa.field("is_active", pa.uint8()),
            pa.field("active_status_producer", pa.string()),
            pa.field("active_status_observed_at", pa.timestamp("us", tz="UTC")),
            pa.field("organisation_registered_on", pa.date32()),
            pa.field("introduced_at_scb", pa.date32()),
            pa.field("organisation_date_producer", pa.string()),
            pa.field("digital_report_document_count", pa.uint32(), nullable=False),
            pa.field("organisation_found", pa.uint8(), nullable=False),
            pa.field("source_run_id", pa.string(), nullable=False),
            pa.field("organisation_object_key", pa.string(), nullable=False),
            pa.field("organisation_sha256", pa.string(), nullable=False),
            pa.field("organisation_request_id", pa.string(), nullable=False),
            pa.field("document_list_object_key", pa.string(), nullable=False),
            pa.field("document_list_sha256", pa.string(), nullable=False),
            pa.field("document_list_request_id", pa.string(), nullable=False),
            pa.field("observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        ]
    )


def _document_arrow_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("company_id", pa.string(), nullable=False),
            pa.field("bolagsverket_document_id", pa.string(), nullable=False),
            pa.field("reporting_period_end", pa.date32()),
            pa.field("filing_registered_on", pa.date32()),
            pa.field("source_file_format", pa.string()),
            pa.field("source_run_id", pa.string(), nullable=False),
            pa.field("document_list_object_key", pa.string(), nullable=False),
            pa.field("document_list_sha256", pa.string(), nullable=False),
            pa.field("document_list_request_id", pa.string(), nullable=False),
            pa.field("observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
        ]
    )


def _active_value(code: str | None) -> int | None:
    if code == "JA":
        return 1
    if code == "NEJ":
        return 0
    return None


def _parse_datetime(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 string")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 string") from exc


def _optional_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise ValueError("source date must be a string or null")
    try:
        return date.fromisoformat(value[:10])
    except ValueError as exc:
        raise ValueError("source date must use ISO-8601 format") from exc


def _optional_object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _required_string(value: dict[str, Any], field: str) -> str:
    result = value.get(field)
    if not isinstance(result, str) or not result:
        raise ValueError(f"{field} must be a non-empty string")
    return result


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_uint(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("name protection sequence must be a non-negative integer")
    return value
