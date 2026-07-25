import json
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dlt
from dlt.extract.resource import DltResource
from dlt.pipeline.pipeline import Pipeline
from dlt.sources.helpers.requests import Client as DltRequestsClient

GLEIF_API_BASE_URL = "https://api.gleif.org/api/v1"
GLEIF_REFERENCE_API_DLT_PIPELINE_NAME = "gleif_reference_api_raw"
GLEIF_REFERENCE_API_DLT_DATASET_NAME = "gleif_reference_api_raw"
GLEIF_RAW_LEI_ISSUERS_API_TABLE = "gleif_raw_lei_issuers_api"
GLEIF_RAW_CODE_LIST_ENTRIES_API_TABLE = "gleif_raw_code_list_entries_api"

GLEIF_API_PAGE_SIZE = 200
GLEIF_API_TIMEOUT_SECONDS = 120
GLEIF_API_MAX_ATTEMPTS = 5
GLEIF_API_RETRY_INITIAL_DELAY_SECONDS = 2.0
GLEIF_API_RETRY_MAX_DELAY_SECONDS = 60.0
GLEIF_API_USER_AGENT = "corpscout-dagster-v3/0.1"

GLEIF_RAW_LEI_ISSUERS_COLUMNS: dict[str, dict[str, Any]] = {
    "lei": {"data_type": "text", "nullable": False},
    "name": {"data_type": "text", "nullable": False},
    "marketing_name": {"data_type": "text"},
    "website": {"data_type": "text"},
    "accreditation_date": {"data_type": "timestamp"},
    "jurisdictions_json": {"data_type": "text", "nullable": False},
    "fund_jurisdictions_json": {"data_type": "text", "nullable": False},
    "source_url": {"data_type": "text", "nullable": False},
    "raw_payload_json": {"data_type": "text", "nullable": False},
    "source_run_id": {"data_type": "text", "nullable": False},
    "retrieved_at": {"data_type": "timestamp", "nullable": False},
}

GLEIF_RAW_CODE_LIST_ENTRIES_COLUMNS: dict[str, dict[str, Any]] = {
    "code_list": {"data_type": "text", "nullable": False},
    "code": {"data_type": "text", "nullable": False},
    "label": {"data_type": "text", "nullable": False},
    "description": {"data_type": "text"},
    "country_iso2": {"data_type": "text"},
    "valid_from": {"data_type": "date"},
    "valid_to": {"data_type": "date"},
    "source_url": {"data_type": "text", "nullable": False},
    "raw_payload_json": {"data_type": "text", "nullable": False},
    "source_run_id": {"data_type": "text", "nullable": False},
    "retrieved_at": {"data_type": "timestamp", "nullable": False},
}


@dlt.source(name="gleif_reference_api")
def gleif_reference_api_source(
    *,
    source_run_id: str,
    retrieved_at: str,
    timeout_seconds: int = GLEIF_API_TIMEOUT_SECONDS,
    session: Any | None = None,
) -> list[DltResource]:
    return [
        _gleif_lei_issuers_resource(
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            timeout_seconds=timeout_seconds,
            session=session,
        ),
        _gleif_code_list_entries_resource(
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            timeout_seconds=timeout_seconds,
            session=session,
        ),
    ]


@dlt.resource(
    name=GLEIF_RAW_LEI_ISSUERS_API_TABLE,
    write_disposition="replace",
    primary_key="lei",
    columns=GLEIF_RAW_LEI_ISSUERS_COLUMNS,
)
def _gleif_lei_issuers_resource(
    *,
    source_run_id: str,
    retrieved_at: str,
    timeout_seconds: int,
    session: Any | None,
) -> Iterator[dict[str, Any]]:
    yield from iter_lei_issuer_rows(
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
        timeout_seconds=timeout_seconds,
        session=session,
    )


@dlt.resource(
    name=GLEIF_RAW_CODE_LIST_ENTRIES_API_TABLE,
    write_disposition="replace",
    primary_key=("code_list", "code"),
    columns=GLEIF_RAW_CODE_LIST_ENTRIES_COLUMNS,
)
def _gleif_code_list_entries_resource(
    *,
    source_run_id: str,
    retrieved_at: str,
    timeout_seconds: int,
    session: Any | None,
) -> Iterator[dict[str, Any]]:
    yield from iter_code_list_entry_rows(
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
        timeout_seconds=timeout_seconds,
        session=session,
    )


def iter_lei_issuer_rows(
    *,
    source_run_id: str,
    retrieved_at: str,
    timeout_seconds: int,
    session: Any | None,
) -> Iterator[dict[str, Any]]:
    http = session or _gleif_api_client(timeout_seconds=timeout_seconds)
    source_url = f"{GLEIF_API_BASE_URL}/lei-issuers"
    seen = False
    for item in _iter_collection_items(
        source_url,
        timeout_seconds=timeout_seconds,
        session=http,
    ):
        seen = True
        attributes = _attributes(item, source_url=source_url)
        lei = _required_text(attributes, "lei", source_url=source_url)
        jurisdiction_url = f"{source_url}/{lei}/jurisdictions"
        fund_jurisdiction_url = f"{source_url}/{lei}/fundJurisdictions"
        jurisdictions = _jurisdiction_codes(
            jurisdiction_url,
            timeout_seconds=timeout_seconds,
            session=http,
        )
        fund_jurisdictions = _jurisdiction_codes(
            fund_jurisdiction_url,
            timeout_seconds=timeout_seconds,
            session=http,
        )
        yield {
            "lei": lei,
            "name": _required_text(attributes, "name", source_url=source_url),
            "marketing_name": _optional_text(attributes.get("marketingName")),
            "website": _optional_text(attributes.get("website")),
            "accreditation_date": _optional_text(attributes.get("accreditationDate")),
            "jurisdictions_json": _compact_json(jurisdictions),
            "fund_jurisdictions_json": _compact_json(fund_jurisdictions),
            "source_url": source_url,
            "raw_payload_json": _compact_json(
                {
                    "issuer": item,
                    "jurisdictions": jurisdictions,
                    "fundJurisdictions": fund_jurisdictions,
                }
            ),
            "source_run_id": source_run_id,
            "retrieved_at": retrieved_at,
        }
    if not seen:
        raise ValueError("GLEIF lei-issuers returned no rows; refusing to replace the table")


def iter_code_list_entry_rows(
    *,
    source_run_id: str,
    retrieved_at: str,
    timeout_seconds: int,
    session: Any | None,
) -> Iterator[dict[str, Any]]:
    http = session or _gleif_api_client(timeout_seconds=timeout_seconds)

    registration_authorities_url = f"{GLEIF_API_BASE_URL}/registration-authorities"
    yield from _mapped_collection_rows(
        source_url=registration_authorities_url,
        code_list="REGISTRATION_AUTHORITY",
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
        timeout_seconds=timeout_seconds,
        session=http,
        row_builder=_registration_authority_row,
    )

    entity_legal_forms_url = f"{GLEIF_API_BASE_URL}/entity-legal-forms"
    yield from _mapped_collection_rows(
        source_url=entity_legal_forms_url,
        code_list="ENTITY_LEGAL_FORM",
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
        timeout_seconds=timeout_seconds,
        session=http,
        row_builder=_entity_legal_form_row,
    )

    official_roles_url = f"{GLEIF_API_BASE_URL}/official-organizational-roles"
    yield from _mapped_collection_rows(
        source_url=official_roles_url,
        code_list="OFFICIAL_ORGANIZATIONAL_ROLE",
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
        timeout_seconds=timeout_seconds,
        session=http,
        row_builder=_official_organizational_role_row,
    )

    jurisdictions_url = f"{GLEIF_API_BASE_URL}/jurisdictions"
    yield from _mapped_collection_rows(
        source_url=jurisdictions_url,
        code_list="ACCEPTED_LEGAL_JURISDICTION",
        source_run_id=source_run_id,
        retrieved_at=retrieved_at,
        timeout_seconds=timeout_seconds,
        session=http,
        row_builder=_accepted_jurisdiction_row,
    )


def gleif_reference_api_dlt_pipeline(database_path: str | Path) -> Pipeline:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    return dlt.pipeline(
        pipeline_name=GLEIF_REFERENCE_API_DLT_PIPELINE_NAME,
        destination=dlt.destinations.duckdb(str(database_file)),
        dataset_name=GLEIF_REFERENCE_API_DLT_DATASET_NAME,
        dev_mode=False,
        pipelines_dir=str(database_file.parent / ".dlt" / "gleif_reference_api"),
    )


def load_gleif_reference_api_raw_tables(
    *,
    database_path: str | Path,
    source_run_id: str,
    retrieved_at: str,
    timeout_seconds: int,
    session: Any | None,
) -> None:
    pipeline = gleif_reference_api_dlt_pipeline(database_path)
    pipeline.drop_pending_packages()
    load_info = pipeline.run(
        gleif_reference_api_source(
            source_run_id=source_run_id,
            retrieved_at=retrieved_at,
            timeout_seconds=timeout_seconds,
            session=session,
        )
    )
    load_info.raise_on_failed_jobs()


def _gleif_api_client(*, timeout_seconds: int) -> DltRequestsClient:
    return DltRequestsClient(
        request_timeout=timeout_seconds,
        request_max_attempts=GLEIF_API_MAX_ATTEMPTS,
        request_backoff_factor=GLEIF_API_RETRY_INITIAL_DELAY_SECONDS,
        request_max_retry_delay=GLEIF_API_RETRY_MAX_DELAY_SECONDS,
        respect_retry_after_header=True,
        session_attrs={"headers": {"User-Agent": GLEIF_API_USER_AGENT}},
    )


def _iter_collection_items(
    source_url: str,
    *,
    timeout_seconds: int,
    session: Any,
) -> Iterator[dict[str, Any]]:
    page_number = 1
    while True:
        params = {
            "page[number]": page_number,
            "page[size]": GLEIF_API_PAGE_SIZE,
        }
        response = session.get(source_url, params=params, timeout=timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"GLEIF API {source_url} returned a non-object payload")
        data = payload.get("data")
        if not isinstance(data, list):
            raise ValueError(f"GLEIF API {source_url} returned no data array")
        for item in data:
            if not isinstance(item, dict):
                raise ValueError(f"GLEIF API {source_url} returned a non-object data item")
            yield item
        last_page = _last_page(payload, source_url=source_url)
        if page_number >= last_page:
            break
        page_number += 1


def _last_page(payload: dict[str, Any], *, source_url: str) -> int:
    meta = payload.get("meta")
    pagination = meta.get("pagination") if isinstance(meta, dict) else None
    last_page = pagination.get("lastPage", 1) if isinstance(pagination, dict) else 1
    if not isinstance(last_page, int) or isinstance(last_page, bool) or last_page < 1:
        raise ValueError(f"GLEIF API {source_url} returned an invalid lastPage")
    return last_page


def _mapped_collection_rows(
    *,
    source_url: str,
    code_list: str,
    source_run_id: str,
    retrieved_at: str,
    timeout_seconds: int,
    session: Any,
    row_builder: Callable[..., dict[str, Any]],
) -> Iterator[dict[str, Any]]:
    seen = False
    for item in _iter_collection_items(
        source_url,
        timeout_seconds=timeout_seconds,
        session=session,
    ):
        seen = True
        yield {
            **row_builder(item, source_url=source_url),
            "code_list": code_list,
            "source_url": source_url,
            "raw_payload_json": _compact_json(item),
            "source_run_id": source_run_id,
            "retrieved_at": retrieved_at,
        }
    if not seen:
        endpoint_name = source_url.rsplit("/", maxsplit=1)[-1]
        raise ValueError(
            f"GLEIF {endpoint_name} returned no rows; refusing to replace the table"
        )


def _registration_authority_row(
    item: dict[str, Any],
    *,
    source_url: str,
) -> dict[str, Any]:
    attributes = _attributes(item, source_url=source_url)
    code = _required_text(attributes, "code", source_url=source_url)
    label = (
        _optional_text(attributes.get("internationalName"))
        or _optional_text(attributes.get("localName"))
        or code
    )
    description = _first_distinct_text(
        label,
        attributes.get("internationalOrganizationName"),
        attributes.get("localOrganizationName"),
        attributes.get("localName"),
        attributes.get("website"),
    )
    jurisdictions = attributes.get("jurisdictions")
    country_codes: list[str] = []
    if isinstance(jurisdictions, list):
        country_codes = sorted(
            {
                country_code
                for jurisdiction in jurisdictions
                if isinstance(jurisdiction, dict)
                if (
                    country_code := _optional_text(jurisdiction.get("countryCode"))
                )
                is not None
            }
        )
    return {
        "code": code,
        "label": label,
        "description": description,
        "country_iso2": country_codes[0] if len(country_codes) == 1 else None,
        "valid_from": None,
        "valid_to": None,
    }


def _entity_legal_form_row(
    item: dict[str, Any],
    *,
    source_url: str,
) -> dict[str, Any]:
    attributes = _attributes(item, source_url=source_url)
    code = _required_text(attributes, "code", source_url=source_url)
    label, description = _localized_names(
        attributes.get("names"),
        native_key="localName",
    )
    return {
        "code": code,
        "label": label or code,
        "description": description,
        "country_iso2": _optional_text(attributes.get("countryCode")),
        "valid_from": _optional_text(attributes.get("dateCreated")),
        "valid_to": None,
    }


def _official_organizational_role_row(
    item: dict[str, Any],
    *,
    source_url: str,
) -> dict[str, Any]:
    attributes = _attributes(item, source_url=source_url)
    code = _required_text(attributes, "code", source_url=source_url)
    label, description = _localized_names(
        attributes.get("names"),
        native_key="name",
    )
    elf_code = _optional_text(attributes.get("elfCode"))
    if elf_code is not None:
        description = (
            f"{description} | ELF {elf_code}" if description is not None else f"ELF {elf_code}"
        )
    return {
        "code": code,
        "label": label or code,
        "description": description,
        "country_iso2": _optional_text(attributes.get("countryCode")),
        "valid_from": _optional_text(attributes.get("dateCreated")),
        "valid_to": None,
    }


def _accepted_jurisdiction_row(
    item: dict[str, Any],
    *,
    source_url: str,
) -> dict[str, Any]:
    attributes = _attributes(item, source_url=source_url)
    code = _required_text(attributes, "code", source_url=source_url)
    country_code = code.split("-", maxsplit=1)[0]
    return {
        "code": code,
        "label": _optional_text(attributes.get("name")) or code,
        "description": None,
        "country_iso2": country_code if len(country_code) == 2 else None,
        "valid_from": None,
        "valid_to": None,
    }


def _jurisdiction_codes(
    source_url: str,
    *,
    timeout_seconds: int,
    session: Any,
) -> list[str]:
    return sorted(
        {
            country_code
            for item in _iter_collection_items(
                source_url,
                timeout_seconds=timeout_seconds,
                session=session,
            )
            if (
                country_code := _optional_text(
                    _attributes(item, source_url=source_url).get("countryCode")
                )
            )
            is not None
        }
    )


def _localized_names(
    value: Any,
    *,
    native_key: str,
) -> tuple[str | None, str | None]:
    if not isinstance(value, list):
        return None, None
    labels: list[str] = []
    native_names: list[str] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        native_name = _optional_text(item.get(native_key))
        transliterated_name = _optional_text(item.get("transliteratedName"))
        label = transliterated_name or native_name
        if label is not None and label not in labels:
            labels.append(label)
        if native_name is not None and native_name != label and native_name not in native_names:
            native_names.append(native_name)
    return (
        labels[0] if labels else None,
        " | ".join([*labels[1:], *native_names]) or None,
    )


def _attributes(item: dict[str, Any], *, source_url: str) -> dict[str, Any]:
    attributes = item.get("attributes")
    if not isinstance(attributes, dict):
        raise ValueError(f"GLEIF API {source_url} item has no attributes object")
    return attributes


def _required_text(
    attributes: dict[str, Any],
    key: str,
    *,
    source_url: str,
) -> str:
    value = _optional_text(attributes.get(key))
    if value is None:
        raise ValueError(f"GLEIF API {source_url} item has no {key}")
    return value


def _first_distinct_text(primary: str, *values: Any) -> str | None:
    for value in values:
        text = _optional_text(value)
        if text is not None and text != primary:
            return text
    return None


def _optional_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
