import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, Self

import dagster as dg
import duckdb
import pyarrow as pa
from dagster_duckdb import DuckDBResource
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    duckdb_resource,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.denmark_cvr.assets import DENMARK_CVR_BUCKET
from dagster_v3.defs.denmark_cvr.models import (
    CompanySearchResult,
    PersonSearchResult,
)
from dagster_v3.defs.denmark_cvr.resources import (
    DATACVR_COMPANY_ENTITY_TYPE,
    DATACVR_PERSON_ENTITY_TYPE,
)

type DenmarkCvrSearchEntityType = Literal["virksomhed", "person"]

DENMARK_CVR_DUCKDB_PATH = Path("data/denmark_cvr_source.duckdb")
DENMARK_CVR_DUCKDB_SCHEMA = "denmark_cvr"
DENMARK_CVR_COMPANIES_TABLE = "companies"
DENMARK_CVR_PRODUCTION_UNITS_TABLE = "production_units"
DENMARK_CVR_PERSONS_TABLE = "persons"
DENMARK_CVR_INGESTED_OBJECTS_TABLE = "ingested_objects"
DENMARK_CVR_DUCKDB_POOL = "denmark_cvr_duckdb"
DENMARK_CVR_SOURCE_PREFIXES = (
    "denmark_cvr/backfill/",
    "denmark_cvr/active/",
)
DENMARK_CVR_RESULT_FILENAMES: dict[DenmarkCvrSearchEntityType, frozenset[str]] = {
    DATACVR_COMPANY_ENTITY_TYPE: frozenset(
        {"companies.json", "companies_incomplete.json"}
    ),
    DATACVR_PERSON_ENTITY_TYPE: frozenset({"persons.json", "persons_incomplete.json"}),
}

_SOURCE_COLUMNS = (
    "source_capture_type",
    "source_partition_key",
    "source_object_key",
    "source_run_id",
    "source_retrieved_at",
    "source_row_number",
    "source_payload_hash",
    "raw_record",
    "ingestion_run_id",
    "ingested_at",
)

_COMPANY_COLUMNS = (
    "cvr",
    "entity_number",
    "name",
    "address",
    "city",
    "co_name",
    "postal_code",
    "register",
    "email",
    "phone",
    "industry",
    "legal_form",
    "status",
    "start_date",
    "cessation_date",
    "advertising_protected",
    "pseudo_cvr",
    "display_name_postfix",
    "highlight_secondary_name",
    "highlight_historical_secondary_name",
    "highlight_historical_primary_name",
    *_SOURCE_COLUMNS,
)

_SOURCE_ARROW_FIELDS = (
    pa.field("source_capture_type", pa.string(), nullable=False),
    pa.field("source_partition_key", pa.string(), nullable=False),
    pa.field("source_object_key", pa.string(), nullable=False),
    pa.field("source_run_id", pa.string(), nullable=False),
    pa.field("source_retrieved_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("source_row_number", pa.int64(), nullable=False),
    pa.field("source_payload_hash", pa.string(), nullable=False),
    pa.field("raw_record", pa.string(), nullable=False),
    pa.field("ingestion_run_id", pa.string(), nullable=False),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
)

_COMPANY_ARROW_SCHEMA = pa.schema(
    [
        pa.field("cvr", pa.string(), nullable=False),
        pa.field("entity_number", pa.string(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("address", pa.string(), nullable=False),
        pa.field("city", pa.string()),
        pa.field("co_name", pa.string()),
        pa.field("postal_code", pa.string()),
        pa.field("register", pa.string()),
        pa.field("email", pa.string()),
        pa.field("phone", pa.string()),
        pa.field("industry", pa.string()),
        pa.field("legal_form", pa.string()),
        pa.field("status", pa.string(), nullable=False),
        pa.field("start_date", pa.date32(), nullable=False),
        pa.field("cessation_date", pa.date32()),
        pa.field("advertising_protected", pa.bool_(), nullable=False),
        pa.field("pseudo_cvr", pa.bool_(), nullable=False),
        pa.field("display_name_postfix", pa.bool_(), nullable=False),
        pa.field("highlight_secondary_name", pa.bool_(), nullable=False),
        pa.field("highlight_historical_secondary_name", pa.bool_(), nullable=False),
        pa.field("highlight_historical_primary_name", pa.bool_(), nullable=False),
        *_SOURCE_ARROW_FIELDS,
    ]
)

_PERSON_COLUMNS = (
    "entity_number",
    "name",
    "address",
    "city",
    "co_name",
    "postal_code",
    "person_type",
    "has_active_relations",
    "active_affiliations",
    "affiliations",
    *_SOURCE_COLUMNS,
)

_PERSON_ARROW_SCHEMA = pa.schema(
    [
        pa.field("entity_number", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("address", pa.string(), nullable=False),
        pa.field("city", pa.string()),
        pa.field("co_name", pa.string()),
        pa.field("postal_code", pa.string()),
        pa.field("person_type", pa.string(), nullable=False),
        pa.field("has_active_relations", pa.bool_(), nullable=False),
        pa.field("active_affiliations", pa.string(), nullable=False),
        pa.field("affiliations", pa.string(), nullable=False),
        *_SOURCE_ARROW_FIELDS,
    ]
)


class DenmarkCvrStoredCaptureMetadata(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_version: Literal[1]
    source: Literal["denmark_cvr"]
    source_url: str
    partition_key: str
    start_date: date
    end_date: date
    retrieved_at: datetime
    run_id: str
    is_complete: bool
    generic_advertised_count: int = Field(ge=0)
    filtered_advertised_count: int = Field(ge=0)
    downloaded_entity_count: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_metadata(self) -> Self:
        if self.start_date > self.end_date:
            raise ValueError("capture start date must not exceed end date")
        if self.retrieved_at.utcoffset() is None:
            raise ValueError("capture retrieval timestamp must include a timezone")
        return self


class DenmarkCvrStoredCompanyCapture(DenmarkCvrStoredCaptureMetadata):
    entity_type: Literal["virksomhed"]
    enheder: list[CompanySearchResult]

    @model_validator(mode="after")
    def validate_entity_count(self) -> Self:
        if self.downloaded_entity_count != len(self.enheder):
            raise ValueError("capture downloaded count must match company rows")
        return self


@dataclass(frozen=True)
class ParsedDenmarkCvrCompanyCapture:
    capture: DenmarkCvrStoredCompanyCapture
    raw_entities: tuple[dict[str, Any], ...]


class DenmarkCvrStoredPersonCapture(DenmarkCvrStoredCaptureMetadata):
    entity_type: Literal["person"]
    enheder: list[PersonSearchResult]

    @model_validator(mode="after")
    def validate_entity_count(self) -> Self:
        if self.downloaded_entity_count != len(self.enheder):
            raise ValueError("capture downloaded count must match person rows")
        return self


@dataclass(frozen=True)
class ParsedDenmarkCvrPersonCapture:
    capture: DenmarkCvrStoredPersonCapture
    raw_entities: tuple[dict[str, Any], ...]


type ParsedDenmarkCvrCapture = (
    ParsedDenmarkCvrCompanyCapture | ParsedDenmarkCvrPersonCapture
)


@dataclass(frozen=True)
class DenmarkCvrDuckdbSummary:
    database_path: str
    discovered_object_count: int
    already_ingested_object_count: int
    processed_object_count: int
    processed_row_count: int
    processed_size_bytes: int
    entity_count: int
    incomplete_object_count: int
    min_start_date: date | None
    max_start_date: date | None
    database_size_bytes: int


class DenmarkCvrStoredObjectError(ValueError):
    def __init__(self, object_key: str, issue_summary: str) -> None:
        super().__init__(
            f"Denmark CVR stored object failed validation: "
            f"object_key={object_key} issues={issue_summary}"
        )
        self.object_key = object_key
        self.issue_summary = issue_summary


def source_result_object_keys(
    object_store: ObjectStoreResource,
    *,
    entity_type: DenmarkCvrSearchEntityType,
) -> tuple[str, ...]:
    result_filenames = DENMARK_CVR_RESULT_FILENAMES[entity_type]
    keys: list[str] = []
    for prefix in DENMARK_CVR_SOURCE_PREFIXES:
        prefix_keys = object_store.list_keys(prefix, bucket=DENMARK_CVR_BUCKET)
        keys.extend(
            key
            for key in sorted(prefix_keys)
            if key.rsplit("/", maxsplit=1)[-1] in result_filenames
        )
    return tuple(dict.fromkeys(keys))


def update_denmark_cvr_companies_duckdb(
    *,
    object_store: ObjectStoreResource,
    denmark_cvr_duckdb: DuckDBResource,
    ingestion_run_id: str,
    processed_at: datetime,
    log_info: Callable[..., object] | None = None,
) -> DenmarkCvrDuckdbSummary:
    return _update_denmark_cvr_entity_duckdb(
        object_store=object_store,
        denmark_cvr_duckdb=denmark_cvr_duckdb,
        entity_type=DATACVR_COMPANY_ENTITY_TYPE,
        ingestion_run_id=ingestion_run_id,
        processed_at=processed_at,
        log_info=log_info,
    )


def update_denmark_cvr_persons_duckdb(
    *,
    object_store: ObjectStoreResource,
    denmark_cvr_duckdb: DuckDBResource,
    ingestion_run_id: str,
    processed_at: datetime,
    log_info: Callable[..., object] | None = None,
) -> DenmarkCvrDuckdbSummary:
    return _update_denmark_cvr_entity_duckdb(
        object_store=object_store,
        denmark_cvr_duckdb=denmark_cvr_duckdb,
        entity_type=DATACVR_PERSON_ENTITY_TYPE,
        ingestion_run_id=ingestion_run_id,
        processed_at=processed_at,
        log_info=log_info,
    )


def _update_denmark_cvr_entity_duckdb(
    *,
    object_store: ObjectStoreResource,
    denmark_cvr_duckdb: DuckDBResource,
    entity_type: DenmarkCvrSearchEntityType,
    ingestion_run_id: str,
    processed_at: datetime,
    log_info: Callable[..., object] | None,
) -> DenmarkCvrDuckdbSummary:
    if ingestion_run_id.strip() == "":
        raise ValueError("Denmark CVR ingestion run ID must not be blank")
    if processed_at.utcoffset() is None:
        raise ValueError("Denmark CVR processing timestamp must include a timezone")

    database_path = duckdb_database_path(denmark_cvr_duckdb)
    if str(database_path) != ":memory:":
        database_path.parent.mkdir(parents=True, exist_ok=True)
    source_keys = source_result_object_keys(
        object_store,
        entity_type=entity_type,
    )
    processed_row_count = 0
    processed_size_bytes = 0
    processed_object_count = 0

    with denmark_cvr_duckdb.get_connection() as connection:
        _ensure_denmark_cvr_tables(connection)
        ingested_keys = {
            row[0]
            for row in connection.execute(
                f"select object_key from {DENMARK_CVR_DUCKDB_SCHEMA}."
                f"{DENMARK_CVR_INGESTED_OBJECTS_TABLE}"
            ).fetchall()
        }
        pending_keys = tuple(key for key in source_keys if key not in ingested_keys)
        connection.execute("begin transaction")
        try:
            for object_index, object_key in enumerate(pending_keys, start=1):
                try:
                    raw_body = object_store.read_bytes(
                        object_key,
                        bucket=DENMARK_CVR_BUCKET,
                    )
                    parsed = _parse_stored_capture(
                        raw_body,
                        object_key=object_key,
                        entity_type=entity_type,
                    )
                    rows = _normalized_entity_rows(
                        parsed,
                        object_key=object_key,
                        ingestion_run_id=ingestion_run_id,
                        processed_at=processed_at,
                    )
                    _upsert_entity_rows(
                        connection,
                        rows,
                        entity_type=entity_type,
                    )
                    _record_ingested_object(
                        connection,
                        parsed=parsed,
                        object_key=object_key,
                        raw_body=raw_body,
                        ingestion_run_id=ingestion_run_id,
                        processed_at=processed_at,
                    )
                except DenmarkCvrStoredObjectError:
                    raise
                except Exception as exc:
                    raise DenmarkCvrStoredObjectError(
                        object_key,
                        f"processing:{type(exc).__name__}",
                    ) from None
                processed_object_count += 1
                processed_row_count += len(rows)
                processed_size_bytes += len(raw_body)
                if log_info is not None:
                    log_info(
                        "Denmark CVR %s DuckDB progress: object=%s/%s object_key=%s "
                        "object_rows=%s processed_rows=%s processed_bytes=%s",
                        entity_type,
                        object_index,
                        len(pending_keys),
                        object_key,
                        len(rows),
                        processed_row_count,
                        processed_size_bytes,
                    )
            connection.execute("commit")
        except Exception:
            connection.execute("rollback")
            raise

        entity_count, min_start_date, max_start_date = _entity_table_statistics(
            connection,
            entity_type=entity_type,
        )
        ingested_object_completeness = connection.execute(
            f"""
            select object_key, is_complete
            from {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_INGESTED_OBJECTS_TABLE}
            """
        ).fetchall()
        source_key_set = set(source_keys)
        incomplete_object_count = sum(
            not is_complete
            for object_key, is_complete in ingested_object_completeness
            if object_key in source_key_set
        )

    database_size_bytes = (
        database_path.stat().st_size
        if str(database_path) != ":memory:" and database_path.exists()
        else 0
    )
    return DenmarkCvrDuckdbSummary(
        database_path=str(database_path),
        discovered_object_count=len(source_keys),
        already_ingested_object_count=len(source_keys) - len(pending_keys),
        processed_object_count=processed_object_count,
        processed_row_count=processed_row_count,
        processed_size_bytes=processed_size_bytes,
        entity_count=int(entity_count),
        incomplete_object_count=int(incomplete_object_count),
        min_start_date=min_start_date,
        max_start_date=max_start_date,
        database_size_bytes=database_size_bytes,
    )


def _ensure_denmark_cvr_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema if not exists {DENMARK_CVR_DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create table if not exists
          {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_COMPANIES_TABLE} (
            cvr varchar primary key,
            entity_number varchar not null,
            name varchar,
            address varchar not null,
            city varchar,
            co_name varchar,
            postal_code varchar,
            register varchar,
            email varchar,
            phone varchar,
            industry varchar,
            legal_form varchar,
            status varchar not null,
            start_date date not null,
            cessation_date date,
            advertising_protected boolean not null,
            pseudo_cvr boolean not null,
            display_name_postfix boolean not null,
            highlight_secondary_name boolean not null,
            highlight_historical_secondary_name boolean not null,
            highlight_historical_primary_name boolean not null,
            source_capture_type varchar not null,
            source_partition_key varchar not null,
            source_object_key varchar not null,
            source_run_id varchar not null,
            source_retrieved_at timestamptz not null,
            source_row_number bigint not null,
            source_payload_hash varchar not null,
            raw_record json not null,
            ingestion_run_id varchar not null,
            ingested_at timestamptz not null
          )
        """
    )
    connection.execute(
        f"""
        create table if not exists
          {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PERSONS_TABLE} (
            entity_number varchar primary key,
            name varchar not null,
            address varchar not null,
            city varchar,
            co_name varchar,
            postal_code varchar,
            person_type varchar not null,
            has_active_relations boolean not null,
            active_affiliations json not null,
            affiliations json not null,
            source_capture_type varchar not null,
            source_partition_key varchar not null,
            source_object_key varchar not null,
            source_run_id varchar not null,
            source_retrieved_at timestamptz not null,
            source_row_number bigint not null,
            source_payload_hash varchar not null,
            raw_record json not null,
            ingestion_run_id varchar not null,
            ingested_at timestamptz not null
          )
        """
    )
    connection.execute(
        f"""
        create table if not exists
          {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_INGESTED_OBJECTS_TABLE} (
            object_key varchar primary key,
            source_capture_type varchar not null,
            partition_key varchar not null,
            is_complete boolean not null,
            source_run_id varchar not null,
            source_retrieved_at timestamptz not null,
            generic_advertised_count bigint not null,
            filtered_advertised_count bigint not null,
            source_row_count bigint not null,
            payload_sha256 varchar not null,
            payload_size_bytes bigint not null,
            ingestion_run_id varchar not null,
            processed_at timestamptz not null
          )
        """
    )


def _parse_stored_capture(
    raw_body: bytes,
    *,
    object_key: str,
    entity_type: DenmarkCvrSearchEntityType,
) -> ParsedDenmarkCvrCapture:
    try:
        payload = json.loads(raw_body)
    except UnicodeDecodeError, json.JSONDecodeError:
        raise DenmarkCvrStoredObjectError(object_key, "invalid_json") from None
    try:
        if entity_type == DATACVR_COMPANY_ENTITY_TYPE:
            capture = DenmarkCvrStoredCompanyCapture.model_validate(payload)
        else:
            capture = DenmarkCvrStoredPersonCapture.model_validate(payload)
    except ValidationError as exc:
        issues = ",".join(
            f"{'.'.join(str(part) for part in issue['loc'])}:{issue['type']}"
            for issue in exc.errors(include_input=False, include_url=False)[:10]
        )
        raise DenmarkCvrStoredObjectError(
            object_key,
            issues or "schema_validation",
        ) from None
    if not isinstance(payload, dict) or not isinstance(payload.get("enheder"), list):
        raise DenmarkCvrStoredObjectError(object_key, "enheder:not_list")
    raw_entities = payload["enheder"]
    if any(not isinstance(entity, dict) for entity in raw_entities):
        raise DenmarkCvrStoredObjectError(object_key, "enheder:item_not_object")
    if isinstance(capture, DenmarkCvrStoredCompanyCapture):
        return ParsedDenmarkCvrCompanyCapture(
            capture=capture,
            raw_entities=tuple(raw_entities),
        )
    return ParsedDenmarkCvrPersonCapture(
        capture=capture,
        raw_entities=tuple(raw_entities),
    )


def _normalized_entity_rows(
    parsed: ParsedDenmarkCvrCapture,
    *,
    object_key: str,
    ingestion_run_id: str,
    processed_at: datetime,
) -> list[dict[str, Any]]:
    if isinstance(parsed, ParsedDenmarkCvrCompanyCapture):
        return _normalized_company_rows(
            parsed,
            object_key=object_key,
            ingestion_run_id=ingestion_run_id,
            processed_at=processed_at,
        )
    return _normalized_person_rows(
        parsed,
        object_key=object_key,
        ingestion_run_id=ingestion_run_id,
        processed_at=processed_at,
    )


def _normalized_company_rows(
    parsed: ParsedDenmarkCvrCompanyCapture,
    *,
    object_key: str,
    ingestion_run_id: str,
    processed_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row_number, (company, raw_entity) in enumerate(
        zip(parsed.capture.enheder, parsed.raw_entities, strict=True)
    ):
        rows.append(
            {
                "cvr": company.cvr,
                "entity_number": company.enhedsnummer,
                "name": company.seneste_navn,
                "address": company.beliggenhedsadresse,
                "city": company.by,
                "co_name": company.co_navn,
                "postal_code": company.postnummer,
                "register": company.reg,
                "email": company.email,
                "phone": company.telefonnummer,
                "industry": company.hovedbranche,
                "legal_form": company.virksomhedsform,
                "status": company.status,
                "start_date": company.start_dato,
                "cessation_date": company.ophoers_dato,
                "advertising_protected": company.reklame_beskyttet,
                "pseudo_cvr": company.har_pseudo_cvr,
                "display_name_postfix": company.vis_navn_postfix,
                "highlight_secondary_name": company.highlight_binavn,
                "highlight_historical_secondary_name": (
                    company.highlight_historisk_binavn
                ),
                "highlight_historical_primary_name": (
                    company.highlight_historisk_hovednavn
                ),
                **_source_row_metadata(
                    capture=parsed.capture,
                    raw_entity=raw_entity,
                    object_key=object_key,
                    row_number=row_number,
                    ingestion_run_id=ingestion_run_id,
                    processed_at=processed_at,
                ),
            }
        )
    return rows


def _normalized_person_rows(
    parsed: ParsedDenmarkCvrPersonCapture,
    *,
    object_key: str,
    ingestion_run_id: str,
    processed_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row_number, (person, raw_entity) in enumerate(
        zip(parsed.capture.enheder, parsed.raw_entities, strict=True)
    ):
        rows.append(
            {
                "entity_number": person.enhedsnummer,
                "name": person.seneste_navn,
                "address": person.beliggenhedsadresse,
                "city": person.by,
                "co_name": person.co_navn,
                "postal_code": person.postnummer,
                "person_type": person.person_type,
                "has_active_relations": person.har_aktive_relationer,
                "active_affiliations": json.dumps(
                    person.aktive_tilknytninger,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "affiliations": json.dumps(
                    person.tilknytning,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                **_source_row_metadata(
                    capture=parsed.capture,
                    raw_entity=raw_entity,
                    object_key=object_key,
                    row_number=row_number,
                    ingestion_run_id=ingestion_run_id,
                    processed_at=processed_at,
                ),
            }
        )
    return rows


def _source_row_metadata(
    *,
    capture: DenmarkCvrStoredCaptureMetadata,
    raw_entity: dict[str, Any],
    object_key: str,
    row_number: int,
    ingestion_run_id: str,
    processed_at: datetime,
) -> dict[str, Any]:
    raw_record = json.dumps(
        raw_entity,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "source_capture_type": _capture_type(object_key),
        "source_partition_key": capture.partition_key,
        "source_object_key": object_key,
        "source_run_id": capture.run_id,
        "source_retrieved_at": capture.retrieved_at.astimezone(UTC),
        "source_row_number": row_number,
        "source_payload_hash": hashlib.sha256(raw_record.encode("utf-8")).hexdigest(),
        "raw_record": raw_record,
        "ingestion_run_id": ingestion_run_id,
        "ingested_at": processed_at.astimezone(UTC),
    }


def _upsert_entity_rows(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
    *,
    entity_type: DenmarkCvrSearchEntityType,
) -> None:
    if not rows:
        return
    if entity_type == DATACVR_COMPANY_ENTITY_TYPE:
        table_name = DENMARK_CVR_COMPANIES_TABLE
        registered_table_name = "denmark_cvr_company_rows"
        entity_columns = _COMPANY_COLUMNS
        arrow_schema = _COMPANY_ARROW_SCHEMA
        primary_key = "cvr"
    else:
        table_name = DENMARK_CVR_PERSONS_TABLE
        registered_table_name = "denmark_cvr_person_rows"
        entity_columns = _PERSON_COLUMNS
        arrow_schema = _PERSON_ARROW_SCHEMA
        primary_key = "entity_number"
    connection.register(
        registered_table_name,
        pa.Table.from_pylist(rows, schema=arrow_schema),
    )
    columns = ", ".join(entity_columns)
    try:
        connection.execute(
            f"""
            insert or replace into
              {DENMARK_CVR_DUCKDB_SCHEMA}.{table_name} ({columns})
            select {columns}
            from {registered_table_name}
            qualify row_number() over (
              partition by {primary_key} order by source_row_number desc
            ) = 1
            """
        )
    finally:
        connection.unregister(registered_table_name)


def _entity_table_statistics(
    connection: duckdb.DuckDBPyConnection,
    *,
    entity_type: DenmarkCvrSearchEntityType,
) -> tuple[int, date | None, date | None]:
    if entity_type == DATACVR_COMPANY_ENTITY_TYPE:
        table_name = DENMARK_CVR_COMPANIES_TABLE
    else:
        person_count = connection.execute(
            f"select count(*) from {DENMARK_CVR_DUCKDB_SCHEMA}."
            f"{DENMARK_CVR_PERSONS_TABLE}"
        ).fetchone()[0]
        return int(person_count), None, None
    entity_count, min_start_date, max_start_date = connection.execute(
        f"""
        select count(*), min(start_date), max(start_date)
        from {DENMARK_CVR_DUCKDB_SCHEMA}.{table_name}
        """
    ).fetchone()
    return int(entity_count), min_start_date, max_start_date


def _record_ingested_object(
    connection: duckdb.DuckDBPyConnection,
    *,
    parsed: ParsedDenmarkCvrCapture,
    object_key: str,
    raw_body: bytes,
    ingestion_run_id: str,
    processed_at: datetime,
) -> None:
    connection.execute(
        f"""
        insert into {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_INGESTED_OBJECTS_TABLE}
          values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            object_key,
            _capture_type(object_key),
            parsed.capture.partition_key,
            parsed.capture.is_complete,
            parsed.capture.run_id,
            parsed.capture.retrieved_at.astimezone(UTC),
            parsed.capture.generic_advertised_count,
            parsed.capture.filtered_advertised_count,
            len(parsed.capture.enheder),
            hashlib.sha256(raw_body).hexdigest(),
            len(raw_body),
            ingestion_run_id,
            processed_at.astimezone(UTC),
        ],
    )


def _capture_type(object_key: str) -> Literal["backfill", "active"]:
    if object_key.startswith(DENMARK_CVR_SOURCE_PREFIXES[0]):
        return "backfill"
    if object_key.startswith(DENMARK_CVR_SOURCE_PREFIXES[1]):
        return "active"
    raise ValueError(f"Unknown Denmark CVR source object key: {object_key}")


def _duckdb_materialization_metadata(
    summary: DenmarkCvrDuckdbSummary,
    *,
    table_name: str,
) -> dict[str, Any]:
    return {
        "database_path": summary.database_path,
        "duckdb_schema": DENMARK_CVR_DUCKDB_SCHEMA,
        "duckdb_table": table_name,
        "state_table": DENMARK_CVR_INGESTED_OBJECTS_TABLE,
        "discovered_object_count": summary.discovered_object_count,
        "already_ingested_object_count": summary.already_ingested_object_count,
        "processed_object_count": summary.processed_object_count,
        "processed_row_count": summary.processed_row_count,
        "processed_size_bytes": summary.processed_size_bytes,
        "incomplete_object_count": summary.incomplete_object_count,
        "min_start_date": (
            summary.min_start_date.isoformat() if summary.min_start_date else None
        ),
        "max_start_date": (
            summary.max_start_date.isoformat() if summary.max_start_date else None
        ),
        "database_size_bytes": summary.database_size_bytes,
    }


@dg.asset(
    deps=[
        dg.AssetKey("denmark_cvr_backfill_s3"),
        dg.AssetKey("denmark_cvr_active_s3"),
    ],
    group_name="denmark_cvr",
    kinds={"python", "s3", "json", "duckdb"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "layer": "normalized",
    },
    pool=DENMARK_CVR_DUCKDB_POOL,
    metadata={
        "duckdb_schema": DENMARK_CVR_DUCKDB_SCHEMA,
        "duckdb_table": DENMARK_CVR_COMPANIES_TABLE,
    },
    description=(
        "Incrementally normalizes Denmark CVR backfill and daily company JSON "
        "objects into one CVR-deduplicated DuckDB companies table."
    ),
)
def denmark_cvr_companies_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    denmark_cvr_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    summary = update_denmark_cvr_companies_duckdb(
        object_store=object_store,
        denmark_cvr_duckdb=denmark_cvr_duckdb,
        ingestion_run_id=context.run_id,
        processed_at=datetime.now(UTC),
        log_info=context.log.info,
    )
    context.log.info(
        "Denmark CVR DuckDB complete: discovered_objects=%s existing_objects=%s "
        "processed_objects=%s processed_rows=%s companies=%s incomplete_objects=%s "
        "database_bytes=%s",
        summary.discovered_object_count,
        summary.already_ingested_object_count,
        summary.processed_object_count,
        summary.processed_row_count,
        summary.entity_count,
        summary.incomplete_object_count,
        summary.database_size_bytes,
    )
    return dg.MaterializeResult(
        metadata={
            **_duckdb_materialization_metadata(
                summary,
                table_name=DENMARK_CVR_COMPANIES_TABLE,
            ),
            "company_count": summary.entity_count,
        }
    )


@dg.asset(
    deps=[
        dg.AssetKey("denmark_cvr_persons_backfill_s3"),
        dg.AssetKey("denmark_cvr_persons_active_s3"),
    ],
    group_name="denmark_cvr",
    kinds={"python", "s3", "json", "duckdb"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "entity_type": DATACVR_PERSON_ENTITY_TYPE,
        "layer": "normalized",
    },
    pool=DENMARK_CVR_DUCKDB_POOL,
    metadata={
        "duckdb_schema": DENMARK_CVR_DUCKDB_SCHEMA,
        "duckdb_table": DENMARK_CVR_PERSONS_TABLE,
    },
    description=(
        "Incrementally normalizes Denmark CVR backfill and daily person JSON "
        "objects into one entity-number-deduplicated DuckDB table."
    ),
)
def denmark_cvr_persons_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    denmark_cvr_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    summary = update_denmark_cvr_persons_duckdb(
        object_store=object_store,
        denmark_cvr_duckdb=denmark_cvr_duckdb,
        ingestion_run_id=context.run_id,
        processed_at=datetime.now(UTC),
        log_info=context.log.info,
    )
    context.log.info(
        "Denmark CVR person DuckDB complete: discovered_objects=%s "
        "existing_objects=%s processed_objects=%s processed_rows=%s persons=%s "
        "incomplete_objects=%s database_bytes=%s",
        summary.discovered_object_count,
        summary.already_ingested_object_count,
        summary.processed_object_count,
        summary.processed_row_count,
        summary.entity_count,
        summary.incomplete_object_count,
        summary.database_size_bytes,
    )
    return dg.MaterializeResult(
        metadata={
            **_duckdb_materialization_metadata(
                summary,
                table_name=DENMARK_CVR_PERSONS_TABLE,
            ),
            "person_count": summary.entity_count,
        }
    )


defs = dg.Definitions(
    assets=[denmark_cvr_companies_duckdb],
    resources={
        "denmark_cvr_duckdb": duckdb_resource(DENMARK_CVR_DUCKDB_PATH),
    },
)
