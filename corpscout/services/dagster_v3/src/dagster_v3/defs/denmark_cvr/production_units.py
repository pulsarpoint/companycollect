import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Literal
from urllib.parse import urlparse

import dagster as dg
import duckdb
import pyarrow as pa
from dagster_duckdb import DuckDBResource

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_database_path,
    duckdb_resource,
)
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.denmark_cvr.assets import DENMARK_CVR_BUCKET
from dagster_v3.defs.denmark_cvr.company_details import (
    DENMARK_CVR_COMPANY_DETAIL_PARTITIONS,
    DenmarkCvrCompanyDetailHttpFailure,
    DenmarkCvrCompanyDetailResource,
    company_detail_bucket_key,
    company_detail_partition_cvrs,
    company_detail_update_cvrs,
)
from dagster_v3.defs.denmark_cvr.duckdb_asset import (
    DENMARK_CVR_DUCKDB_PATH,
    DENMARK_CVR_DUCKDB_POOL,
    DENMARK_CVR_DUCKDB_SCHEMA,
    DENMARK_CVR_PRODUCTION_UNITS_TABLE,
)
from dagster_v3.defs.denmark_cvr.partitions import DENMARK_CVR_ACTIVE_PARTITIONS

DENMARK_CVR_PRODUCTION_UNIT_PREFIX = "denmark_cvr/production_units"
DENMARK_CVR_PRODUCTION_UNIT_GROUP = "denmark_cvr_production_units"
DENMARK_CVR_PRODUCTION_UNIT_DOWNLOAD_POOL = "denmark_cvr_production_units"

type DenmarkCvrProductionUnitCaptureType = Literal[
    "production_unit_snapshot",
    "production_unit_update",
]

_PRODUCTION_UNIT_COLUMNS = (
    "p_number",
    "company_cvr",
    "is_active",
    "name",
    "address",
    "postal_code_and_city",
    "email",
    "phone",
    "primary_industry_code",
    "primary_industry_title",
    "secondary_industries",
    "start_date",
    "cessation_date",
    "advertising_protected",
    "building_number",
    "open_on_public_holidays",
    "registered_in_anti_money_laundering_register",
    "accounting_period_start",
    "accounting_period_end",
    "foreign_address",
    "foreign_address_country",
    "foreign_address_country_code",
    "company_name",
    "employee_count",
    "historical_master_data",
    "audit_firm",
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

_PRODUCTION_UNIT_ARROW_SCHEMA = pa.schema(
    [
        pa.field("p_number", pa.string(), nullable=False),
        pa.field("company_cvr", pa.string(), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("address", pa.string()),
        pa.field("postal_code_and_city", pa.string()),
        pa.field("email", pa.string()),
        pa.field("phone", pa.string()),
        pa.field("primary_industry_code", pa.string()),
        pa.field("primary_industry_title", pa.string()),
        pa.field("secondary_industries", pa.string(), nullable=False),
        pa.field("start_date", pa.date32()),
        pa.field("cessation_date", pa.date32()),
        pa.field("advertising_protected", pa.bool_()),
        pa.field("building_number", pa.string()),
        pa.field("open_on_public_holidays", pa.bool_()),
        pa.field("registered_in_anti_money_laundering_register", pa.bool_()),
        pa.field("accounting_period_start", pa.date32()),
        pa.field("accounting_period_end", pa.date32()),
        pa.field("foreign_address", pa.string()),
        pa.field("foreign_address_country", pa.string()),
        pa.field("foreign_address_country_code", pa.string()),
        pa.field("company_name", pa.string()),
        pa.field("employee_count", pa.string(), nullable=False),
        pa.field("historical_master_data", pa.string(), nullable=False),
        pa.field("audit_firm", pa.string(), nullable=False),
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
    ]
)


@dataclass(frozen=True)
class DenmarkCvrProductionUnitRawSummary:
    partition_key: str
    selected_company_count: int
    already_captured_company_count: int
    downloaded_company_count: int
    written_object_count: int
    downloaded_size_bytes: int
    stored_size_bytes: int


@dataclass(frozen=True)
class DenmarkCvrProductionUnitSummary:
    source_partition_key: str
    capture_object_count: int
    company_count: int
    production_unit_count: int
    active_production_unit_count: int
    ceased_production_unit_count: int
    table_production_unit_count: int
    database_size_bytes: int


@dataclass(frozen=True)
class ParsedProductionUnitCapture:
    cvr: str
    capture_type: DenmarkCvrProductionUnitCaptureType
    partition_key: str
    retrieved_at: datetime
    run_id: str
    production_units: dict[str, Any]


class DenmarkCvrProductionUnitCaptureError(ValueError):
    pass


def production_unit_object_key(partition_key: str, cvr: str) -> str:
    _validate_cvr(cvr)
    if company_detail_bucket_key(cvr) != partition_key:
        raise ValueError(f"CVR {cvr} does not belong to partition {partition_key}")
    return (
        f"{DENMARK_CVR_PRODUCTION_UNIT_PREFIX}/{partition_key}/"
        f"cvr={cvr}/production_units.json"
    )


def production_unit_update_object_key(update_date: str, cvr: str) -> str:
    _validate_date_partition(update_date)
    _validate_cvr(cvr)
    return (
        f"{DENMARK_CVR_PRODUCTION_UNIT_PREFIX}/updates/date={update_date}/"
        f"{company_detail_bucket_key(cvr)}/cvr={cvr}/production_units.json"
    )


def write_production_unit_partition(
    *,
    object_store: ObjectStoreResource,
    details: DenmarkCvrCompanyDetailResource,
    partition_key: str,
    cvrs: Sequence[str],
    run_id: str,
    retrieved_at: datetime,
    log_info: Callable[..., object] | None = None,
) -> DenmarkCvrProductionUnitRawSummary:
    selected_cvrs = tuple(cvrs)
    object_keys: dict[str, str] = {}
    for cvr in selected_cvrs:
        object_keys[cvr] = production_unit_object_key(partition_key, cvr)
    return _write_production_unit_captures(
        object_store=object_store,
        details=details,
        partition_key=partition_key,
        capture_type="production_unit_snapshot",
        object_prefix=f"{DENMARK_CVR_PRODUCTION_UNIT_PREFIX}/{partition_key}/",
        object_keys=object_keys,
        run_id=run_id,
        retrieved_at=retrieved_at,
        log_info=log_info,
    )


def write_production_unit_updates(
    *,
    object_store: ObjectStoreResource,
    details: DenmarkCvrCompanyDetailResource,
    update_date: str,
    cvrs: Sequence[str],
    run_id: str,
    retrieved_at: datetime,
    log_info: Callable[..., object] | None = None,
) -> DenmarkCvrProductionUnitRawSummary:
    _validate_date_partition(update_date)
    object_keys = {
        cvr: production_unit_update_object_key(update_date, cvr) for cvr in cvrs
    }
    return _write_production_unit_captures(
        object_store=object_store,
        details=details,
        partition_key=update_date,
        capture_type="production_unit_update",
        object_prefix=(
            f"{DENMARK_CVR_PRODUCTION_UNIT_PREFIX}/updates/date={update_date}/"
        ),
        object_keys=object_keys,
        run_id=run_id,
        retrieved_at=retrieved_at,
        log_info=log_info,
    )


def _write_production_unit_captures(
    *,
    object_store: ObjectStoreResource,
    details: DenmarkCvrCompanyDetailResource,
    partition_key: str,
    capture_type: DenmarkCvrProductionUnitCaptureType,
    object_prefix: str,
    object_keys: Mapping[str, str],
    run_id: str,
    retrieved_at: datetime,
    log_info: Callable[..., object] | None,
) -> DenmarkCvrProductionUnitRawSummary:
    if run_id.strip() == "":
        raise ValueError("Production-unit capture run ID must not be blank")
    if retrieved_at.utcoffset() is None:
        raise ValueError("Production-unit retrieval timestamp must include a timezone")
    object_store.ensure_bucket(DENMARK_CVR_BUCKET)
    existing_keys = set(
        object_store.list_keys(object_prefix, bucket=DENMARK_CVR_BUCKET)
    )
    pending_cvrs = tuple(
        cvr
        for cvr, object_key in object_keys.items()
        if object_key not in existing_keys
    )
    downloaded_count = 0
    downloaded_size_bytes = 0
    stored_size_bytes = 0
    returned_cvrs: set[str] = set()
    for download in details.iter_company_details(pending_cvrs):
        if isinstance(download, DenmarkCvrCompanyDetailHttpFailure):
            raise DenmarkCvrProductionUnitCaptureError(
                "DataCVR production-unit capture returned HTTP "
                f"{download.status} for CVR {download.cvr}"
            )
        if download.cvr not in object_keys or download.cvr in returned_cvrs:
            raise DenmarkCvrProductionUnitCaptureError(
                "DataCVR production-unit capture returned an unexpected company"
            )
        returned_cvrs.add(download.cvr)
        production_units = _production_units_from_detail(
            download.payload,
            cvr=download.cvr,
        )
        stored_body = _capture_bytes(
            cvr=download.cvr,
            source_url=download.source_url,
            capture_type=capture_type,
            partition_key=partition_key,
            retrieved_at=retrieved_at,
            run_id=run_id,
            production_units=production_units,
        )
        object_store.write_bytes(
            object_keys[download.cvr],
            stored_body,
            bucket=DENMARK_CVR_BUCKET,
        )
        downloaded_count += 1
        downloaded_size_bytes += download.downloaded_size_bytes
        stored_size_bytes += len(stored_body)
        if log_info is not None and (
            downloaded_count == 1
            or downloaded_count % 100 == 0
            or downloaded_count == len(pending_cvrs)
        ):
            log_info(
                "DataCVR production-unit capture progress: partition=%s "
                "downloaded=%s/%s downloaded_bytes=%s",
                partition_key,
                downloaded_count,
                len(pending_cvrs),
                downloaded_size_bytes,
            )
    if returned_cvrs != set(pending_cvrs):
        raise DenmarkCvrProductionUnitCaptureError(
            "DataCVR production-unit capture did not return every selected company"
        )
    return DenmarkCvrProductionUnitRawSummary(
        partition_key=partition_key,
        selected_company_count=len(object_keys),
        already_captured_company_count=len(object_keys) - len(pending_cvrs),
        downloaded_company_count=downloaded_count,
        written_object_count=downloaded_count,
        downloaded_size_bytes=downloaded_size_bytes,
        stored_size_bytes=stored_size_bytes,
    )


@dg.asset(
    deps=[dg.AssetKey("denmark_cvr_companies_duckdb")],
    group_name=DENMARK_CVR_PRODUCTION_UNIT_GROUP,
    kinds={"python", "browser", "duckdb", "json", "s3"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "entity_type": "produktionsenhed",
        "layer": "raw",
    },
    partitions_def=DENMARK_CVR_COMPANY_DETAIL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=DENMARK_CVR_PRODUCTION_UNIT_DOWNLOAD_POOL,
    description=(
        "Reads one stable CVR hash bucket directly from the company DuckDB table, "
        "downloads each company's production-unit section in one browser session, "
        "and checkpoints one Danish-key JSON capture per company."
    ),
)
def denmark_cvr_production_units_s3(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    denmark_cvr_production_units_api: DenmarkCvrCompanyDetailResource,
    denmark_cvr_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    summary = write_production_unit_partition(
        object_store=object_store,
        details=denmark_cvr_production_units_api,
        partition_key=partition_key,
        cvrs=company_detail_partition_cvrs(denmark_cvr_duckdb, partition_key),
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=_raw_summary_metadata(summary))


@dg.asset(
    deps=[dg.AssetKey("denmark_cvr_companies_duckdb")],
    group_name=DENMARK_CVR_PRODUCTION_UNIT_GROUP,
    kinds={"python", "browser", "duckdb", "json", "s3"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "entity_type": "produktionsenhed",
        "layer": "raw_update",
    },
    partitions_def=DENMARK_CVR_ACTIVE_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=DENMARK_CVR_PRODUCTION_UNIT_DOWNLOAD_POOL,
    description=(
        "Reads CVRs assigned to one active company DuckDB date, downloads their "
        "production-unit sections, and writes date-versioned raw JSON captures."
    ),
)
def denmark_cvr_production_unit_updates_s3(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    denmark_cvr_production_units_api: DenmarkCvrCompanyDetailResource,
    denmark_cvr_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    update_date = context.partition_key
    summary = write_production_unit_updates(
        object_store=object_store,
        details=denmark_cvr_production_units_api,
        update_date=update_date,
        cvrs=company_detail_update_cvrs(denmark_cvr_duckdb, update_date),
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=_raw_summary_metadata(summary))


def replace_production_units_from_captures(
    *,
    object_store: ObjectStoreResource,
    denmark_cvr_duckdb: DuckDBResource,
    source_prefix: str,
    expected_capture_type: DenmarkCvrProductionUnitCaptureType,
    expected_partition_key: str,
    ingestion_run_id: str,
    processed_at: datetime,
    log_info: Callable[..., object] | None = None,
) -> DenmarkCvrProductionUnitSummary:
    if ingestion_run_id.strip() == "":
        raise ValueError("Production-unit ingestion run ID must not be blank")
    if processed_at.utcoffset() is None:
        raise ValueError("Production-unit processing timestamp must include a timezone")
    capture_keys = tuple(
        sorted(
            key
            for key in object_store.list_keys(
                source_prefix,
                bucket=DENMARK_CVR_BUCKET,
            )
            if key.endswith("/production_units.json")
        )
    )
    parent_cvrs: set[str] = set()
    rows: list[dict[str, Any]] = []
    seen_p_numbers: set[str] = set()
    for object_index, object_key in enumerate(capture_keys):
        capture = _parse_capture(
            object_store.read_bytes(object_key, bucket=DENMARK_CVR_BUCKET),
            object_key=object_key,
            expected_capture_type=expected_capture_type,
            expected_partition_key=expected_partition_key,
        )
        if capture.cvr in parent_cvrs:
            raise DenmarkCvrProductionUnitCaptureError(
                f"Duplicate company CVR in production-unit partition: {capture.cvr}"
            )
        parent_cvrs.add(capture.cvr)
        extracted_rows = _production_unit_rows(
            capture,
            source_object_key=object_key,
            first_source_row_number=len(rows),
            ingestion_run_id=ingestion_run_id,
            processed_at=processed_at,
        )
        for row in extracted_rows:
            p_number = row["p_number"]
            if p_number in seen_p_numbers:
                raise DenmarkCvrProductionUnitCaptureError(
                    f"Duplicate production-unit number in partition: {p_number}"
                )
            seen_p_numbers.add(p_number)
        rows.extend(extracted_rows)
        if log_info is not None and (
            object_index == 0
            or (object_index + 1) % 500 == 0
            or object_index + 1 == len(capture_keys)
        ):
            log_info(
                "Denmark CVR production-unit normalization progress: partition=%s "
                "company_objects=%s/%s production_units=%s",
                expected_partition_key,
                object_index + 1,
                len(capture_keys),
                len(rows),
            )

    with denmark_cvr_duckdb.get_connection() as connection:
        connection.execute("BEGIN TRANSACTION")
        try:
            _ensure_production_unit_table(connection)
            _replace_parent_company_rows(
                connection,
                parent_cvrs=parent_cvrs,
                rows=rows,
            )
            table_count = connection.execute(
                f"SELECT count(*) FROM "
                f"{DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PRODUCTION_UNITS_TABLE}"
            ).fetchone()[0]
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise

    database_path = duckdb_database_path(denmark_cvr_duckdb)
    return DenmarkCvrProductionUnitSummary(
        source_partition_key=expected_partition_key,
        capture_object_count=len(capture_keys),
        company_count=len(parent_cvrs),
        production_unit_count=len(rows),
        active_production_unit_count=sum(row["is_active"] for row in rows),
        ceased_production_unit_count=sum(not row["is_active"] for row in rows),
        table_production_unit_count=int(table_count),
        database_size_bytes=(
            database_path.stat().st_size if database_path.exists() else 0
        ),
    )


@dg.asset(
    deps=[dg.AssetKey("denmark_cvr_production_units_s3")],
    group_name=DENMARK_CVR_PRODUCTION_UNIT_GROUP,
    kinds={"python", "s3", "json", "duckdb"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "entity_type": "produktionsenhed",
        "layer": "normalized",
    },
    partitions_def=DENMARK_CVR_COMPANY_DETAIL_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=DENMARK_CVR_DUCKDB_POOL,
    metadata={
        "duckdb_schema": DENMARK_CVR_DUCKDB_SCHEMA,
        "duckdb_table": DENMARK_CVR_PRODUCTION_UNITS_TABLE,
    },
    description=(
        "Normalizes one production-unit capture bucket and transactionally "
        "replaces those parent companies' DuckDB rows."
    ),
)
def denmark_cvr_production_units_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    denmark_cvr_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    summary = replace_production_units_from_captures(
        object_store=object_store,
        denmark_cvr_duckdb=denmark_cvr_duckdb,
        source_prefix=f"{DENMARK_CVR_PRODUCTION_UNIT_PREFIX}/{partition_key}/",
        expected_capture_type="production_unit_snapshot",
        expected_partition_key=partition_key,
        ingestion_run_id=context.run_id,
        processed_at=datetime.now(UTC),
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=_duckdb_summary_metadata(summary))


@dg.asset(
    deps=[dg.AssetKey("denmark_cvr_production_unit_updates_s3")],
    group_name=DENMARK_CVR_PRODUCTION_UNIT_GROUP,
    kinds={"python", "s3", "json", "duckdb"},
    tags={
        "country": "denmark",
        "source": "cvr",
        "source_name": "denmark_cvr",
        "entity_type": "produktionsenhed",
        "layer": "normalized_update",
    },
    partitions_def=DENMARK_CVR_ACTIVE_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=DENMARK_CVR_DUCKDB_POOL,
    metadata={
        "duckdb_schema": DENMARK_CVR_DUCKDB_SCHEMA,
        "duckdb_table": DENMARK_CVR_PRODUCTION_UNITS_TABLE,
    },
    description=(
        "Normalizes one date-versioned production-unit capture partition and "
        "replaces DuckDB rows for only those companies."
    ),
)
def denmark_cvr_production_unit_updates_duckdb(
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    denmark_cvr_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    update_date = context.partition_key
    summary = replace_production_units_from_captures(
        object_store=object_store,
        denmark_cvr_duckdb=denmark_cvr_duckdb,
        source_prefix=(
            f"{DENMARK_CVR_PRODUCTION_UNIT_PREFIX}/updates/date={update_date}/"
        ),
        expected_capture_type="production_unit_update",
        expected_partition_key=update_date,
        ingestion_run_id=context.run_id,
        processed_at=datetime.now(UTC),
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=_duckdb_summary_metadata(summary))


def _production_units_from_detail(
    payload: dict[str, Any],
    *,
    cvr: str,
) -> dict[str, Any]:
    master_data = payload.get("stamdata")
    response_cvr = (
        master_data.get("cvrnummer") if isinstance(master_data, dict) else None
    )
    if response_cvr != cvr:
        raise DenmarkCvrProductionUnitCaptureError(
            f"DataCVR production-unit response CVR mismatch for {cvr}"
        )
    production_units = payload.get("produktionsenheder")
    _validate_production_unit_collection(production_units, object_key=f"CVR {cvr}")
    return production_units


def _capture_bytes(
    *,
    cvr: str,
    source_url: str,
    capture_type: DenmarkCvrProductionUnitCaptureType,
    partition_key: str,
    retrieved_at: datetime,
    run_id: str,
    production_units: dict[str, Any],
) -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "source": "denmark_cvr",
            "source_url": source_url,
            "source_capture_type": capture_type,
            "source_partition_key": partition_key,
            "retrieved_at": retrieved_at.astimezone(UTC).isoformat(),
            "run_id": run_id,
            "cvrnummer": cvr,
            "produktionsenheder": production_units,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _parse_capture(
    raw_body: bytes,
    *,
    object_key: str,
    expected_capture_type: DenmarkCvrProductionUnitCaptureType,
    expected_partition_key: str,
) -> ParsedProductionUnitCapture:
    try:
        payload = json.loads(raw_body)
    except UnicodeDecodeError, json.JSONDecodeError:
        raise DenmarkCvrProductionUnitCaptureError(
            f"Invalid Denmark CVR production-unit JSON object: {object_key}"
        ) from None
    if not isinstance(payload, dict):
        raise DenmarkCvrProductionUnitCaptureError(
            f"Denmark CVR production-unit capture must be an object: {object_key}"
        )
    if payload.get("schema_version") != 1 or payload.get("source") != "denmark_cvr":
        raise DenmarkCvrProductionUnitCaptureError(
            f"Invalid Denmark CVR production-unit capture metadata: {object_key}"
        )
    if payload.get("source_capture_type") != expected_capture_type:
        raise DenmarkCvrProductionUnitCaptureError(
            f"Unexpected Denmark CVR production-unit capture type: {object_key}"
        )
    if payload.get("source_partition_key") != expected_partition_key:
        raise DenmarkCvrProductionUnitCaptureError(
            f"Unexpected Denmark CVR production-unit partition: {object_key}"
        )
    source_url = payload.get("source_url")
    if not isinstance(source_url, str) or urlparse(source_url).scheme != "https":
        raise DenmarkCvrProductionUnitCaptureError(
            f"Invalid Denmark CVR production-unit source URL: {object_key}"
        )
    cvr = payload.get("cvrnummer")
    if not isinstance(cvr, str):
        raise DenmarkCvrProductionUnitCaptureError(
            f"Invalid Denmark CVR production-unit company CVR: {object_key}"
        )
    _validate_cvr(cvr)
    if f"/cvr={cvr}/" not in object_key:
        raise DenmarkCvrProductionUnitCaptureError(
            f"Production-unit object path does not match its CVR: {object_key}"
        )
    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or run_id.strip() == "":
        raise DenmarkCvrProductionUnitCaptureError(
            f"Invalid Denmark CVR production-unit source run: {object_key}"
        )
    retrieved_at = _capture_datetime(payload.get("retrieved_at"), object_key=object_key)
    production_units = payload.get("produktionsenheder")
    _validate_production_unit_collection(production_units, object_key=object_key)
    return ParsedProductionUnitCapture(
        cvr=cvr,
        capture_type=expected_capture_type,
        partition_key=expected_partition_key,
        retrieved_at=retrieved_at,
        run_id=run_id,
        production_units=production_units,
    )


def _validate_production_unit_collection(value: Any, *, object_key: str) -> None:
    if not isinstance(value, dict):
        raise DenmarkCvrProductionUnitCaptureError(
            f"Invalid production-unit collection: {object_key}"
        )
    for source_key in (
        "aktiveProduktionsenheder",
        "ophoerteProduktionsenheder",
    ):
        if not isinstance(value.get(source_key), list):
            raise DenmarkCvrProductionUnitCaptureError(
                f"Invalid {source_key} collection: {object_key}"
            )


def _capture_datetime(value: Any, *, object_key: str) -> datetime:
    if not isinstance(value, str):
        raise DenmarkCvrProductionUnitCaptureError(
            f"Invalid production-unit retrieval timestamp: {object_key}"
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise DenmarkCvrProductionUnitCaptureError(
            f"Invalid production-unit retrieval timestamp: {object_key}"
        ) from None
    if parsed.utcoffset() is None:
        raise DenmarkCvrProductionUnitCaptureError(
            f"Production-unit retrieval timestamp has no timezone: {object_key}"
        )
    return parsed.astimezone(UTC)


def _production_unit_rows(
    capture: ParsedProductionUnitCapture,
    *,
    source_object_key: str,
    first_source_row_number: int,
    ingestion_run_id: str,
    processed_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source_key, is_active in (
        ("aktiveProduktionsenheder", True),
        ("ophoerteProduktionsenheder", False),
    ):
        for unit in capture.production_units[source_key]:
            rows.append(
                _production_unit_row(
                    unit,
                    capture=capture,
                    is_active=is_active,
                    source_object_key=source_object_key,
                    source_row_number=first_source_row_number + len(rows),
                    ingestion_run_id=ingestion_run_id,
                    processed_at=processed_at,
                )
            )
    return rows


def _production_unit_row(
    unit: Any,
    *,
    capture: ParsedProductionUnitCapture,
    is_active: bool,
    source_object_key: str,
    source_row_number: int,
    ingestion_run_id: str,
    processed_at: datetime,
) -> dict[str, Any]:
    if not isinstance(unit, dict):
        raise DenmarkCvrProductionUnitCaptureError(
            f"Production unit must be an object: {source_object_key}"
        )
    master_data = unit.get("stamdata")
    if not isinstance(master_data, dict):
        raise DenmarkCvrProductionUnitCaptureError(
            f"Production unit has invalid master data: {source_object_key}"
        )
    p_number = master_data.get("pnummer")
    if not isinstance(p_number, str) or len(p_number) != 10 or not p_number.isdigit():
        raise DenmarkCvrProductionUnitCaptureError(
            f"Production unit has an invalid P-number: {source_object_key}"
        )
    if master_data.get("cvrnummer") != capture.cvr:
        raise DenmarkCvrProductionUnitCaptureError(
            f"Production unit company mismatch: {source_object_key}"
        )
    primary_industry = master_data.get("hovedbranche")
    if not isinstance(primary_industry, dict):
        primary_industry = {}
    raw_record = _json_text(unit)
    return {
        "p_number": p_number,
        "company_cvr": capture.cvr,
        "is_active": is_active,
        "name": _optional_string(master_data.get("navn")),
        "address": _optional_string(master_data.get("adresse")),
        "postal_code_and_city": _optional_string(master_data.get("postnummerOgBy")),
        "email": _optional_string(master_data.get("email")),
        "phone": _optional_string(master_data.get("telefon")),
        "primary_industry_code": _optional_string(primary_industry.get("branchekode")),
        "primary_industry_title": _optional_string(primary_industry.get("titel")),
        "secondary_industries": _json_text(master_data.get("bibrancher", [])),
        "start_date": _optional_date(master_data.get("startdato")),
        "cessation_date": _optional_date(master_data.get("ophoersdato")),
        "advertising_protected": _optional_bool(master_data.get("reklamebeskyttet")),
        "building_number": _optional_string(master_data.get("bygningsnummer")),
        "open_on_public_holidays": _optional_bool(master_data.get("helligdagsaabent")),
        "registered_in_anti_money_laundering_register": _optional_bool(
            master_data.get("registreretIHvidvaskregistret")
        ),
        "accounting_period_start": _optional_date(
            master_data.get("regnskabsperiodeStart")
        ),
        "accounting_period_end": _optional_date(
            master_data.get("regnskabsperiodeSlut")
        ),
        "foreign_address": _optional_string(master_data.get("udenlandskAdresse")),
        "foreign_address_country": _optional_string(
            master_data.get("udenlandskAdresseLand")
        ),
        "foreign_address_country_code": _optional_string(
            master_data.get("udenlandskAdresseLandekode")
        ),
        "company_name": _optional_string(master_data.get("virksomhedsnavn")),
        "employee_count": _json_text(unit.get("antalAnsatte")),
        "historical_master_data": _json_text(unit.get("historiskStamdata")),
        "audit_firm": _json_text(unit.get("revisionsvirksomhed")),
        "source_capture_type": capture.capture_type,
        "source_partition_key": capture.partition_key,
        "source_object_key": source_object_key,
        "source_run_id": capture.run_id,
        "source_retrieved_at": capture.retrieved_at,
        "source_row_number": source_row_number,
        "source_payload_hash": hashlib.sha256(raw_record.encode()).hexdigest(),
        "raw_record": raw_record,
        "ingestion_run_id": ingestion_run_id,
        "ingested_at": processed_at.astimezone(UTC),
    }


def _ensure_production_unit_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {DENMARK_CVR_DUCKDB_SCHEMA}")
    existing_columns = {
        row[0]
        for row in connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            """,
            [DENMARK_CVR_DUCKDB_SCHEMA, DENMARK_CVR_PRODUCTION_UNITS_TABLE],
        ).fetchall()
    }
    if existing_columns and "company_cvr" not in existing_columns:
        connection.execute(
            f"DROP TABLE {DENMARK_CVR_DUCKDB_SCHEMA}."
            f"{DENMARK_CVR_PRODUCTION_UNITS_TABLE}"
        )
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS
          {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PRODUCTION_UNITS_TABLE} (
            p_number varchar primary key,
            company_cvr varchar not null,
            is_active boolean not null,
            name varchar,
            address varchar,
            postal_code_and_city varchar,
            email varchar,
            phone varchar,
            primary_industry_code varchar,
            primary_industry_title varchar,
            secondary_industries json not null,
            start_date date,
            cessation_date date,
            advertising_protected boolean,
            building_number varchar,
            open_on_public_holidays boolean,
            registered_in_anti_money_laundering_register boolean,
            accounting_period_start date,
            accounting_period_end date,
            foreign_address varchar,
            foreign_address_country varchar,
            foreign_address_country_code varchar,
            company_name varchar,
            employee_count json not null,
            historical_master_data json not null,
            audit_firm json not null,
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


def _replace_parent_company_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    parent_cvrs: set[str],
    rows: list[dict[str, Any]],
) -> None:
    if parent_cvrs:
        parent_table_name = "denmark_cvr_production_unit_parent_cvrs"
        connection.register(
            parent_table_name,
            pa.table({"company_cvr": sorted(parent_cvrs)}),
        )
        try:
            connection.execute(
                f"""
                DELETE FROM
                  {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PRODUCTION_UNITS_TABLE}
                WHERE company_cvr IN (
                  SELECT company_cvr FROM {parent_table_name}
                )
                """
            )
        finally:
            connection.unregister(parent_table_name)
    if not rows:
        return
    rows_table_name = "denmark_cvr_production_unit_rows"
    connection.register(
        rows_table_name,
        pa.Table.from_pylist(rows, schema=_PRODUCTION_UNIT_ARROW_SCHEMA),
    )
    columns = ", ".join(_PRODUCTION_UNIT_COLUMNS)
    try:
        connection.execute(
            f"""
            INSERT OR REPLACE INTO
              {DENMARK_CVR_DUCKDB_SCHEMA}.{DENMARK_CVR_PRODUCTION_UNITS_TABLE}
              ({columns})
            SELECT {columns}
            FROM {rows_table_name}
            """
        )
    finally:
        connection.unregister(rows_table_name)


def _validate_cvr(cvr: str) -> None:
    if len(cvr) != 8 or not cvr.isascii() or not cvr.isdigit():
        raise ValueError("DataCVR production-unit CVR must contain eight digits")


def _validate_date_partition(value: str) -> None:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"Invalid production-unit update date: {value!r}") from None
    if parsed.isoformat() != value:
        raise ValueError(f"Invalid production-unit update date: {value!r}")


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value != "" else None


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _optional_date(value: Any) -> date | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise DenmarkCvrProductionUnitCaptureError("Invalid production-unit date")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise DenmarkCvrProductionUnitCaptureError(
            "Invalid production-unit date"
        ) from None


def _json_text(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _raw_summary_metadata(
    summary: DenmarkCvrProductionUnitRawSummary,
) -> dict[str, Any]:
    return {
        "partition_key": summary.partition_key,
        "selected_company_count": summary.selected_company_count,
        "already_captured_company_count": summary.already_captured_company_count,
        "downloaded_company_count": summary.downloaded_company_count,
        "written_object_count": summary.written_object_count,
        "downloaded_size_bytes": summary.downloaded_size_bytes,
        "stored_size_bytes": summary.stored_size_bytes,
        "s3_bucket": DENMARK_CVR_BUCKET,
    }


def _duckdb_summary_metadata(
    summary: DenmarkCvrProductionUnitSummary,
) -> dict[str, Any]:
    return {
        "source_partition_key": summary.source_partition_key,
        "capture_object_count": summary.capture_object_count,
        "company_count": summary.company_count,
        "processed_production_unit_count": summary.production_unit_count,
        "active_production_unit_count": summary.active_production_unit_count,
        "ceased_production_unit_count": summary.ceased_production_unit_count,
        "table_production_unit_count": summary.table_production_unit_count,
        "database_path": str(DENMARK_CVR_DUCKDB_PATH),
        "database_size_bytes": summary.database_size_bytes,
        "duckdb_schema": DENMARK_CVR_DUCKDB_SCHEMA,
        "duckdb_table": DENMARK_CVR_PRODUCTION_UNITS_TABLE,
        "s3_bucket": DENMARK_CVR_BUCKET,
    }


defs = dg.Definitions(
    assets=[
        denmark_cvr_production_units_s3,
        denmark_cvr_production_unit_updates_s3,
        denmark_cvr_production_units_duckdb,
        denmark_cvr_production_unit_updates_duckdb,
    ],
    resources={
        "denmark_cvr_production_units_api": DenmarkCvrCompanyDetailResource(),
        "denmark_cvr_duckdb": duckdb_resource(DENMARK_CVR_DUCKDB_PATH),
    },
)
