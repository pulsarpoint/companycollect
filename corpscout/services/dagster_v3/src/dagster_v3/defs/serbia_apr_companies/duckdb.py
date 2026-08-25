import json
import tempfile
from collections.abc import Mapping
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import duckdb
import ijson
import pyarrow as pa

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.serbia_apr_companies import tables
from dagster_v3.defs.serbia_apr_companies.resources import (
    MINIMUM_RECORD_COUNT,
    validate_snapshot_manifest,
)

MAXIMUM_BATCH_SIZE = 50_000
DEFAULT_BATCH_SIZE = 25_000
_STAGING_TABLE = "serbia_apr_company_observations_stage"
_ARROW_RELATION = "serbia_apr_company_arrow_batch"

SOURCE_FIELDS = {
    "legal_name": "PoslovnoIme",
    "municipality_code": "SifraOpstine",
    "municipality_name_original": "NazivOpstine",
    "source_status_original": "NazivStatus",
    "incorporation_date": "DatumOsnivanja",
    "legal_form_original": "NazivPravneForme",
    "primary_activity_code": "SifraDelatnosti",
}

STATUS_VALUES = {
    "Активан": ("active", True),
    "У ликвидацији": ("liquidation", False),
    "У стечају": ("bankruptcy", False),
    "У принудној ликвидацији": ("compulsory_liquidation", False),
}

_SCHEMA_CONTRACT = {
    "company_columns": tables.COMPANY_COLUMNS,
    "snapshot_run_columns": tables.SNAPSHOT_RUN_COLUMNS,
    "source_fields": SOURCE_FIELDS,
    "status_values": STATUS_VALUES,
    "version": 1,
}
SCHEMA_FINGERPRINT = sha256(
    json.dumps(
        _SCHEMA_CONTRACT,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
).hexdigest()

_COMPANY_ARROW_SCHEMA = pa.schema(
    [
        pa.field("company_id", pa.string(), nullable=False),
        pa.field("registration_number", pa.string(), nullable=False),
        pa.field("legal_name", pa.string(), nullable=False),
        pa.field("municipality_code", pa.string(), nullable=False),
        pa.field("municipality_name_original", pa.string(), nullable=False),
        pa.field("source_status_original", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
        pa.field("incorporation_date", pa.date32(), nullable=False),
        pa.field("legal_form_original", pa.string(), nullable=False),
        pa.field("primary_activity_code", pa.string(), nullable=False),
        pa.field("source_run_id", pa.string(), nullable=False),
        pa.field("source_record_id", pa.string(), nullable=False),
        pa.field("source_record_number", pa.int64(), nullable=False),
        pa.field("source_payload_hash", pa.string(), nullable=False),
        pa.field("source_record_uid", pa.string(), nullable=False),
        pa.field("state_fingerprint", pa.string(), nullable=False),
        pa.field("snapshot_date", pa.date32(), nullable=False),
        pa.field("source_url", pa.string(), nullable=False),
        pa.field("source_bucket", pa.string(), nullable=False),
        pa.field("source_object_key", pa.string(), nullable=False),
        pa.field("raw_entity", pa.string(), nullable=False),
        pa.field(
            "updated_from_raw_at",
            pa.timestamp("us", tz="UTC"),
            nullable=False,
        ),
        pa.field("observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

_COMPANY_TABLE_FIELDS_SQL = """
    company_id VARCHAR NOT NULL,
    registration_number VARCHAR NOT NULL,
    legal_name VARCHAR NOT NULL,
    municipality_code VARCHAR NOT NULL,
    municipality_name_original VARCHAR NOT NULL,
    source_status_original VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    is_active BOOLEAN NOT NULL,
    incorporation_date DATE NOT NULL,
    legal_form_original VARCHAR NOT NULL,
    primary_activity_code VARCHAR NOT NULL,
    source_run_id VARCHAR NOT NULL,
    source_record_id VARCHAR NOT NULL,
    source_record_number BIGINT NOT NULL,
    source_payload_hash VARCHAR NOT NULL,
    source_record_uid VARCHAR NOT NULL,
    state_fingerprint VARCHAR NOT NULL,
    snapshot_date DATE NOT NULL,
    source_url VARCHAR NOT NULL,
    source_bucket VARCHAR NOT NULL,
    source_object_key VARCHAR NOT NULL,
    raw_entity VARCHAR NOT NULL,
    updated_from_raw_at TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL
"""


def replace_serbia_apr_companies_duckdb(
    *,
    connection: duckdb.DuckDBPyConnection,
    object_store: ObjectStoreResource,
    manifest: Mapping[str, object],
    loaded_at: datetime,
    minimum_record_count: int = MINIMUM_RECORD_COUNT,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict[str, int]:
    """Validate one raw snapshot and atomically maintain all APR DuckDB tables."""
    clean_manifest = validate_snapshot_manifest(manifest)
    if loaded_at.tzinfo is None:
        raise ValueError("loaded_at must include a timezone")
    loaded_at_utc = loaded_at.astimezone(UTC)
    if not 1 <= batch_size <= MAXIMUM_BATCH_SIZE:
        raise ValueError(
            f"batch_size must be between 1 and {MAXIMUM_BATCH_SIZE}, got {batch_size}"
        )

    expected_record_count = int(clean_manifest["record_count"])
    if expected_record_count < minimum_record_count:
        raise ValueError(
            "APR companies manifest has an implausibly small population: "
            f"{expected_record_count}, expected at least {minimum_record_count}"
        )

    with tempfile.TemporaryDirectory(prefix="serbia_apr_companies_duckdb_") as temp_dir:
        snapshot_path = Path(temp_dir) / "companies.json"
        object_store.download_file(
            str(clean_manifest["object_key"]),
            snapshot_path,
            bucket=tables.S3_BUCKET,
        )
        _verify_downloaded_object(snapshot_path, clean_manifest)
        embedded_snapshot_date = _read_snapshot_date(snapshot_path)
        manifest_snapshot_date = date.fromisoformat(
            str(clean_manifest["snapshot_date"])
        )
        if embedded_snapshot_date != manifest_snapshot_date:
            raise ValueError(
                "APR companies snapshot date does not match its manifest: "
                f"{embedded_snapshot_date.isoformat()} != "
                f"{manifest_snapshot_date.isoformat()}"
            )

        _create_staging_table(connection)
        try:
            parsed_record_count = _load_staging_rows(
                connection=connection,
                snapshot_path=snapshot_path,
                manifest=clean_manifest,
                snapshot_date=manifest_snapshot_date,
                loaded_at=loaded_at_utc,
                batch_size=batch_size,
            )
            _validate_staging_table(
                connection=connection,
                parsed_record_count=parsed_record_count,
                expected_record_count=expected_record_count,
            )
            return _replace_durable_tables(
                connection=connection,
                manifest=clean_manifest,
                snapshot_date=manifest_snapshot_date,
                loaded_at=loaded_at_utc,
            )
        finally:
            connection.execute(f"drop table if exists {_STAGING_TABLE}")


def _verify_downloaded_object(
    snapshot_path: Path,
    manifest: Mapping[str, object],
) -> None:
    digest = sha256()
    size_bytes = 0
    with snapshot_path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
            size_bytes += len(chunk)

    expected_size = int(manifest["size_bytes"])
    if size_bytes != expected_size:
        raise ValueError(
            "downloaded APR companies object size does not match its manifest: "
            f"{size_bytes} != {expected_size}"
        )
    expected_hash = str(manifest["sha256"])
    if digest.hexdigest() != expected_hash:
        raise ValueError(
            "downloaded APR companies object SHA-256 does not match its manifest"
        )


def _read_snapshot_date(snapshot_path: Path) -> date:
    with snapshot_path.open("rb") as handle:
        raw_snapshot_date = next(ijson.items(handle, "DatumPreseka"), None)
    if not isinstance(raw_snapshot_date, str):
        raise ValueError("APR companies snapshot is missing DatumPreseka")
    try:
        return date.fromisoformat(raw_snapshot_date)
    except ValueError as exc:
        raise ValueError(
            f"APR companies DatumPreseka is not an ISO date: {raw_snapshot_date}"
        ) from exc


def _create_staging_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"drop table if exists {_STAGING_TABLE}")
    connection.execute(
        f"create temporary table {_STAGING_TABLE} ({_COMPANY_TABLE_FIELDS_SQL})"
    )


def _load_staging_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    snapshot_path: Path,
    manifest: Mapping[str, object],
    snapshot_date: date,
    loaded_at: datetime,
    batch_size: int,
) -> int:
    retrieved_at = _parse_retrieved_at(manifest)
    municipality_names: dict[str, str] = {}
    seen_registration_numbers: set[str] = set()
    batch: list[dict[str, Any]] = []
    record_count = 0

    with snapshot_path.open("rb") as handle:
        for record_number, (record_id, raw_record) in enumerate(
            ijson.kvitems(handle, "Podaci", use_float=True),
            start=1,
        ):
            if not isinstance(record_id, str) or not isinstance(raw_record, dict):
                raise ValueError(
                    f"APR company record {record_number} has an invalid object shape"
                )
            if record_id in seen_registration_numbers:
                raise ValueError(
                    f"APR companies snapshot repeats registration number {record_id}"
                )
            seen_registration_numbers.add(record_id)

            normalized = _normalize_company_record(
                record_id=record_id,
                record_number=record_number,
                raw_record=raw_record,
                manifest=manifest,
                snapshot_date=snapshot_date,
                loaded_at=loaded_at,
                retrieved_at=retrieved_at,
            )
            municipality_code = str(normalized["municipality_code"])
            municipality_name = str(normalized["municipality_name_original"])
            previous_name = municipality_names.setdefault(
                municipality_code,
                municipality_name,
            )
            if previous_name != municipality_name:
                raise ValueError(
                    "APR municipality code maps to multiple names in one snapshot: "
                    f"{municipality_code}"
                )

            batch.append(normalized)
            record_count = record_number
            if len(batch) >= batch_size:
                _insert_arrow_batch(connection, batch)
                batch.clear()

    if batch:
        _insert_arrow_batch(connection, batch)
    return record_count


def _normalize_company_record(
    *,
    record_id: str,
    record_number: int,
    raw_record: dict[str, Any],
    manifest: Mapping[str, object],
    snapshot_date: date,
    loaded_at: datetime,
    retrieved_at: datetime,
) -> dict[str, Any]:
    if len(record_id) != 8 or not record_id.isdigit():
        raise ValueError(
            f"APR company record has an invalid registration number: {record_id!r}"
        )

    values: dict[str, str] = {}
    for target_field, source_field in SOURCE_FIELDS.items():
        raw_value = raw_record.get(source_field)
        if not isinstance(raw_value, str) or raw_value.strip() == "":
            raise ValueError(
                f"APR company {record_id} has an invalid {source_field} value"
            )
        values[target_field] = raw_value.strip()

    municipality_code = values["municipality_code"]
    if len(municipality_code) != 5 or not municipality_code.isdigit():
        raise ValueError(f"APR company {record_id} has an invalid municipality code")
    primary_activity_code = values["primary_activity_code"]
    if len(primary_activity_code) != 4 or not primary_activity_code.isdigit():
        raise ValueError(
            f"APR company {record_id} has an invalid primary activity code"
        )

    try:
        incorporation_date = date.fromisoformat(values["incorporation_date"])
    except ValueError as exc:
        raise ValueError(
            f"APR company {record_id} has an invalid incorporation date"
        ) from exc
    if incorporation_date > snapshot_date:
        raise ValueError(
            f"APR company {record_id} was incorporated after the snapshot date"
        )

    source_status = values["source_status_original"]
    status_value = STATUS_VALUES.get(source_status)
    if status_value is None:
        raise ValueError(
            f"APR company {record_id} has unrecognized APR company status: "
            f"{source_status!r}"
        )
    status, is_active = status_value

    raw_entity = json.dumps(
        raw_record,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    source_payload_hash = sha256(raw_entity.encode("utf-8")).hexdigest()
    source_record_uid = sha256(
        (
            "company-source-record-v1\n"
            "structured\n"
            f"{tables.SOURCE_NAME}\n"
            "registry_company\n"
            f"{record_id}\n"
            f"{source_payload_hash}"
        ).encode("utf-8")
    ).hexdigest()
    state_fingerprint = sha256(
        json.dumps(
            {
                "incorporation_date": incorporation_date.isoformat(),
                "is_active": is_active,
                "legal_form_original": values["legal_form_original"],
                "legal_name": values["legal_name"],
                "municipality_code": municipality_code,
                "municipality_name_original": values["municipality_name_original"],
                "primary_activity_code": primary_activity_code,
                "source_status_original": source_status,
                "status": status,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()

    return {
        "company_id": record_id,
        "registration_number": record_id,
        "legal_name": values["legal_name"],
        "municipality_code": municipality_code,
        "municipality_name_original": values["municipality_name_original"],
        "source_status_original": source_status,
        "status": status,
        "is_active": is_active,
        "incorporation_date": incorporation_date,
        "legal_form_original": values["legal_form_original"],
        "primary_activity_code": primary_activity_code,
        "source_run_id": str(manifest["source_run_id"]),
        "source_record_id": record_id,
        "source_record_number": record_number,
        "source_payload_hash": source_payload_hash,
        "source_record_uid": source_record_uid,
        "state_fingerprint": state_fingerprint,
        "snapshot_date": snapshot_date,
        "source_url": tables.SOURCE_URL,
        "source_bucket": tables.S3_BUCKET,
        "source_object_key": str(manifest["object_key"]),
        "raw_entity": raw_entity,
        "updated_from_raw_at": loaded_at,
        "observed_at": retrieved_at,
    }


def _insert_arrow_batch(
    connection: duckdb.DuckDBPyConnection,
    batch: list[dict[str, Any]],
) -> None:
    arrow_table = pa.Table.from_pylist(batch, schema=_COMPANY_ARROW_SCHEMA)
    column_list = ", ".join(tables.COMPANY_COLUMNS)
    connection.register(_ARROW_RELATION, arrow_table)
    try:
        connection.execute(
            f"""
            insert into {_STAGING_TABLE} ({column_list})
            select {column_list}
            from {_ARROW_RELATION}
            """
        )
    finally:
        connection.unregister(_ARROW_RELATION)


def _validate_staging_table(
    *,
    connection: duckdb.DuckDBPyConnection,
    parsed_record_count: int,
    expected_record_count: int,
) -> None:
    if parsed_record_count != expected_record_count:
        raise ValueError(
            "APR companies parsed record count does not match its manifest: "
            f"{parsed_record_count} != {expected_record_count}"
        )
    row_count, distinct_registration_count = connection.execute(
        f"""
        select count(*), count(distinct registration_number)
        from {_STAGING_TABLE}
        """
    ).fetchone()
    if row_count != expected_record_count:
        raise ValueError(
            "APR companies staging row count does not match its manifest: "
            f"{row_count} != {expected_record_count}"
        )
    if distinct_registration_count != row_count:
        raise ValueError("APR companies staging table contains duplicate companies")


def _replace_durable_tables(
    *,
    connection: duckdb.DuckDBPyConnection,
    manifest: Mapping[str, object],
    snapshot_date: date,
    loaded_at: datetime,
) -> dict[str, int]:
    schema = tables.DUCKDB_SCHEMA
    company_columns = ", ".join(tables.COMPANY_COLUMNS)
    retrieved_at = _parse_retrieved_at(manifest)
    source_updated_at = datetime(
        snapshot_date.year,
        snapshot_date.month,
        snapshot_date.day,
        tzinfo=UTC,
    )
    raw_manifest = json.dumps(
        manifest,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )

    connection.execute("begin transaction")
    try:
        connection.execute(f"create schema if not exists {schema}")
        connection.execute(
            f"""
            create table if not exists {schema}.{tables.SNAPSHOT_RUNS_TABLE} (
                source_run_id VARCHAR NOT NULL,
                snapshot_date DATE NOT NULL,
                source_url VARCHAR NOT NULL,
                source_license VARCHAR NOT NULL,
                source_bucket VARCHAR NOT NULL,
                source_object_key VARCHAR NOT NULL,
                payload_sha256 VARCHAR NOT NULL,
                payload_bytes BIGINT NOT NULL,
                record_count BIGINT NOT NULL,
                schema_fingerprint VARCHAR NOT NULL,
                run_status VARCHAR NOT NULL,
                retrieved_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                accepted_at TIMESTAMPTZ NOT NULL,
                raw_manifest VARCHAR NOT NULL
            )
            """
        )
        for table_name in (
            tables.COMPANY_OBSERVATIONS_TABLE,
            tables.COMPANIES_CURRENT_TABLE,
        ):
            connection.execute(
                f"""
                create table if not exists {schema}.{table_name} (
                    {_COMPANY_TABLE_FIELDS_SQL}
                )
                """
            )

        connection.execute(
            f"""
            delete from {schema}.{tables.COMPANY_OBSERVATIONS_TABLE}
            where snapshot_date = ?
            """,
            [snapshot_date],
        )
        connection.execute(
            f"""
            insert into {schema}.{tables.COMPANY_OBSERVATIONS_TABLE} ({company_columns})
            select {company_columns}
            from {_STAGING_TABLE}
            """
        )
        connection.execute(f"delete from {schema}.{tables.COMPANIES_CURRENT_TABLE}")
        connection.execute(
            f"""
            insert into {schema}.{tables.COMPANIES_CURRENT_TABLE} ({company_columns})
            select {company_columns}
            from {_STAGING_TABLE}
            """
        )

        connection.execute(
            f"""
            delete from {schema}.{tables.SNAPSHOT_RUNS_TABLE}
            where source_run_id = ?
            """,
            [str(manifest["source_run_id"])],
        )
        snapshot_columns = ", ".join(tables.SNAPSHOT_RUN_COLUMNS)
        connection.execute(
            f"""
            insert into {schema}.{tables.SNAPSHOT_RUNS_TABLE} ({snapshot_columns})
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(manifest["source_run_id"]),
                snapshot_date,
                tables.SOURCE_URL,
                tables.SOURCE_LICENSE,
                tables.S3_BUCKET,
                str(manifest["object_key"]),
                str(manifest["sha256"]),
                int(manifest["size_bytes"]),
                int(manifest["record_count"]),
                SCHEMA_FINGERPRINT,
                "accepted",
                retrieved_at,
                source_updated_at,
                loaded_at,
                raw_manifest,
            ],
        )

        counts = {
            table_name: int(
                connection.execute(
                    f"select count(*) from {schema}.{table_name}"
                ).fetchone()[0]
            )
            for table_name in (
                tables.SNAPSHOT_RUNS_TABLE,
                tables.COMPANY_OBSERVATIONS_TABLE,
                tables.COMPANIES_CURRENT_TABLE,
            )
        }
        connection.execute("commit")
        return counts
    except Exception:
        connection.execute("rollback")
        raise


def _parse_retrieved_at(manifest: Mapping[str, object]) -> datetime:
    retrieved_at = datetime.fromisoformat(
        str(manifest["retrieved_at"]).replace("Z", "+00:00")
    )
    return retrieved_at.astimezone(UTC)
