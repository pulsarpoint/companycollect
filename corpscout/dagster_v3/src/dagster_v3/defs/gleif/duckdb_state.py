from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.gleif import parser
from dagster_v3.defs.gleif import tables
from dagster_v3.defs.gleif.source import (
    GLEIF_RAW_BUCKET,
    ensure_bootstrap_state_for_delta,
    read_gleif_state,
    write_gleif_state,
)

DUCKDB_SCHEMA = "gleif"


@dataclass
class GleifStateRows:
    lei_records: list[dict[str, Any]] = field(default_factory=list)
    lei_names: list[dict[str, Any]] = field(default_factory=list)
    lei_addresses: list[dict[str, Any]] = field(default_factory=list)
    lei_identifiers: list[dict[str, Any]] = field(default_factory=list)
    relationships: list[dict[str, Any]] = field(default_factory=list)
    relationship_periods: list[dict[str, Any]] = field(default_factory=list)
    reporting_exceptions: list[dict[str, Any]] = field(default_factory=list)
    lei_issuers: list[dict[str, Any]] = field(default_factory=list)
    code_list_entries: list[dict[str, Any]] = field(default_factory=list)


TABLE_ROW_FIELDS = {
    tables.GLEIF_LEI_RECORDS_TABLE: "lei_records",
    tables.GLEIF_LEI_NAMES_TABLE: "lei_names",
    tables.GLEIF_LEI_ADDRESSES_TABLE: "lei_addresses",
    tables.GLEIF_LEI_IDENTIFIERS_TABLE: "lei_identifiers",
    tables.GLEIF_LEI_RELATIONSHIPS_TABLE: "relationships",
    tables.GLEIF_LEI_RELATIONSHIP_PERIODS_TABLE: "relationship_periods",
    tables.GLEIF_LEI_REPORTING_EXCEPTIONS_TABLE: "reporting_exceptions",
    tables.GLEIF_LEI_ISSUERS_TABLE: "lei_issuers",
    tables.GLEIF_CODE_LIST_ENTRIES_TABLE: "code_list_entries",
}

TABLE_KEYS = {
    tables.GLEIF_LEI_RECORDS_TABLE: ("lei",),
    tables.GLEIF_LEI_NAMES_TABLE: ("lei", "name_type", "name_normalized", "sequence"),
    tables.GLEIF_LEI_ADDRESSES_TABLE: ("lei", "address_role"),
    tables.GLEIF_LEI_IDENTIFIERS_TABLE: ("identifier_type", "identifier_value", "lei"),
    tables.GLEIF_LEI_RELATIONSHIPS_TABLE: ("relationship_record_id",),
    tables.GLEIF_LEI_RELATIONSHIP_PERIODS_TABLE: (
        "relationship_record_id",
        "period_type",
        "start_date",
    ),
    tables.GLEIF_LEI_REPORTING_EXCEPTIONS_TABLE: ("exception_record_id",),
    tables.GLEIF_LEI_ISSUERS_TABLE: ("lei",),
    tables.GLEIF_CODE_LIST_ENTRIES_TABLE: ("code_list", "code"),
}

DUCKDB_COLUMN_TYPES = {
    "address_lines": "varchar[]",
    "sequence": "integer",
    "is_primary": "integer",
    "latitude": "double",
    "longitude": "double",
    "jurisdictions": "varchar[]",
    "fund_jurisdictions": "varchar[]",
    "start_date": "date",
    "end_date": "date",
    "valid_from": "timestamp",
    "valid_to": "timestamp",
    "creation_date": "timestamp",
    "expiration_date": "timestamp",
    "initial_registration_date": "timestamp",
    "last_update_date": "timestamp",
    "next_renewal_date": "timestamp",
    "deleted_at": "timestamp",
    "accreditation_date": "timestamp",
    "golden_copy_publish_date": "timestamp",
    "retrieved_at": "timestamp",
    "resolved_at": "timestamp",
}


def replace_current_state(database_path: str | Path, rows: GleifStateRows) -> dict[str, int]:
    database_file = Path(database_path)
    database_file.parent.mkdir(parents=True, exist_ok=True)
    catalog_name = database_file.stem
    with duckdb.connect(str(database_file)) as connection:
        _ensure_schema(connection, catalog_name)
        for table_name in tables.GLEIF_TABLES:
            _replace_table(
                connection,
                catalog_name=catalog_name,
                table_name=table_name,
                columns=tables.GLEIF_TABLE_COLUMNS[table_name],
                rows=_rows_for_table(rows, table_name),
            )
    return _row_counts(database_file)


def apply_delta_state(database_path: str | Path, rows: GleifStateRows) -> dict[str, int]:
    database_file = Path(database_path)
    catalog_name = database_file.stem
    with duckdb.connect(str(database_file)) as connection:
        _ensure_schema(connection, catalog_name)
        _ensure_all_tables(connection, catalog_name=catalog_name)
        for table_name in tables.GLEIF_TABLES:
            table_rows = _rows_for_table(rows, table_name)
            if not table_rows:
                continue
            _upsert_table_rows(
                connection,
                catalog_name=catalog_name,
                table_name=table_name,
                columns=tables.GLEIF_TABLE_COLUMNS[table_name],
                key_columns=TABLE_KEYS[table_name],
                rows=table_rows,
            )
    return _row_counts(database_file)


def refresh_gleif_duckdb_state(
    *,
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    database_path: str | Path,
) -> dg.MaterializeResult:
    manifest = _manifest_for_run(object_store, context.run_id)
    state_rows = _rows_from_manifest(object_store, manifest)
    load_mode = str(manifest["load_mode"])
    if load_mode == "full":
        row_counts = replace_current_state(database_path, state_rows)
        new_state = {
            "last_full_publish_date": manifest["publish_date"],
            "last_delta_publish_date": None,
            "last_successful_run_id": manifest["run_id"],
            "row_counts": row_counts,
        }
    elif load_mode == "delta":
        current_state = read_gleif_state(object_store)
        ensure_bootstrap_state_for_delta(current_state)
        row_counts = apply_delta_state(database_path, state_rows)
        new_state = {
            **(current_state or {}),
            "last_delta_publish_date": manifest["publish_date"],
            "last_successful_run_id": manifest["run_id"],
            "row_counts": row_counts,
        }
    else:
        raise ValueError(f"Unsupported GLEIF load mode: {load_mode}")

    write_gleif_state(object_store, new_state)
    return dg.MaterializeResult(
        metadata={
            "load_mode": load_mode,
            "publish_date": str(manifest["publish_date"]),
            "duckdb_path": str(database_path),
            **{f"{table_name}_row_count": count for table_name, count in row_counts.items()},
        }
    )


def _replace_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    table_name: str,
    columns: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> None:
    connection.execute(
        f"create or replace table "
        f"{_qualified_table(table_name, catalog_name=catalog_name)} ({_column_sql(columns)})"
    )
    _insert_rows(
        connection,
        catalog_name=catalog_name,
        table_name=table_name,
        columns=columns,
        rows=rows,
    )


def _upsert_table_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    table_name: str,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> None:
    temp_table = f"delta_{table_name}"
    connection.execute(f'create or replace temporary table "{temp_table}" ({_column_sql(columns)})')
    _insert_rows(
        connection,
        table_name=temp_table,
        columns=columns,
        rows=rows,
        catalog_name=None,
    )
    join_predicate = " and ".join(
        f'current."{column}" is not distinct from delta."{column}"'
        for column in key_columns
    )
    connection.execute(
        f"delete from {_qualified_table(table_name, catalog_name=catalog_name)} as current "
        f'using "{temp_table}" as delta where {join_predicate}'
    )
    connection.execute(
        f"insert into {_qualified_table(table_name, catalog_name=catalog_name)} "
        f'select {", ".join(_quote(column) for column in columns)} from "{temp_table}"'
    )


def _insert_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    table_name: str,
    columns: tuple[str, ...],
    rows: list[dict[str, Any]],
    catalog_name: str | None,
) -> None:
    if not rows:
        return
    qualified_table = _qualified_table(table_name, catalog_name=catalog_name)
    parameter_markers = ", ".join("?" for _ in columns)
    values = [[row.get(column) for column in columns] for row in rows]
    connection.executemany(
        f"insert into {qualified_table} values ({parameter_markers})",
        values,
    )


def _ensure_schema(connection: duckdb.DuckDBPyConnection, catalog_name: str) -> None:
    connection.execute(
        f"create schema if not exists {_quote(catalog_name)}.{_quote(DUCKDB_SCHEMA)}"
    )


def _ensure_all_tables(connection: duckdb.DuckDBPyConnection, *, catalog_name: str) -> None:
    for table_name in tables.GLEIF_TABLES:
        connection.execute(
            f"create table if not exists {_qualified_table(table_name, catalog_name=catalog_name)} "
            f"({_column_sql(tables.GLEIF_TABLE_COLUMNS[table_name])})"
        )


def _row_counts(database_path: Path) -> dict[str, int]:
    catalog_name = database_path.stem
    with duckdb.connect(str(database_path), read_only=True) as connection:
        return {
            table_name: int(
                connection.execute(
                    f"select count(*) from {_qualified_table(table_name, catalog_name=catalog_name)}"
                ).fetchone()[0]
            )
            for table_name in tables.GLEIF_TABLES
        }


def _rows_for_table(rows: GleifStateRows, table_name: str) -> list[dict[str, Any]]:
    return getattr(rows, TABLE_ROW_FIELDS[table_name])


def _column_sql(columns: tuple[str, ...]) -> str:
    return ", ".join(f'{_quote(column)} {_duckdb_column_type(column)}' for column in columns)


def _duckdb_column_type(column: str) -> str:
    return DUCKDB_COLUMN_TYPES.get(column, "varchar")


def _quote(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) + chr(34))}"'


def _qualified_table(table_name: str, *, catalog_name: str | None) -> str:
    quoted_table = _quote(table_name)
    if catalog_name is None:
        return quoted_table
    return f"{_quote(catalog_name)}.{_quote(DUCKDB_SCHEMA)}.{quoted_table}"


def _latest_manifest(object_store: ObjectStoreResource) -> dict[str, Any]:
    manifest_keys = _manifest_keys(object_store)
    if not manifest_keys:
        raise ValueError("No GLEIF raw manifest found in object storage")
    return max(
        (_read_manifest(object_store, key) for key in manifest_keys),
        key=_manifest_order_key,
    )


def _manifest_for_run(object_store: ObjectStoreResource, run_id: str) -> dict[str, Any]:
    manifest_keys = _manifest_keys(object_store)
    run_manifest_keys = [
        key
        for key in manifest_keys
        if f"/run_id={run_id}/" in key
    ]
    if not run_manifest_keys:
        return _latest_manifest(object_store)
    return max(
        (_read_manifest(object_store, key) for key in run_manifest_keys),
        key=_manifest_order_key,
    )


def _manifest_keys(object_store: ObjectStoreResource) -> list[str]:
    return [
        key
        for key in object_store.list_keys("gleif/raw/", bucket=GLEIF_RAW_BUCKET)
        if key.endswith("/manifest.json")
    ]


def _read_manifest(object_store: ObjectStoreResource, key: str) -> dict[str, Any]:
    return json.loads(object_store.read_bytes(key, bucket=GLEIF_RAW_BUCKET))


def _manifest_order_key(manifest: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(manifest.get("publish_date") or ""),
        str(manifest.get("pulled_at") or ""),
        str(manifest.get("run_id") or ""),
    )


def _rows_from_manifest(
    object_store: ObjectStoreResource,
    manifest: dict[str, Any],
) -> GleifStateRows:
    rows = GleifStateRows()
    retrieved_at = str(manifest["pulled_at"])
    resolved_at = str(manifest["pulled_at"])
    source_run_id = str(manifest["run_id"])
    publish_date = str(manifest["publish_date"])
    for item in manifest.get("files", []):
        file_kind = item.get("file_kind")
        body = object_store.read_bytes(str(item["s3_key"]), bucket=GLEIF_RAW_BUCKET)
        if file_kind == "lei_records":
            parsed = parser.parse_lei_records_zip(
                body,
                source_run_id=source_run_id,
                retrieved_at=retrieved_at,
                resolved_at=resolved_at,
                golden_copy_publish_date=publish_date,
            )
            rows.lei_records.extend(parsed.lei_records)
            rows.lei_names.extend(parsed.lei_names)
            rows.lei_addresses.extend(parsed.lei_addresses)
            rows.lei_identifiers.extend(parsed.lei_identifiers)
        elif file_kind == "relationships":
            parsed = parser.parse_relationships_zip(
                body,
                source_run_id=source_run_id,
                retrieved_at=retrieved_at,
                resolved_at=resolved_at,
            )
            rows.relationships.extend(parsed.relationships)
            rows.relationship_periods.extend(parsed.relationship_periods)
        elif file_kind == "reporting_exceptions":
            parsed = parser.parse_reporting_exceptions_zip(
                body,
                source_run_id=source_run_id,
                retrieved_at=retrieved_at,
                resolved_at=resolved_at,
            )
            rows.reporting_exceptions.extend(parsed.reporting_exceptions)
    return rows
