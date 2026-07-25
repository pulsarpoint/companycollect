from __future__ import annotations

import dagster as dg
import duckdb

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.gleif import tables
from dagster_v3.defs.gleif.source import (
    ensure_bootstrap_state_for_delta,
    manifest_for_run,
    read_gleif_state,
    write_gleif_state,
)

DUCKDB_SCHEMA = "gleif"
DUCKDB_STAGING_SCHEMA = "gleif_staging"

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

FULL_REFRESH_TABLES = frozenset(
    {
        tables.GLEIF_LEI_ISSUERS_TABLE,
        tables.GLEIF_CODE_LIST_ENTRIES_TABLE,
    }
)

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


def refresh_gleif_duckdb_state(
    *,
    context: dg.AssetExecutionContext,
    object_store: ObjectStoreResource,
    connection: duckdb.DuckDBPyConnection,
    catalog_name: str,
    database_label: str,
) -> dg.MaterializeResult:
    from dagster_v3.defs.gleif.csv_transforms import replace_current_from_dlt_raw_tables

    manifest = manifest_for_run(object_store, context.run_id)
    load_mode = str(manifest["load_mode"])
    if load_mode == "full":
        row_counts = replace_current_from_dlt_raw_tables(
            connection=connection,
            catalog_name=catalog_name,
            load_mode="full",
            publish_date=str(manifest["publish_date"]),
            run_id=str(manifest["run_id"]),
        )
        new_state = {
            "last_full_publish_date": manifest["publish_date"],
            "last_delta_publish_date": None,
            "last_successful_run_id": manifest["run_id"],
            "row_counts": row_counts,
        }
    elif load_mode == "delta":
        current_state = read_gleif_state(object_store)
        ensure_bootstrap_state_for_delta(current_state)
        row_counts = replace_current_from_dlt_raw_tables(
            connection=connection,
            catalog_name=catalog_name,
            load_mode="delta",
            publish_date=str(manifest["publish_date"]),
            run_id=str(manifest["run_id"]),
        )
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
            "duckdb_path": database_label,
            **{f"{table_name}_row_count": count for table_name, count in row_counts.items()},
        }
    )


def _ensure_empty_tables(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    schema_name: str,
) -> None:
    for table_name in tables.GLEIF_TABLES:
        connection.execute(
            f"create or replace table "
            f"{_qualified_table(table_name, catalog_name=catalog_name, schema_name=schema_name)} "
            f"({_column_sql(tables.GLEIF_TABLE_COLUMNS[table_name])})"
        )


def _replace_current_tables_from_schema(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    source_schema_name: str,
) -> None:
    connection.execute("begin transaction")
    try:
        for table_name in tables.GLEIF_TABLES:
            columns = ", ".join(_quote(column) for column in tables.GLEIF_TABLE_COLUMNS[table_name])
            connection.execute(
                f"create or replace table "
                f"{_qualified_table(table_name, catalog_name=catalog_name, schema_name=DUCKDB_SCHEMA)} "
                f"as select {columns} from "
                f"{_qualified_table(table_name, catalog_name=catalog_name, schema_name=source_schema_name)}"
            )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise


def _upsert_current_tables_from_schema(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    source_schema_name: str,
    source_row_counts: dict[str, int],
) -> None:
    connection.execute("begin transaction")
    try:
        for table_name in tables.GLEIF_TABLES:
            if source_row_counts.get(table_name, 0) == 0:
                continue
            if table_name in FULL_REFRESH_TABLES:
                _replace_table_rows_from_schema(
                    connection,
                    catalog_name=catalog_name,
                    table_name=table_name,
                    source_schema_name=source_schema_name,
                )
                continue
            _upsert_table_from_schema(
                connection,
                catalog_name=catalog_name,
                table_name=table_name,
                source_schema_name=source_schema_name,
            )
        connection.execute("commit")
    except Exception:
        connection.execute("rollback")
        raise


def _replace_table_rows_from_schema(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    table_name: str,
    source_schema_name: str,
) -> None:
    columns = tables.GLEIF_TABLE_COLUMNS[table_name]
    source_table = _qualified_table(
        table_name,
        catalog_name=catalog_name,
        schema_name=source_schema_name,
    )
    target_table = _qualified_table(
        table_name,
        catalog_name=catalog_name,
        schema_name=DUCKDB_SCHEMA,
    )
    connection.execute(f"delete from {target_table}")
    connection.execute(
        f"insert into {target_table} "
        f"select {', '.join(_quote(column) for column in columns)} from {source_table}"
    )


def _upsert_table_from_schema(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    table_name: str,
    source_schema_name: str,
) -> None:
    columns = tables.GLEIF_TABLE_COLUMNS[table_name]
    key_columns = TABLE_KEYS[table_name]
    source_table = _qualified_table(
        table_name,
        catalog_name=catalog_name,
        schema_name=source_schema_name,
    )
    target_table = _qualified_table(
        table_name,
        catalog_name=catalog_name,
        schema_name=DUCKDB_SCHEMA,
    )
    join_predicate = " and ".join(
        f'current."{column}" is not distinct from delta."{column}"'
        for column in key_columns
    )
    connection.execute(
        f"delete from {target_table} as current "
        f"using {source_table} as delta where {join_predicate}"
    )
    connection.execute(
        f"insert into {target_table} "
        f"select {', '.join(_quote(column) for column in columns)} from {source_table}"
    )


def _ensure_schema(
    connection: duckdb.DuckDBPyConnection,
    catalog_name: str,
    *,
    schema_name: str = DUCKDB_SCHEMA,
) -> None:
    connection.execute(
        f"create schema if not exists {_quote(catalog_name)}.{_quote(schema_name)}"
    )


def _ensure_all_tables(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
    schema_name: str,
) -> None:
    for table_name in tables.GLEIF_TABLES:
        connection.execute(
            f"create table if not exists {_qualified_table(table_name, catalog_name=catalog_name, schema_name=schema_name)} "
            f"({_column_sql(tables.GLEIF_TABLE_COLUMNS[table_name])})"
        )


def _row_counts(
    connection: duckdb.DuckDBPyConnection,
    *,
    catalog_name: str,
) -> dict[str, int]:
    return {
        table_name: int(
            connection.execute(
                f"select count(*) from {_qualified_table(table_name, catalog_name=catalog_name, schema_name=DUCKDB_SCHEMA)}"
            ).fetchone()[0]
        )
        for table_name in tables.GLEIF_TABLES
    }


def _column_sql(columns: tuple[str, ...]) -> str:
    return ", ".join(f'{_quote(column)} {_duckdb_column_type(column)}' for column in columns)


def _duckdb_column_type(column: str) -> str:
    return DUCKDB_COLUMN_TYPES.get(column, "varchar")


def _quote(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) + chr(34))}"'


def _qualified_table(
    table_name: str,
    *,
    catalog_name: str | None,
    schema_name: str | None = DUCKDB_SCHEMA,
) -> str:
    quoted_table = _quote(table_name)
    if catalog_name is None:
        return quoted_table
    if schema_name is None:
        return f"{_quote(catalog_name)}.{quoted_table}"
    return f"{_quote(catalog_name)}.{_quote(schema_name)}.{quoted_table}"
