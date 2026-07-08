from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pyarrow as pa
from dagster_clickhouse import ClickhouseResource

RESOLVED_DATABASE = "corpscout"

REQUIRED_FINLAND_RESOLVED_TABLES = (
    "fi_companies",
    "fi_websites",
    "fi_industries",
    "fi_addresses",
    "fi_registered_entries",
    "fi_legal_forms",
    "fi_financial_statements",
)

DEFAULT_CLICKHOUSE_INSERT_BATCH_SIZE = 50_000


def assert_clickhouse_tables_exist(
    clickhouse: ClickhouseResource,
    *,
    database: str,
    tables: Sequence[str],
) -> None:
    requested_tables = tuple(tables)
    with clickhouse.get_connection() as client:
        rows = client.execute(
            """
            SELECT name
            FROM system.tables
            WHERE database = %(database)s
              AND name IN %(tables)s
            """,
            {"database": database, "tables": requested_tables},
        )

    existing = {str(row[0]) for row in rows}
    missing = [table for table in requested_tables if table not in existing]
    if missing:
        raise ValueError(
            f"Missing ClickHouse tables in {database}: {', '.join(missing)}"
        )


def export_duckdb_connection_table_to_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    duckdb_schema: str,
    duckdb_table: str,
    clickhouse_database: str,
    clickhouse_table: str,
    columns: Sequence[str],
    truncate: bool,
    batch_size: int = DEFAULT_CLICKHOUSE_INSERT_BATCH_SIZE,
    column_expressions: Mapping[str, str] | None = None,
    log: Callable[..., object] | None = None,
) -> int:
    _validate_batch_size(batch_size)
    _validate_column_expressions(columns, column_expressions)
    clickhouse_columns = ", ".join(
        _quote_clickhouse_identifier(column) for column in columns
    )
    clickhouse_qualified_table = (
        f"{_quote_clickhouse_identifier(clickhouse_database)}."
        f"{_quote_clickhouse_identifier(clickhouse_table)}"
    )
    if not truncate:
        return _insert_duckdb_table_in_batches(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=duckdb_schema,
            duckdb_table=duckdb_table,
            clickhouse_database=clickhouse_database,
            clickhouse_table=clickhouse_table,
            clickhouse_qualified_table=clickhouse_qualified_table,
            clickhouse_columns=clickhouse_columns,
            columns=columns,
            batch_size=batch_size,
            column_expressions=column_expressions,
            log=log,
            log_table=f"{clickhouse_database}.{clickhouse_table}",
        )

    clickhouse_stage_table = _clickhouse_stage_table_name(clickhouse_table)
    clickhouse_qualified_stage_table = _quote_clickhouse_qualified_table(
        clickhouse_database,
        clickhouse_stage_table,
    )
    clickhouse_client.execute(
        f"CREATE TABLE {clickhouse_qualified_stage_table} AS {clickhouse_qualified_table}"
    )
    primary_error: Exception | None = None
    row_count = 0
    try:
        row_count = _insert_duckdb_table_in_batches(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=duckdb_schema,
            duckdb_table=duckdb_table,
            clickhouse_database=clickhouse_database,
            clickhouse_table=clickhouse_stage_table,
            clickhouse_qualified_table=clickhouse_qualified_stage_table,
            clickhouse_columns=clickhouse_columns,
            columns=columns,
            batch_size=batch_size,
            column_expressions=column_expressions,
            log=log,
            log_table=f"{clickhouse_database}.{clickhouse_table}",
        )
        clickhouse_client.execute(
            f"EXCHANGE TABLES {clickhouse_qualified_stage_table} AND {clickhouse_qualified_table}"
        )
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        _drop_clickhouse_stage_tables(
            clickhouse_client,
            (clickhouse_qualified_stage_table,),
            suppress_errors=primary_error is not None,
        )
    return row_count


def replace_duckdb_connection_tables_in_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    duckdb_schema: str,
    clickhouse_database: str,
    tables: Sequence[tuple[str, Sequence[str]]],
    batch_size: int = DEFAULT_CLICKHOUSE_INSERT_BATCH_SIZE,
) -> dict[str, int]:
    _validate_batch_size(batch_size)
    requested_tables = tuple(
        (clickhouse_table, tuple(columns)) for clickhouse_table, columns in tables
    )

    clickhouse_columns_by_table = {
        clickhouse_table: ", ".join(
            _quote_clickhouse_identifier(column) for column in columns
        )
        for clickhouse_table, columns in requested_tables
    }
    clickhouse_qualified_tables = {
        clickhouse_table: _quote_clickhouse_qualified_table(
            clickhouse_database, clickhouse_table
        )
        for clickhouse_table, _ in requested_tables
    }
    clickhouse_qualified_stage_tables: dict[str, str] = {}
    created_stage_tables: list[str] = []
    exchanged_tables: list[str] = []
    row_counts: dict[str, int] = {}
    primary_error: Exception | None = None

    try:
        for clickhouse_table, _ in requested_tables:
            clickhouse_stage_table = _clickhouse_stage_table_name(clickhouse_table)
            clickhouse_qualified_stage_table = _quote_clickhouse_qualified_table(
                clickhouse_database,
                clickhouse_stage_table,
            )
            clickhouse_qualified_stage_tables[clickhouse_table] = (
                clickhouse_qualified_stage_table
            )
            created_stage_tables.append(clickhouse_table)
            clickhouse_client.execute(
                f"CREATE TABLE {clickhouse_qualified_stage_table} AS "
                f"{clickhouse_qualified_tables[clickhouse_table]}"
            )

        for clickhouse_table, columns in requested_tables:
            clickhouse_stage_table = clickhouse_qualified_stage_tables[clickhouse_table]
            row_counts[clickhouse_table] = _insert_duckdb_table_in_batches(
                duckdb_connection=duckdb_connection,
                clickhouse_client=clickhouse_client,
                duckdb_schema=duckdb_schema,
                duckdb_table=clickhouse_table,
                clickhouse_database=clickhouse_database,
                clickhouse_table=_unqualified_clickhouse_table(clickhouse_stage_table),
                clickhouse_qualified_table=clickhouse_stage_table,
                clickhouse_columns=clickhouse_columns_by_table[clickhouse_table],
                columns=columns,
                batch_size=batch_size,
            )

        for clickhouse_table, _ in requested_tables:
            clickhouse_client.execute(
                f"EXCHANGE TABLES {clickhouse_qualified_stage_tables[clickhouse_table]} "
                f"AND {clickhouse_qualified_tables[clickhouse_table]}"
            )
            exchanged_tables.append(clickhouse_table)
    except Exception as exc:
        primary_error = exc
        rollback_failures: list[str] = []
        for clickhouse_table in reversed(exchanged_tables):
            try:
                clickhouse_client.execute(
                    f"EXCHANGE TABLES {clickhouse_qualified_stage_tables[clickhouse_table]} "
                    f"AND {clickhouse_qualified_tables[clickhouse_table]}"
                )
            except Exception:
                rollback_failures.append(
                    f"{clickhouse_table} "
                    f"({clickhouse_qualified_stage_tables[clickhouse_table]} <-> "
                    f"{clickhouse_qualified_tables[clickhouse_table]})"
                )
        if rollback_failures:
            raise RuntimeError(
                "Rollback failed after ClickHouse publish error; ClickHouse may be inconsistent. "
                "Failed rollback exchange(s): " + ", ".join(rollback_failures)
            ) from exc
        raise
    finally:
        _drop_clickhouse_stage_tables(
            clickhouse_client,
            tuple(
                clickhouse_qualified_stage_tables[clickhouse_table]
                for clickhouse_table in reversed(created_stage_tables)
            ),
            suppress_errors=primary_error is not None,
        )

    return row_counts


def _insert_duckdb_table_in_batches(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    duckdb_schema: str,
    duckdb_table: str,
    clickhouse_database: str,
    clickhouse_table: str,
    clickhouse_qualified_table: str,
    clickhouse_columns: str,
    columns: Sequence[str],
    batch_size: int,
    column_expressions: Mapping[str, str] | None = None,
    log: Callable[..., object] | None = None,
    log_table: str = "",
) -> int:
    if callable(getattr(clickhouse_client, "insert_arrow", None)):
        return _insert_duckdb_arrow_batches(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=duckdb_schema,
            duckdb_table=duckdb_table,
            clickhouse_database=clickhouse_database,
            clickhouse_table=clickhouse_table,
            clickhouse_qualified_table=clickhouse_qualified_table,
            clickhouse_columns=clickhouse_columns,
            columns=columns,
            batch_size=batch_size,
            column_expressions=column_expressions,
            log=log,
            log_table=log_table,
        )
    return _insert_duckdb_rows_in_batches(
        duckdb_connection=duckdb_connection,
        clickhouse_client=clickhouse_client,
        duckdb_schema=duckdb_schema,
        duckdb_table=duckdb_table,
        clickhouse_qualified_table=clickhouse_qualified_table,
        clickhouse_columns=clickhouse_columns,
        columns=columns,
        batch_size=batch_size,
        column_expressions=column_expressions,
        log=log,
        log_table=log_table,
    )


def _insert_duckdb_arrow_batches(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    duckdb_schema: str,
    duckdb_table: str,
    clickhouse_database: str,
    clickhouse_table: str,
    clickhouse_qualified_table: str,
    clickhouse_columns: str,
    columns: Sequence[str],
    batch_size: int,
    column_expressions: Mapping[str, str] | None = None,
    log: Callable[..., object] | None = None,
    log_table: str = "",
) -> int:
    duckdb_columns = _duckdb_select_list(columns, column_expressions)
    duckdb_qualified_table = (
        f"{_quote_duckdb_qualified_schema(duckdb_schema)}."
        f"{_quote_duckdb_identifier(duckdb_table)}"
    )
    result = duckdb_connection.execute(
        f"select {duckdb_columns} from {duckdb_qualified_table}"
    )

    row_count = 0
    batch_number = 0
    for record_batch in result.to_arrow_reader(batch_size=batch_size):
        if record_batch.num_rows < 1:
            continue
        batch_number += 1
        if _arrow_batch_has_nullable_date(record_batch):
            rows = _arrow_batch_rows(record_batch, columns)
            insert_rows = getattr(clickhouse_client, "insert_rows", None)
            if callable(insert_rows):
                insert_rows(
                    clickhouse_table,
                    rows,
                    columns=tuple(columns),
                    database=clickhouse_database,
                )
            else:
                clickhouse_client.execute(
                    f"INSERT INTO {clickhouse_qualified_table} ({clickhouse_columns}) VALUES",
                    rows,
                )
            row_count += record_batch.num_rows
            _log_clickhouse_batch(
                log,
                batch_kind="row",
                log_table=log_table,
                batch_number=batch_number,
                batch_rows=record_batch.num_rows,
                total_rows=row_count,
            )
            continue
        clickhouse_client.insert_arrow(
            clickhouse_table,
            pa.Table.from_batches([record_batch]),
            database=clickhouse_database,
        )
        row_count += record_batch.num_rows
        _log_clickhouse_batch(
            log,
            batch_kind="arrow",
            log_table=log_table,
            batch_number=batch_number,
            batch_rows=record_batch.num_rows,
            total_rows=row_count,
        )
    return row_count


def _arrow_batch_has_nullable_date(record_batch: pa.RecordBatch) -> bool:
    return any(
        pa.types.is_date(field.type) and record_batch.column(index).null_count > 0
        for index, field in enumerate(record_batch.schema)
    )


def _arrow_batch_rows(
    record_batch: pa.RecordBatch,
    columns: Sequence[str],
) -> list[tuple[object, ...]]:
    return [
        tuple(row[column] for column in columns) for row in record_batch.to_pylist()
    ]


def _insert_duckdb_rows_in_batches(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    duckdb_schema: str,
    duckdb_table: str,
    clickhouse_qualified_table: str,
    clickhouse_columns: str,
    columns: Sequence[str],
    batch_size: int,
    column_expressions: Mapping[str, str] | None = None,
    log: Callable[..., object] | None = None,
    log_table: str = "",
) -> int:
    duckdb_columns = _duckdb_select_list(columns, column_expressions)
    duckdb_qualified_table = (
        f"{_quote_duckdb_qualified_schema(duckdb_schema)}."
        f"{_quote_duckdb_identifier(duckdb_table)}"
    )
    result = duckdb_connection.execute(
        f"select {duckdb_columns} from {duckdb_qualified_table}"
    )

    row_count = 0
    batch_number = 0
    while True:
        rows = result.fetchmany(batch_size)
        if not rows:
            return row_count
        clickhouse_client.execute(
            f"INSERT INTO {clickhouse_qualified_table} ({clickhouse_columns}) VALUES",
            rows,
        )
        row_count += len(rows)
        batch_number += 1
        _log_clickhouse_batch(
            log,
            batch_kind="row",
            log_table=log_table,
            batch_number=batch_number,
            batch_rows=len(rows),
            total_rows=row_count,
        )


def _validate_batch_size(batch_size: int) -> None:
    if batch_size < 1:
        raise ValueError("batch_size must be greater than zero")


def _validate_column_expressions(
    columns: Sequence[str],
    column_expressions: Mapping[str, str] | None,
) -> None:
    if column_expressions is None:
        return
    unknown_columns = sorted(set(column_expressions) - set(columns))
    if unknown_columns:
        raise ValueError(
            "column_expressions contains columns not present in export columns: "
            + ", ".join(unknown_columns)
        )


def _duckdb_select_list(
    columns: Sequence[str],
    column_expressions: Mapping[str, str] | None,
) -> str:
    expressions = column_expressions or {}
    return ", ".join(
        f"{expressions[column]} as {_quote_duckdb_identifier(column)}"
        if column in expressions
        else _quote_duckdb_identifier(column)
        for column in columns
    )


def _log_clickhouse_batch(
    log: Callable[..., object] | None,
    *,
    batch_kind: str,
    log_table: str,
    batch_number: int,
    batch_rows: int,
    total_rows: int,
) -> None:
    if log is None:
        return
    log(
        "Inserted ClickHouse %s batch: table=%s batch_number=%d "
        "batch_rows=%d total_rows=%d",
        batch_kind,
        log_table,
        batch_number,
        batch_rows,
        total_rows,
    )


def _quote_duckdb_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _quote_duckdb_qualified_schema(schema: str) -> str:
    return ".".join(_quote_duckdb_identifier(part) for part in schema.split("."))


def _quote_clickhouse_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def _quote_clickhouse_qualified_table(database: str, table: str) -> str:
    return (
        f"{_quote_clickhouse_identifier(database)}."
        f"{_quote_clickhouse_identifier(table)}"
    )


def _unqualified_clickhouse_table(qualified_table: str) -> str:
    escaped_table = qualified_table.rsplit(".", maxsplit=1)[-1]
    if escaped_table.startswith("`") and escaped_table.endswith("`"):
        escaped_table = escaped_table[1:-1]
    return escaped_table.replace("``", "`")


def _clickhouse_stage_table_name(table: str) -> str:
    return f"_tmp_{table}_{uuid.uuid4().hex}"


def _drop_clickhouse_stage_tables(
    clickhouse_client: Any,
    qualified_stage_tables: Sequence[str],
    *,
    suppress_errors: bool,
) -> None:
    failed_stage_tables: list[str] = []
    first_error: Exception | None = None

    for qualified_stage_table in qualified_stage_tables:
        try:
            clickhouse_client.execute(f"DROP TABLE IF EXISTS {qualified_stage_table}")
        except Exception as exc:
            if suppress_errors:
                continue
            if first_error is None:
                first_error = exc
            failed_stage_tables.append(qualified_stage_table)

    if failed_stage_tables and first_error is not None:
        raise RuntimeError(
            "Failed to drop ClickHouse stage table(s): "
            + ", ".join(failed_stage_tables)
        ) from first_error
