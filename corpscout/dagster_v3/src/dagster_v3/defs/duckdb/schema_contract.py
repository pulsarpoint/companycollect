from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import dagster as dg


@dataclass(frozen=True)
class DuckDBColumnContract:
    name: str
    duckdb_type: str
    nullable: bool = True
    description: str | None = None


@dataclass(frozen=True)
class DuckDBTableContract:
    columns: tuple[DuckDBColumnContract, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    @property
    def column_types(self) -> dict[str, str]:
        return {column.name: column.duckdb_type for column in self.columns}


def create_duckdb_table_from_contract(
    connection: Any,
    *,
    schema: str,
    table: str,
    contract: DuckDBTableContract,
) -> None:
    columns_sql = ", ".join(_column_definition(column) for column in contract.columns)
    connection.execute(
        f"""
        create table if not exists {_quote_duckdb_identifier(schema)}.{_quote_duckdb_identifier(table)}
        ({columns_sql})
        """
    )
    validate_duckdb_table_contract(
        connection,
        schema=schema,
        table=table,
        contract=contract,
    )


def validate_duckdb_table_contract(
    connection: Any,
    *,
    schema: str,
    table: str,
    contract: DuckDBTableContract,
) -> None:
    rows = connection.execute(
        """
        select column_name, data_type, is_nullable
        from information_schema.columns
        where table_schema = ?
          and table_name = ?
        order by ordinal_position
        """,
        [schema, table],
    ).fetchall()
    actual = {
        str(column_name): {
            "type": _normalize_duckdb_type(str(data_type)),
            "nullable": str(is_nullable).upper() == "YES",
        }
        for column_name, data_type, is_nullable in rows
    }
    missing = [column.name for column in contract.columns if column.name not in actual]
    if missing:
        raise ValueError(
            f"DuckDB table {schema}.{table} is missing column(s): {', '.join(missing)}"
        )
    failures: list[str] = []
    for column in contract.columns:
        actual_column = actual[column.name]
        expected_type = _normalize_duckdb_type(column.duckdb_type)
        if actual_column["type"] != expected_type:
            failures.append(
                f"{column.name} type expected {column.duckdb_type}, got {actual_column['type']}"
            )
        if actual_column["nullable"] != column.nullable:
            expected_nullability = "nullable" if column.nullable else "not null"
            actual_nullability = "nullable" if actual_column["nullable"] else "not null"
            failures.append(
                f"{column.name} nullability expected {expected_nullability}, got {actual_nullability}"
            )
    if failures:
        raise ValueError(
            f"DuckDB table {schema}.{table} does not match contract: "
            + "; ".join(failures)
        )


def dagster_table_schema_from_contract(contract: DuckDBTableContract) -> dg.TableSchema:
    return dg.TableSchema(
        columns=[
            dg.TableColumn(
                name=column.name,
                type=column.duckdb_type,
                description=column.description,
            )
            for column in contract.columns
        ]
    )


def _column_definition(column: DuckDBColumnContract) -> str:
    nullability = "" if column.nullable else " not null"
    return f"{_quote_duckdb_identifier(column.name)} {column.duckdb_type}{nullability}"


def _quote_duckdb_identifier(identifier: str) -> str:
    return f"\"{identifier.replace('\"', '\"\"')}\""


def _normalize_duckdb_type(duckdb_type: str) -> str:
    return duckdb_type.upper().replace(" ", "")
