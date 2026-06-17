from pathlib import Path

import dagster as dg
import duckdb
import pytest

from dagster_v3.defs.duckdb.schema_contract import (
    DuckDBColumnContract,
    DuckDBTableContract,
    create_duckdb_table_from_contract,
    dagster_table_schema_from_contract,
    validate_duckdb_table_contract,
)


def test_create_duckdb_table_from_contract_creates_typed_table(tmp_path: Path) -> None:
    database_path = tmp_path / "contracts.duckdb"
    contract = DuckDBTableContract(
        columns=(
            DuckDBColumnContract("rate_date", "DATE", nullable=False),
            DuckDBColumnContract("rate", "DECIMAL(38, 12)"),
            DuckDBColumnContract("source", "VARCHAR"),
        )
    )

    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema exchange_rates_stage")
        create_duckdb_table_from_contract(
            connection,
            schema="exchange_rates_stage",
            table="ecb_rates",
            contract=contract,
        )
        rows = connection.execute(
            """
            select column_name, data_type, is_nullable
            from information_schema.columns
            where table_schema = 'exchange_rates_stage'
              and table_name = 'ecb_rates'
            order by ordinal_position
            """
        ).fetchall()

    assert rows == [
        ("rate_date", "DATE", "NO"),
        ("rate", "DECIMAL(38,12)", "YES"),
        ("source", "VARCHAR", "YES"),
    ]


def test_validate_duckdb_table_contract_rejects_type_mismatch(tmp_path: Path) -> None:
    database_path = tmp_path / "contracts.duckdb"
    contract = DuckDBTableContract(
        columns=(
            DuckDBColumnContract("rate_date", "DATE"),
            DuckDBColumnContract("rate", "DECIMAL(38, 12)"),
        )
    )

    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema exchange_rates_stage")
        connection.execute(
            """
            create table exchange_rates_stage.ecb_rates (
              rate_date varchar,
              rate decimal(38, 12)
            )
            """
        )

        with pytest.raises(ValueError, match="rate_date"):
            validate_duckdb_table_contract(
                connection,
                schema="exchange_rates_stage",
                table="ecb_rates",
                contract=contract,
            )


def test_dagster_table_schema_from_contract() -> None:
    contract = DuckDBTableContract(
        columns=(
            DuckDBColumnContract("rate_date", "DATE", description="Rate date."),
            DuckDBColumnContract("rate", "DECIMAL(38, 12)"),
        )
    )

    schema = dagster_table_schema_from_contract(contract)

    assert isinstance(schema, dg.TableSchema)
    assert [(column.name, column.type, column.description) for column in schema.columns] == [
        ("rate_date", "DATE", "Rate date."),
        ("rate", "DECIMAL(38, 12)", None),
    ]
