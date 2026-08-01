"""The export must export the month it was asked for.

2026-07-28: 37 partitions (2022-01..2025-01) all materialized successfully and
36 of them left ClickHouse EMPTY. `normalize.py` does `create or replace table
contract_candidates`, so the DuckDB buffer holds only the month just processed;
the export read that buffer unfiltered and assumed it was the month it had been
asked to export. Every `…_duckdb` run finished 2026-07-27 and every
`…_clickhouse` run started 2026-07-28, so all 37 exports read the same leftover
2025-01 rows.

`ALTER TABLE t REPLACE PARTITION 202403 FROM stage` means "drop 202403, then copy
stage's 202403 in". The stage held 2025-01 rows, so its 202403 partition was
empty and the statement deleted a populated partition and copied nothing --
which ClickHouse reports as success. Partition 2024-03 had really produced
65,218 rows.

The existing guard asks "did the DuckDB read return anything?" It returned
116,226 valid rows. The question that mattered was "are they THIS month's?".
"""

import duckdb
import pytest

from dagster_v3.defs.brazil_pncp import tables
from dagster_v3.defs.brazil_pncp.clickhouse import export_contracts_clickhouse


class _StubClickhouse:
    """Answers the two counts the export makes and records the DDL/DML."""

    def __init__(self, existing_in_partition: int = 0) -> None:
        self.existing = existing_in_partition
        self.queries: list[str] = []

    def execute(self, query: str, params=None):
        self.queries.append(query)
        stripped = query.strip().upper()
        # Order matters: the stage's "count(), countIf(...)" also starts with
        # "SELECT COUNT()".
        if "COUNTIF" in stripped:
            return [(1, 1)]
        if stripped.startswith("SELECT COUNT()"):
            return [(self.existing,)]
        return []

    @property
    def replaced_partitions(self) -> list[str]:
        return [q for q in self.queries if "REPLACE PARTITION" in q]


def _candidates(connection, publication_dates: list[str]) -> None:
    """Partitioned candidates with one row per given publication date."""
    connection.execute(f"create schema if not exists {tables.DUCKDB_SCHEMA}")
    columns = ", ".join(
        f"{name} date" if name == "data_publicacao_pncp" else f"{name} varchar"
        for name in tables.CANDIDATE_COLUMNS
    )
    connection.execute(
        f"create or replace table "
        f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE} "
        f"({tables.CANDIDATE_PARTITION_COLUMN} varchar, {columns})"
    )
    for value in publication_dates:
        partition = value[:7].replace("-", "")
        connection.execute(
            f"insert into {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE} "
            f"({tables.CANDIDATE_PARTITION_COLUMN}, data_publicacao_pncp) "
            f"values ('{partition}', date '{value}')"
        )


def test_exporting_the_month_in_the_buffer_works() -> None:
    connection = duckdb.connect(":memory:")
    _candidates(connection, ["2025-01-31", "2025-01-02"])
    client = _StubClickhouse()

    result = export_contracts_clickhouse(
        duckdb_connection=connection, clickhouse_client=client, partition="202501"
    )

    assert result["contract_rows"] == 1
    assert len(client.replaced_partitions) == 1
    assert "REPLACE PARTITION 202501" in client.replaced_partitions[0]


def test_exporting_a_month_the_buffer_does_not_hold_is_refused() -> None:
    """The actual 36-month deletion. The buffer holds 2025-01 while the export
    was asked for 2024-03, and the old code silently emptied 202403."""
    connection = duckdb.connect(":memory:")
    _candidates(connection, ["2025-01-31", "2025-01-02"])
    client = _StubClickhouse(existing_in_partition=65218)

    with pytest.raises(ValueError, match="202403"):
        export_contracts_clickhouse(
            duckdb_connection=connection, clickhouse_client=client, partition="202403"
        )

    # Nothing may be replaced: the whole point is that the destructive statement
    # never runs.
    assert client.replaced_partitions == []


def test_the_refusal_names_the_month_the_buffer_actually_holds() -> None:
    """A message saying only "wrong month" sends the reader back to the code.
    Naming what was found points straight at the stale-buffer cause."""
    connection = duckdb.connect(":memory:")
    _candidates(connection, ["2025-01-31"])
    client = _StubClickhouse()

    with pytest.raises(ValueError, match="202501"):
        export_contracts_clickhouse(
            duckdb_connection=connection, clickhouse_client=client, partition="202403"
        )


def test_export_reads_only_the_requested_month_from_partitioned_candidates() -> None:
    connection = duckdb.connect(":memory:")
    _candidates(connection, ["2024-03-05", "2025-01-31"])
    client = _StubClickhouse()

    result = export_contracts_clickhouse(
        duckdb_connection=connection, clickhouse_client=client, partition="202403"
    )

    assert result["contract_rows"] == 1
    assert len(client.replaced_partitions) == 1


def test_an_unattributable_publication_date_is_refused_not_misfiled() -> None:
    """data_publicacao_pncp is a try_cast and can be NULL. ClickHouse partitions
    on ifNull(date, 1970-01-01), so such a row lands in 197001 -- a partition no
    month's export ever targets, so it would be invisible forever. 0 rows today
    (malformed_publication_dates), and it must stay loud if that changes."""
    connection = duckdb.connect(":memory:")
    _candidates(connection, ["2025-01-31"])
    connection.execute(
        f"insert into {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE} "
        f"({tables.CANDIDATE_PARTITION_COLUMN}, data_publicacao_pncp) "
        f"values ('202501', NULL)"
    )
    client = _StubClickhouse()

    with pytest.raises(ValueError, match="197001"):
        export_contracts_clickhouse(
            duckdb_connection=connection, clickhouse_client=client, partition="202501"
        )


def test_an_empty_buffer_still_refuses_to_blank_a_populated_partition() -> None:
    """The pre-existing guard, kept: an empty fetch is a degraded run, not a
    month in which Brazil awarded no contracts."""
    connection = duckdb.connect(":memory:")
    _candidates(connection, [])
    client = _StubClickhouse(existing_in_partition=65218)

    with pytest.raises(ValueError, match="refusing to blank"):
        export_contracts_clickhouse(
            duckdb_connection=connection, clickhouse_client=client, partition="202403"
        )
