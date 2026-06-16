from __future__ import annotations

from collections.abc import Sequence

from dagster_clickhouse import ClickhouseResource

RESOLVED_DATABASE = "corpscout_resolved"

REQUIRED_FINLAND_RESOLVED_TABLES = (
    "fi_companies",
    "fi_websites",
    "fi_industries",
    "fi_addresses",
    "fi_registered_entries",
    "fi_legal_forms",
    "fi_financial_statements",
    "fi_financial_metrics",
)


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
        raise ValueError(f"Missing ClickHouse tables in {database}: {', '.join(missing)}")
