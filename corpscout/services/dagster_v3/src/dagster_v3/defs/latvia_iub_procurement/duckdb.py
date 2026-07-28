from typing import Any

from dagster_v3.defs.latvia_iub_procurement import tables
from dagster_v3.defs.latvia_iub_procurement.parser import ParsedIubDaily

_COLUMN_TYPES = {
    "lot_sequence": "INTEGER",
    "winner_ordinal": "INTEGER",
    "party_ordinal": "INTEGER",
    "is_natural_person": "TINYINT",
    "tender_value_attributable": "TINYINT",
    "received_tenders": "INTEGER",
    "publication_date": "DATE",
    "decision_date": "DATE",
    "contract_conclusion_date": "DATE",
    "actual_end_date": "DATE",
    "estimated_value_amount_eur": "DECIMAL(38, 2)",
    "lowest_tender_amount_eur": "DECIMAL(38, 2)",
    "highest_tender_amount_eur": "DECIMAL(38, 2)",
    "tender_value_amount_eur": "DECIMAL(38, 2)",
    "tender_value_amount_usd": "DECIMAL(38, 2)",
    "source_retrieved_at": "TIMESTAMP",
    "resolved_at": "TIMESTAMP",
}


def write_parsed_month(
    connection: Any, parsed_days: list[ParsedIubDaily]
) -> dict[str, int]:
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {tables.DUCKDB_SCHEMA}")
    table_rows = (
        (
            tables.NOTICES_TABLE,
            tables.NOTICES_COLUMNS,
            [row for parsed in parsed_days for row in parsed.notices],
        ),
        (
            tables.LOTS_TABLE,
            tables.LOTS_COLUMNS,
            [row for parsed in parsed_days for row in parsed.lots],
        ),
        (
            tables.WINNER_CANDIDATES_TABLE,
            tables.WINNER_CANDIDATE_COLUMNS,
            [row for parsed in parsed_days for row in parsed.winners],
        ),
        (
            tables.EXECUTIONS_TABLE,
            tables.EXECUTIONS_COLUMNS,
            [row for parsed in parsed_days for row in parsed.executions],
        ),
    )
    counts: dict[str, int] = {}
    for table, columns, rows in table_rows:
        _replace_table(connection, table=table, columns=columns, rows=rows)
        counts[table] = len(rows)
    if counts[tables.NOTICES_TABLE] == 0:
        raise ValueError("IUB month produced zero notices")
    return counts


def _replace_table(
    connection: Any,
    *,
    table: str,
    columns: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> None:
    definitions = ", ".join(
        f"{column} {_COLUMN_TYPES.get(column, 'VARCHAR')}" for column in columns
    )
    qualified = f"{tables.DUCKDB_SCHEMA}.{table}"
    connection.execute(f"CREATE OR REPLACE TABLE {qualified} ({definitions})")
    if not rows:
        return
    placeholders = ", ".join("?" for _ in columns)
    connection.executemany(
        f"INSERT INTO {qualified} ({', '.join(columns)}) VALUES ({placeholders})",
        [tuple(row[column] for column in columns) for row in rows],
    )
