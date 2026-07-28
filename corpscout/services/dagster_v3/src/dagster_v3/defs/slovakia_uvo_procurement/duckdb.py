from typing import Any

from dagster_v3.defs.slovakia_uvo_procurement import tables

_COLUMN_TYPES = {
    "publication_date": "DATE",
    "winner_ordinal": "INTEGER",
    "contract_conclusion_date": "DATE",
    "awarded_amount_eur": "DECIMAL(38, 2)",
    "awarded_amount_usd": "DECIMAL(38, 2)",
    "lowest_tender_amount_eur": "DECIMAL(38, 2)",
    "highest_tender_amount_eur": "DECIMAL(38, 2)",
    "notice_value_amount_eur": "DECIMAL(38, 2)",
    "received_tenders": "INTEGER",
    "source_retrieved_at": "TIMESTAMP",
    "resolved_at": "TIMESTAMP",
}


def replace_candidates(connection: Any, rows: list[dict[str, Any]]) -> int:
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {tables.DUCKDB_SCHEMA}")
    definitions = ", ".join(
        f"{column} {_COLUMN_TYPES.get(column, 'VARCHAR')}"
        for column in tables.CANDIDATE_COLUMNS
    )
    qualified = f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
    connection.execute(f"CREATE OR REPLACE TABLE {qualified} ({definitions})")
    if rows:
        placeholders = ", ".join("?" for _ in tables.CANDIDATE_COLUMNS)
        connection.executemany(
            f"INSERT INTO {qualified} ({', '.join(tables.CANDIDATE_COLUMNS)}) "
            f"VALUES ({placeholders})",
            [tuple(row[column] for column in tables.CANDIDATE_COLUMNS) for row in rows],
        )
    return len(rows)
