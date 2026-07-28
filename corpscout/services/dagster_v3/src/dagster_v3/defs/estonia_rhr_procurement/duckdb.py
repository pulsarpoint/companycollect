from typing import Any

from dagster_v3.defs.estonia_rhr_procurement import tables
from dagster_v3.defs.estonia_rhr_procurement.parser import ParsedRhrMonth

_DECIMAL_COLUMNS = {
    column
    for columns in (
        tables.NOTICES_COLUMNS,
        tables.LOTS_COLUMNS,
        tables.WINNER_CANDIDATE_COLUMNS,
    )
    for column in columns
    if "_amount_" in column
}
_COLUMN_TYPES = {
    **{column: "DECIMAL(38, 2)" for column in _DECIMAL_COLUMNS},
    "winner_ordinal": "INTEGER",
    "settled_contract_count": "INTEGER",
    "awarded_value_attributable": "TINYINT",
    "publication_date": "DATE",
    "ted_publication_date": "DATE",
    "source_retrieved_at": "TIMESTAMP",
    "resolved_at": "TIMESTAMP",
}


def write_parsed_month(
    connection: Any, parsed: ParsedRhrMonth
) -> dict[str, int]:
    connection.execute(f"CREATE SCHEMA IF NOT EXISTS {tables.DUCKDB_SCHEMA}")
    table_rows = (
        (tables.NOTICES_TABLE, tables.NOTICES_COLUMNS, parsed.notices),
        (tables.LOTS_TABLE, tables.LOTS_COLUMNS, parsed.lots),
        (
            tables.WINNER_CANDIDATES_TABLE,
            tables.WINNER_CANDIDATE_COLUMNS,
            parsed.winners,
        ),
    )
    counts: dict[str, int] = {}
    for table, columns, rows in table_rows:
        definitions = ", ".join(
            f"{column} {_COLUMN_TYPES.get(column, 'VARCHAR')}" for column in columns
        )
        qualified = f"{tables.DUCKDB_SCHEMA}.{table}"
        connection.execute(f"CREATE OR REPLACE TABLE {qualified} ({definitions})")
        if rows:
            placeholders = ", ".join("?" for _ in columns)
            connection.executemany(
                f"INSERT INTO {qualified} ({', '.join(columns)}) "
                f"VALUES ({placeholders})",
                [tuple(row[column] for column in columns) for row in rows],
            )
        counts[table] = len(rows)
    return counts
