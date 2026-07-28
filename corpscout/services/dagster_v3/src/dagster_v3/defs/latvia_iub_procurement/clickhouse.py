import re
import uuid
from typing import Any

from dagster_v3.defs.clickhouse.resolved import (
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.latvia_iub_procurement import tables

_COLUMN_TYPES = {
    "lot_sequence": "Int32",
    "winner_ordinal": "Int32",
    "party_ordinal": "Int32",
    "is_natural_person": "UInt8",
    "tender_value_attributable": "UInt8",
    "received_tenders": "Nullable(Int32)",
    "publication_date": "Date",
    "decision_date": "Nullable(Date)",
    "contract_conclusion_date": "Nullable(Date)",
    "actual_end_date": "Nullable(Date)",
    "estimated_value_amount_eur": "Nullable(Decimal(38, 2))",
    "lowest_tender_amount_eur": "Nullable(Decimal(38, 2))",
    "highest_tender_amount_eur": "Nullable(Decimal(38, 2))",
    "tender_value_amount_eur": "Nullable(Decimal(38, 2))",
    "tender_value_amount_usd": "Nullable(Decimal(38, 2))",
    "source_retrieved_at": "DateTime64(3, 'UTC')",
    "resolved_at": "DateTime64(3, 'UTC')",
}


def winner_candidate_stage_ddl(table: str) -> str:
    columns = ",\n    ".join(
        f"{column} {_COLUMN_TYPES.get(column, 'String')}"
        for column in tables.WINNER_CANDIDATE_COLUMNS
    )
    return f"""
    CREATE TABLE {table}
    (
        {columns}
    )
    ENGINE = MergeTree
    ORDER BY source_record_id
    """


def winners_insert_sql(*, target_table: str, candidate_table: str) -> str:
    passthrough = ",\n        ".join(
        f"w.{column}" for column in tables.WINNER_CANDIDATE_COLUMNS
    )
    return f"""
    INSERT INTO {target_table} ({", ".join(tables.WINNERS_COLUMNS)})
    SELECT
        if(c.regcode != '', c.regcode, '') AS company_id,
        multiIf(
            w.match_eligibility != 'eligible', w.match_eligibility,
            c.regcode != '', 'exact',
            'unmatched_company'
        ) AS company_match_status,
        {passthrough}
    FROM {candidate_table} AS w
    LEFT ANY JOIN
    (
        SELECT regcode
        FROM corpscout.lv_companies
        WHERE regcode IN (
            SELECT winner_regcode FROM {candidate_table}
            WHERE winner_regcode != ''
        )
    ) AS c
        ON c.regcode = w.winner_regcode
    """


def export_iub_partition(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    partition_key: str,
) -> dict[str, int]:
    if not re.fullmatch(r"\d{4}-\d{2}-01", partition_key):
        raise ValueError(
            f"partition_key must be the first day of a month, got {partition_key!r}"
        )
    counts: dict[str, int] = {}
    for table, columns in (
        (tables.NOTICES_TABLE, tables.NOTICES_COLUMNS),
        (tables.LOTS_TABLE, tables.LOTS_COLUMNS),
        (tables.EXECUTIONS_TABLE, tables.EXECUTIONS_COLUMNS),
    ):
        counts[table] = _replace_direct_partition(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            table=table,
            columns=columns,
            partition_key=partition_key,
        )
    counts[tables.WINNERS_TABLE] = _replace_winner_partition(
        duckdb_connection=duckdb_connection,
        clickhouse_client=clickhouse_client,
        partition_key=partition_key,
    )
    return counts


def _replace_direct_partition(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    table: str,
    columns: tuple[str, ...],
    partition_key: str,
) -> int:
    suffix = uuid.uuid4().hex
    stage_name = f"_tmp_{table}_{suffix}"
    stage = _qualified(stage_name)
    target = _qualified(table)
    clickhouse_client.execute(f"CREATE TABLE {stage} AS {target}")
    try:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=tables.DUCKDB_SCHEMA,
            duckdb_table=table,
            clickhouse_database=tables.CLICKHOUSE_DATABASE,
            clickhouse_table=stage_name,
            columns=columns,
            truncate=False,
        )
        clickhouse_client.execute(
            f"ALTER TABLE {target} REPLACE PARTITION '{partition_key}' FROM {stage}"
        )
    finally:
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage}")
    return int(rows)


def _replace_winner_partition(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    partition_key: str,
) -> int:
    suffix = uuid.uuid4().hex
    candidate_name = f"_tmp_iub_winner_candidates_{suffix}"
    candidate = _qualified(candidate_name)
    stage = _qualified(f"_tmp_{tables.WINNERS_TABLE}_{suffix}")
    target = _qualified(tables.WINNERS_TABLE)
    clickhouse_client.execute(winner_candidate_stage_ddl(candidate))
    clickhouse_client.execute(f"CREATE TABLE {stage} AS {target}")
    try:
        source_rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=tables.DUCKDB_SCHEMA,
            duckdb_table=tables.WINNER_CANDIDATES_TABLE,
            clickhouse_database=tables.CLICKHOUSE_DATABASE,
            clickhouse_table=candidate_name,
            columns=tables.WINNER_CANDIDATE_COLUMNS,
            truncate=False,
        )
        clickhouse_client.execute(
            winners_insert_sql(target_table=stage, candidate_table=candidate)
        )
        rows = int(clickhouse_client.execute(f"SELECT count() FROM {stage}")[0][0])
        if rows != int(source_rows):
            raise ValueError(
                "IUB winner publish row mismatch: "
                f"candidates={source_rows} published={rows}"
            )
        clickhouse_client.execute(
            f"ALTER TABLE {target} REPLACE PARTITION '{partition_key}' FROM {stage}"
        )
    finally:
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage}")
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {candidate}")
    return rows


def _qualified(table: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table}`"
