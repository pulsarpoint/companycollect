import re
import uuid
from typing import Any

from dagster_v3.defs.clickhouse.resolved import (
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.slovakia_uvo_procurement import tables

_COLUMN_TYPES = {
    "publication_date": "Date",
    "winner_ordinal": "Int32",
    "contract_conclusion_date": "Nullable(Date)",
    "awarded_amount_eur": "Nullable(Decimal(38, 2))",
    "awarded_amount_usd": "Nullable(Decimal(38, 2))",
    "lowest_tender_amount_eur": "Nullable(Decimal(38, 2))",
    "highest_tender_amount_eur": "Nullable(Decimal(38, 2))",
    "notice_value_amount_eur": "Nullable(Decimal(38, 2))",
    "received_tenders": "Nullable(Int32)",
    "source_retrieved_at": "DateTime64(3, 'UTC')",
    "resolved_at": "DateTime64(3, 'UTC')",
}


def candidate_stage_ddl(table: str) -> str:
    columns = ",\n    ".join(
        f"{column} {_COLUMN_TYPES.get(column, 'String')}"
        for column in tables.CANDIDATE_COLUMNS
    )
    return f"""
    CREATE TABLE {table}
    (
        {columns}
    )
    ENGINE = MergeTree
    ORDER BY source_record_id
    """


def notices_insert_sql(*, target_table: str, candidate_table: str) -> str:
    passthrough = ",\n        ".join(
        f"u.{column}" for column in tables.CANDIDATE_COLUMNS
    )
    return f"""
    INSERT INTO {target_table} ({", ".join(tables.NOTICES_COLUMNS)})
    SELECT
        if(c.ico != '', c.ico, '') AS company_id,
        multiIf(
            u.match_eligibility != 'eligible', u.match_eligibility,
            c.ico != '', 'exact',
            'unmatched_company'
        ) AS company_match_status,
        {passthrough}
    FROM {candidate_table} AS u
    LEFT ANY JOIN
    (
        SELECT ico
        FROM corpscout.sk_companies
        WHERE ico IN (
            SELECT winner_ico FROM {candidate_table} WHERE winner_ico != ''
        )
    ) AS c
        ON c.ico = u.winner_ico
    """


def export_uvo_partition(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    partition_key: str,
) -> dict[str, int]:
    if not re.fullmatch(r"\d{4}-\d{2}-01", partition_key):
        raise ValueError(
            f"partition_key must be the first day of a month, got {partition_key!r}"
        )
    suffix = uuid.uuid4().hex
    candidate_name = f"_tmp_uvo_candidates_{suffix}"
    candidate = _qualified(candidate_name)
    stage = _qualified(f"_tmp_{tables.NOTICES_TABLE}_{suffix}")
    target = _qualified(tables.NOTICES_TABLE)
    clickhouse_client.execute(candidate_stage_ddl(candidate))
    clickhouse_client.execute(f"CREATE TABLE {stage} AS {target}")
    try:
        source_rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=clickhouse_client,
            duckdb_schema=tables.DUCKDB_SCHEMA,
            duckdb_table=tables.CANDIDATES_TABLE,
            clickhouse_database=tables.CLICKHOUSE_DATABASE,
            clickhouse_table=candidate_name,
            columns=tables.CANDIDATE_COLUMNS,
            truncate=False,
        )
        clickhouse_client.execute(
            notices_insert_sql(target_table=stage, candidate_table=candidate)
        )
        row = clickhouse_client.execute(
            f"""
            SELECT
                count(),
                countIf(company_match_status = 'exact'),
                countIf(company_match_status = 'unmatched_company'),
                countIf(awarded_amount_eur IS NOT NULL)
            FROM {stage}
            """
        )[0]
        if int(row[0]) != int(source_rows):
            raise ValueError(
                f"UVO publish row mismatch: candidates={source_rows} published={row[0]}"
            )
        clickhouse_client.execute(
            f"ALTER TABLE {target} REPLACE PARTITION '{partition_key}' FROM {stage}"
        )
    finally:
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage}")
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {candidate}")
    return {
        "notice_winner_rows": int(row[0]),
        "matched_companies": int(row[1]),
        "unmatched_companies": int(row[2]),
        "rows_with_awarded_value": int(row[3]),
    }


def _qualified(table: str) -> str:
    return f"`{tables.CLICKHOUSE_DATABASE}`.`{table}`"
