"""Export Doffin candidates to ClickHouse, resolving the winning company.

The identity problem that dominated the other national sources does not exist
here: Doffin publishes ``organizationId`` and it joins ``no_companies.org_number``
with no transformation. Sweden needed person-vs-company classification, Brazil
needed a 14-digit establishment resolved to an 8-digit base past a half-working
``headquarters_cnpj`` trap. Norway needs neither.

What it does need is honesty about the winners that do not resolve. Doffin
carries foreign companies -- Swedish, Danish, Spanish and British numbers all
appear inside a single month -- and their contracts are real. They are stored
with ``company_match_status`` saying why they did not match, rather than dropped
or coerced into a Norwegian-shaped id.
"""

from __future__ import annotations

import re
from typing import Any

from dagster_v3.defs.norway_doffin import tables

_STAGE_COLUMN_TYPES = {
    "issue_date": "Nullable(Date)",
    "publication_date": "Nullable(Date)",
    "deadline_date": "Nullable(Date)",
    "fx_rate_date": "Nullable(Date)",
    "cpv_codes": "Array(String)",
    "location_ids": "Array(String)",
    "winner_ordinal": "Int32",
    "received_tenders": "Int32",
    "fx_rate_to_usd": "Nullable(Decimal(38, 12))",
    "source_retrieved_at": "DateTime64(3, 'UTC')",
    "resolved_at": "DateTime64(3, 'UTC')",
}


def _stage_column_type(name: str) -> str:
    if name in _STAGE_COLUMN_TYPES:
        return _STAGE_COLUMN_TYPES[name]
    if name.endswith("_amount_original") or name.endswith("_amount_usd"):
        return "Nullable(Decimal(38, 2))"
    return "String"


def candidate_stage_ddl(stage_table: str) -> str:
    columns = ",\n    ".join(
        f"{name} {_stage_column_type(name)}" for name in tables.CANDIDATE_COLUMNS
    )
    return f"""
    CREATE TABLE {stage_table}
    (
    {columns}
    )
    ENGINE = MergeTree
    ORDER BY (doffin_id, lot_id, winner_ordinal)
    """


def notices_insert_sql(*, target_table: str, stage_table: str) -> str:
    """Resolve the winner and project into the table's column order."""
    passthrough = ",\n        ".join(f"u.{name}" for name in tables.CANDIDATE_COLUMNS)
    return f"""
    INSERT INTO {target_table} ({", ".join(tables.NOTICES_COLUMNS)})
    SELECT
        if(c.org_number != '', c.org_number, '') AS company_id,
        multiIf(
            u.winner_org_number_raw = '', 'no_winner_named',
            -- A winner whose number is not nine digits is a foreign company,
            -- not a Norwegian one we failed to find. Saying so is the
            -- difference between a gap that is explained and one that looks
            -- like a matching failure.
            u.winner_org_number = '', 'foreign_winner',
            c.org_number != '', 'exact',
            'unmatched_company'
        ) AS company_match_status,
        {passthrough}
    FROM {stage_table} AS u
    -- Restricted to the org numbers this batch references. ClickHouse
    -- materialises the whole right side of a join, and joining the register
    -- unrestricted reads every Norwegian company to resolve a few hundred
    -- winners.
    LEFT ANY JOIN
    (
        SELECT org_number
        FROM corpscout.no_companies
        WHERE org_number IN (
            SELECT winner_org_number FROM {stage_table} WHERE winner_org_number != ''
        )
    ) AS c
        ON c.org_number = u.winner_org_number
    """


def export_notices_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse_client: Any,
    partition: str,
    batch_size: int = 50_000,
) -> dict[str, int]:
    """Replace one partition's notices with the candidates currently in DuckDB.

    REPLACE PARTITION rather than a plain insert, so re-running a month yields
    that month rather than two copies of it. Refuses to blank a partition that
    currently holds rows: an empty fetch is a degraded run, not a month in which
    Norway awarded nothing.
    """
    # The partition key is interpolated into DDL, which cannot be parameterised.
    # It comes from Dagster's partition definition and so is already controlled,
    # but validating it here means a future caller cannot make this the one
    # injection site in the module.
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", partition):
        raise ValueError(f"partition must be YYYY-MM-DD, got {partition!r}")

    def _qualified(name: str) -> str:
        return f"`{tables.CLICKHOUSE_DATABASE}`.`{name}`"

    qualified = _qualified(tables.NOTICES_TABLE)
    stage = _qualified(f"_tmp_{tables.NOTICES_TABLE}_{partition.replace('-', '')}")
    stage_candidates = _qualified(
        f"_tmp_{tables.NOTICES_TABLE}_{partition.replace('-', '')}_src"
    )

    rows = duckdb_connection.execute(
        f"select {', '.join(tables.CANDIDATE_COLUMNS)} "
        f"from {tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"
    ).fetchall()

    if not rows:
        existing = clickhouse_client.execute(
            f"SELECT count() FROM {qualified} WHERE partition_key = %(p)s",
            {"p": partition},
        )[0][0]
        if int(existing) > 0:
            raise ValueError(
                f"Doffin produced no notices for {partition}, but that partition "
                f"holds {existing} rows -- refusing to blank it"
            )

    clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage_candidates}")
    clickhouse_client.execute(candidate_stage_ddl(stage_candidates))
    clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage}")
    clickhouse_client.execute(f"CREATE TABLE {stage} AS {qualified}")
    try:
        for start in range(0, len(rows), batch_size):
            clickhouse_client.execute(
                f"INSERT INTO {stage_candidates} "
                f"({', '.join(tables.CANDIDATE_COLUMNS)}) VALUES",
                rows[start : start + batch_size],
            )
        clickhouse_client.execute(
            notices_insert_sql(target_table=stage, stage_table=stage_candidates)
        )
        counts = clickhouse_client.execute(
            f"SELECT count(), "
            f"countIf(company_match_status = 'exact'), "
            f"countIf(company_match_status = 'foreign_winner'), "
            f"countIf(company_match_status = 'unmatched_company'), "
            f"countIf(value_amount_original IS NOT NULL) "
            f"FROM {stage}"
        )[0]
        clickhouse_client.execute(
            f"ALTER TABLE {qualified} REPLACE PARTITION '{partition}' FROM {stage}"
        )
    finally:
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage_candidates}")
        clickhouse_client.execute(f"DROP TABLE IF EXISTS {stage}")

    return {
        "notice_winner_rows": int(counts[0]),
        "matched_companies": int(counts[1]),
        "foreign_winners": int(counts[2]),
        "unmatched_companies": int(counts[3]),
        "rows_with_realized_value": int(counts[4]),
    }
