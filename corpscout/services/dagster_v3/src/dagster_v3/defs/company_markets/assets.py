"""Precomputed market facts: which companies trade, and what changed hands.

Runs daily, after the EODHD price load. Everything happens inside ClickHouse —
there is no download and no DuckDB staging, because every input is already a
ClickHouse table. Each asset fills a staging table and swaps it in with
EXCHANGE TABLES, so a page reading these never sees a partial result.
"""

from datetime import UTC, datetime
import uuid

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_markets import sql, tables

GROUP_NAME = "company_markets"


# The identity joins behind these SELECTs (instrument_venues 15.0M x
# instrument_issuer 9.1M x company_identifier) held their whole hash tables in
# memory and hit the server's total-memory cap (Code 241, 2026-08-28) once
# corpscout.se_companies_serving's 15-minute refresh started sharing the same
# 27.31 GiB budget. grace_hash spills join buckets to disk and the external
# group-by/sort thresholds do the same for aggregation, so the query is slower
# but bounded -- max_memory_usage keeps it well under the shared cap even when
# a serving refresh runs concurrently.
_HEAVY_QUERY_SETTINGS = {
    # A priority list, not one algorithm: ClickHouse picks the first that fits
    # each join. The Brazil B3 branch joins on OR'd keys, which ONLY plain hash
    # supports (Code 48) -- its right side is just the B3 symbol subset, so an
    # in-memory hash there is small; the big equi-key LEI joins take grace_hash
    # and spill.
    "join_algorithm": "grace_hash,hash",
    "grace_hash_join_initial_buckets": 16,
    "max_bytes_before_external_group_by": 8 * 1024**3,
    "max_bytes_before_external_sort": 8 * 1024**3,
    "max_memory_usage": 14 * 1024**3,
}


def _replace_from_select(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    *,
    table: str,
    select: str,
    minimum_rows: int = 0,
) -> int:
    """Fill a staging copy, check it, then swap it in."""
    qualified = f"`{tables.CLICKHOUSE_DATABASE}`.`{table}`"
    stage = f"`{tables.CLICKHOUSE_DATABASE}`.`_tmp_{table}_{uuid.uuid4().hex}`"
    resolved_at = datetime.now(UTC).replace(tzinfo=None)

    with clickhouse.get_connection() as client:
        try:
            client.execute(f"CREATE TABLE {stage} AS {qualified}")
            client.execute(
                f"INSERT INTO {stage} {select}",
                {"resolved_at": resolved_at},
                settings=_HEAVY_QUERY_SETTINGS,
            )
            rows = int(client.execute(f"SELECT count() FROM {stage}")[0][0])
            if rows < minimum_rows:
                # Refuse to swap on a short result rather than empty a table the
                # country pages read.
                raise ValueError(
                    f"{table} produced {rows} rows, below the {minimum_rows} floor"
                )
            client.execute(f"EXCHANGE TABLES {stage} AND {qualified}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")

    context.log.info("%s: %d rows", table, rows)
    return rows


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql"},
    description=(
        "Resolves every country's companies to their EODHD symbols, so a page "
        "does not repeat an 11-second identity join per request."
    ),
)
def company_traded_symbols_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.TRADED_SYMBOLS_TABLE,),
    )
    rows = _replace_from_select(
        context,
        clickhouse,
        table=tables.TRADED_SYMBOLS_TABLE,
        select=sql.TRADED_SYMBOLS_SELECT,
        minimum_rows=tables.MIN_TRADED_SYMBOLS,
    )
    with clickhouse.get_connection() as client:
        by_country = client.execute(
            f"SELECT country_code, uniqExact(company_id), count() "
            f"FROM `{tables.CLICKHOUSE_DATABASE}`.`{tables.TRADED_SYMBOLS_TABLE}` "
            f"GROUP BY country_code ORDER BY country_code"
        )
    return dg.MaterializeResult(
        metadata={
            "rows": rows,
            **{f"{row[0]}_companies": int(row[1]) for row in by_country},
        }
    )


@dg.asset(
    deps=[dg.AssetKey("company_traded_symbols_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql"},
    description=(
        "Traded value per country per month, in USD. Turnover, not market "
        "capitalisation."
    ),
)
def company_market_monthly_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.MARKET_MONTHLY_TABLE,),
    )
    rows = _replace_from_select(
        context,
        clickhouse,
        table=tables.MARKET_MONTHLY_TABLE,
        select=sql.MARKET_MONTHLY_SELECT,
        minimum_rows=1,
    )
    return dg.MaterializeResult(metadata={"rows": rows})


@dg.asset(
    deps=[dg.AssetKey("company_traded_symbols_clickhouse")],
    group_name=GROUP_NAME,
    kinds={"clickhouse", "sql"},
    description=(
        "One row per traded company, folded across venues, ranked by traded value."
    ),
)
def company_market_summary_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.CLICKHOUSE_DATABASE,
        tables=(tables.MARKET_SUMMARY_TABLE,),
    )
    rows = _replace_from_select(
        context,
        clickhouse,
        table=tables.MARKET_SUMMARY_TABLE,
        select=sql.MARKET_SUMMARY_SELECT,
        minimum_rows=1,
    )
    return dg.MaterializeResult(metadata={"rows": rows})


company_markets_job = dg.define_asset_job(
    name="company_markets_job",
    selection=dg.AssetSelection.assets(
        company_traded_symbols_clickhouse,
        company_market_monthly_clickhouse,
        company_market_summary_clickhouse,
    ),
)

# After the EODHD daily load, which is what moves these numbers. Minute staggered
# per the scheduling guidance so sources do not all fire together.
company_markets_daily = dg.ScheduleDefinition(
    name="company_markets_daily",
    job=company_markets_job,
    cron_schedule="35 6 * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
