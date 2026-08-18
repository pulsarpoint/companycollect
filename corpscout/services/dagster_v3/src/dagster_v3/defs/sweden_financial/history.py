"""Resolve Swedish multi-year history from Bolagsverket observations.

The source-owned observation table keeps every recognized numeric XBRL
assertion. This module is the downstream resolution policy: it selects one
value per metric, statement, and represented year; prefers reported assertions
over comparative ones; and rejects all comparative years from a statement when
any overlapping revenue assertion disagrees with a directly reported value.

``Soliditet`` remains unscaled because the source mixes fractional and percent
representations. The observation layer marks that ambiguity explicitly.
"""

import uuid
from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.sweden_financial.observations import (
    MAX_COMPARATIVE_YEARS_BACK,
    OVERLAP_AGREEMENT_TOLERANCE,
    SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE,
    SWEDEN_FINANCIAL_DATABASE,
)

SE_FINANCIAL_HISTORY_TABLE = "se_financial_history"
QUALIFIED_SE_FINANCIAL_HISTORY_TABLE = (
    f"{SWEDEN_FINANCIAL_DATABASE}.{SE_FINANCIAL_HISTORY_TABLE}"
)

SE_FINANCIAL_HISTORY_COLUMNS = (
    "company_id",
    "fiscal_year",
    "observation",
    "source_statement_key",
    "source_fiscal_year",
    "currency",
    "revenue_amount_original",
    "revenue_amount_usd",
    "result_after_financial_items_amount_original",
    "result_after_financial_items_amount_usd",
    "solidity_pct",
    "total_assets_amount_original",
    "total_assets_amount_usd",
    "resolved_at",
)

HISTORY_CONCEPTS: dict[str, str] = {
    "Nettoomsattning": "revenue_amount_original",
    "ResultatEfterFinansiellaPoster": "result_after_financial_items_amount_original",
    "Soliditet": "solidity_pct",
    "Tillgangar": "total_assets_amount_original",
    "Balansomslutning": "total_assets_amount_original",
}

_CONTEXT_YEAR_PATTERN = r"(?i)^(period|balans)([0-9]+)$"


def build_history_guard_ctes() -> str:
    """Return the observation resolution and revenue-guard CTEs."""
    return f"""source_period_observations AS (
    SELECT
        *,
        if(
            upperUTF8(ifNull(decimals, '')) = 'INF',
            100000,
            ifNull(toInt32OrNull(decimals), -100000)
        ) AS precision_rank,
        if(source_concept_local_name = 'Tillgangar', 2, 1)
            AS concept_rank
    FROM corpscout.se_bolagsverket_financial_observations
    PREWHERE observation_kind IN ('reported', 'comparative')
    WHERE dimensions = '{{}}'
      AND match(source_context_id, '{_CONTEXT_YEAR_PATTERN}')
      AND toInt32OrZero(
          extract(lowerUTF8(source_context_id), '^(?:period|balans)([0-9]+)$')
      ) BETWEEN 0 AND {MAX_COMPARATIVE_YEARS_BACK}
      AND metric_code IN (
          'revenue',
          'result_after_financial_items',
          'solidity',
          'total_assets'
      )
),
history_rows AS (
    SELECT
        company_id,
        represented_fiscal_year AS fiscal_year,
        observation_kind AS observation,
        source_statement_key,
        source_fiscal_year,
        'SEK' AS currency,
        argMaxIf(
            value_original,
            tuple(concept_rank, precision_rank, source_fact_ordinal),
            metric_code = 'revenue'
        ) AS revenue_amount_original,
        argMaxIf(
            value_usd,
            tuple(concept_rank, precision_rank, source_fact_ordinal),
            metric_code = 'revenue'
        ) AS revenue_amount_usd,
        argMaxIf(
            value_original,
            tuple(concept_rank, precision_rank, source_fact_ordinal),
            metric_code = 'result_after_financial_items'
        ) AS result_after_financial_items_amount_original,
        argMaxIf(
            value_usd,
            tuple(concept_rank, precision_rank, source_fact_ordinal),
            metric_code = 'result_after_financial_items'
        ) AS result_after_financial_items_amount_usd,
        argMaxIf(
            value_original,
            tuple(concept_rank, precision_rank, source_fact_ordinal),
            metric_code = 'solidity'
        ) AS solidity_pct,
        argMaxIf(
            value_original,
            tuple(concept_rank, precision_rank, source_fact_ordinal),
            metric_code = 'total_assets'
        ) AS total_assets_amount_original,
        argMaxIf(
            value_usd,
            tuple(concept_rank, precision_rank, source_fact_ordinal),
            metric_code = 'total_assets'
        ) AS total_assets_amount_usd,
        maxOrNull(revenue_overlap_relative_diff)
            AS revenue_overlap_relative_diff
    FROM source_period_observations
    GROUP BY
        company_id,
        represented_fiscal_year,
        observation_kind,
        source_statement_key,
        source_fiscal_year
),
overlap_checks AS (
    SELECT
        source_statement_key AS statement_key,
        revenue_overlap_relative_diff AS max_relative_diff
    FROM history_rows
    WHERE observation = 'comparative'
      AND revenue_overlap_relative_diff IS NOT NULL
),
disqualified_statements AS (
    SELECT DISTINCT statement_key
    FROM overlap_checks
    WHERE max_relative_diff > {OVERLAP_AGREEMENT_TOLERANCE}
)"""


def build_history_insert_sql(qualified_stage_table: str) -> str:
    """Build the canonical history table strictly from source observations."""
    columns = ",\n    ".join(SE_FINANCIAL_HISTORY_COLUMNS)
    return f"""INSERT INTO {qualified_stage_table} (
    {columns}
)
WITH
{build_history_guard_ctes()},
ranked_history_rows AS (
    SELECT
        company_id,
        fiscal_year,
        observation,
        source_statement_key,
        source_fiscal_year,
        currency,
        cast(revenue_amount_original AS Nullable(Float64))
            AS revenue_amount_original,
        cast(revenue_amount_usd AS Nullable(Float64)) AS revenue_amount_usd,
        cast(result_after_financial_items_amount_original AS Nullable(Float64))
            AS result_after_financial_items_amount_original,
        cast(result_after_financial_items_amount_usd AS Nullable(Float64))
            AS result_after_financial_items_amount_usd,
        cast(solidity_pct AS Nullable(Float64)) AS solidity_pct,
        cast(total_assets_amount_original AS Nullable(Float64))
            AS total_assets_amount_original,
        cast(total_assets_amount_usd AS Nullable(Float64))
            AS total_assets_amount_usd
    FROM history_rows
    WHERE observation = 'reported'
       OR source_statement_key NOT IN (
           SELECT statement_key FROM disqualified_statements
       )
)
SELECT
    company_id,
    fiscal_year,
    observation,
    source_statement_key,
    source_fiscal_year,
    currency,
    revenue_amount_original,
    revenue_amount_usd,
    result_after_financial_items_amount_original,
    result_after_financial_items_amount_usd,
    solidity_pct,
    total_assets_amount_original,
    total_assets_amount_usd,
    now64(3) AS resolved_at
FROM ranked_history_rows
ORDER BY
    observation = 'reported' DESC,
    source_fiscal_year DESC,
    source_statement_key DESC
LIMIT 1 BY company_id, fiscal_year"""


def build_history_guard_metadata_sql() -> str:
    """Measure the exact revenue guard used by the history insert."""
    return f"""WITH
{build_history_guard_ctes()}
SELECT
    (SELECT count() FROM overlap_checks) AS overlap_pair_count,
    (
        SELECT countIf(max_relative_diff <= {OVERLAP_AGREEMENT_TOLERANCE})
        FROM overlap_checks
    ) AS overlap_agree_count,
    (SELECT uniqExact(statement_key) FROM disqualified_statements)
        AS disqualified_statement_count,
    (
        SELECT count()
        FROM overlap_checks
        WHERE statement_key NOT IN (
            SELECT statement_key FROM disqualified_statements
        )
    ) AS post_guard_overlap_pair_count,
    (
        SELECT countIf(max_relative_diff <= {OVERLAP_AGREEMENT_TOLERANCE})
        FROM overlap_checks
        WHERE statement_key NOT IN (
            SELECT statement_key FROM disqualified_statements
        )
    ) AS post_guard_overlap_agree_count"""


def build_history_quality_sql(qualified_stage_table: str) -> str:
    return f"""SELECT
    count() AS row_count,
    countIf(observation = 'reported') AS reported_count,
    countIf(observation = 'comparative') AS comparative_count,
    uniqExact(company_id) AS company_count
FROM {qualified_stage_table}"""


_QUALITY_COLUMNS = ("row_count", "reported_count", "comparative_count", "company_count")

_GUARD_COLUMNS = (
    "overlap_pair_count",
    "overlap_agree_count",
    "disqualified_statement_count",
    "post_guard_overlap_pair_count",
    "post_guard_overlap_agree_count",
)


def _history_quality_metadata(row: tuple[Any, ...]) -> dict[str, int]:
    return {
        column: int(value) for column, value in zip(_QUALITY_COLUMNS, row, strict=True)
    }


def _validate_history_quality(quality: dict[str, int]) -> None:
    if quality["row_count"] == 0:
        raise ValueError(
            "Sweden financial history build produced no rows; refusing to "
            f"replace {QUALIFIED_SE_FINANCIAL_HISTORY_TABLE}"
        )


def _agreement_rate(agree_count: int, pair_count: int) -> float | None:
    return agree_count / pair_count if pair_count > 0 else None


def _history_guard_metadata(row: tuple[Any, ...]) -> dict[str, int | float | None]:
    raw = {
        column: int(value) for column, value in zip(_GUARD_COLUMNS, row, strict=True)
    }
    return {
        "overlap_pair_count": raw["overlap_pair_count"],
        "overlap_agreement_rate": _agreement_rate(
            raw["overlap_agree_count"], raw["overlap_pair_count"]
        ),
        "disqualified_statement_count": raw["disqualified_statement_count"],
        "post_guard_overlap_pair_count": raw["post_guard_overlap_pair_count"],
        "post_guard_overlap_agreement_rate": _agreement_rate(
            raw["post_guard_overlap_agree_count"], raw["post_guard_overlap_pair_count"]
        ),
    }


def replace_se_financial_history_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> dict[str, int | str | float | None]:
    """Atomically rebuild ``se_financial_history`` from observations."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(
            SE_FINANCIAL_HISTORY_TABLE,
            SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE,
        ),
    )
    stage_table = f"_tmp_{SE_FINANCIAL_HISTORY_TABLE}_{uuid.uuid4().hex}"
    qualified_stage_table = f"`{SWEDEN_FINANCIAL_DATABASE}`.`{stage_table}`"
    qualified_target_table = (
        f"`{SWEDEN_FINANCIAL_DATABASE}`.`{SE_FINANCIAL_HISTORY_TABLE}`"
    )
    if log is not None:
        log(
            "Building Sweden financial history from Bolagsverket observations: "
            "target=%s",
            QUALIFIED_SE_FINANCIAL_HISTORY_TABLE,
        )

    with clickhouse.get_connection() as client:
        client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
        )
        primary_error: Exception | None = None
        try:
            client.execute(build_history_insert_sql(qualified_stage_table))
            quality_row = client.execute(
                build_history_quality_sql(qualified_stage_table)
            )[0]
            metadata: dict[str, int | str | float | None] = dict(
                _history_quality_metadata(quality_row)
            )
            _validate_history_quality(metadata)
            guard_row = client.execute(build_history_guard_metadata_sql())[0]
            metadata.update(_history_guard_metadata(guard_row))
            client.execute(
                f"EXCHANGE TABLES {qualified_stage_table} AND {qualified_target_table}"
            )
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {qualified_stage_table}")
            except Exception:
                if primary_error is None:
                    raise

    metadata["table"] = QUALIFIED_SE_FINANCIAL_HISTORY_TABLE
    if log is not None:
        log(
            "Finished Sweden financial history: rows=%s reported=%s comparative=%s "
            "companies=%s disqualified=%s post_guard_agreement_rate=%s",
            metadata["row_count"],
            metadata["reported_count"],
            metadata["comparative_count"],
            metadata["company_count"],
            metadata["disqualified_statement_count"],
            metadata["post_guard_overlap_agreement_rate"],
        )
    return metadata
