"""Resolve reported and trusted comparative Bolagsverket observations.

Each output row represents one fiscal year as asserted by one source filing.
Directly reported rows expose the full stable metric set. Comparative rows
backfill revenue and total assets only, retain the later filing as provenance,
and are rejected when that filing disagrees with overlapping reported revenue.
"""

import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.sweden_financial.clickhouse import (
    clickhouse_table_row_count,
    guard_against_clickhouse_table_shrink,
)
from dagster_v3.defs.sweden_financial.observations import (
    MAX_COMPARATIVE_YEARS_BACK,
    OVERLAP_AGREEMENT_TOLERANCE,
    SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE,
)
from dagster_v3.defs.sweden_financial.resources import (
    DEFAULT_ARCHIVE_BASE_URL,
    SWEDEN_FINANCIAL_RAW_BUCKET,
)

SWEDEN_FINANCIAL_DATABASE = "corpscout"
SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE = "se_bolagsverket_financial_metrics"
QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE = (
    f"{SWEDEN_FINANCIAL_DATABASE}.{SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE}"
)
SWEDEN_FINANCIAL_MAPPING_VERSION = "sweden-bolagsverket-observations-metrics-v4"

MONEY_METRIC_NAMES = (
    "revenue",
    "operating_profit_loss",
    "profit_loss",
    "total_assets",
    "equity",
    "liabilities",
    "cash_and_bank",
    "current_assets",
    "current_receivables",
    "current_liabilities",
    "personnel_expenses",
    "wages_and_salaries",
)

SE_BOLAGSVERKET_FINANCIAL_METRICS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "statement_key",
    "company_id",
    "report_period_start",
    "report_period_end",
    "fiscal_year",
    "observation_kind",
    "source_fiscal_year",
    "reported_company_name",
    "source_archive_url",
    "source_archive_key",
    "source_archive_name",
    "nested_zip_name",
    "xhtml_object_key",
    "xhtml_source_uri",
    "taxonomy_entrypoint",
    "currency",
    *(
        column
        for metric_name in MONEY_METRIC_NAMES
        for column in (
            f"{metric_name}_amount_original",
            f"{metric_name}_amount_usd",
        )
    ),
    "employees",
    "source_fact_count",
    "mapped_fact_count",
    "unmapped_numeric_fact_count",
    "metric_warnings",
    "mapping_version",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "source_payload_hash",
    "resolved_at",
)

_QUALITY_COLUMNS = (
    "row_count",
    "company_count",
    "reported_row_count",
    "comparative_row_count",
    "min_fiscal_year",
    "max_fiscal_year",
    "missing_fx_count",
    "invalid_liabilities_statement_count",
    "mapped_statement_count",
    *(f"{metric_name}_statement_count" for metric_name in MONEY_METRIC_NAMES),
    "employees_statement_count",
)


def replace_se_bolagsverket_financial_metrics_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    resolved_at: datetime,
    log: Callable[..., object] | None = None,
    allow_shrink: bool = False,
) -> dict[str, int | str | None]:
    """Rebuild canonical Sweden yearly metrics from source-owned observations."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(
            SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE,
            SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE,
        ),
    )
    stage_table = f"_tmp_{SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE}_{uuid.uuid4().hex}"
    qualified_stage_table = f"`{SWEDEN_FINANCIAL_DATABASE}`.`{stage_table}`"
    qualified_target_table = (
        f"`{SWEDEN_FINANCIAL_DATABASE}`.`{SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE}`"
    )
    if log is not None:
        log(
            "Building Sweden financial metrics in ClickHouse: target=%s mapping=%s",
            QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE,
            SWEDEN_FINANCIAL_MAPPING_VERSION,
        )

    with clickhouse.get_connection() as client:
        client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
        )
        primary_error: Exception | None = None
        try:
            client.execute(
                build_se_bolagsverket_financial_metrics_insert_sql(
                    qualified_stage_table
                ),
                {
                    "source_run_id": source_run_id,
                    "mapping_version": SWEDEN_FINANCIAL_MAPPING_VERSION,
                    "resolved_at": resolved_at,
                    "source_archive_base_url": DEFAULT_ARCHIVE_BASE_URL,
                    "xhtml_uri_prefix": f"s3://{SWEDEN_FINANCIAL_RAW_BUCKET}/",
                },
            )
            quality_row = client.execute(
                _sweden_financial_metrics_quality_sql(qualified_stage_table)
            )[0]
            quality = _quality_metadata(quality_row)
            _validate_quality(quality)
            existing_row_count = clickhouse_table_row_count(
                client, qualified_target_table
            )
            guard_against_clickhouse_table_shrink(
                qualified_table=QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE,
                existing_row_count=existing_row_count,
                staged_row_count=int(quality["row_count"]),
                allow_shrink=allow_shrink,
            )
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

    quality["table"] = QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE
    quality["mapping_version"] = SWEDEN_FINANCIAL_MAPPING_VERSION
    if log is not None:
        log(
            "Finished Sweden financial metrics: rows=%s companies=%s reported=%s "
            "comparative=%s mapped=%s missing_fx=%s",
            quality["row_count"],
            quality["company_count"],
            quality["reported_row_count"],
            quality["comparative_row_count"],
            quality["mapped_statement_count"],
            quality["missing_fx_count"],
        )
    return quality


def build_se_bolagsverket_financial_metrics_insert_sql(
    qualified_stage_table: str,
) -> str:
    columns = ",\n    ".join(SE_BOLAGSVERKET_FINANCIAL_METRICS_COLUMNS)
    return f"""INSERT INTO {qualified_stage_table} (
    {columns}
)
WITH
disqualified_comparative_statements AS (
    SELECT DISTINCT source_statement_key
    FROM corpscout.se_bolagsverket_financial_observations
    PREWHERE observation_kind = 'comparative'
    WHERE revenue_overlap_relative_diff > {OVERLAP_AGREEMENT_TOLERANCE}
),
eligible_observations AS (
    SELECT
        *,
        if(
            upperUTF8(ifNull(decimals, '')) = 'INF',
            100000,
            ifNull(toInt32OrNull(decimals), -100000)
        ) AS precision_rank,
        multiIf(
            source_concept_local_name IN ('Tillgangar', 'KassaBank'), 2,
            source_concept_local_name IN (
                'Balansomslutning',
                'KassaBankExklRedovisningsmedel'
            ), 1,
            0
        ) AS concept_rank
    FROM corpscout.se_bolagsverket_financial_observations
    PREWHERE observation_kind IN ('reported', 'comparative')
    WHERE dimensions = '{{}}'
      AND (
          (
              observation_kind = 'reported'
              AND lowerUTF8(source_context_id) IN ('period0', 'balans0')
              AND represented_period_end = source_report_period_end
          )
          OR (
              observation_kind = 'comparative'
              AND match(source_context_id, '(?i)^(period|balans)([0-9]+)$')
              AND toInt32OrZero(
                  extract(
                      lowerUTF8(source_context_id),
                      '^(?:period|balans)([0-9]+)$'
                  )
              ) BETWEEN 1 AND {MAX_COMPARATIVE_YEARS_BACK}
              AND metric_code IN ('revenue', 'total_assets')
              AND source_statement_key NOT IN (
                  SELECT source_statement_key
                  FROM disqualified_comparative_statements
              )
          )
      )
),
observations_by_statement AS (
    SELECT
        source_statement_key AS statement_key,
        any(country_iso2) AS country_iso2,
        any(source_slug) AS source_slug,
        any(source_record_id) AS source_record_id,
        any(company_id) AS company_id,
        if(
            observation_kind = 'reported',
            any(source_report_period_start),
            any(represented_period_start)
        ) AS report_period_start,
        if(
            observation_kind = 'reported',
            any(source_report_period_end),
            any(represented_period_end)
        ) AS report_period_end,
        any(represented_fiscal_year) AS fiscal_year,
        observation_kind,
        any(source_fiscal_year) AS source_fiscal_year,
        any(source_reported_company_name) AS reported_company_name,
        any(source_archive_key) AS source_archive_key,
        any(source_archive_name) AS source_archive_name,
        any(source_nested_zip_name) AS nested_zip_name,
        any(source_xhtml_object_key) AS xhtml_object_key,
        any(source_taxonomy_entrypoint) AS taxonomy_entrypoint,
        any(source_payload_hash) AS source_payload_hash,
        any(source_fact_count) AS source_fact_count,
        if(
            observation_kind = 'reported',
            any(source_unmapped_numeric_fact_count)
                + countIf(metric_code IN ('result_after_financial_items', 'solidity')),
            0
        )
            AS unmapped_numeric_fact_count,
        argMaxIf(
            upperUTF8(ifNull(currency, '')),
            tuple(precision_rank, source_fact_ordinal),
            metric_code != 'employees' AND notEmpty(ifNull(currency, ''))
        ) AS statement_currency,
        countIf(metric_code IN (
            'revenue',
            'operating_profit_loss',
            'profit_loss',
            'total_assets',
            'equity',
            'equity_liabilities',
            'cash_and_bank',
            'current_assets',
            'current_receivables',
            'current_liabilities',
            'personnel_expenses',
            'wages_and_salaries',
            'employees'
        )) AS mapped_fact_count,
        argMaxIf(
            value_original,
            tuple(concept_rank, precision_rank, source_fact_ordinal),
            metric_code = 'revenue'
        ) AS revenue,
        argMaxIf(
            value_usd,
            tuple(concept_rank, precision_rank, source_fact_ordinal),
            metric_code = 'revenue'
        ) AS revenue_usd,
        argMaxIf(value_original, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'operating_profit_loss')
            AS operating_profit_loss,
        argMaxIf(value_usd, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'operating_profit_loss')
            AS operating_profit_loss_usd,
        argMaxIf(value_original, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'profit_loss')
            AS profit_loss,
        argMaxIf(value_usd, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'profit_loss')
            AS profit_loss_usd,
        argMaxIf(value_original, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'total_assets')
            AS total_assets,
        argMaxIf(value_usd, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'total_assets')
            AS total_assets_usd,
        argMaxIf(value_original, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'equity')
            AS equity,
        argMaxIf(value_usd, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'equity')
            AS equity_usd,
        argMaxIf(value_original, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'equity_liabilities')
            AS equity_liabilities,
        argMaxIf(value_usd, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'equity_liabilities')
            AS equity_liabilities_usd,
        argMaxIf(value_original, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'cash_and_bank')
            AS cash_and_bank,
        argMaxIf(value_usd, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'cash_and_bank')
            AS cash_and_bank_usd,
        argMaxIf(value_original, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'current_assets')
            AS current_assets,
        argMaxIf(value_usd, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'current_assets')
            AS current_assets_usd,
        argMaxIf(value_original, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'current_receivables')
            AS current_receivables,
        argMaxIf(value_usd, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'current_receivables')
            AS current_receivables_usd,
        argMaxIf(value_original, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'current_liabilities')
            AS current_liabilities,
        argMaxIf(value_usd, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'current_liabilities')
            AS current_liabilities_usd,
        argMaxIf(value_original, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'personnel_expenses')
            AS personnel_expenses,
        argMaxIf(value_usd, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'personnel_expenses')
            AS personnel_expenses_usd,
        argMaxIf(value_original, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'wages_and_salaries')
            AS wages_and_salaries,
        argMaxIf(value_usd, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'wages_and_salaries')
            AS wages_and_salaries_usd,
        argMaxIf(value_original, tuple(concept_rank, precision_rank, source_fact_ordinal), metric_code = 'employees')
            AS employees,
        argMaxIf(fx_rate_to_usd, source_fact_ordinal, fx_rate_to_usd IS NOT NULL)
            AS fx_rate_to_usd,
        argMaxIf(fx_rate_date, source_fact_ordinal, fx_rate_date IS NOT NULL)
            AS fx_rate_date,
        argMaxIf(fx_source, source_fact_ordinal, fx_source != '') AS fx_source
    FROM eligible_observations
    GROUP BY source_statement_key, represented_fiscal_year, observation_kind
),
native_metrics AS (
    SELECT
        country_iso2,
        source_slug,
        %(source_run_id)s AS source_run_id,
        source_record_id,
        statement_key,
        company_id,
        report_period_start,
        report_period_end,
        toUInt16(fiscal_year) AS fiscal_year,
        observation_kind,
        toUInt16(source_fiscal_year) AS source_fiscal_year,
        reported_company_name,
        concat(
            %(source_archive_base_url)s,
            '/arsredovisningar/',
            extract(source_archive_key, 'year=([^/]+)'),
            '/',
            source_archive_name
        ) AS source_archive_url,
        source_archive_key,
        source_archive_name,
        nested_zip_name,
        xhtml_object_key,
        concat(%(xhtml_uri_prefix)s, xhtml_object_key) AS xhtml_source_uri,
        taxonomy_entrypoint,
        if(empty(statement_currency), 'SEK', statement_currency) AS currency,
        cast(revenue AS Nullable(Decimal(38, 6))) AS revenue_amount_original,
        cast(revenue_usd AS Nullable(Decimal(38, 6))) AS revenue_amount_usd,
        cast(operating_profit_loss AS Nullable(Decimal(38, 6)))
            AS operating_profit_loss_amount_original,
        cast(operating_profit_loss_usd AS Nullable(Decimal(38, 6)))
            AS operating_profit_loss_amount_usd,
        cast(profit_loss AS Nullable(Decimal(38, 6))) AS profit_loss_amount_original,
        cast(profit_loss_usd AS Nullable(Decimal(38, 6))) AS profit_loss_amount_usd,
        cast(total_assets AS Nullable(Decimal(38, 6))) AS total_assets_amount_original,
        cast(total_assets_usd AS Nullable(Decimal(38, 6))) AS total_assets_amount_usd,
        cast(equity AS Nullable(Decimal(38, 6))) AS equity_amount_original,
        cast(equity_usd AS Nullable(Decimal(38, 6))) AS equity_amount_usd,
        cast(
            if(
                coalesce(equity_liabilities, total_assets) >= equity,
                coalesce(equity_liabilities, total_assets) - equity,
                NULL
            ) AS Nullable(Decimal(38, 6))
        ) AS liabilities_amount_original,
        cast(
            if(
                coalesce(equity_liabilities_usd, total_assets_usd) >= equity_usd,
                coalesce(equity_liabilities_usd, total_assets_usd) - equity_usd,
                NULL
            ) AS Nullable(Decimal(38, 6))
        ) AS liabilities_amount_usd,
        toUInt8(coalesce(equity_liabilities, total_assets) < equity)
            AS invalid_liabilities,
        cast(cash_and_bank AS Nullable(Decimal(38, 6))) AS cash_and_bank_amount_original,
        cast(cash_and_bank_usd AS Nullable(Decimal(38, 6))) AS cash_and_bank_amount_usd,
        cast(current_assets AS Nullable(Decimal(38, 6))) AS current_assets_amount_original,
        cast(current_assets_usd AS Nullable(Decimal(38, 6))) AS current_assets_amount_usd,
        cast(current_receivables AS Nullable(Decimal(38, 6)))
            AS current_receivables_amount_original,
        cast(current_receivables_usd AS Nullable(Decimal(38, 6)))
            AS current_receivables_amount_usd,
        cast(current_liabilities AS Nullable(Decimal(38, 6)))
            AS current_liabilities_amount_original,
        cast(current_liabilities_usd AS Nullable(Decimal(38, 6)))
            AS current_liabilities_amount_usd,
        cast(personnel_expenses AS Nullable(Decimal(38, 6)))
            AS personnel_expenses_amount_original,
        cast(personnel_expenses_usd AS Nullable(Decimal(38, 6)))
            AS personnel_expenses_amount_usd,
        cast(wages_and_salaries AS Nullable(Decimal(38, 6)))
            AS wages_and_salaries_amount_original,
        cast(wages_and_salaries_usd AS Nullable(Decimal(38, 6)))
            AS wages_and_salaries_amount_usd,
        if(
            employees >= 0,
            toUInt64(round(employees)),
            cast(NULL AS Nullable(UInt64))
        ) AS employees,
        source_fact_count,
        mapped_fact_count,
        unmapped_numeric_fact_count,
        fx_rate_to_usd,
        fx_rate_date,
        fx_source,
        source_payload_hash
    FROM observations_by_statement
    -- Registration envelopes are never statements. A race document is kept
    -- only when the source observation layer mapped a generic metric from it.
    WHERE ifNull(taxonomy_entrypoint, '') NOT LIKE '%%/ar/rar%%'
      AND NOT (
          ifNull(taxonomy_entrypoint, '') LIKE '%%/misc/race/%%'
          AND mapped_fact_count = 0
      )
)
SELECT
    country_iso2,
    source_slug,
    source_run_id,
    source_record_id,
    statement_key,
    company_id,
    report_period_start,
    report_period_end,
    fiscal_year,
    observation_kind,
    source_fiscal_year,
    reported_company_name,
    source_archive_url,
    source_archive_key,
    source_archive_name,
    nested_zip_name,
    xhtml_object_key,
    xhtml_source_uri,
    taxonomy_entrypoint,
    currency,
    revenue_amount_original,
    revenue_amount_usd,
    operating_profit_loss_amount_original,
    operating_profit_loss_amount_usd,
    profit_loss_amount_original,
    profit_loss_amount_usd,
    total_assets_amount_original,
    total_assets_amount_usd,
    equity_amount_original,
    equity_amount_usd,
    liabilities_amount_original,
    liabilities_amount_usd,
    cash_and_bank_amount_original,
    cash_and_bank_amount_usd,
    current_assets_amount_original,
    current_assets_amount_usd,
    current_receivables_amount_original,
    current_receivables_amount_usd,
    current_liabilities_amount_original,
    current_liabilities_amount_usd,
    personnel_expenses_amount_original,
    personnel_expenses_amount_usd,
    wages_and_salaries_amount_original,
    wages_and_salaries_amount_usd,
    employees,
    source_fact_count,
    mapped_fact_count,
    unmapped_numeric_fact_count,
    multiIf(
        invalid_liabilities = 1 AND unmapped_numeric_fact_count > 0,
        concat(
            '["negative derived liabilities","unmapped numeric facts: ',
            toString(unmapped_numeric_fact_count),
            '"]'
        ),
        invalid_liabilities = 1,
        '["negative derived liabilities"]',
        mapped_fact_count = 0,
        '["no mapped metrics"]',
        unmapped_numeric_fact_count > 0,
        concat('["unmapped numeric facts: ', toString(unmapped_numeric_fact_count), '"]'),
        '[]'
    ),
    %(mapping_version)s,
    fx_rate_to_usd,
    fx_rate_date,
    fx_source,
    source_payload_hash,
    %(resolved_at)s
FROM native_metrics"""


def _sweden_financial_metrics_quality_sql(qualified_stage_table: str) -> str:
    coverage_columns = ",\n    ".join(
        f"countIf({metric_name}_amount_original IS NOT NULL) "
        f"AS {metric_name}_statement_count"
        for metric_name in MONEY_METRIC_NAMES
    )
    monetary_value_present = " OR\n        ".join(
        f"{metric_name}_amount_original IS NOT NULL"
        for metric_name in MONEY_METRIC_NAMES
    )
    return f"""SELECT
    count() AS row_count,
    uniqExact(company_id) AS company_count,
    countIf(observation_kind = 'reported') AS reported_row_count,
    countIf(observation_kind = 'comparative') AS comparative_row_count,
    min(fiscal_year) AS min_fiscal_year,
    max(fiscal_year) AS max_fiscal_year,
    countIf(
        fx_rate_to_usd IS NULL
        AND (
        {monetary_value_present}
        )
    ) AS missing_fx_count,
    countIf(position(metric_warnings, 'negative derived liabilities') > 0)
        AS invalid_liabilities_statement_count,
    countIf(mapped_fact_count > 0) AS mapped_statement_count,
    {coverage_columns},
    countIf(employees IS NOT NULL) AS employees_statement_count
FROM {qualified_stage_table}"""


def _quality_metadata(row: tuple[Any, ...]) -> dict[str, int | str | None]:
    values: dict[str, int | str | None] = {}
    for column, value in zip(_QUALITY_COLUMNS, row, strict=True):
        if column in {"min_fiscal_year", "max_fiscal_year"} and value is None:
            values[column] = None
        else:
            values[column] = int(value)
    return values


def _validate_quality(quality: dict[str, int | str | None]) -> None:
    if quality["row_count"] == 0:
        raise ValueError(
            "Sweden financial metric mapping produced no rows; refusing to replace "
            f"{QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE}"
        )
    if quality["mapped_statement_count"] == 0:
        raise ValueError(
            "Sweden financial metric mapping matched no XBRL statements; refusing "
            "to replace "
            f"{QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE}"
        )
    if quality["missing_fx_count"] != 0:
        raise ValueError(
            "Sweden financial metric mapping is missing currency/USD exchange rates for "
            f"{quality['missing_fx_count']} statements; refusing to replace "
            f"{QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_METRICS_TABLE}"
        )
