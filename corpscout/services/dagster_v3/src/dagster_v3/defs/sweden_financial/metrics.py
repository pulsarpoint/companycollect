import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.sweden_financial.resources import (
    DEFAULT_ARCHIVE_BASE_URL,
    SWEDEN_FINANCIAL_RAW_BUCKET,
)

SWEDEN_FINANCIAL_DATABASE = "corpscout"
SE_FINANCIAL_REPORTS_TABLE = "se_financial_reports"
SE_FINANCIAL_FACTS_TABLE = "se_financial_facts"
SE_FINANCIAL_METRICS_TABLE = "se_financial_metrics"
EXCHANGE_RATES_TABLE = "exchange_rates"
QUALIFIED_SE_FINANCIAL_METRICS_TABLE = (
    f"{SWEDEN_FINANCIAL_DATABASE}.{SE_FINANCIAL_METRICS_TABLE}"
)
SWEDEN_FINANCIAL_MAPPING_VERSION = "sweden-bolagsverket-ixbrl-metrics-v1"

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

SE_FINANCIAL_METRICS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "statement_key",
    "company_id",
    "report_period_start",
    "report_period_end",
    "fiscal_year",
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
    "min_fiscal_year",
    "max_fiscal_year",
    "missing_fx_count",
    "invalid_liabilities_statement_count",
    "mapped_statement_count",
    *(f"{metric_name}_statement_count" for metric_name in MONEY_METRIC_NAMES),
    "employees_statement_count",
)


def replace_sweden_financial_metrics_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    resolved_at: datetime,
    log: Callable[..., object] | None = None,
) -> dict[str, int | str | None]:
    """Atomically rebuild the canonical Sweden metrics table from all XBRL facts."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(
            SE_FINANCIAL_REPORTS_TABLE,
            SE_FINANCIAL_FACTS_TABLE,
            SE_FINANCIAL_METRICS_TABLE,
            EXCHANGE_RATES_TABLE,
        ),
    )
    stage_table = f"_tmp_{SE_FINANCIAL_METRICS_TABLE}_{uuid.uuid4().hex}"
    qualified_stage_table = f"`{SWEDEN_FINANCIAL_DATABASE}`.`{stage_table}`"
    qualified_target_table = (
        f"`{SWEDEN_FINANCIAL_DATABASE}`.`{SE_FINANCIAL_METRICS_TABLE}`"
    )
    if log is not None:
        log(
            "Building Sweden financial metrics in ClickHouse: target=%s mapping=%s",
            QUALIFIED_SE_FINANCIAL_METRICS_TABLE,
            SWEDEN_FINANCIAL_MAPPING_VERSION,
        )

    with clickhouse.get_connection() as client:
        client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
        )
        primary_error: Exception | None = None
        try:
            client.execute(
                build_sweden_financial_metrics_insert_sql(qualified_stage_table),
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

    quality["table"] = QUALIFIED_SE_FINANCIAL_METRICS_TABLE
    quality["mapping_version"] = SWEDEN_FINANCIAL_MAPPING_VERSION
    if log is not None:
        log(
            "Finished Sweden financial metrics: rows=%s companies=%s mapped=%s missing_fx=%s",
            quality["row_count"],
            quality["company_count"],
            quality["mapped_statement_count"],
            quality["missing_fx_count"],
        )
    return quality


def build_sweden_financial_metrics_insert_sql(qualified_stage_table: str) -> str:
    columns = ",\n    ".join(SE_FINANCIAL_METRICS_COLUMNS)
    return f"""INSERT INTO {qualified_stage_table} (
    {columns}
)
WITH
latest_exchange_rates AS (
    SELECT
        rate_date,
        quote_currency,
        argMax(rate, pulled_at) AS rate,
        argMax(source, pulled_at) AS source
    FROM corpscout.exchange_rates
    WHERE base_currency = 'EUR'
      AND quote_currency IN ('SEK', 'USD')
    GROUP BY rate_date, quote_currency
),
exchange_rate_pairs AS (
    SELECT
        rate_date,
        maxIf(rate, quote_currency = 'USD')
            / maxIf(rate, quote_currency = 'SEK') AS fx_rate_to_usd,
        anyIf(source, quote_currency = 'SEK') AS fx_source
    FROM latest_exchange_rates
    GROUP BY rate_date
    HAVING countDistinct(quote_currency) = 2
),
requested_rate_dates AS (
    SELECT DISTINCT report_period_end AS requested_rate_date
    FROM corpscout.se_financial_reports
    WHERE report_period_end IS NOT NULL
),
rates_for_reports AS (
    SELECT
        requested_rate_date,
        if(
            countIf(rate_date <= requested_rate_date) > 0,
            argMaxIf(fx_rate_to_usd, rate_date, rate_date <= requested_rate_date),
            argMinIf(fx_rate_to_usd, rate_date, rate_date > requested_rate_date)
        ) AS fx_rate_to_usd,
        if(
            countIf(rate_date <= requested_rate_date) > 0,
            maxIf(rate_date, rate_date <= requested_rate_date),
            minIf(rate_date, rate_date > requested_rate_date)
        ) AS fx_rate_date,
        if(
            countIf(rate_date <= requested_rate_date) > 0,
            argMaxIf(fx_source, rate_date, rate_date <= requested_rate_date),
            argMinIf(fx_source, rate_date, rate_date > requested_rate_date)
        ) AS fx_source
    FROM requested_rate_dates
    CROSS JOIN exchange_rate_pairs
    GROUP BY requested_rate_date
),
current_numeric_facts AS (
    SELECT
        statement_key,
        concept_local_name,
        amount_original,
        currency,
        fact_ordinal,
        if(
            upperUTF8(ifNull(decimals, '')) = 'INF',
            100000,
            ifNull(toInt32OrNull(decimals), -100000)
        ) AS precision_rank,
        multiIf(
            concept_local_name = 'Nettoomsattning', 'revenue',
            concept_local_name = 'Rorelseresultat', 'operating_profit_loss',
            concept_local_name = 'AretsResultat', 'profit_loss',
            concept_local_name = 'Tillgangar', 'total_assets',
            concept_local_name = 'Balansomslutning', 'total_assets_fallback',
            concept_local_name = 'EgetKapital', 'equity',
            concept_local_name = 'EgetKapitalSkulder', 'equity_liabilities',
            concept_local_name = 'KassaBank', 'cash_and_bank',
            concept_local_name = 'KassaBankExklRedovisningsmedel', 'cash_and_bank_fallback',
            concept_local_name = 'Omsattningstillgangar', 'current_assets',
            concept_local_name = 'KortfristigaFordringar', 'current_receivables',
            concept_local_name = 'KortfristigaSkulder', 'current_liabilities',
            concept_local_name = 'Personalkostnader', 'personnel_expenses',
            concept_local_name = 'LonerAndraErsattningar', 'wages_and_salaries',
            concept_local_name = 'MedelantaletAnstallda', 'employees',
            ''
        ) AS metric_code
    FROM corpscout.se_financial_facts
    PREWHERE amount_original IS NOT NULL
    WHERE dimensions = '{{}}'
      AND lowerUTF8(context_id) IN ('period0', 'balans0')
),
facts_by_statement AS (
    SELECT
        statement_key,
        countIf(metric_code != '') AS mapped_fact_count,
        countIf(metric_code = '' AND (currency = 'SEK' OR currency IS NULL))
            AS unmapped_numeric_fact_count,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'revenue')
            AS revenue,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'operating_profit_loss')
            AS operating_profit_loss,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'profit_loss')
            AS profit_loss,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'total_assets')
            AS total_assets,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'total_assets_fallback')
            AS total_assets_fallback,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'equity')
            AS equity,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'equity_liabilities')
            AS equity_liabilities,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'cash_and_bank')
            AS cash_and_bank,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'cash_and_bank_fallback')
            AS cash_and_bank_fallback,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'current_assets')
            AS current_assets,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'current_receivables')
            AS current_receivables,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'current_liabilities')
            AS current_liabilities,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'personnel_expenses')
            AS personnel_expenses,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'wages_and_salaries')
            AS wages_and_salaries,
        argMaxIf(amount_original, tuple(precision_rank, fact_ordinal), metric_code = 'employees')
            AS employees
    FROM current_numeric_facts
    GROUP BY statement_key
),
native_metrics AS (
    SELECT
        reports.country_iso2 AS country_iso2,
        reports.source_slug AS source_slug,
        %(source_run_id)s AS source_run_id,
        reports.source_record_id AS source_record_id,
        reports.statement_key AS statement_key,
        reports.company_id AS company_id,
        reports.report_period_start AS report_period_start,
        reports.report_period_end AS report_period_end,
        reports.fiscal_year AS fiscal_year,
        reports.reported_company_name AS reported_company_name,
        concat(
            %(source_archive_base_url)s,
            '/arsredovisningar/',
            extract(reports.source_archive_key, 'year=([^/]+)'),
            '/',
            reports.source_archive_name
        ) AS source_archive_url,
        reports.source_archive_key AS source_archive_key,
        reports.source_archive_name AS source_archive_name,
        reports.nested_zip_name AS nested_zip_name,
        reports.xhtml_object_key AS xhtml_object_key,
        concat(%(xhtml_uri_prefix)s, reports.xhtml_object_key) AS xhtml_source_uri,
        reports.taxonomy_entrypoint AS taxonomy_entrypoint,
        'SEK' AS currency,
        cast(facts.revenue AS Nullable(Decimal(38, 6))) AS revenue_amount_original,
        cast(facts.operating_profit_loss AS Nullable(Decimal(38, 6)))
            AS operating_profit_loss_amount_original,
        cast(facts.profit_loss AS Nullable(Decimal(38, 6))) AS profit_loss_amount_original,
        cast(coalesce(facts.total_assets, facts.total_assets_fallback) AS Nullable(Decimal(38, 6)))
            AS total_assets_amount_original,
        cast(facts.equity AS Nullable(Decimal(38, 6))) AS equity_amount_original,
        cast(
            if(
                coalesce(
                    facts.equity_liabilities,
                    facts.total_assets,
                    facts.total_assets_fallback
                ) >= facts.equity,
                coalesce(
                    facts.equity_liabilities,
                    facts.total_assets,
                    facts.total_assets_fallback
                ) - facts.equity,
                NULL
            )
            AS Nullable(Decimal(38, 6))
        ) AS liabilities_amount_original,
        toUInt8(
            coalesce(
                facts.equity_liabilities,
                facts.total_assets,
                facts.total_assets_fallback
            ) < facts.equity
        ) AS invalid_liabilities,
        cast(coalesce(facts.cash_and_bank, facts.cash_and_bank_fallback) AS Nullable(Decimal(38, 6)))
            AS cash_and_bank_amount_original,
        cast(facts.current_assets AS Nullable(Decimal(38, 6)))
            AS current_assets_amount_original,
        cast(facts.current_receivables AS Nullable(Decimal(38, 6)))
            AS current_receivables_amount_original,
        cast(facts.current_liabilities AS Nullable(Decimal(38, 6)))
            AS current_liabilities_amount_original,
        cast(facts.personnel_expenses AS Nullable(Decimal(38, 6)))
            AS personnel_expenses_amount_original,
        cast(facts.wages_and_salaries AS Nullable(Decimal(38, 6)))
            AS wages_and_salaries_amount_original,
        if(
            facts.employees >= 0,
            toUInt64(round(facts.employees)),
            cast(NULL AS Nullable(UInt64))
        ) AS employees,
        reports.facts_count AS source_fact_count,
        ifNull(facts.mapped_fact_count, 0) AS mapped_fact_count,
        ifNull(facts.unmapped_numeric_fact_count, 0) AS unmapped_numeric_fact_count,
        rates.fx_rate_to_usd AS fx_rate_to_usd,
        rates.fx_rate_date AS fx_rate_date,
        rates.fx_source AS fx_source,
        reports.source_payload_hash AS source_payload_hash
    FROM corpscout.se_financial_reports AS reports
    LEFT JOIN facts_by_statement AS facts
        ON facts.statement_key = reports.statement_key
    LEFT JOIN rates_for_reports AS rates
        ON rates.requested_rate_date = reports.report_period_end
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
    cast(revenue_amount_original * fx_rate_to_usd AS Nullable(Decimal(38, 6))),
    operating_profit_loss_amount_original,
    cast(operating_profit_loss_amount_original * fx_rate_to_usd AS Nullable(Decimal(38, 6))),
    profit_loss_amount_original,
    cast(profit_loss_amount_original * fx_rate_to_usd AS Nullable(Decimal(38, 6))),
    total_assets_amount_original,
    cast(total_assets_amount_original * fx_rate_to_usd AS Nullable(Decimal(38, 6))),
    equity_amount_original,
    cast(equity_amount_original * fx_rate_to_usd AS Nullable(Decimal(38, 6))),
    liabilities_amount_original,
    cast(liabilities_amount_original * fx_rate_to_usd AS Nullable(Decimal(38, 6))),
    cash_and_bank_amount_original,
    cast(cash_and_bank_amount_original * fx_rate_to_usd AS Nullable(Decimal(38, 6))),
    current_assets_amount_original,
    cast(current_assets_amount_original * fx_rate_to_usd AS Nullable(Decimal(38, 6))),
    current_receivables_amount_original,
    cast(current_receivables_amount_original * fx_rate_to_usd AS Nullable(Decimal(38, 6))),
    current_liabilities_amount_original,
    cast(current_liabilities_amount_original * fx_rate_to_usd AS Nullable(Decimal(38, 6))),
    personnel_expenses_amount_original,
    cast(personnel_expenses_amount_original * fx_rate_to_usd AS Nullable(Decimal(38, 6))),
    wages_and_salaries_amount_original,
    cast(wages_and_salaries_amount_original * fx_rate_to_usd AS Nullable(Decimal(38, 6))),
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
    return f"""SELECT
    count() AS row_count,
    uniqExact(company_id) AS company_count,
    min(fiscal_year) AS min_fiscal_year,
    max(fiscal_year) AS max_fiscal_year,
    countIf(report_period_end IS NOT NULL AND fx_rate_to_usd IS NULL) AS missing_fx_count,
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
            f"{QUALIFIED_SE_FINANCIAL_METRICS_TABLE}"
        )
    if quality["mapped_statement_count"] == 0:
        raise ValueError(
            "Sweden financial metric mapping matched no XBRL statements; refusing "
            f"to replace {QUALIFIED_SE_FINANCIAL_METRICS_TABLE}"
        )
    if quality["missing_fx_count"] != 0:
        raise ValueError(
            "Sweden financial metric mapping is missing SEK/USD exchange rates for "
            f"{quality['missing_fx_count']} statements; refusing to replace "
            f"{QUALIFIED_SE_FINANCIAL_METRICS_TABLE}"
        )
