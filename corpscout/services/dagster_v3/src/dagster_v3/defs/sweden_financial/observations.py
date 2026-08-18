"""Source-owned Bolagsverket financial observations.

The table built here deliberately does not choose a canonical value for a
company and year. Every recognized numeric XBRL fact with a representable
period remains tied to its source statement, context, and fact ordinal.
Reported and comparative assertions can therefore coexist, and quality checks
annotate observations instead of deleting them. Counts of unrecognized current
numeric facts remain attached as source-owned statement metadata.
"""

import uuid
from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.sweden_financial.clickhouse import (
    clickhouse_table_row_count,
    guard_against_clickhouse_table_shrink,
)

SWEDEN_FINANCIAL_DATABASE = "corpscout"
SE_FINANCIAL_REPORTS_TABLE = "se_financial_reports"
SE_FINANCIAL_FACTS_TABLE = "se_financial_facts"
EXCHANGE_RATES_TABLE = "exchange_rates"

SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE = "se_bolagsverket_financial_observations"
QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE = (
    f"{SWEDEN_FINANCIAL_DATABASE}.{SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE}"
)
BOLAGSVERKET_FINANCIAL_OBSERVATIONS_MAPPING_VERSION = (
    "se-bolagsverket-financial-observations-v2"
)

SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "source_statement_key",
    "company_id",
    "source_fiscal_year",
    "source_report_period_start",
    "source_report_period_end",
    "source_reported_company_name",
    "source_archive_key",
    "source_archive_name",
    "source_nested_zip_name",
    "source_xhtml_object_key",
    "source_taxonomy_entrypoint",
    "source_payload_hash",
    "source_fact_count",
    "source_unmapped_numeric_fact_count",
    "represented_fiscal_year",
    "represented_period_start",
    "represented_period_end",
    "observation_kind",
    "source_context_id",
    "source_fact_ordinal",
    "source_concept_qname",
    "source_concept_namespace",
    "source_concept_local_name",
    "metric_code",
    "unit_id",
    "decimals",
    "precision",
    "source_raw_value",
    "value_original",
    "currency",
    "value_usd",
    "fx_rate_to_usd",
    "fx_rate_date",
    "fx_source",
    "dimensions",
    "mapping_version",
    "revenue_overlap_relative_diff",
    "quality_flags",
    "parser_version",
    "resolved_at",
)

# Source concept -> semantic label. Concepts that can act as fallbacks share a
# metric code but remain separate rows with their original concept and ordinal;
# choosing between them belongs to a later resolution layer.
BOLAGSVERKET_FINANCIAL_CONCEPTS: dict[str, str] = {
    "Nettoomsattning": "revenue",
    "Rorelseresultat": "operating_profit_loss",
    "AretsResultat": "profit_loss",
    "ResultatEfterFinansiellaPoster": "result_after_financial_items",
    "Soliditet": "solidity",
    "Tillgangar": "total_assets",
    "Balansomslutning": "total_assets",
    "EgetKapital": "equity",
    "EgetKapitalSkulder": "equity_liabilities",
    "KassaBank": "cash_and_bank",
    "KassaBankExklRedovisningsmedel": "cash_and_bank",
    "Omsattningstillgangar": "current_assets",
    "KortfristigaFordringar": "current_receivables",
    "KortfristigaSkulder": "current_liabilities",
    "Personalkostnader": "personnel_expenses",
    "LonerAndraErsattningar": "wages_and_salaries",
    "MedelantaletAnstallda": "employees",
}

_CONTEXT_YEAR_EXTRACT_PATTERN = r"^(?:period|balans)([0-9]+)$"
MAX_COMPARATIVE_YEARS_BACK = 4
OVERLAP_AGREEMENT_TOLERANCE = 0.005


def _metric_code_sql() -> str:
    branches = ",\n            ".join(
        f"facts.concept_local_name = '{concept}', '{metric}'"
        for concept, metric in BOLAGSVERKET_FINANCIAL_CONCEPTS.items()
    )
    return f"multiIf(\n            {branches},\n            ''\n        )"


def build_bolagsverket_financial_observations_insert_sql(
    qualified_stage_table: str,
) -> str:
    """Build all mapped source observations without canonical resolution."""
    columns = ",\n    ".join(SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_COLUMNS)
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
exchange_rates_to_usd AS (
    SELECT
        rate_date,
        'SEK' AS currency,
        maxIf(rate, quote_currency = 'USD')
            / maxIf(rate, quote_currency = 'SEK') AS fx_rate_to_usd,
        anyIf(source, quote_currency = 'SEK') AS fx_source
    FROM latest_exchange_rates
    GROUP BY rate_date
    HAVING countDistinct(quote_currency) = 2
    UNION ALL
    SELECT
        rate_date,
        'EUR' AS currency,
        maxIf(rate, quote_currency = 'USD') AS fx_rate_to_usd,
        anyIf(source, quote_currency = 'USD') AS fx_source
    FROM latest_exchange_rates
    GROUP BY rate_date
    HAVING countIf(quote_currency = 'USD') > 0
),
source_numeric_facts AS (
    SELECT
        facts.country_iso2 AS country_iso2,
        facts.source_slug AS source_slug,
        facts.source_run_id AS source_run_id,
        facts.source_record_id AS source_record_id,
        facts.statement_key AS source_statement_key,
        facts.company_id AS company_id,
        toInt32(reports.fiscal_year) AS source_fiscal_year,
        reports.report_period_start AS source_report_period_start,
        reports.report_period_end AS source_report_period_end,
        reports.reported_company_name AS source_reported_company_name,
        reports.source_archive_key AS source_archive_key,
        reports.source_archive_name AS source_archive_name,
        reports.nested_zip_name AS source_nested_zip_name,
        reports.xhtml_object_key AS source_xhtml_object_key,
        reports.taxonomy_entrypoint AS source_taxonomy_entrypoint,
        reports.source_payload_hash AS source_payload_hash,
        reports.facts_count AS source_fact_count,
        facts.context_id AS source_context_id,
        facts.context_period_start AS context_period_start,
        facts.context_period_end AS context_period_end,
        toInt32OrNull(
            extract(
                lowerUTF8(facts.context_id),
                '{_CONTEXT_YEAR_EXTRACT_PATTERN}'
            )
        ) AS context_years_back,
        facts.fact_ordinal AS source_fact_ordinal,
        facts.concept_qname AS source_concept_qname,
        facts.concept_namespace AS source_concept_namespace,
        facts.concept_local_name AS source_concept_local_name,
        {_metric_code_sql()} AS metric_code,
        facts.unit_id AS unit_id,
        facts.decimals AS decimals,
        facts.precision AS precision,
        facts.raw_value AS source_raw_value,
        facts.amount_original AS value_original,
        facts.currency AS currency,
        facts.dimensions AS dimensions,
        facts.parser_version AS parser_version
    FROM corpscout.se_financial_facts AS facts
    INNER JOIN corpscout.se_financial_reports AS reports
        ON reports.statement_key = facts.statement_key
    PREWHERE facts.amount_original IS NOT NULL
),
current_statement_fact_counts AS (
    SELECT
        source_statement_key,
        countIf(
            metric_code = ''
            AND dimensions = '{{}}'
            AND lowerUTF8(source_context_id) IN ('period0', 'balans0')
            AND (
                upperUTF8(ifNull(currency, '')) IN ('SEK', 'EUR')
                OR currency IS NULL
            )
        ) AS source_unmapped_numeric_fact_count
    FROM source_numeric_facts
    GROUP BY source_statement_key
),
mapped_source_facts AS (
    SELECT *
    FROM source_numeric_facts
    WHERE metric_code != ''
),
dated_observations AS (
    SELECT
        *,
        coalesce(
            context_period_start,
            if(
                context_years_back >= 0
                AND context_years_back <= {MAX_COMPARATIVE_YEARS_BACK},
                addYears(source_report_period_start, -context_years_back),
                NULL
            )
        ) AS represented_period_start,
        coalesce(
            context_period_end,
            if(
                context_years_back >= 0
                AND context_years_back <= {MAX_COMPARATIVE_YEARS_BACK},
                addYears(source_report_period_end, -context_years_back),
                NULL
            )
        ) AS represented_period_end
    FROM mapped_source_facts
),
classified_observations AS (
    SELECT
        *,
        multiIf(
            context_period_end IS NOT NULL
                AND source_report_period_end IS NOT NULL
                AND context_period_end = source_report_period_end,
            'reported',
            context_period_end IS NOT NULL
                AND source_report_period_end IS NOT NULL
                AND context_period_end < source_report_period_end,
            'comparative',
            context_years_back = 0,
            'reported',
            context_years_back >= 1
                AND context_years_back <= {MAX_COMPARATIVE_YEARS_BACK},
            'comparative',
            'other'
        ) AS observation_kind
    FROM dated_observations
    WHERE represented_period_end IS NOT NULL
),
ranked_revenue_observations AS (
    SELECT
        source_statement_key,
        company_id,
        represented_fiscal_year,
        observation_kind,
        argMax(
            value_original,
            tuple(
                if(
                    upperUTF8(ifNull(decimals, '')) = 'INF',
                    100000,
                    ifNull(toInt32OrNull(decimals), -100000)
                ),
                source_fact_ordinal
            )
        ) AS revenue
    FROM (
        SELECT
            *,
            toInt32(toYear(represented_period_end)) AS represented_fiscal_year
        FROM classified_observations
        WHERE metric_code = 'revenue'
          AND dimensions = '{{}}'
          AND observation_kind IN ('reported', 'comparative')
    )
    GROUP BY
        source_statement_key,
        company_id,
        represented_fiscal_year,
        observation_kind
),
direct_revenue_by_company_year AS (
    SELECT
        company_id,
        represented_fiscal_year,
        argMin(revenue, source_statement_key) AS direct_revenue
    FROM ranked_revenue_observations
    WHERE observation_kind = 'reported'
    GROUP BY company_id, represented_fiscal_year
),
revenue_overlap_by_period AS (
    SELECT
        comparative.source_statement_key,
        comparative.represented_fiscal_year,
        abs(comparative.revenue - direct.direct_revenue)
            / nullIf(abs(direct.direct_revenue), 0) AS revenue_overlap_relative_diff
    FROM ranked_revenue_observations AS comparative
    INNER JOIN direct_revenue_by_company_year AS direct
        ON direct.company_id = comparative.company_id
       AND direct.represented_fiscal_year = comparative.represented_fiscal_year
    WHERE comparative.observation_kind = 'comparative'
),
observation_rate_dates AS (
    SELECT DISTINCT
        represented_period_end AS requested_rate_date,
        upperUTF8(ifNull(currency, '')) AS requested_currency
    FROM classified_observations
    WHERE upperUTF8(ifNull(currency, '')) IN ('SEK', 'EUR')
),
observation_rates AS (
    SELECT
        requested_rate_date,
        requested_currency,
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
    FROM observation_rate_dates
    CROSS JOIN exchange_rates_to_usd AS available_rates
    WHERE available_rates.currency = requested_currency
    GROUP BY requested_rate_date, requested_currency
)
SELECT
    observations.country_iso2,
    observations.source_slug,
    observations.source_run_id,
    observations.source_record_id,
    observations.source_statement_key,
    observations.company_id,
    observations.source_fiscal_year,
    observations.source_report_period_start,
    observations.source_report_period_end,
    observations.source_reported_company_name,
    observations.source_archive_key,
    observations.source_archive_name,
    observations.source_nested_zip_name,
    observations.source_xhtml_object_key,
    observations.source_taxonomy_entrypoint,
    observations.source_payload_hash,
    observations.source_fact_count,
    counts.source_unmapped_numeric_fact_count,
    toInt32(toYear(observations.represented_period_end)) AS represented_fiscal_year,
    observations.represented_period_start,
    observations.represented_period_end,
    observations.observation_kind,
    observations.source_context_id,
    observations.source_fact_ordinal,
    observations.source_concept_qname,
    observations.source_concept_namespace,
    observations.source_concept_local_name,
    observations.metric_code,
    observations.unit_id,
    observations.decimals,
    observations.precision,
    observations.source_raw_value,
    cast(observations.value_original AS Nullable(Decimal(38, 10))) AS value_original,
    observations.currency,
    cast(
        if(
            upperUTF8(ifNull(observations.currency, '')) IN ('SEK', 'EUR'),
            observations.value_original * rates.fx_rate_to_usd,
            NULL
        ) AS Nullable(Decimal(38, 10))
    ) AS value_usd,
    cast(
        if(
            upperUTF8(ifNull(observations.currency, '')) IN ('SEK', 'EUR'),
            rates.fx_rate_to_usd,
            NULL
        ) AS Nullable(Decimal(38, 12))
    ) AS fx_rate_to_usd,
    if(
        upperUTF8(ifNull(observations.currency, '')) IN ('SEK', 'EUR'),
        rates.fx_rate_date,
        NULL
    ) AS fx_rate_date,
    if(
        upperUTF8(ifNull(observations.currency, '')) IN ('SEK', 'EUR'),
        ifNull(rates.fx_source, ''),
        ''
    ) AS fx_source,
    observations.dimensions,
    '{BOLAGSVERKET_FINANCIAL_OBSERVATIONS_MAPPING_VERSION}' AS mapping_version,
    if(
        observations.observation_kind = 'comparative',
        overlap.revenue_overlap_relative_diff,
        NULL
    ) AS revenue_overlap_relative_diff,
    arrayFilter(
        flag -> flag != '',
        [
            if(
                observations.observation_kind = 'comparative'
                    AND overlap.revenue_overlap_relative_diff
                        > {OVERLAP_AGREEMENT_TOLERANCE},
                'revenue_overlap_disagreement',
                ''
            ),
            if(
                observations.metric_code = 'solidity',
                'ambiguous_solidity_scale',
                ''
            ),
            if(
                observations.context_period_end IS NULL,
                'represented_period_approximated',
                ''
            )
        ]
    ) AS quality_flags,
    observations.parser_version,
    now64(3) AS resolved_at
FROM classified_observations AS observations
LEFT JOIN observation_rates AS rates
    ON rates.requested_rate_date = observations.represented_period_end
   AND rates.requested_currency = upperUTF8(ifNull(observations.currency, ''))
LEFT JOIN revenue_overlap_by_period AS overlap
    ON overlap.source_statement_key = observations.source_statement_key
   AND overlap.represented_fiscal_year = toInt32(
       toYear(observations.represented_period_end)
   )
INNER JOIN current_statement_fact_counts AS counts
    ON counts.source_statement_key = observations.source_statement_key"""


def build_bolagsverket_financial_observations_quality_sql(
    qualified_stage_table: str,
) -> str:
    return f"""SELECT
    count() AS row_count,
    uniqExact(company_id) AS company_count,
    uniqExact(source_statement_key) AS statement_count,
    countIf(observation_kind = 'reported') AS reported_count,
    countIf(observation_kind = 'comparative') AS comparative_count,
    countIf(observation_kind = 'other') AS other_count,
    countIf(notEmpty(quality_flags)) AS flagged_count,
    countIf(
        value_original IS NOT NULL
        AND upperUTF8(ifNull(currency, '')) IN ('SEK', 'EUR')
        AND fx_rate_to_usd IS NULL
    ) AS missing_fx_count,
    min(represented_fiscal_year) AS min_fiscal_year,
    max(represented_fiscal_year) AS max_fiscal_year
FROM {qualified_stage_table}"""


_QUALITY_COLUMNS = (
    "row_count",
    "company_count",
    "statement_count",
    "reported_count",
    "comparative_count",
    "other_count",
    "flagged_count",
    "missing_fx_count",
    "min_fiscal_year",
    "max_fiscal_year",
)


def _quality_metadata(row: tuple[Any, ...]) -> dict[str, int | None]:
    return {
        column: None if value is None else int(value)
        for column, value in zip(_QUALITY_COLUMNS, row, strict=True)
    }


def _validate_quality(quality: dict[str, int | None]) -> None:
    if quality["row_count"] == 0:
        raise ValueError(
            "Bolagsverket financial observations build produced no rows; "
            f"refusing to replace "
            f"{QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE}"
        )
    if quality["missing_fx_count"] != 0:
        raise ValueError(
            "Bolagsverket financial observations are missing currency/USD "
            f"exchange rates for {quality['missing_fx_count']} monetary facts; "
            "refusing to replace "
            f"{QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE}"
        )


def replace_se_bolagsverket_financial_observations_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
    allow_shrink: bool = False,
) -> dict[str, int | str | None]:
    """Atomically rebuild all source-owned Bolagsverket observations."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(
            SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE,
            SE_FINANCIAL_REPORTS_TABLE,
            SE_FINANCIAL_FACTS_TABLE,
            EXCHANGE_RATES_TABLE,
        ),
    )
    stage_table = (
        f"_tmp_{SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE}_{uuid.uuid4().hex}"
    )
    qualified_stage_table = f"`{SWEDEN_FINANCIAL_DATABASE}`.`{stage_table}`"
    qualified_target_table = (
        f"`{SWEDEN_FINANCIAL_DATABASE}`."
        f"`{SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE}`"
    )
    if log is not None:
        log(
            "Building source-owned Bolagsverket financial observations: target=%s",
            QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE,
        )

    with clickhouse.get_connection() as client:
        client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
        )
        primary_error: Exception | None = None
        try:
            client.execute(
                build_bolagsverket_financial_observations_insert_sql(
                    qualified_stage_table
                )
            )
            quality_row = client.execute(
                build_bolagsverket_financial_observations_quality_sql(
                    qualified_stage_table
                )
            )[0]
            metadata: dict[str, int | str | None] = dict(_quality_metadata(quality_row))
            _validate_quality(metadata)
            existing_row_count = clickhouse_table_row_count(
                client, qualified_target_table
            )
            guard_against_clickhouse_table_shrink(
                qualified_table=(
                    QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE
                ),
                existing_row_count=existing_row_count,
                staged_row_count=int(metadata["row_count"] or 0),
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

    metadata["table"] = QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE
    if log is not None:
        log(
            "Finished Bolagsverket financial observations: rows=%s companies=%s "
            "statements=%s reported=%s comparative=%s flagged=%s missing_fx=%s",
            metadata["row_count"],
            metadata["company_count"],
            metadata["statement_count"],
            metadata["reported_count"],
            metadata["comparative_count"],
            metadata["flagged_count"],
            metadata["missing_fx_count"],
        )
    return metadata
