"""Ratsit candidates for the SE info registry.

The normalized Ratsit tables are content-addressed: several reports per company may coexist
(one per result hash and normalizer version), and the se_ratsit_company row is written last,
as the completion marker for all child segments of that hash. So: newest COMPLETE report per
company by normalized_at, then that report's first listed industry and its newest financial
periods. Ratsit has no record uid; the uid is built from the report hash and the row index.

Revenue is stored in the report's own unit (SEK / TSEK / MSEK) and Ratsit carries no USD
twin, so this extractor rescales to SEK and converts to USD itself from corpscout.exchange_rates
(ECB, EUR base): the latest SEK and USD rates on or before the period end, ASOF-joined. The
float arithmetic is exact to the cent for any revenue below 1e13 SEK.
"""

from functools import partial

import dagster as dg

from dagster_v3.defs.se_company.common import DATABASE
from dagster_v3.defs.se_company.fields.candidates.common import (
    CandidateExtractor,
    changed_companies_scope_sql,
    candidate_rows_from_result,
    clean_text_sql,
    compare_key_text_sql,
    define_candidate_asset,
    employee_count_json_sql,
    json_object_sql,
    json_string_sql,
    latest_revenue_json_sql,
    nace_labels_cte_sql,
    revenue_value_sql,
)
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION

SOURCE = "ratsit"
EXTRACTOR_VERSION = "ratsit-candidates-v1"
COMPANY_TABLE = "se_ratsit_company"
INDUSTRY_TABLE = "se_ratsit_company_industry_codes"
PERIODS_TABLE = "se_ratsit_financial_periods"
NACE_TABLE = "nace_categories"
RATES_TABLE = "exchange_rates"
CURRENCY = "SEK"


def build_scope_sql() -> str:
    return changed_companies_scope_sql(source=SOURCE, changes_sql=f"""    SELECT company_id, toDateTime64(normalized_at, 3, 'UTC') AS changed_at FROM {DATABASE}.{COMPANY_TABLE}
    WHERE normalizer_version = '{RATSIT_NORMALIZER_VERSION}'""")


def build_candidates_sql() -> str:
    employee_json = employee_count_json_sql(count="employees", as_of="period_end_text", period="toString(fiscal_year)")
    revenue_json = latest_revenue_json_sql(
        amount="amount", currency=f"'{CURRENCY}'", amount_usd="amount_usd", fiscal_year="fiscal_year", period_end="period_end_text")
    revenue_value = revenue_value_sql(amount="amount", currency=f"'{CURRENCY}'", fiscal_year="fiscal_year")
    return f"""WITH report AS (
    SELECT company_id, argMax(result_sha256, normalized_at) AS result_sha256,
        toDateTime64(max(normalized_at), 3, 'UTC') AS observed_at
    FROM {DATABASE}.{COMPANY_TABLE} FINAL
    WHERE normalizer_version = '{RATSIT_NORMALIZER_VERSION}' AND company_id IN %(company_ids)s
    GROUP BY company_id
),
industry AS (
    SELECT codes.company_id AS company_id,
        concat('ratsit:', toString(codes.result_sha256), ':industry:', toString(codes.industry_index)) AS source_record_uid,
        report.observed_at AS observed_at,
        trim(ifNull(codes.source_industry_code, ifNull(codes.industry_code, ''))) AS sni_code,
        toString(codes.source_industry_code_set) AS code_set,
        if(codes.nace_mapping_status = 'mapped', ifNull(codes.nace_normalized_code, ''), '') AS nace_digits,
        toString(codes.nace_revision) AS nace_revision
    FROM {DATABASE}.{INDUSTRY_TABLE} AS codes FINAL
    INNER JOIN report ON report.company_id = codes.company_id AND report.result_sha256 = codes.result_sha256
    WHERE codes.normalizer_version = '{RATSIT_NORMALIZER_VERSION}'
    ORDER BY codes.industry_index ASC
    LIMIT 1 BY codes.company_id
),
labels AS (
    {nace_labels_cte_sql()}
),
industry_labelled AS (
    SELECT industry.company_id AS company_id, industry.source_record_uid AS source_record_uid,
        industry.observed_at AS observed_at, industry.sni_code AS sni_code,
        industry.code_set AS code_set, industry.nace_digits AS nace_code,
        industry.nace_revision AS nace_revision,
        {clean_text_sql('labels.label_en')} AS label_en
    FROM industry
    LEFT JOIN labels ON labels.classification_version = industry.nace_revision AND labels.normalized_code = industry.nace_digits
),
periods AS (
    SELECT p.company_id AS company_id,
        concat('ratsit:', toString(p.result_sha256), ':financial:', toString(p.financial_report_index), ':', toString(p.period_index)) AS source_record_uid,
        ifNull(p.period_end, makeDate32(p.fiscal_year, 12, 31)) AS period_end,
        p.fiscal_year AS fiscal_year,
        if(p.monetary_unit IS NULL, NULL,
            toDecimal128(p.revenue_amount * multiIf(p.monetary_unit = 'TSEK', 1000, p.monetary_unit = 'MSEK', 1000000, 1), 2)) AS amount,
        p.employee_count AS employee_count,
        1 AS k
    FROM {DATABASE}.{PERIODS_TABLE} AS p FINAL
    INNER JOIN report ON report.company_id = p.company_id AND report.result_sha256 = p.result_sha256
    WHERE p.normalizer_version = '{RATSIT_NORMALIZER_VERSION}'
),
fx AS (
    SELECT toDate32(rate_date) AS rate_date, quote_currency, argMax(rate, pulled_at) AS rate, 1 AS k
    FROM {DATABASE}.{RATES_TABLE}
    WHERE base_currency = 'EUR' AND quote_currency IN ('{CURRENCY}', 'USD')
    GROUP BY rate_date, quote_currency
),
latest_employees AS (
    SELECT company_id, source_record_uid, toDateTime64(period_end, 3, 'UTC') AS observed_at,
        toString(period_end) AS period_end_text, fiscal_year, assumeNotNull(employee_count) AS employees
    FROM periods
    WHERE employee_count IS NOT NULL
    ORDER BY period_end DESC, fiscal_year DESC, source_record_uid DESC
    LIMIT 1 BY company_id
),
latest_revenue AS (
    SELECT periods.company_id AS company_id, periods.source_record_uid AS source_record_uid,
        toDateTime64(periods.period_end, 3, 'UTC') AS observed_at, toString(periods.period_end) AS period_end_text,
        periods.fiscal_year AS fiscal_year, assumeNotNull(periods.amount) AS amount,
        if(ifNull(sek.rate, 0) > 0 AND ifNull(usd.rate, 0) > 0,
           toDecimal128(toFloat64(periods.amount) / toFloat64(sek.rate) * toFloat64(usd.rate), 2),
           CAST(NULL AS Nullable(Decimal128(2)))) AS amount_usd
    FROM periods
    ASOF LEFT JOIN (SELECT rate_date, rate, k FROM fx WHERE quote_currency = '{CURRENCY}') AS sek ON periods.k = sek.k AND sek.rate_date <= periods.period_end
    ASOF LEFT JOIN (SELECT rate_date, rate, k FROM fx WHERE quote_currency = 'USD') AS usd ON periods.k = usd.k AND usd.rate_date <= periods.period_end
    WHERE periods.amount IS NOT NULL
    ORDER BY periods.period_end DESC, periods.fiscal_year DESC, periods.source_record_uid DESC
    LIMIT 1 BY periods.company_id
)
SELECT company_id, 'primary_sni_code', source_record_uid, observed_at, sni_code,
    {json_object_sql({'code_set': json_string_sql('code_set'), 'compare_key': json_string_sql('sni_code')})}
FROM industry_labelled WHERE sni_code != ''
UNION ALL
SELECT company_id, 'primary_nace_code', source_record_uid, observed_at, nace_code,
    {json_object_sql({'compare_key': json_string_sql('nace_code'), 'revision': json_string_sql('nace_revision')})}
FROM industry_labelled WHERE nace_code != ''
UNION ALL
SELECT company_id, 'industry_label_en', source_record_uid, observed_at, label_en,
    {json_object_sql({'compare_key': json_string_sql(compare_key_text_sql('label_en'))})}
FROM industry_labelled WHERE label_en != ''
UNION ALL
SELECT company_id, 'employee_count', source_record_uid, observed_at, toString(employees),
    {employee_json}
FROM latest_employees
UNION ALL
SELECT company_id, 'latest_revenue', source_record_uid, observed_at, {revenue_value},
    {revenue_json}
FROM latest_revenue"""


rows_from_result = partial(candidate_rows_from_result, source=SOURCE, extractor_version=EXTRACTOR_VERSION)

EXTRACTOR = CandidateExtractor(
    source=SOURCE, extractor_version=EXTRACTOR_VERSION,
    source_tables=(COMPANY_TABLE, INDUSTRY_TABLE, PERIODS_TABLE, NACE_TABLE, RATES_TABLE),
    build_scope_sql=build_scope_sql, build_candidates_sql=build_candidates_sql,
)

se_company_field_candidates_ratsit = define_candidate_asset(
    EXTRACTOR,
    deps=("se_ratsit_company", "se_ratsit_company_industry_codes", "se_ratsit_financial_periods",
          "nace_categories_clickhouse", "exchange_rates_v2_clickhouse"),
    description=(
        "Ratsit field candidates for Swedish companies from the newest normalized report: "
        "first listed SNI/NACE with its label, employee count and latest revenue (rescaled to "
        "SEK, converted to USD from the ECB rates). Preview by default; execute: true appends new evidence."
    ),
)

defs = dg.Definitions(assets=[se_company_field_candidates_ratsit])
