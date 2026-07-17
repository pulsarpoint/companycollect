"""Per-country SELECTs producing the uniform latest-financials row set.

Latest year per company mirrors each country's backoffice detail-query
tiebreakers. USD falls back to original * fx_rate_to_usd where stored USD
is null (Norway's stored USD covers ~14% of rows but fx_rate ~100%).

``years_count`` uses ``uniqExact(fiscal_year) OVER (PARTITION BY <id>)``
rather than a plain ``count() OVER (...)``. Live-CH verification (2026-07-17)
found real duplicate ``(id, fiscal_year)`` rows in three of the seven wide
sources -- fi (264 groups), se (178,126 groups!), lv (5,238 groups), e.g.
multiple XBRL filings/restatements landing under the same fiscal year. A raw
``count()`` there overcounts years_count (one sampled Swedish company: 8 raw
rows across only 3 distinct fiscal years). ``uniqExact(fiscal_year)`` counts
distinct years, which is what the column name promises, and is unaffected by
duplicate same-year rows.

Window functions execute before ``LIMIT 1 BY`` in ClickHouse, so
``uniqExact(fiscal_year) OVER (PARTITION BY <id>)`` is computed over every row
for that company/entity *before* ``LIMIT 1 BY <id>`` collapses to one row --
confirmed live against a Norwegian company with 3 filed years (result: 3, not
1).
"""

# Wide (one-row-per-company-per-year) sources: {code}_financial_metrics /
# {code}_financial_statements. Every column referenced here (currency, the
# four *_amount_original/*_amount_usd metric pairs, fx_rate_to_usd,
# resolved_at) was verified live via system.columns on 2026-07-17.
#
# NOTE on the `usd_amount` coalesce: `coalesce(usd_col, orig_col * fx_rate)`
# alone raises `ILLEGAL_TYPE_OF_ARGUMENT` in ClickHouse 26.5 -- coalescing a
# Decimal(38,6) column with a Float64 expression produces a Variant type that
# toFloat64() cannot accept. Casting the *_usd column to Float64 before the
# coalesce (so both arms are Float64) avoids the Variant and was verified
# live against all seven wide sources.
#
# NOTE on the ORDER BY tiebreak: the SELECT list also has `now64(3) AS
# resolved_at` (a constant, evaluated once per query). A bare `resolved_at`
# in ORDER BY resolves to that constant alias, not the source column, making
# the tiebreak a no-op -- confirmed live against ClickHouse (2026-07-17).
# Qualifying with the source table name (`{table}`.resolved_at) forces
# resolution to the real per-row column instead of the alias.
_WIDE_TEMPLATE = """
SELECT
  toString({id}) AS company_id,
  toInt32(fiscal_year) AS fiscal_year,
  {period_end} AS period_end_date,
  coalesce({currency}, '') AS currency,
  toFloat64({rev}_amount_original) AS revenue_amount_original,
  coalesce(toFloat64({rev}_amount_usd), toFloat64({rev}_amount_original) * toFloat64(fx_rate_to_usd)) AS revenue_amount_usd,
  toFloat64({net}_amount_original) AS net_result_amount_original,
  coalesce(toFloat64({net}_amount_usd), toFloat64({net}_amount_original) * toFloat64(fx_rate_to_usd)) AS net_result_amount_usd,
  toFloat64(total_assets_amount_original) AS total_assets_amount_original,
  coalesce(toFloat64(total_assets_amount_usd), toFloat64(total_assets_amount_original) * toFloat64(fx_rate_to_usd)) AS total_assets_amount_usd,
  toFloat64(equity_amount_original) AS equity_amount_original,
  coalesce(toFloat64(equity_amount_usd), toFloat64(equity_amount_original) * toFloat64(fx_rate_to_usd)) AS equity_amount_usd,
  {employees} AS employees,
  toUInt32(uniqExact(fiscal_year) OVER (PARTITION BY {id})) AS years_count,
  now64(3) AS resolved_at
FROM corpscout.{table}
ORDER BY fiscal_year DESC NULLS LAST, `{table}`.resolved_at DESC, isNull({rev}_amount_original) ASC, source_record_id DESC
LIMIT 1 BY {id}
"""

# code -> _WIDE_TEMPLATE fill-in spec. "br" is included so
# `SOURCES["br"]["table"]` resolves for the upstream-table-existence check in
# assets.py, even though BR's SELECT is hand-built (see _BR_SELECT) rather
# than rendered from _WIDE_TEMPLATE.
SOURCES = {
    "no": {
        "table": "no_financial_statements",
        "id": "org_number",
        "currency": "currency",
        "rev": "operating_revenue",
        "net": "net_result",
        "employees": "CAST(NULL AS Nullable(Float64))",
        "period_end": "period_end_date",
    },
    "fi": {
        "table": "fi_financial_metrics",
        "id": "business_id",
        "currency": "currency_original",
        "rev": "revenue",
        "net": "profit_loss",
        "employees": "toFloat64(employees)",
        # fi.period_end is already Nullable(Date) (verified live 2026-07-17) --
        # no toDate() wrapper needed.
        "period_end": "period_end",
    },
    "se": {
        "table": "se_financial_metrics",
        "id": "company_id",
        "currency": "currency",
        "rev": "revenue",
        "net": "profit_loss",
        "employees": "toFloat64(employees)",
        # se.report_period_end is Nullable(Date32) (verified live 2026-07-17),
        # not Date like the migration column -- toDate() normalizes it.
        "period_end": "toDate(report_period_end)",
    },
    "ee": {
        "table": "ee_financial_metrics",
        "id": "reg_code",
        "currency": "currency",
        "rev": "revenue",
        "net": "net_result",
        "employees": "CAST(NULL AS Nullable(Float64))",
        "period_end": "period_end_date",
    },
    "lv": {
        "table": "lv_financial_metrics",
        "id": "regcode",
        "currency": "currency",
        "rev": "revenue",
        "net": "net_result",
        "employees": "toFloat64(employees)",
        "period_end": "period_end_date",
    },
    "gb": {
        "table": "gb_financial_metrics",
        "id": "company_number",
        "currency": "currency",
        "rev": "revenue",
        "net": "net_result",
        "employees": "CAST(NULL AS Nullable(Float64))",
        "period_end": "period_end_date",
    },
    "sk": {
        "table": "sk_financial_metrics",
        "id": "ico",
        "currency": "currency_original",
        "rev": "revenue",
        "net": "net_result",
        "employees": "CAST(NULL AS Nullable(Float64))",
        "period_end": "period_end_date",
    },
    "br": {
        "table": "br_cvm_financial_metrics",
        "id": "cnpj_basico",
    },
}

# BR is long-format (one row per company/reference_date/metric, no fx_rate --
# `amount_usd` is already resolved upstream) and needs a pivot rather than the
# wide template. Mirrors the backoffice `br` financialsQuery: latest annual
# fiscal year per company, consolidated statements preferred over individual,
# newest reference_date/version as the tiebreak.
#
# The inner subquery first collapses to one row per
# (cnpj_basico, fiscal_year, metric) -- `LIMIT 1 BY cnpj_basico, fy, metric`
# after ordering by consolidation preference/recency -- so the outer
# `anyIf(...)` pivots are picking from a single deduplicated row per metric,
# not an arbitrary one among duplicates.
#
# years_count: `uniqExact(fy) OVER (PARTITION BY cnpj_basico)` is evaluated
# in the OUTER query, i.e. AFTER `GROUP BY cnpj_basico, fy` has already
# collapsed to one row per (company, year) -- so it correctly counts distinct
# annual fiscal years per company. Verified live against real BR data
# (2026-07-17): a company with 17 filed annual years returns years_count=17.
_BR_SELECT = """
SELECT
  toString(cnpj_basico) AS company_id,
  toInt32(fy) AS fiscal_year,
  toDate(max(ped)) AS period_end_date,
  coalesce(any(cur), '') AS currency,
  anyIf(orig, metric = 'revenue') AS revenue_amount_original,
  anyIf(usd, metric = 'revenue') AS revenue_amount_usd,
  anyIf(orig, metric = 'net_income') AS net_result_amount_original,
  anyIf(usd, metric = 'net_income') AS net_result_amount_usd,
  anyIf(orig, metric = 'total_assets') AS total_assets_amount_original,
  anyIf(usd, metric = 'total_assets') AS total_assets_amount_usd,
  anyIf(orig, metric = 'equity') AS equity_amount_original,
  anyIf(usd, metric = 'equity') AS equity_amount_usd,
  CAST(NULL AS Nullable(Float64)) AS employees,
  toUInt32(uniqExact(fy) OVER (PARTITION BY cnpj_basico)) AS years_count,
  now64(3) AS resolved_at
FROM (
  SELECT
    cnpj_basico,
    toYear(period_end_date) AS fy,
    max(period_end_date) OVER (PARTITION BY cnpj_basico, toYear(period_end_date)) AS ped,
    metric_name AS metric,
    toFloat64(amount_original) AS orig,
    toFloat64(amount_usd) AS usd,
    currency AS cur
  FROM corpscout.br_cvm_financial_metrics
  WHERE period_type = 'annual'
  ORDER BY consolidation_type = 'consolidated' DESC, reference_date DESC, version DESC, source_record_id DESC
  LIMIT 1 BY cnpj_basico, fy, metric
)
GROUP BY cnpj_basico, fy
ORDER BY fy DESC
LIMIT 1 BY cnpj_basico
"""


def build_latest_insert_sql(code: str) -> str:
    """Return the latest-financials SELECT body for one country `code`.

    The caller (the asset) wraps this in
    ``INSERT INTO corpscout.{stage} ({columns}) {select}`` using the explicit
    `COMPANY_FINANCIALS_LATEST_COLUMNS` column list, so SELECT order and
    stage-table order can never silently misalign.
    """
    if code not in SOURCES:
        raise ValueError(
            f"Unknown company_financials_latest source code: {code!r}; "
            f"expected one of {sorted(SOURCES)}"
        )
    if code == "br":
        return _BR_SELECT
    return _WIDE_TEMPLATE.format(**SOURCES[code])
