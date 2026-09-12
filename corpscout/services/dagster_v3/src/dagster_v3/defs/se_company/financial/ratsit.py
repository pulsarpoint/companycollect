"""Ratsit financial periods -> financial suggestions (spec 2026-09-11 section 7), after
slice 0 filled the USD twins.

se_ratsit_financial_periods holds each company's latest report (older report hashes are
replaced by the normalizer), one row per report period: scope `company` is the entity's
standalone, `consolidated` stays consolidated, anything else is skipped. Figures are
published in the row's monetary_unit (MSEK for every row today), so the original is the
figure times the unit's scale and amount_scale records it; the USD twins are already
full-unit dollars. A missing period end becomes Dec 31 of the fiscal year with
period_end_derived = 1 -- guarded to Date32's range, because makeDate32 returns 1970-01-01
for an out-of-range year instead of clamping; a row with neither a date nor a fiscal year
in range is skipped, as is a row without a unit (it would scale as SEK). Where a report
carries two periods with one scope and end (577 on 2026-09-11) the longer one wins, then
the later report and period index.
"""

import dagster as dg

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.suggestions import (
    MONEY_NULL_SQL,
    NULL_SQL,
    define_financial_suggestion_asset,
    financial_changed_scope_sql,
    financial_select_sql,
    live_select_sql,
    universe_join_sql,
)

SOURCE = "ratsit"
RATSIT_EXTRACTOR_VERSION = "ratsit-financial-v1"

# entity field -> periods column stem (seventeen fields; cash_and_bank, personnel_expenses
# and wages_and_salaries are NULL: Ratsit does not publish them).
RATSIT_MONEY: dict[str, str] = {
    "revenue": "revenue_amount",
    "operating_costs": "operating_costs_amount",
    "operating_result": "operating_profit_amount",
    "result_after_financial_items": "profit_after_financial_items_amount",
    "net_result": "net_income_amount",
    "ebitda": "ebitda_amount",
    "total_assets": "total_assets_amount",
    "fixed_assets": "fixed_assets_amount",
    "current_assets": "current_assets_amount",
    "equity": "equity_amount",
    "share_capital": "share_capital_amount",
    "untaxed_reserves": "untaxed_reserves_amount",
    "provisions": "provisions_amount",
    "liabilities": "liabilities_amount",
    "long_term_liabilities": "long_term_liabilities_amount",
    "current_liabilities": "current_liabilities_amount",
    "dividend": "dividend_amount",
}
SCALE_SQL = "multiIf(p.monetary_unit = 'MSEK', 1000000, p.monetary_unit = 'TSEK', 1000, 1)"
SCOPE_SQL = "multiIf(p.scope = 'company', 'standalone', p.scope = 'consolidated', 'consolidated', '')"
EFFECTIVE_END_SQL = "ifNull(p.period_end, makeDate32(p.fiscal_year, 12, 31))"


def _money_sql() -> dict[str, str]:
    values: dict[str, str] = dict(MONEY_NULL_SQL)
    for field, stem in RATSIT_MONEY.items():
        values[tables.original_column(field)] = f"p.{stem} * {SCALE_SQL}"
        values[tables.usd_column(field)] = f"p.{stem}_usd"
    return values


def _column_sql() -> dict[str, str]:
    return {
        "company_id": "p.company_id",
        "source": f"'{SOURCE}'",
        "period_key": "concat(p.entity_scope, ':', toString(p.effective_end))",
        "source_record_uid": (
            "concat('ratsit:', p.company_id, ':', toString(p.financial_report_index), ':', "
            "toString(p.period_index))"
        ),
        "scope": "p.entity_scope",
        "period_end": "p.effective_end",
        "period_end_derived": "toUInt8(p.period_end IS NULL)",
        "period_start": "p.period_start",
        "fiscal_year": "CAST(p.fiscal_year AS Nullable(UInt16))",
        "period_months": "p.period_months",
        "filing_fiscal_year": NULL_SQL["filing_fiscal_year"],
        "currency": "'SEK'",
        "amount_scale": f"toUInt32({SCALE_SQL})",
        **_money_sql(),
        "employees": "CAST(p.employee_count AS Nullable(UInt64))",
        "fx_rate_to_usd": "p.fx_rate_to_usd",
        "fx_rate_date": "p.fx_rate_date",
        "fx_source": "toString(p.fx_source)",
        "decided_by": "''",
        "note": "''",
    }


def _ranked_cte_sql(*, scoped: bool) -> str:
    company_filter = "\n        AND r.company_id IN %(company_ids)s" if scoped else ""
    period_filter = "\n        AND p.company_id IN %(company_ids)s" if scoped else ""
    return (
        "WITH report AS (\n"
        "    SELECT r.company_id AS company_id, argMax(r.result_sha256, r.normalized_at) AS result_sha256\n"
        "    FROM corpscout.se_ratsit_financial_reports AS r FINAL\n"
        f"    {universe_join_sql('r')}\n"
        f"    WHERE 1 = 1{company_filter}\n"
        "    GROUP BY r.company_id\n"
        "),\n"
        "ranked AS (\n"
        "    SELECT\n"
        "        p.*,\n"
        f"        {SCOPE_SQL} AS entity_scope,\n"
        f"        {EFFECTIVE_END_SQL} AS effective_end,\n"
        "        row_number() OVER (\n"
        "            PARTITION BY p.company_id, entity_scope, effective_end\n"
        "            ORDER BY ifNull(p.period_months, 0) DESC, p.financial_report_index DESC, p.period_index DESC\n"
        "        ) AS rn\n"
        "    FROM corpscout.se_ratsit_financial_periods AS p FINAL\n"
        "    INNER JOIN report ON report.company_id = p.company_id AND report.result_sha256 = p.result_sha256\n"
        "    WHERE p.monetary_unit IS NOT NULL\n"
        "        AND p.scope IN ('company', 'consolidated')\n"
        f"        AND (p.period_end IS NOT NULL OR p.fiscal_year BETWEEN 1900 AND 2299){period_filter}\n"
        ")\n"
    )


def ratsit_live_sql(*, scoped: bool = False) -> str:
    return live_select_sql(
        columns=_column_sql(),
        from_sql="FROM ranked AS p",
        where_sql="WHERE p.rn = 1",
        with_sql=_ranked_cte_sql(scoped=scoped),
    )


def ratsit_current_sql() -> str:
    """(company_id, observed_at) for `since` only; the change scan is the state hash."""
    return (
        "SELECT r.company_id AS company_id, toDateTime64(max(r.normalized_at), 3, 'UTC') AS observed_at\n"
        "FROM corpscout.se_ratsit_financial_reports AS r FINAL\n"
        f"{universe_join_sql('r')}\n"
        "GROUP BY r.company_id"
    )


def ratsit_changed_scope_sql() -> str:
    return financial_changed_scope_sql(source=SOURCE, live_sql=ratsit_live_sql())


def ratsit_select_sql() -> str:
    return financial_select_sql(source=SOURCE, live_sql=ratsit_live_sql(scoped=True))


se_company_financial_suggestions_ratsit = define_financial_suggestion_asset(
    source=SOURCE,
    extractor_version=RATSIT_EXTRACTOR_VERSION,
    current_sql=ratsit_current_sql(),
    select_sql=ratsit_select_sql(),
    changed_scope_override=ratsit_changed_scope_sql(),
    deps=[
        dg.AssetKey("se_ratsit_financial_periods_usd"),
        dg.AssetKey("se_ratsit_financial_reports"),
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description=(
        "Every period of a published company's latest Ratsit report as a financial suggestion "
        "in se_company_financial_suggestion (standalone or consolidated; figures scaled from "
        "the published unit, USD twins from the source); a period a re-scan no longer "
        "delivers is tombstoned. execute=false previews."
    ),
)
