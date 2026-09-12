"""Bolagsverket annual accounts -> financial suggestions (spec 2026-09-11 section 7): the
reported filings as source `bolagsverket`, the restated prior-year columns of later filings
as source `bolagsverket_comparative`.

se_bolagsverket_financial_metrics holds one row per filing and represented period:
observation_kind `reported` for the period the filing covers, `comparative` for the
prior-year column it restates. Both are standalone (legal-entity) accounts, so every period
key is `standalone:<report_period_end>`. Where a period has two reported statements (14
groups on 2026-09-11, the same filing archived twice) the one with more figures wins, then
the smaller statement key; where several later filings restate the same period the newest
filing (greatest source_fiscal_year) wins. The table is rebuilt whole every Saturday with a
single resolved_at, which is why the change scan is the state hash.
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
    period_months_sql,
    universe_join_sql,
)

SOURCE_REPORTED = "bolagsverket"
SOURCE_COMPARATIVE = "bolagsverket_comparative"
BOLAGSVERKET_EXTRACTOR_VERSION = "bolagsverket-financial-v1"
BOLAGSVERKET_COMPARATIVE_EXTRACTOR_VERSION = "bolagsverket-comparative-financial-v1"

# entity field -> metrics column stem (the twelve register metrics; the other eight are NULL).
BOLAGSVERKET_MONEY: dict[str, str] = {
    "revenue": "revenue",
    "operating_result": "operating_profit_loss",
    "net_result": "profit_loss",
    "total_assets": "total_assets",
    "equity": "equity",
    "liabilities": "liabilities",
    "cash_and_bank": "cash_and_bank",
    "current_assets": "current_assets",
    "current_liabilities": "current_liabilities",
    "personnel_expenses": "personnel_expenses",
    "wages_and_salaries": "wages_and_salaries",
}
# The two columns a later filing restates (spec section 7).
COMPARATIVE_MONEY: dict[str, str] = {"revenue": "revenue", "total_assets": "total_assets"}


def _money_sql(mapping: dict[str, str]) -> dict[str, str]:
    values: dict[str, str] = dict(MONEY_NULL_SQL)
    for field, stem in mapping.items():
        values[tables.original_column(field)] = f"m.{stem}_amount_original"
        values[tables.usd_column(field)] = f"m.{stem}_amount_usd"
    return values


def _column_sql(source: str, mapping: dict[str, str], employees: bool) -> dict[str, str]:
    return {
        "company_id": "m.company_id",
        "source": f"'{source}'",
        "period_key": "concat('standalone:', toString(m.report_period_end))",
        "source_record_uid": "m.statement_key",
        "scope": "'standalone'",
        "period_end": "m.report_period_end",
        "period_end_derived": "toUInt8(0)",
        "period_start": "m.report_period_start",
        "fiscal_year": "m.fiscal_year",
        "period_months": period_months_sql("m.report_period_start", "m.report_period_end"),
        "filing_fiscal_year": "m.source_fiscal_year",
        "currency": "nullIf(toString(m.currency), '')",
        "amount_scale": "toUInt32(1)",
        **_money_sql(mapping),
        "employees": "m.employees" if employees else NULL_SQL["employees"],
        "fx_rate_to_usd": "m.fx_rate_to_usd",
        "fx_rate_date": "m.fx_rate_date",
        "fx_source": "m.fx_source",
        "decided_by": "''",
        "note": "''",
    }


FIGURE_COUNT_SQL = " + ".join(
    f"toUInt8(m.{stem}_amount_original IS NOT NULL)" for stem in BOLAGSVERKET_MONEY.values()
) + " + toUInt8(m.employees IS NOT NULL)"


def _ranked_cte_sql(*, observation_kind: str, order_sql: str, scoped: bool) -> str:
    company_filter = "\n        AND m.company_id IN %(company_ids)s" if scoped else ""
    return (
        "WITH ranked AS (\n"
        "    SELECT\n"
        "        m.*,\n"
        "        row_number() OVER (\n"
        f"            PARTITION BY m.company_id, m.report_period_end ORDER BY {order_sql}\n"
        "        ) AS rn\n"
        "    FROM corpscout.se_bolagsverket_financial_metrics AS m FINAL\n"
        f"    {universe_join_sql('m')}\n"
        f"    WHERE m.observation_kind = '{observation_kind}'\n"
        f"        AND m.report_period_end IS NOT NULL{company_filter}\n"
        ")\n"
    )


REPORTED_ORDER_SQL = f"{FIGURE_COUNT_SQL} DESC, m.statement_key ASC"
COMPARATIVE_ORDER_SQL = "ifNull(m.source_fiscal_year, 0) DESC, m.statement_key ASC"
LIVE_WHERE_SQL = "WHERE m.rn = 1"


def reported_live_sql(*, scoped: bool = False) -> str:
    return live_select_sql(
        columns=_column_sql(SOURCE_REPORTED, BOLAGSVERKET_MONEY, employees=True),
        from_sql="FROM ranked AS m",
        where_sql=LIVE_WHERE_SQL,
        with_sql=_ranked_cte_sql(observation_kind="reported", order_sql=REPORTED_ORDER_SQL, scoped=scoped),
    )


def comparative_live_sql(*, scoped: bool = False) -> str:
    return live_select_sql(
        columns=_column_sql(SOURCE_COMPARATIVE, COMPARATIVE_MONEY, employees=False),
        from_sql="FROM ranked AS m",
        where_sql=LIVE_WHERE_SQL,
        with_sql=_ranked_cte_sql(observation_kind="comparative", order_sql=COMPARATIVE_ORDER_SQL, scoped=scoped),
    )


def bolagsverket_current_sql() -> str:
    """(company_id, observed_at) for `since` only; the change scan is the state hash."""
    return (
        "SELECT m.company_id AS company_id, max(m.resolved_at) AS observed_at\n"
        "FROM corpscout.se_bolagsverket_financial_metrics AS m FINAL\n"
        f"{universe_join_sql('m')}\n"
        "GROUP BY m.company_id"
    )


def reported_changed_scope_sql() -> str:
    return financial_changed_scope_sql(source=SOURCE_REPORTED, live_sql=reported_live_sql())


def reported_select_sql() -> str:
    return financial_select_sql(source=SOURCE_REPORTED, live_sql=reported_live_sql(scoped=True))


def comparative_changed_scope_sql() -> str:
    return financial_changed_scope_sql(source=SOURCE_COMPARATIVE, live_sql=comparative_live_sql())


def comparative_select_sql() -> str:
    return financial_select_sql(source=SOURCE_COMPARATIVE, live_sql=comparative_live_sql(scoped=True))


BOLAGSVERKET_DEPS = [
    dg.AssetKey("se_bolagsverket_financial_metrics_clickhouse"),
    dg.AssetKey("se_company_basic_info_fold"),
]

se_company_financial_suggestions_bolagsverket = define_financial_suggestion_asset(
    source=SOURCE_REPORTED,
    extractor_version=BOLAGSVERKET_EXTRACTOR_VERSION,
    current_sql=bolagsverket_current_sql(),
    select_sql=reported_select_sql(),
    changed_scope_override=reported_changed_scope_sql(),
    deps=BOLAGSVERKET_DEPS,
    description=(
        "Every reported Bolagsverket annual-account period of a published company as a "
        "standalone financial suggestion in se_company_financial_suggestion (one row per "
        "period end; the fuller statement wins a duplicated period); a period the rebuilt "
        "source no longer delivers is tombstoned. execute=false previews."
    ),
)

se_company_financial_suggestions_bolagsverket_comparative = define_financial_suggestion_asset(
    source=SOURCE_COMPARATIVE,
    extractor_version=BOLAGSVERKET_COMPARATIVE_EXTRACTOR_VERSION,
    current_sql=bolagsverket_current_sql(),
    select_sql=comparative_select_sql(),
    changed_scope_override=comparative_changed_scope_sql(),
    deps=BOLAGSVERKET_DEPS,
    description=(
        "The prior-year revenue and total assets a later Bolagsverket filing restates, as "
        "source bolagsverket_comparative in se_company_financial_suggestion (one row per "
        "restated period end, from the newest restating filing; filing_fiscal_year names it); "
        "a restatement the rebuilt source no longer delivers is tombstoned. execute=false previews."
    ),
)
